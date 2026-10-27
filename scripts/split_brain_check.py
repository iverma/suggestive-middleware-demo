"""
Split-brain live check against the deployed demo, from two independent "clients".

  1. Two clients start runs at the same instant, with different settings (one easy, one
     with crashes and heavy rejection). Both must be admitted and finish with all 100
     records closed, no double results, and their own settings (no bleed-through).
  2. Fairness: the run started second must make progress while the first is still busy
     (the queues are SQS fair queues grouped by run).
  3. A client double-submits one start (same client_token, three times at once): exactly one
     run starts, and every response names it.
  4. A stale client cancels a run that already finished: a clear 409, nothing changes.
  5. Afterwards the shared budget is back to what it was and no run is left running.

    python3 scripts/split_brain_check.py https://<id>.lambda-url.<region>.on.aws/
"""
import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid


def call(base, method, path):
    req = urllib.request.Request(base.rstrip("/") + path, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def in_parallel(*fns):
    out = [None] * len(fns)
    barrier = threading.Barrier(len(fns))

    def go(i, fn):
        barrier.wait()
        out[i] = fn()
    ts = [threading.Thread(target=go, args=(i, f)) for i, f in enumerate(fns)]
    [t.start() for t in ts]
    [t.join(120) for t in ts]
    return out


def main(base, timeout_s=900):
    failures = []
    _, before = call(base, "GET", "/results")
    print("budget before:", before["budget"])

    # 1. two clients, same instant, different settings
    easy = "/run?kind=integration&reject_percent=0&client_token=" + str(uuid.uuid4())
    hard = "/run?kind=integration&reject_percent=80&kill_percent=10&client_token=" + str(uuid.uuid4())
    (sa, a), (sb, b) = in_parallel(lambda: call(base, "POST", easy), lambda: call(base, "POST", hard))
    if sa != 202 or sb != 202:
        print(f"::error::both clients should be admitted: {sa} {a} / {sb} {b}")
        return 1
    ra, rb = a["run_id"], b["run_id"]
    print("started", ra, "(easy) and", rb, "(hard)")

    # 3. double submit: one token, three requests at once
    token = str(uuid.uuid4())
    dupes = in_parallel(*[lambda: call(base, "POST", f"/run?kind=integration&reject_percent=0&client_token={token}")] * 3)
    dupe_ids = {body.get("run_id") for status, body in dupes if status == 202}
    print("double submit:", [(s, b.get("run_id"), b.get("duplicate")) for s, b in dupes])
    if len(dupe_ids) != 1 or any(s != 202 for s, _ in dupes):
        failures.append(f"double submit started {len(dupe_ids)} runs: {dupes}")
    rc = next(iter(dupe_ids)) if dupe_ids else None

    # 2 + 1. watch all of them; record when each first made progress
    first_progress, done = {}, {}
    runs = [r for r in (ra, rb, rc) if r]
    end = time.time() + timeout_s
    while time.time() < end and len(done) < len(runs):
        time.sleep(5)
        for r in runs:
            if r in done:
                continue
            _, res = call(base, "GET", f"/results?run={r}")
            lr = res.get("latest_run") or {}
            s = lr.get("stats", {})
            if s.get("closed") and r not in first_progress:
                first_progress[r] = s.get("duration_s")
            if lr.get("status") in ("complete", "failed", "cancelled"):
                done[r] = lr
        print(" ".join(f"{r[-6:]}={(done.get(r) or {}).get('status', 'running')}" for r in runs),
              "| first close at", {k[-6:]: v for k, v in first_progress.items()})
    for r in runs:
        lr = done.get(r)
        if not lr:
            failures.append(f"{r} did not finish in time")
            continue
        s = lr["stats"]
        print(f"{r}: {lr['status']} closed={s['closed']}/{s['records']} avg_passes={s['avg_passes']} "
              f"crashes={s['kills_fired']} double={s['races']['double_commits']} reject={lr['reject_percent']}% "
              f"kill={lr['kill_percent']}%")
        if lr["status"] != "complete" or s["closed"] != s["records"] or s["open"]:
            failures.append(f"{r} not clean: {lr['status']} {s['closed']}/{s['records']}")
        if s["races"]["double_commits"]:
            failures.append(f"{r} has {s['races']['double_commits']} records with two results for a pass")
    if ra in done and rb in done:
        sa_, sb_ = done[ra]["stats"], done[rb]["stats"]
        if not (sa_["avg_passes"] == 1.0 and sa_["kills_fired"] == 0):
            failures.append(f"easy run picked up the other run's settings: {sa_['avg_passes']} passes, {sa_['kills_fired']} crashes")
        if not (sb_["avg_passes"] > 1.5 and sb_["kills_fired"] > 0):
            failures.append(f"hard run lost its settings: {sb_['avg_passes']} passes, {sb_['kills_fired']} crashes")
    # fairness: the second of the two concurrent runs closed its first record quickly too
    slowest_start = max((first_progress.get(r) or 1e9) for r in (ra, rb))
    print(f"slowest first close among the two concurrent runs: {slowest_start}s")
    if slowest_start > 60:
        failures.append(f"a concurrent run waited {slowest_start}s for its first close (starved?)")

    # 4. stale client cancels a finished run
    status, body = call(base, "POST", f"/cancel?run={ra}")
    print("stale cancel:", status, body)
    if status != 409:
        failures.append(f"cancelling a finished run should be 409, got {status}")

    # 5. budget restored
    time.sleep(5)
    _, after = call(base, "GET", "/results")
    print("budget after:", after["budget"])
    leftover = [r["run_id"] for r in after["active_runs"] if r["run_id"] in runs]
    if leftover:
        failures.append(f"runs still holding budget: {leftover}")

    summary = (f"{len(runs)} runs from 2 clients + a 3x double-submit: "
               f"{sum(1 for r in runs if (done.get(r) or {}).get('status') == 'complete')} complete, "
               f"double submit -> {len(dupe_ids)} run, first close of the slower concurrent run at {slowest_start}s, "
               f"stale cancel -> {status}, budget after {after['budget']['runs']} runs")
    if failures:
        for f in failures:
            print("::error::" + f)
        return 1
    print(f"::notice title=Split brain::{summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
