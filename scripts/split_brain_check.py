"""
Split-brain live check against the deployed demo: five independent "clients" at once.

  1. Five clients start runs at the same instant, each with different settings:
       easy      no rejections, no faults
       hard      80% rejections, 10% crashes
       poison-a  5% poison messages
       poison-b  5% poison messages, 30% rejections
       cancel    50% rejections, slow workers; cancelled mid-flight by its client
     All five must be admitted (the check first waits for room in the shared budget).
  2. Each finished run has every record closed, no double results, and its own settings
     (no bleed-through from the others).
  3. Mid-flight cancel: the cancelled run stops closing records (beyond what was already in a
     worker's hands) and the other four still finish.
  4. Poison in two concurrent runs: each poison run's dead letters are swept to a person in
     *its own* partition, and the runs without poison have no sweeps at all.
  5. Fairness: every run's records are queued promptly even when start invocations are
     throttled, and once queued each run closes a record quickly (SQS fair queues by run).
  6. Double submit (same client_token three times at once): exactly one run starts.
  7. A stale client cancels a finished run: a clear 409, nothing changes.
  8. Afterwards no run from this check still holds budget.

    python3 scripts/split_brain_check.py https://<id>.lambda-url.<region>.on.aws/
"""
import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

RUNS = {
    "easy": "reject_percent=0",
    "hard": "reject_percent=80&kill_percent=10",
    "poison-a": "reject_percent=0&poison_percent=5",
    "poison-b": "reject_percent=30&poison_percent=5",
    "cancel": "reject_percent=50&slow_percent=4",
}
FINISHED = ("complete", "failed", "cancelled")


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


def wait_for_room(base, runs, records, timeout_s=900):
    """Another check (or a person on the UI) may be using the shared budget: wait our turn."""
    end = time.time() + timeout_s
    while True:
        _, top = call(base, "GET", "/results")
        b = top["budget"]
        room_runs = b["max_runs"] - b["runs"]
        room_records = b["max_records"] - b["records"]
        if room_runs >= runs and room_records >= records:
            return b
        if time.time() > end:
            raise SystemExit(f"::error::no room in the shared budget for {runs} runs after {timeout_s}s: {b}")
        print("waiting for budget room:", b)
        time.sleep(15)


def detail(base, run_id):
    _, res = call(base, "GET", f"/results?run={run_id}")
    return res.get("latest_run") or {}


def main(base, timeout_s=1200):
    failures = []
    _, top = call(base, "GET", "/results")
    per_run = top.get("records_in_table") or 100
    print("budget before:", wait_for_room(base, len(RUNS) + 1, per_run * (len(RUNS) + 1)))

    # 1. five clients, same instant, different settings
    names = list(RUNS)
    starts = in_parallel(*[
        (lambda n=n: call(base, "POST", f"/run?kind=integration&{RUNS[n]}&client_token={uuid.uuid4()}"))
        for n in names])
    if any(s != 202 for s, _ in starts):
        print(f"::error::all {len(names)} clients should be admitted: {starts}")
        for s, b in starts:                     # don't leave the admitted ones running
            if s == 202:
                call(base, "POST", f"/cancel?run={b['run_id']}")
        return 1
    ids = {n: b["run_id"] for n, (_, b) in zip(names, starts)}
    print("started:", ids)

    # 6. double submit: one token, three requests at once (a sixth run, within MAX_ACTIVE_RUNS)
    token = str(uuid.uuid4())
    dupes = in_parallel(*[lambda: call(base, "POST", f"/run?kind=integration&reject_percent=0&client_token={token}")] * 3)
    dupe_ids = {b.get("run_id") for s, b in dupes if s == 202}
    print("double submit:", [(s, b.get("run_id"), b.get("duplicate"), b.get("error")) for s, b in dupes])
    if len(dupe_ids) != 1 or any(s != 202 for s, _ in dupes):
        failures.append(f"double submit started {len(dupe_ids)} runs: {dupes}")
    if dupe_ids:
        ids["double-submit"] = next(iter(dupe_ids))

    # 3. mid-flight cancel, then 2 + 5: watch everything
    first_progress, queued, done, cancel_at = {}, {}, {}, None
    end = time.time() + timeout_s
    while time.time() < end and len(done) < len(ids):
        time.sleep(5)
        for n, r in ids.items():
            if n in done:
                continue
            lr = detail(base, r)
            s = lr.get("stats", {})
            if s.get("queued_s") is not None and n not in queued:
                queued[n] = s["queued_s"]
            if s.get("closed") and n not in first_progress:
                first_progress[n] = s.get("duration_s")
            if n == "cancel" and cancel_at is None and 10 <= (s.get("closed") or 0) < s.get("records", 0):
                status, body = call(base, "POST", f"/cancel?run={r}")
                cancel_at = {"closed": s["closed"], "status": status, "t": time.time()}
                print("cancelled mid-flight at", s["closed"], "closed:", status, body)
                if status != 200:
                    failures.append(f"mid-flight cancel returned {status}: {body}")
            if lr.get("status") in FINISHED:
                done[n] = lr
        print(" ".join(f"{n}={(done.get(n) or {}).get('status', 'running')}" for n in ids),
              "| first close at", first_progress)

    for n, r in ids.items():
        lr = done.get(n)
        if not lr:
            failures.append(f"{n} ({r}) did not finish in time")
            continue
        s = lr["stats"]
        print(f"{n} {r}: {lr['status']} closed={s['closed']}/{s['records']} avg_passes={s['avg_passes']} "
              f"crashes={s['kills_fired']} double={s['races']['double_commits']} sweeps={len(lr.get('sweeps', []))}")
        if s["races"]["double_commits"]:
            failures.append(f"{n} has {s['races']['double_commits']} records with two results for a pass")
        if n == "cancel":
            continue
        if lr["status"] != "complete" or s["closed"] != s["records"] or s["open"]:
            failures.append(f"{n} not clean: {lr['status']} {s['closed']}/{s['records']}")

    # 2. no bleed-through
    if "easy" in done and not (done["easy"]["stats"]["avg_passes"] == 1.0 and done["easy"]["stats"]["kills_fired"] == 0):
        failures.append(f"easy run picked up another run's settings: {done['easy']['stats']['avg_passes']} passes")
    if "hard" in done and not (done["hard"]["stats"]["avg_passes"] > 1.5 and done["hard"]["stats"]["kills_fired"] > 0):
        failures.append(f"hard run lost its settings: {done['hard']['stats']['avg_passes']} passes")

    # 3. the cancelled run stopped: at most what was already in workers' hands closed afterwards
    c = done.get("cancel")
    if cancel_at is None:
        failures.append("never got to cancel the cancel run mid-flight")
    elif c:
        time.sleep(30)
        late = detail(base, ids["cancel"])
        grew = late["stats"]["closed"] - cancel_at["closed"]
        print(f"cancel: status={late['status']} closed {cancel_at['closed']} at cancel, {late['stats']['closed']} 30s+ later")
        if late["status"] != "cancelled":
            failures.append(f"cancelled run ended {late['status']}")
        if grew > 10:
            failures.append(f"cancelled run kept closing records: +{grew} after the cancel")

    # 4. poison swept in its own partition, and only there
    for n in ("poison-a", "poison-b"):
        lr = done.get(n)
        if not lr:
            continue
        sweeps = lr.get("sweeps", [])
        own = {x["case_id"] for x in lr["records"]}
        swept_records = {x["case_id"] for x in lr["records"] if x.get("swept")}
        print(f"{n}: {len(sweeps)} swept: {sorted(x['case_id'] for x in sweeps)}")
        if not sweeps:
            failures.append(f"{n}: poison run has no sweeps")
        if any(x["case_id"] not in own for x in sweeps) or {x["case_id"] for x in sweeps} != swept_records:
            failures.append(f"{n}: sweep markers and swept records disagree: {sweeps} vs {swept_records}")
    for n in ("easy", "hard", "double-submit"):
        if done.get(n) and done[n].get("sweeps"):
            failures.append(f"{n}: has sweeps but no poison (a sweep landed in the wrong partition)")

    # 5. fairness. Set-up: every run's records are queued promptly even when the start
    # invocations are throttled (the workers can hold 8 of the account's 10 Lambdas).
    # Queues: once queued, every run closes a record quickly (SQS fair queues grouped by run).
    slowest_setup = max((queued.get(n) or 1e9) for n in RUNS)
    slowest = max((first_progress.get(n) or 1e9) - (queued.get(n) or 0) for n in RUNS)
    print(f"set-up (records queued) at {queued}; slowest first close after queueing: {slowest:.1f}s")
    if slowest_setup > 60:
        failures.append(f"a concurrent run took {slowest_setup}s to queue its records (start invocations starved?)")
    if slowest > 60:
        failures.append(f"a concurrent run waited {slowest:.1f}s after queueing for its first close (starved?)")

    # 7. stale client cancels a finished run
    status, body = call(base, "POST", f"/cancel?run={ids['easy']}")
    print("stale cancel:", status, body)
    if status != 409:
        failures.append(f"cancelling a finished run should be 409, got {status}")

    # 8. budget released
    time.sleep(5)
    _, after = call(base, "GET", "/results")
    print("budget after:", after["budget"])
    leftover = [r["run_id"] for r in after["active_runs"] if r["run_id"] in ids.values()]
    if leftover:
        failures.append(f"runs still holding budget: {leftover}")

    summary = (f"{len(RUNS)} concurrent clients + a 3x double submit: "
               f"{sum(1 for n in ids if (done.get(n) or {}).get('status') == 'complete')} complete, "
               f"cancel mid-flight at {cancel_at and cancel_at['closed']} closed -> "
               f"{(done.get('cancel') or {}).get('status')}, sweeps "
               f"{ {n: len((done.get(n) or {}).get('sweeps', [])) for n in ('poison-a', 'poison-b', 'easy', 'hard')} }, "
               f"slowest set-up {slowest_setup}s, slowest first close after queueing {slowest:.1f}s, stale cancel -> {status}")
    if failures:
        for f in failures:
            print("::error::" + f)
        print(f"::warning title=Split brain::{summary}")
        return 1
    print(f"::notice title=Split brain::{summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
