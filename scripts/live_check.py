"""
Live check against the deployed demo: start a run where a share of worker
passes crash (at any of the 8 kill points, process exit or exception), wait for
it to finish, and fail if any customer record was lost or left open, if any
crash wasn't recovered the way its kill point predicts, or if the worker
mappings were left polling.

Also races slow workers and duplicate deliveries, and fails if any record ever
gets two results for one pass.

    python3 scripts/live_check.py https://<id>.lambda-url.<region>.on.aws/ <reject%> <crash%> <slow%> <dup%>
"""
import json
import sys
import time
import urllib.request


def call(base, method, path):
    req = urllib.request.Request(base.rstrip("/") + path, method=method)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main(base, reject="50", kill_percent="10", slow_percent="0", dup_percent="0", poison_percent="0",
         throttle_percent="0", timeout_s=900):
    before = call(base, "GET", "/results")
    if before["records_in_table"] == 0:
        print("no records loaded; loading the golden set first")
        print(call(base, "POST", "/load"))
    run = call(base, "POST", f"/run?reject_percent={reject}&kill_percent={kill_percent}"
                             f"&slow_percent={slow_percent}&dup_percent={dup_percent}"
                             f"&poison_percent={poison_percent}&throttle_percent={throttle_percent}")
    print("started", run)
    end = time.time() + timeout_s
    while time.time() < end:
        time.sleep(5)
        res = call(base, "GET", "/results")
        lr = res["latest_run"]
        if not lr or lr["run_id"] != run["run_id"]:
            continue
        s = lr["stats"]
        print(f"{s['duration_s']:6.1f}s {lr['status']:9} closed={s['resolved'] + s['human_review']}/{s['records']} "
              f"busy={s['workers_busy_now']} peak={s['workers_peak']} used={s['workers_used']} "
              f"crashes={s['kills_recovered']}/{s['kills_fired']} takeovers={s['takeovers_after_crash']} events={s['events']} "
              f"queues={ {k: v['visible'] + v['in_flight'] for k, v in res['queues'].items()} }")
        if lr["status"] in ("complete", "failed"):
            break
    else:
        print("::error::run did not finish in time")
        return 1
    time.sleep(8)                                    # let the mapping switch-off settle, then confirm it
    res = call(base, "GET", "/results")
    maps = {k: v["state"] for k, v in res["worker_mappings"].items()}
    s = res["latest_run"]["stats"]
    print(json.dumps({"mappings_after": maps, "timeline_total": res["latest_run"]["timeline"]["total"]}))
    print(f"{'kill point':32} {'crashed':>7} {'exit':>4} {'raise':>5} {'passes':12} {'recovered':>9}  recovered by (avg s)")
    for k in s["kills_by_point"]:
        print(f"{k['point']:32} {k['fired']:7} {k['exit']:4} {k['raise']:5} {str(k['passes']):12} {k['recovered']:9}  "
              f"{k['recovered_by']} ({k['avg_recovery_s']})")
    print("::notice title=Crashes by kill point (recovered/crashed, exit+raise, passes, avg s to recover)::" + "; ".join(
        f"K{i + 1} {k['point']}: {k['recovered']}/{k['fired']} ({k['exit']}+{k['raise']}) p{k['passes']} {k['avg_recovery_s']}s"
        for i, k in enumerate(s["kills_by_point"])))
    r = s["races"]
    print(f"::notice title=Races::{r['duplicates_sent']} duplicate deliveries ({r['duplicates_held_back']} held back, "
          f"{r['duplicates_dropped']} dropped); {r['zombies']} zombies ({r['zombies_fenced']} fenced, "
          f"{r['zombies_uncontested']} uncontested); {s['events']['fenced']} late writes fenced in all; "
          f"records with two results for one pass: {r['double_commits']}")
    d = s["degradation"]
    print(f"::notice title=Degraded dependencies::{d['throttles_injected']} AWS calls throttled (HTTP 400), "
          f"{d['sdk_retries']} SDK retries, {d['throttled_after_retries']} still throttled after retries "
          f"({d['message_backoffs']} message backoffs), {d['own_writes_recognized']} own writes recognized, "
          f"{d['poison_failures']} poison deliveries, {d['dead_lettered_swept']} records dead-lettered and swept "
          f"to a person, {d['other_errors']} other errors caught")
    unrecovered = [k for k in res["latest_run"]["kills"] if not k["recovered"]]
    for k in unrecovered:
        print("::warning::not recovered:", json.dumps(k))
    print(f"::notice title=Live run {lr['run_id']}::{s['resolved']} accepted ({s['satisfactory']} inside golden set), "
          f"{s['human_review']} to a person, {s['open']} open, avg {s['avg_passes']} passes in {s['duration_s']}s. "
          f"Workers autoscaled to a peak of {s['workers_peak']} ({s['workers_used']} environments). "
          f"{s['kills_fired']} crashes at {sum(1 for k in s['kills_by_point'] if k['fired'])} of 8 kill points, "
          f"on passes {sorted({p for k in s['kills_by_point'] for p in k['passes']})}: "
          f"{s['kills_recovered']} recovered ({s['takeovers_after_crash']} lease takeovers, "
          f"{s['events']['repaired']} handoffs repaired, {s['events']['stale']} duplicates dropped). "
          f"Mappings after: {maps}")
    ok = (res["latest_run"]["status"] == "complete" and s["open"] == 0 and s["records"] == 100
          and not unrecovered and (s["kills_fired"] > 0 or float(kill_percent) == 0)
          and r["double_commits"] == 0 and r["zombies_fenced"] + r["zombies_uncontested"] == r["zombies"]
          and res["queues"]["dead_letter"]["visible"] == 0
          and all(v in ("Disabled", "Disabling") for v in maps.values()))
    if not ok:
        print(f"::error::run not clean: status={res['latest_run']['status']} open={s['open']} "
              f"crashes recovered={s['kills_recovered']}/{s['kills_fired']} double results={r['double_commits']} "
              f"mappings={maps}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
