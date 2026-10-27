"""
Live check against the deployed demo: start a run with killed workers, wait for
it to finish, and fail if any customer record was lost or left open, if a
planned crash didn't fire, or if the worker mappings were left polling.

    python3 scripts/live_check.py https://<id>.lambda-url.<region>.on.aws/ 50 3
"""
import json
import sys
import time
import urllib.request


def call(base, method, path):
    req = urllib.request.Request(base.rstrip("/") + path, method=method)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main(base, reject="50", kills="3", timeout_s=780):
    before = call(base, "GET", "/results")
    if before["records_in_table"] == 0:
        print("no records loaded; loading the golden set first")
        print(call(base, "POST", "/load"))
    run = call(base, "POST", f"/run?reject_percent={reject}&kill_workers={kills}")
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
              f"killed={s['kills_fired']} takeovers={s['takeovers_after_crash']} events={s['events']} "
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
    print(json.dumps({"stats": s, "mappings_after": maps,
                      "kills": [(k["worker"], k["stage"], k["case_id"]) for k in res["latest_run"]["kills"]],
                      "timeline_total": res["latest_run"]["timeline"]["total"]}, indent=1))
    print(f"::notice title=Live run {lr['run_id']}::{s['resolved']} accepted ({s['satisfactory']} inside golden set), "
          f"{s['human_review']} to a person, {s['open']} open, avg {s['avg_passes']} passes in {s['duration_s']}s. "
          f"Workers autoscaled to a peak of {s['workers_peak']} ({s['workers_used']} environments). "
          f"{s['kills_fired']} killed: {s['takeovers_after_crash']} records taken over, "
          f"{s['events']['repaired']} handoffs repaired, {s['events']['stale']} finished duplicates dropped. "
          f"Mappings after: {maps}")
    ok = (res["latest_run"]["status"] == "complete" and s["open"] == 0 and s["records"] == 100
          and s["kills_fired"] == int(kills) and all(v in ("Disabled", "Disabling") for v in maps.values()))
    if not ok:
        print(f"::error::run not clean: status={res['latest_run']['status']} open={s['open']} "
              f"killed={s['kills_fired']}/{kills} mappings={maps}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
