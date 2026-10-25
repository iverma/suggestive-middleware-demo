"""
Live check against the deployed demo: start a run with killed workers, wait for
it to finish, and fail if any customer record was lost or left open.

    python3 scripts/live_check.py https://<id>.lambda-url.<region>.on.aws/ 4 50 2
"""
import json
import sys
import time
import urllib.request


def call(base, method, path):
    req = urllib.request.Request(base.rstrip("/") + path, method=method)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main(base, workers="4", reject="50", kills="2", timeout_s=780):
    before = call(base, "GET", "/results")
    if before["records_in_table"] == 0:
        print("no records loaded; loading the golden set first")
        print(call(base, "POST", "/load"))
    run = call(base, "POST", f"/run?workers={workers}&reject_percent={reject}&kill_workers={kills}")
    print("started", run)
    end = time.time() + timeout_s
    while time.time() < end:
        time.sleep(10)
        res = call(base, "GET", "/results")
        lr = res["latest_run"]
        if not lr or lr["run_id"] != run["run_id"]:
            continue
        s = lr["stats"]
        print(f"{lr['status']:9} closed={s['resolved'] + s['human_review']}/{s['records']} open={s['open']} "
              f"alive={s['workers_alive']} takeovers={s['takeovers_after_crash']} "
              f"repaired={s['worker_totals']['repaired']} fenced={s['worker_totals']['fenced']}")
        if lr["status"] in ("complete", "failed"):
            break
    else:
        print("::error::run did not finish in time")
        return 1
    workers_list = [(w["worker_id"], w["status"], w.get("kill_plan", "")) for w in lr["workers"]]
    print(json.dumps({"stats": s, "workers": workers_list}, indent=1))
    print(f"::notice title=Live run {lr['run_id']}::{s['resolved']} accepted ({s['satisfactory']} inside golden set), "
          f"{s['human_review']} to a person, {s['open']} open, avg {s['avg_passes']} passes, "
          f"{s['takeovers_after_crash']} records recovered after {kills} killed workers, "
          f"{s['worker_totals']['repaired']} handoffs repaired, {s['duration_s']}s")
    killed = sum(1 for w in lr["workers"] if w["status"] == "killed")
    ok = lr["status"] == "complete" and s["open"] == 0 and s["records"] == 100 and killed == int(kills)
    if not ok:
        print(f"::error::run not clean: status={lr['status']} open={s['open']} killed={killed}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
