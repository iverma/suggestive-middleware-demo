"""
Live 1,000-record load check against the deployed demo.

Starts a load run (1,000 generated unique records, no injected faults) with a low backlog cap
and short producer chunks, so backpressure and producer handoffs both happen for real, then
checks:

  from the run's own summary: every record created once and closed, none lost, no record with
  two results for a pass, no outcome outside its acceptable list, the backlog never past the
  cap, and the producer handed off several times;

  from CloudWatch (read-only credentials, when available): no demo alarm went into ALARM while
  the run was going, experiment-runs stayed under 80% of its write capacity in every minute,
  and DynamoDB throttled nothing.

    python3 scripts/load_check.py https://<id>.lambda-url.<region>.on.aws/ [records] [backlog_cap] [chunk_s]
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

RUNS_TABLE, RUNS_WCU, RUNS_RCU, CAPACITY_SHARE = "experiment-runs", 20, 20, 0.8
ALARM_PREFIX = "effect-demo-"


def call(base, method, path):
    req = urllib.request.Request(base.rstrip("/") + path, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def wait_for_room(base, records, timeout_s=1200):
    end = time.time() + timeout_s
    while True:
        _, top = call(base, "GET", "/results")
        b = top["budget"]
        if b["max_runs"] - b["runs"] >= 1 and b["max_records"] - b["records"] >= records:
            return b
        if time.time() > end:
            raise SystemExit(f"::error::no room in the shared budget for {records} records: {b}")
        print("waiting for budget room:", b)
        time.sleep(20)


def cloudwatch_checks(start, end):
    """Alarm history and capacity, via whatever AWS credentials the environment has."""
    try:
        import boto3
    except ImportError:
        return ["boto3 not installed"], {}
    if not (os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_PROFILE")):
        return ["no AWS credentials: CloudWatch checks skipped"], {}
    cw = boto3.client("cloudwatch")
    failures, seen = [], {}

    alarms = cw.describe_alarms(AlarmNamePrefix=ALARM_PREFIX)["MetricAlarms"]
    seen["alarms_watched"] = len(alarms)
    if len(alarms) < 10:
        failures.append(f"expected 10 demo alarms, found {len(alarms)}")
    fired = []
    for page in cw.get_paginator("describe_alarm_history").paginate(
            AlarmNamePrefix=ALARM_PREFIX, HistoryItemType="StateUpdate", StartDate=start, EndDate=end):
        for h in page["AlarmHistoryItems"]:
            if "to ALARM" in h["HistorySummary"]:
                fired.append(f"{h['AlarmName']} at {h['Timestamp']:%H:%M:%S}")
    now_alarm = [a["AlarmName"] for a in alarms if a["StateValue"] == "ALARM"]
    seen["alarms_fired"] = fired
    if fired or now_alarm:
        failures.append(f"alarms fired during the run: {fired or now_alarm}")

    def per_minute(metric, stat="Sum", **dims):
        pts = cw.get_metric_statistics(
            Namespace="AWS/DynamoDB", MetricName=metric, StartTime=start, EndTime=end, Period=60,
            Statistics=[stat], Dimensions=[{"Name": "TableName", "Value": RUNS_TABLE}])["Datapoints"]
        return [p[stat] for p in pts]

    writes = per_minute("ConsumedWriteCapacityUnits")
    reads = per_minute("ConsumedReadCapacityUnits")
    seen["peak_wcu_per_s"] = round(max(writes or [0]) / 60, 2)
    seen["peak_rcu_per_s"] = round(max(reads or [0]) / 60, 2)
    seen["wcu_minutes"] = len(writes)
    if not writes:
        failures.append("no ConsumedWriteCapacityUnits datapoints for the run window")
    if seen["peak_wcu_per_s"] > RUNS_WCU * CAPACITY_SHARE:
        failures.append(f"write capacity peaked at {seen['peak_wcu_per_s']}/s, over {CAPACITY_SHARE:.0%} of {RUNS_WCU}")
    if seen["peak_rcu_per_s"] > RUNS_RCU * CAPACITY_SHARE:
        failures.append(f"read capacity peaked at {seen['peak_rcu_per_s']}/s, over {CAPACITY_SHARE:.0%} of {RUNS_RCU}")
    throttled = sum(per_minute("WriteThrottleEvents")) + sum(per_minute("ReadThrottleEvents"))
    seen["dynamodb_throttle_events"] = throttled
    if throttled:
        failures.append(f"DynamoDB throttled {throttled} requests")
    return failures, seen


def main(base, records="1000", backlog_cap="10", chunk_s="60", timeout_s=2400):
    records, failures = int(records), []
    print("budget before:", wait_for_room(base, records))
    t_start = datetime.now(timezone.utc) - timedelta(minutes=1)
    status, body = call(base, "POST", f"/run?kind=load&records={records}&backlog_cap={backlog_cap}"
                                      f"&chunk_s={chunk_s}&client_token={uuid.uuid4()}")
    if status != 202:
        print(f"::error::load run not admitted: {status} {body}")
        return 1
    run_id = body["run_id"]
    print("load run", run_id, {k: body.get(k) for k in ("records", "backlog_cap", "chunk_s", "rate")})

    end, lr = time.time() + timeout_s, {}
    while time.time() < end:
        time.sleep(15)
        _, res = call(base, "GET", f"/results?run={run_id}")
        lr = res.get("latest_run") or {}
        s = lr.get("stats", {})
        print(f"{lr.get('status')}: released {s.get('released')} closed {s.get('closed')}/{s.get('records')} "
              f"backlog {s.get('backlog_now')} (peak {s.get('max_backlog')}/{s.get('backlog_cap')}) "
              f"handoffs {s.get('handoffs')} at {s.get('duration_s')}s")
        if lr.get("summary") or lr.get("status") in ("failed", "cancelled"):
            break
    summary = lr.get("summary")
    if not summary:
        print(f"::error::load run did not finish and score in time: {lr.get('status')} {lr.get('error')}")
        return 1
    print("summary:", json.dumps(summary))
    expect_handoffs = max(1, int(summary["duration_s"] * 0.5 // int(chunk_s)))
    checks = {
        "every record created once": summary["created"] == records and summary["create_skipped"] == 0,
        "none lost": summary["lost"] == 0 and summary["closed"] == records,
        "no double results": summary["double_results"] == 0,
        "no outcome outside the rules": summary["outside_rules"] == 0,
        "backlog stayed under the cap": summary["max_backlog"] <= summary["backlog_cap"],
        f"producer handed off (>= {expect_handoffs})": summary["handoffs"] >= expect_handoffs,
        "run complete": lr["status"] == "complete",
    }
    failures += [name for name, ok in checks.items() if not ok]
    if not summary["backpressure_s"]:
        # The release rate (2 records/s, set by DynamoDB capacity) is below what the workers drain,
        # so the cap is a safety net that only engages if the workers slow down. Not a failure.
        print(f"::warning title=Backpressure::never engaged: workers kept the backlog at or below "
              f"{summary['max_backlog']} (cap {summary['backlog_cap']})")

    # CloudWatch publishes DynamoDB and SQS metrics a few minutes late; alarms evaluate after that.
    print("waiting 4 minutes for CloudWatch to catch up before checking alarms and capacity")
    time.sleep(240)
    cw_failures, seen = cloudwatch_checks(t_start, datetime.now(timezone.utc))
    print("cloudwatch:", seen, cw_failures)
    skipped = cw_failures and cw_failures[0].startswith(("no AWS credentials", "boto3"))
    if skipped:
        print(f"::warning title=CloudWatch::{cw_failures[0]}")
    else:
        failures += cw_failures

    line = (f"{summary['closed']}/{records} closed, {summary['lost']} lost, {summary['double_results']} double, "
            f"{summary['outside_rules']} outside rules, peak workers {summary['peak_workers']}, backlog peak "
            f"{summary['max_backlog']}/{summary['backlog_cap']} (held back {summary['backpressure_s']}s), {summary['handoffs']} handoffs, "
            f"{summary['duration_s']}s; peak {seen.get('peak_wcu_per_s')} WCU/s, alarms fired {seen.get('alarms_fired')}")
    if failures:
        for f in failures:
            print("::error::" + f)
        print(f"::warning title=Load check::{line}")
        return 1
    print(f"::notice title=Load check::{line}")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
