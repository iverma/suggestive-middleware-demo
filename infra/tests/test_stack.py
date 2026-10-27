"""
Alarm wiring and free-tier guardrails, checked on the synthesized CloudFormation template.

Every alarm threshold must come from the capacity constants the stack is built from, so a
changed limit can't leave a stale alarm behind; and the stack as a whole must stay inside the
free tier and the timing rules the worker's correctness depends on.

Run:  python3 -m pytest infra/tests/test_stack.py -q     (needs aws-cdk-lib and node)
"""
import sys
from pathlib import Path

import pytest

INFRA = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(INFRA))

cdk = pytest.importorskip("aws_cdk")
from aws_cdk.assertions import Template  # noqa: E402

import stacks.customer_records_stack as S  # noqa: E402


@pytest.fixture(scope="module")
def template():
    app = cdk.App()
    stack = S.CustomerRecordsStack(app, "test-stack", env=cdk.Environment(account="123456789012", region="us-west-2"))
    return Template.from_stack(stack).to_json()


def resources(t, kind):
    return {k: v["Properties"] for k, v in t["Resources"].items() if v["Type"] == kind}


def alarm(t, name):
    found = [p for p in resources(t, "AWS::CloudWatch::Alarm").values() if p["AlarmName"] == f"effect-demo-{name}"]
    assert len(found) == 1, name
    return found[0]


def test_ten_alarms_all_notify_and_ignore_missing_data(template):
    alarms = resources(template, "AWS::CloudWatch::Alarm")
    assert len(alarms) == 10                                   # the CloudWatch free tier
    for p in alarms.values():
        assert p["TreatMissingData"] == "notBreaching", p["AlarmName"]
        assert p["AlarmActions"] and p["OKActions"], p["AlarmName"]
        assert "Metrics" not in p, f"{p['AlarmName']}: metric math alarms bill per metric"
    subs = list(resources(template, "AWS::SNS::Subscription").values())
    assert [(s["Protocol"], s["Endpoint"]) for s in subs] == [("email", S.ALARM_EMAIL)]


@pytest.mark.parametrize("name,threshold,periods", [
    ("dead-letters-waiting", 1, 5),
    ("rebooking-stuck-messages", S.ALARM_MESSAGE_AGE_S, 3),
    ("rerouting-stuck-messages", S.ALARM_MESSAGE_AGE_S, 3),
    ("rebooking-backlog", S.ALARM_BACKLOG, 3),
    ("runs-table-write-capacity", S.RUNS_WCU * 60 * S.ALARM_CAPACITY_SHARE, 5),
    ("runs-table-read-capacity", S.RUNS_RCU * 60 * S.ALARM_CAPACITY_SHARE, 5),
    ("runs-table-throttled", 1, 1),
    ("worker-throttled", 1, 1),
    ("worker-errors", 20, 1),
    ("run-not-progressing", S.ALARM_NO_PROGRESS_S, 1),
])
def test_alarm_thresholds_derive_from_the_limits(template, name, threshold, periods):
    p = alarm(template, name)
    assert p["Threshold"] == threshold and p["EvaluationPeriods"] == periods


def test_capacity_alarms_point_at_the_right_metric_and_table(template):
    w, r = alarm(template, "runs-table-write-capacity"), alarm(template, "runs-table-read-capacity")
    assert w["MetricName"] == "ConsumedWriteCapacityUnits" and r["MetricName"] == "ConsumedReadCapacityUnits"
    assert w["Statistic"] == r["Statistic"] == "Sum" and w["Period"] == r["Period"] == 60


def test_backlog_alarm_sits_above_the_load_producers_cap():
    # The producer never lets the backlog pass its cap, so the alarm only fires if backpressure breaks.
    assert S.LOAD_BACKLOG_CAP < S.ALARM_BACKLOG


def test_load_release_rate_stays_under_the_write_capacity_alarm():
    wcu_per_record = 7                                          # create, claims, commits, 2x-cost close
    assert S.LOAD_RECORDS_PER_SEC * wcu_per_record <= S.RUNS_WCU * S.ALARM_CAPACITY_SHARE


def test_dynamodb_stays_in_the_free_tier(template):
    tables = resources(template, "AWS::DynamoDB::Table").values()
    assert sum(t["ProvisionedThroughput"]["ReadCapacityUnits"] for t in tables) <= 25
    assert sum(t["ProvisionedThroughput"]["WriteCapacityUnits"] for t in tables) <= 25
    for t in tables:
        assert "BillingMode" not in t or t["BillingMode"] == "PROVISIONED"
        assert not t.get("GlobalSecondaryIndexes"), "a GSI needs its own capacity"
    runs = next(t for t in tables if t["TableName"] == "experiment-runs")
    assert runs["TimeToLiveSpecification"] == {"AttributeName": "ttl", "Enabled": True}


def test_workers_fit_the_account_concurrency_and_leave_room_for_the_ui(template):
    esms = resources(template, "AWS::Lambda::EventSourceMapping").values()
    total = sum(e["ScalingConfig"]["MaximumConcurrency"] for e in esms)
    assert total == S.REBOOKING_MAX_WORKERS + S.REROUTING_MAX_WORKERS
    assert total <= S.LAMBDA_ACCOUNT_CONCURRENCY - 2
    assert all(e["Enabled"] is False for e in esms)             # off until a run needs them
    assert all(e["BatchSize"] == 1 for e in esms)


def test_lease_and_timeouts_keep_redelivery_after_lease_expiry(template):
    queues = {q["QueueName"]: q for q in resources(template, "AWS::SQS::Queue").values()}
    worker = next(f for f in resources(template, "AWS::Lambda::Function").values()
                  if f["FunctionName"] == "effect-demo-worker")
    for name in ("effect-demo-rebooking", "effect-demo-rerouting"):
        vis = queues[name]["VisibilityTimeout"]
        assert S.LEASE_SECONDS < vis, "a crashed worker's message must reappear after its lease ends"
        assert worker["Timeout"] <= vis, "Lambda requires visibility >= function timeout"
        assert queues[name]["MessageRetentionPeriod"] >= 86400, "a backlog must never expire"
        assert queues[name]["RedrivePolicy"]["maxReceiveCount"] == S.MAX_RECEIVES


def test_schedules_are_within_eventbridge_scheduler_free_tier(template):
    per_month = {"rate(1 minute)": 43200, "rate(5 minutes)": 8640}
    total = sum(per_month[s["ScheduleExpression"]] for s in resources(template, "AWS::Scheduler::Schedule").values())
    assert total < 14_000_000
