"""
End-to-end test of the experiment control API and workers against moto
(in-process DynamoDB + SQS). Worker "Lambdas" run as threads; a killed worker
raises instead of exiting the interpreter, then simply stops, holding its lease
and its in-flight message exactly like a crashed process would.

Run:  python3 -m pytest infra/tests -q     (from the repo root)
"""
import json
import os
import sys
import threading
import time
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "infra" / "experiment_control"
LEASE = 3

os.environ.update({
    "AWS_DEFAULT_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x",
    "TABLE_NAME": "customer-records-db", "RUNS_TABLE": "experiment-runs", "TABLE_WCU": "1000",
    "DEFAULT_WCU_PER_SEC": "1000", "RUN_INIT_WCU_PER_SEC": "1000", "LEASE_SECONDS": str(LEASE),
    "WORK_MIN_S": "0.01", "WORK_MAX_S": "0.04", "IDLE_EXIT_S": str(LEASE * 2 + 2), "RESET_SETTLE_S": "0.1",
    "CSV_PATH": str(ROOT / "golden_set" / "golden_set_irops_msp.csv"),
    "AWS_LAMBDA_FUNCTION_NAME": "effect-demo-experiment-control", "WORKER_FUNCTION": "effect-demo-worker",
})


class Killed(BaseException):
    pass


class Ctx:
    log_stream_name = "test"

    def get_remaining_time_in_millis(self):
        return 120_000


@pytest.fixture()
def env():
    with mock_aws():
        ddb = boto3.client("dynamodb")
        ddb.create_table(TableName="customer-records-db", BillingMode="PAY_PER_REQUEST",
                         KeySchema=[{"AttributeName": "case_id", "KeyType": "HASH"}],
                         AttributeDefinitions=[{"AttributeName": "case_id", "AttributeType": "S"}])
        ddb.create_table(TableName="experiment-runs", BillingMode="PAY_PER_REQUEST",
                         KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                                    {"AttributeName": "sk", "KeyType": "RANGE"}],
                         AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                               {"AttributeName": "sk", "AttributeType": "S"}])
        sqs = boto3.client("sqs")
        dlq = sqs.create_queue(QueueName="dlq")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        attrs = {"VisibilityTimeout": str(LEASE),
                 "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "10"})}
        os.environ["REBOOKING_QUEUE_URL"] = sqs.create_queue(QueueName="rebooking", Attributes=attrs)["QueueUrl"]
        os.environ["REROUTING_QUEUE_URL"] = sqs.create_queue(QueueName="rerouting", Attributes=attrs)["QueueUrl"]
        os.environ["DLQ_URL"] = dlq

        sys.path.insert(0, str(SRC))
        for m in ("common", "worker", "handler"):
            sys.modules.pop(m, None)
        import common, handler, worker  # noqa: E401

        def crash():
            raise Killed()
        worker.crash = crash
        threads, outcomes = [], {}

        def fake_invoke(FunctionName, InvocationType, Payload):
            ev = json.loads(Payload)

            def go():
                try:
                    if FunctionName == os.environ["WORKER_FUNCTION"]:
                        outcomes[ev["worker_id"]] = worker.handler(ev, Ctx())
                    else:
                        handler.handler(ev, Ctx())
                except Killed:
                    outcomes[ev.get("worker_id")] = "killed"
            th = threading.Thread(target=go, daemon=True)
            th.start()
            threads.append(th)
            return {"StatusCode": 202}
        common.lam.invoke = fake_invoke
        handler.lam.invoke = fake_invoke
        yield handler, common, threads, outcomes
        sys.path.remove(str(SRC))


def http(handler, method, path, **params):
    ev = {"requestContext": {"http": {"method": method}}, "rawPath": path,
          "queryStringParameters": {k: str(v) for k, v in params.items()}}
    resp = handler.handler(ev, Ctx())
    return resp["statusCode"], (json.loads(resp["body"]) if "json" in resp["headers"]["content-type"] else resp["body"])


def wait_complete(handler, threads, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        _, res = http(handler, "GET", "/results")
        run = res["latest_run"]
        if run and run["status"] == "complete":
            for t in threads:
                t.join(timeout=LEASE * 4)
            return res
        time.sleep(0.5)
    raise AssertionError(f"run did not complete: {json.dumps(res['latest_run']['stats'], indent=1)}")


def test_rules_engine_matches_golden(env):
    handler, common, *_ = env
    gold = common.golden()
    for row in common.read_csv():
        assert common.offer_sequence(row) == gold[row["case_id"]]["acceptable"], row["case_id"]


def test_ui_and_routes(env):
    handler, *_ = env
    status, body = http(handler, "GET", "/")
    assert status == 200 and "re-booking desk" in body
    assert http(handler, "GET", "/reset")[0] == 405          # mutating routes are POST-only
    assert http(handler, "GET", "/nope")[0] == 404
    assert http(handler, "POST", "/run", workers=99)[0] == 400
    assert http(handler, "POST", "/run", workers=3, kill_workers=3)[0] == 400


def test_run_without_records_fails_cleanly(env):
    handler, _, threads, _ = env
    assert http(handler, "POST", "/run")[0] == 202
    for t in threads:
        t.join(5)
    _, res = http(handler, "GET", "/results")
    assert res["recent_runs"][0]["status"] == "failed"


def test_run_with_killed_workers_loses_nothing(env):
    handler, common, threads, outcomes = env
    status, body = http(handler, "POST", "/load")
    assert status == 200 and body["loaded"] == 100

    status, body = http(handler, "POST", "/run", workers=4, reject_percent=50, kill_workers=3)
    assert status == 202, body
    assert http(handler, "POST", "/run")[0] == 409            # one run at a time

    res = wait_complete(handler, threads)
    s = res["latest_run"]["stats"]
    print(json.dumps(s, indent=1))
    assert s["records"] == 100 and s["open"] == 0
    assert s["resolved"] + s["human_review"] == 100            # nothing lost
    assert s["satisfactory"] == s["resolved"]                  # rules pass never leaves the golden set
    assert not any(r["unacceptable_offered"] for r in res["latest_run"]["records"])
    assert list(outcomes.values()).count("killed") == 3
    assert any(isinstance(o, dict) and o.get("successor") for o in outcomes.values())  # survivor handed over
    assert s["takeovers_after_crash"] + s["worker_totals"]["repaired"] >= 3   # every kill was recovered
    assert res["queues"]["dead_letter"]["visible"] == 0
    # fencing tokens only ever go up, and every claim bumped one
    for r in res["latest_run"]["records"]:
        assert r["fencing_token"] >= r["claims"] >= 1

    # a second run reuses the records; tokens keep increasing across runs
    before = {r["case_id"]: r["fencing_token"] for r in res["latest_run"]["records"]}
    assert http(handler, "POST", "/run", workers=2, reject_percent=0)[0] == 202
    res2 = wait_complete(handler, threads)
    assert res2["latest_run"]["stats"]["avg_passes"] <= 1.1
    assert all(r["fencing_token"] > before[r["case_id"]] for r in res2["latest_run"]["records"])
    assert len(res2["recent_runs"]) == 2 and res2["recent_runs"][1]["final"]["records"] == 100

    status, body = http(handler, "POST", "/reset")
    assert status == 200 and body["deleted_records"] == 100
    _, res3 = http(handler, "GET", "/results")
    assert res3["records_in_table"] == 0 and res3["latest_run"] is None


def test_zombie_write_is_fenced(env):
    """A worker that stalls past its lease must not be able to commit."""
    handler, common, *_ = env
    import worker
    http(handler, "POST", "/load")
    run_id = "run-zombie"
    common.ddb.put_item(TableName="experiment-runs", Item=common.to_ddb(
        {"pk": "CONTROL", "sk": "ACTIVE", "run_id": run_id, "status": "running", "expires_at": time.time() + 600}))
    common.ddb.update_item(TableName="customer-records-db", Key=common.to_ddb({"case_id": "IROPS-MSP-0001"}),
                           UpdateExpression="SET run_id = :r, #st = :q, pass_no = :one, history = :e, queued_at = :t",
                           ExpressionAttributeNames={"#st": "status"},
                           ExpressionAttributeValues=common.to_ddb({":r": run_id, ":q": "queued", ":one": 1,
                                                                    ":e": [], ":t": time.time()}))
    common.sqs.send_message(QueueUrl=common.REBOOKING_QUEUE,
                            MessageBody=json.dumps({"run_id": run_id, "case_id": "IROPS-MSP-0001", "pass": 1}))
    zombie, rescuer = worker.Worker(run_id, "zombie", 0), worker.Worker(run_id, "rescuer", 0)

    real_sleep = worker.time.sleep
    q, msg = zombie.receive()

    def stall_then_rescue(_):
        worker.time.sleep = real_sleep
        real_sleep(LEASE + 1)                 # zombie "GC pause" past its lease
        q2, m2 = rescuer.receive()            # SQS redelivers; rescuer claims with a higher token
        rescuer.process(q2, m2)
    worker.time.sleep = stall_then_rescue
    try:
        zombie.process(q, msg)
    finally:
        worker.time.sleep = real_sleep
    assert zombie.stats["fenced"] == 1 and rescuer.stats["takeovers"] == 1
    rec = common.from_ddb(common.ddb.get_item(TableName="customer-records-db",
                                              Key=common.to_ddb({"case_id": "IROPS-MSP-0001"}))["Item"])
    assert rec["status"] == "resolved" and [h["w"] for h in rec["history"]] == ["rescuer"]
