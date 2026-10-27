"""
End-to-end test of the experiment control API and workers against moto
(in-process DynamoDB + SQS).

Moto has no Lambda event source mappings, so FakeEsm stands in for them: per
queue, up to max_workers poller threads receive one message at a time and
invoke worker.handler with a real SQS event shape, honoring batchItemFailures
(listed messages are not deleted) and polling only while the mapping is
enabled. Each poller thread is one execution environment; a killed worker
raises instead of exiting the interpreter, its message is left undeleted (as a
crashed invoke's would be), and the poller comes back as a fresh environment.

Runs are isolated: each one works on its own copy of the records in its own partition, and
several can be active at once within one shared admission budget. The split-brain tests at
the bottom start, cancel and race runs against each other.

Run:  python3 -m pytest infra/tests -q     (from the repo root)
"""
import json
import os
import sys
import threading
import time
import uuid
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "infra" / "experiment_control"
LEASE, VIS = 4, 6          # same shape as AWS (15s / 20s): lease < visibility == worker timeout

os.environ.update({
    "AWS_DEFAULT_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x",
    "TABLE_NAME": "customer-records-db", "RUNS_TABLE": "experiment-runs", "TABLE_WCU": "1000",
    "DEFAULT_WCU_PER_SEC": "1000", "RUN_INIT_WCU_PER_SEC": "1000",
    "LEASE_SECONDS": str(LEASE), "VISIBILITY_SECONDS": str(VIS),
    "WORK_MIN_S": "0.02", "WORK_MAX_S": "0.08", "RESET_SETTLE_S": "0.2", "ESM_RETRY_S": "1",
    "ZOMBIE_MARGIN_S": "0.3",
    "CSV_PATH": str(ROOT / "golden_set" / "golden_set_irops_msp.csv"),
    "AWS_LAMBDA_FUNCTION_NAME": "effect-demo-experiment-control", "WORKER_FUNCTION": "effect-demo-worker",
    "REBOOKING_ESM_ID": "esm-rebooking", "REROUTING_ESM_ID": "esm-rerouting",
    "REBOOKING_MAX_WORKERS": "5", "REROUTING_MAX_WORKERS": "3",
})


class Killed(BaseException):
    pass


class Ctx:
    """Lambda context. Remaining time == visibility timeout, as configured on AWS."""
    log_stream_name = "test"

    def __init__(self, remaining_s=VIS):
        self.deadline = time.time() + remaining_s

    def get_remaining_time_in_millis(self):
        return int(max(0, self.deadline - time.time()) * 1000)


class FakeEsm:
    """SQS -> Lambda event source mappings, with one thread per allowed concurrent worker."""

    def __init__(self, worker, sqs, queues):
        self.worker, self.sqs, self.queues = worker, sqs, queues      # name -> (url, arn, max)
        self.enabled = {name: False for name in queues}
        self.stop = threading.Event()
        self.kills = 0          # process exits: the environment dies and is replaced
        self.faults = 0         # exceptions out of the handler: the environment survives
        self.groups = []        # (run in the body, MessageGroupId) of every delivery
        self.envs = threading.local()
        self.threads = [threading.Thread(target=self.poll, args=(name,), daemon=True)
                        for name, (_, _, mx) in queues.items() for _ in range(mx)]
        worker.env_id = lambda: getattr(self.envs, "id", "env-main")
        for t in self.threads:
            t.start()

    def poll(self, name):
        url, arn, _ = self.queues[name]
        self.envs.id = "env-" + uuid.uuid4().hex[:6]
        while not self.stop.is_set():
            if not self.enabled[name]:
                time.sleep(0.05)
                continue
            msgs = self.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1, WaitTimeSeconds=1,
                                            AttributeNames=["ApproximateReceiveCount", "MessageGroupId"]).get("Messages", [])
            for m in msgs:
                self.groups.append((json.loads(m["Body"]).get("run_id") if m["Body"].startswith("{") else None,
                                    m.get("Attributes", {}).get("MessageGroupId")))
                event = {"Records": [{"messageId": m["MessageId"], "receiptHandle": m["ReceiptHandle"],
                                      "body": m["Body"], "eventSourceARN": arn,
                                      "attributes": {"ApproximateReceiveCount": m["Attributes"]["ApproximateReceiveCount"]}}]}
                try:
                    out = self.worker.handler(event, Ctx())
                except (Killed, self.worker.FunctionTimedOut):
                    self.kills += 1
                    self.envs.id = "env-" + uuid.uuid4().hex[:6]       # crashed env is replaced
                    continue                                           # message stays, reappears later
                except Exception as e:
                    self.faults += 1                                   # failed invoke: message stays
                    print("ESCAPED THE WORKER:", type(e).__name__, str(e)[:300])
                    continue
                if not out["batchItemFailures"]:
                    self.sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])

    # boto3 lambda client surface used by the control function
    def update_event_source_mapping(self, UUID, Enabled):
        self.enabled[UUID.split("-", 1)[1]] = Enabled
        return {"State": "Enabled" if Enabled else "Disabled"}

    def get_event_source_mapping(self, UUID):
        return {"State": "Enabled" if self.enabled[UUID.split("-", 1)[1]] else "Disabled"}


@pytest.fixture(autouse=True)
def atomic_dynamodb(monkeypatch):
    """
    Moto evaluates a ConditionExpression and applies the write as separate steps, with no lock,
    so two threads can both pass the same condition. Real DynamoDB makes each conditional write
    atomic per item; serializing moto's DynamoDB calls restores that guarantee in the simulator.
    """
    import botocore.endpoint
    lock = threading.Lock()
    real = botocore.endpoint.Endpoint._do_get_response

    # Lock each HTTP attempt, not the whole API call: botocore's retry sleeps happen between
    # attempts and must not block other workers (DynamoDB never serializes callers like that).
    def attempt(self, request, operation_model, context):
        if operation_model.service_model.service_name == "dynamodb":
            with lock:
                return real(self, request, operation_model, context)
        return real(self, request, operation_model, context)
    monkeypatch.setattr(botocore.endpoint.Endpoint, "_do_get_response", attempt)


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
        attrs = {"VisibilityTimeout": str(VIS),
                 "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"})}
        queues = {}
        for name, mx in (("rebooking", 5), ("rerouting", 3)):
            url = sqs.create_queue(QueueName=f"effect-demo-{name}", Attributes=attrs)["QueueUrl"]
            arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
            os.environ[f"{name.upper()}_QUEUE_URL"] = url
            queues[name] = (url, arn, mx)
        os.environ["DLQ_URL"] = dlq

        sys.path.insert(0, str(SRC))
        for m in ("common", "worker", "handler", "sweeper", "throttle"):
            sys.modules.pop(m, None)
        import common, handler, worker  # noqa: E401

        def crash():
            raise Killed()
        worker.crash = crash
        esm = FakeEsm(worker, sqs, queues)
        threads = []

        def fake_invoke(FunctionName, InvocationType, Payload):
            ev = json.loads(Payload)
            th = threading.Thread(target=handler.handler, args=(ev, Ctx(600)), daemon=True)
            th.start()
            threads.append(th)
            return {"StatusCode": 202}
        handler.lam.invoke = fake_invoke
        handler.lam.update_event_source_mapping = esm.update_event_source_mapping
        handler.lam.get_event_source_mapping = esm.get_event_source_mapping
        yield handler, common, worker, esm, threads
        esm.stop.set()
        sys.path.remove(str(SRC))


def http(handler, method, path, **params):
    ev = {"requestContext": {"http": {"method": method}}, "rawPath": path,
          "queryStringParameters": {k: str(v) for k, v in params.items()}}
    resp = handler.handler(ev, Ctx(600))
    return resp["statusCode"], (json.loads(resp["body"]) if "json" in resp["headers"]["content-type"] else resp["body"])


def start(handler, **params):
    status, body = http(handler, "POST", "/run", **params)
    assert status == 202, body
    return body["run_id"]


def wait_complete(handler, run_id=None, timeout=150, statuses=("complete",)):
    end = time.time() + timeout
    res = None
    while time.time() < end:
        status, res = http(handler, "GET", "/results", **({"run": run_id} if run_id else {}))
        assert status == 200, res                   # /results must never fail mid-run
        run = res["latest_run"]
        if run and run["status"] in statuses:
            return res
        time.sleep(0.5)
    raise AssertionError(f"run did not finish: {json.dumps((res or {}).get('latest_run', {}).get('stats'), indent=1)}")


def sweeper_ticks(stop, every=1.0):
    """Stand-in for the 1-minute EventBridge schedule, sped up."""
    import sweeper
    while not stop.is_set():
        sweeper.handler({}, Ctx(60))
        time.sleep(every)


def case_items(common, run_id):
    resp = common.ddb.query(TableName="experiment-runs", KeyConditionExpression="pk = :p AND begins_with(sk, :c)",
                            ExpressionAttributeValues={":p": {"S": f"RUN#{run_id}"}, ":c": {"S": "CASE#"}})
    return [common.from_ddb(i) for i in resp["Items"]]


# --- basics ---------------------------------------------------------------------------------
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
    assert http(handler, "POST", "/run", reject_percent=101)[0] == 400
    assert http(handler, "POST", "/run", kill_percent=51)[0] == 400
    assert http(handler, "POST", "/run", kind="nope")[0] == 400
    assert http(handler, "POST", "/reset")[0] == 400          # needs ?confirm=everything
    assert http(handler, "POST", "/cancel")[0] == 400


def test_run_without_records_is_refused(env):
    handler, _, _, esm, _ = env
    status, body = http(handler, "POST", "/run")
    assert status == 409 and "load" in body["error"]
    assert not any(esm.enabled.values())


def test_load_strips_fields_older_versions_left_on_customer_records(env):
    handler, common, *_ = env
    common.ddb.put_item(TableName="customer-records-db", Item=common.to_ddb(
        {"case_id": "IROPS-MSP-0001", "status": "in_progress", "lease_owner": "env-old", "fencing_token": 9}))
    http(handler, "POST", "/load")
    item = common.from_ddb(common.ddb.get_item(TableName="customer-records-db",
                                               Key=common.to_ddb({"case_id": "IROPS-MSP-0001"}))["Item"])
    assert "status" not in item and "lease_owner" not in item and "fencing_token" not in item
    assert item["destination"]


# --- one run --------------------------------------------------------------------------------
def test_autoscaled_run_with_crashes_loses_nothing(env):
    handler, common, worker, esm, threads = env
    status, body = http(handler, "POST", "/load")
    assert status == 200 and body["loaded"] == 100
    assert not any(esm.enabled.values())                      # idle: no polling

    run_id = start(handler, reject_percent=50, kill_percent=15)
    res = wait_complete(handler, run_id)
    run, s = res["latest_run"], res["latest_run"]["stats"]
    print(json.dumps({k: s[k] for k in ("resolved", "human_review", "avg_passes", "workers_peak", "workers_used",
                                        "kills_fired", "takeovers_after_crash", "events", "duration_s")}, indent=1))
    assert s["records"] == 100 and s["open"] == 0 and s["closed"] == 100     # exactly-once counters
    assert s["resolved"] + s["human_review"] == 100
    assert s["satisfactory"] == s["resolved"]
    assert not any(r["unacceptable_offered"] for r in run["records"])
    # exit-mode crashes kill the environment; raise-mode ones are caught by the worker's
    # catch-all and reported as a failed message (the environment survives)
    assert s["kills_fired"] >= 10 and s["kills_fired"] == esm.kills + s["events"]["failed_injected"]
    assert esm.faults == 0
    assert s["kills_recovered"] == s["kills_fired"], [k for k in run["kills"] if not k["recovered"]]
    assert any(int(k["pass"]) > 1 for k in run["kills"])
    assert res["queues"]["dead_letter"]["visible"] == 0
    assert 3 <= s["workers_peak"] <= 8 + esm.kills
    assert max(run["timeline"]["rebooking"]) <= 5 and max(run["timeline"]["rerouting"]) <= 3
    assert not any(esm.enabled.values())                      # last run finished: workers off
    assert res["budget"]["runs"] == 0 and res["budget"]["records"] == 0
    for r in run["records"]:
        assert r["fencing_token"] >= r["claims"] >= 1

    # a second run gets fresh copies; the first run's records are untouched
    first = {r["case_id"]: (r["fencing_token"], r["status"]) for r in run["records"]}
    for t in threads:
        t.join(5)
    run2 = start(handler, reject_percent=0)
    res2 = wait_complete(handler, run2)
    assert res2["latest_run"]["stats"]["avg_passes"] <= 1.1
    assert {r["case_id"]: (r["fencing_token"], r["status"]) for r in wait_complete(handler, run_id)["latest_run"]["records"]} == first
    assert [r["run_id"] for r in res2["recent_runs"][:2]] == [run2, run_id]
    assert res2["recent_runs"][1]["final"]["records"] == 100

    status, body = http(handler, "POST", "/reset", confirm="everything")
    assert status == 200 and body["deleted_run_items"] > 200
    _, res3 = http(handler, "GET", "/results")
    assert res3["records_in_table"] == 100 and res3["latest_run"] is None    # golden set kept


def test_every_kill_point_recovers_on_every_pass_in_both_modes(env):
    """Force each kill point x mode onto first and later passes; each must recover its own way."""
    handler, common, worker, esm, threads = env
    points, modes = common.KILL_POINTS, common.KILL_MODES

    def forced(run_id, case_id, pass_no, kill_percent):
        n = int(case_id[-4:])
        if n % 3:
            return None
        i = n // 3 + pass_no
        return {"at": points[i % len(points)], "mode": modes[(i // len(points)) % 2]}
    common.crash_plan = forced

    http(handler, "POST", "/load")
    run_id = start(handler, reject_percent=60, kill_percent=1)
    res = wait_complete(handler, run_id, timeout=240)
    run, s = res["latest_run"], res["latest_run"]["stats"]
    by_point = {k["point"]: k for k in s["kills_by_point"]}
    assert s["open"] == 0 and s["resolved"] + s["human_review"] == 100
    assert s["kills_recovered"] == s["kills_fired"], [k for k in run["kills"] if not k["recovered"]]
    for p in points:
        assert by_point[p]["fired"] >= 1, p
    assert {int(k["pass"]) for k in run["kills"]} >= {1, 2}
    assert {k["mode"] for k in run["kills"]} == {"exit", "raise"}
    for k in run["kills"]:
        if k["point"] in ("after_claim", "during_offer", "during_customer_response", "before_commit"):
            assert k["recovered_by"] == "lease takeover" and k["recovery_s"] is not None, k
    assert s["events"]["repaired"] >= by_point["after_commit_before_handoff"]["fired"]
    assert res["queues"]["dead_letter"]["visible"] == 0


def test_slow_and_duplicate_workers_race_without_double_results(env):
    handler, common, worker, esm, threads = env
    real_plan = common.race_plan

    def forced(run_id, case_id, pass_no, slow_percent=0, dup_percent=0):
        n = int(case_id[-4:])
        if pass_no == 1 and n in (10, 60):
            return "zombie", True
        if pass_no == 1 and n == 35:
            return "timeout", False
        if pass_no == 2 and n % 20 == 5:
            return "timeout", False
        return real_plan(run_id, case_id, pass_no, 0, dup_percent)
    common.race_plan = forced

    http(handler, "POST", "/load")
    run_id = start(handler, reject_percent=50, dup_percent=30)
    res = wait_complete(handler, run_id, timeout=240)
    run, s = res["latest_run"], res["latest_run"]["stats"]
    r = s["races"]
    assert s["open"] == 0 and s["closed"] == 100
    assert r["double_commits"] == 0, r["double_commit_list"]
    assert r["zombies"] == 2 and r["zombies_fenced"] + r["zombies_uncontested"] == 2
    assert r["zombies_fenced"] >= 1
    timeouts = next(k for k in s["kills_by_point"] if k["point"] == "stalled_past_timeout")
    assert timeouts["fired"] >= 1 and timeouts["recovered"] == timeouts["fired"]
    assert r["duplicates_sent"] >= 20
    assert res["queues"]["dead_letter"]["visible"] == 0


def test_throttled_aws_calls_back_off_and_lose_nothing(env, monkeypatch):
    handler, common, worker, esm, threads = env
    import botocore.retries.standard as std
    real_delay = std.ExponentialBackoff.delay_amount
    monkeypatch.setattr(std.ExponentialBackoff, "delay_amount", lambda self, ctx: real_delay(self, ctx) * LEASE / 15)
    stop = threading.Event()
    threading.Thread(target=sweeper_ticks, args=(stop,), daemon=True).start()
    try:
        http(handler, "POST", "/load")
        run_id = start(handler, reject_percent=50, throttle_percent=20)
        res = wait_complete(handler, run_id, timeout=300)
    finally:
        stop.set()
    s = res["latest_run"]["stats"]
    d = s["degradation"]
    assert s["open"] == 0 and s["closed"] == 100
    assert d["throttles_injected"] >= 50 and d["sdk_retries"] >= d["throttles_injected"] * 0.8
    assert s["races"]["double_commits"] == 0
    assert esm.faults == 0


def test_poison_messages_dead_letter_and_are_swept_to_a_person(env):
    handler, common, worker, esm, threads = env
    poisoned = {"IROPS-MSP-0007", "IROPS-MSP-0042"}
    common.poison_plan = lambda run_id, case_id, pass_no, pct=0: case_id in poisoned and pass_no == 1
    stop = threading.Event()
    threading.Thread(target=sweeper_ticks, args=(stop,), daemon=True).start()
    try:
        http(handler, "POST", "/load")
        run_id = start(handler, reject_percent=30, poison_percent=1)
        res = wait_complete(handler, run_id, timeout=240)
    finally:
        stop.set()
    run, s = res["latest_run"], res["latest_run"]["stats"]
    assert s["open"] == 0 and s["closed"] == 100              # swept records count as closed
    for r in run["records"]:
        if r["case_id"] in poisoned:
            assert r["status"] == "human_review" and r["swept"], r
            assert r["history"][-1]["w"] == "sweeper"
    assert {w["case_id"] for w in run["sweeps"]} == poisoned
    assert any(i["kind"] == "swept" for i in run["incidents"])


def test_unreadable_message_is_dropped_by_the_sweeper(env):
    handler, common, worker, esm, threads = env
    import sweeper
    out = worker.handler({"Records": [{"messageId": "m", "receiptHandle": "r", "body": "{not json",
                                       "eventSourceARN": "arn:aws:sqs:us-east-1:1:effect-demo-rebooking"}]}, Ctx())
    assert out == {"batchItemFailures": [{"itemIdentifier": "m"}]}
    common.sqs.send_message(QueueUrl=common.DLQ, MessageBody="{not json")
    assert sweeper.handler({}, Ctx(60))["unreadable"] == 1


def test_zombie_write_is_fenced(env):
    """A worker that stalls past its lease must not be able to commit over the takeover."""
    handler, common, worker, esm, _ = env
    run_id, t = "run-zombie", time.time()
    common.ddb.put_item(TableName="experiment-runs", Item=common.to_ddb(
        {**common.meta_key(run_id), "run_id": run_id, "status": "running", "reject_percent": 0, "records": 1,
         "started_at": t, "closed": 0, "closed_by_pass": {}}))
    common.ddb.put_item(TableName="experiment-runs", Item=common.to_ddb(
        {**common.case_key(run_id, "IROPS-MSP-0001"), "case_id": "IROPS-MSP-0001", "run_id": run_id,
         "status": "queued", "pass_no": 1, "history": [], "queued_at": t, "fencing_token": 0, "claims": 0,
         "urgency": "flexible", "cabin": "economy", "next_same_route_seat_hrs": 3, "travel_concerns": "none",
         "children": 0}))
    record = {"messageId": "m1", "receiptHandle": "rh", "eventSourceARN": "arn:aws:sqs:us-east-1:1:effect-demo-rebooking",
              "body": json.dumps({"run_id": run_id, "case_id": "IROPS-MSP-0001", "pass": 1})}
    ids = {"who": "zombie"}
    worker.env_id = lambda: ids["who"]
    real_work = worker.simulate_work

    def stall_then_rescue(*_):
        worker.simulate_work = real_work
        time.sleep(LEASE + 0.5)
        ids["who"] = "rescuer"
        assert worker.process(record) is True
        ids["who"] = "zombie"
    worker.simulate_work = stall_then_rescue
    try:
        done = worker.process(record)
    finally:
        worker.simulate_work = real_work
    assert done is False
    rec = case_items(common, run_id)[0]
    assert rec["status"] == "resolved" and [h["w"] for h in rec["history"]] == ["rescuer"]
    assert [h.get("from") for h in rec["history"]] == ["zombie"]
    meta = common.from_ddb(common.ddb.get_item(TableName="experiment-runs", Key=common.to_ddb(common.meta_key(run_id)))["Item"])
    assert meta["closed"] == 1                               # counted once, by the rescuer only


def test_crash_plan_is_deterministic_per_pass_and_never_copied(env):
    _, common, *_ = env
    plans = [common.crash_plan("run-x", f"IROPS-MSP-{i:04d}", p, 30) for i in range(1, 101) for p in (1, 2, 3)]
    assert plans == [common.crash_plan("run-x", f"IROPS-MSP-{i:04d}", p, 30) for i in range(1, 101) for p in (1, 2, 3)]
    hit = [p for p in plans if p]
    assert 60 <= len(hit) <= 120
    assert {p["at"] for p in hit} == set(common.KILL_POINTS) and {p["mode"] for p in hit} == {"exit", "raise"}
    kn = {"kill_percent": 30, "slow_percent": 20, "dup_percent": 25}
    for i in range(1, 101):
        copies = common.outgoing("run-x", f"IROPS-MSP-{i:04d}", 1, kn)
        assert len(copies) in (1, 2) and len(set(copies)) == 1
        if json.loads(copies[0]).get("stall") == "zombie":
            assert len(copies) == 2


def test_crash_flag_fires_only_on_first_delivery(env):
    handler, common, worker, esm, _ = env
    body = {"run_id": "r", "case_id": "c", "pass": 1, "crash": {"at": "after_claim", "mode": "exit"}}
    assert worker.Crash({"attributes": {"ApproximateReceiveCount": "1"}}, body).at == "after_claim"
    assert worker.Crash({"attributes": {"ApproximateReceiveCount": "2"}}, body).at is None


def test_housekeeping_switches_off_abandoned_mappings(env):
    handler, _, _, esm, _ = env
    handler.set_esms(True)
    out = handler.handler({"internal": "housekeeping"}, Ctx(600))
    assert out["mappings"]["wanted"] == "off" and not any(esm.enabled.values())


def test_run_start_failure_fails_only_that_run(env):
    handler, common, worker, esm, threads = env
    http(handler, "POST", "/load")
    real = handler.outgoing
    calls = {"n": 0}

    def flaky(*a, **kw):
        calls["n"] += 1
        if calls["n"] > 30:
            raise RuntimeError("SQS fell over mid-send")
        return real(*a, **kw)
    handler.outgoing = flaky
    try:
        bad = start(handler)
        for t in threads:
            t.join(10)
    finally:
        handler.outgoing = real
    _, res = http(handler, "GET", "/results", run=bad)
    assert res["latest_run"]["status"] == "failed" and "mid-send" in res["latest_run"]["error"]
    assert res["budget"]["runs"] == 0                         # its budget came back
    good = start(handler, reject_percent=0)
    assert wait_complete(handler, good)["latest_run"]["stats"]["closed"] == 100


def test_housekeeping_fails_a_run_whose_start_died(env):
    handler, common, *_ = env
    t = time.time() - handler.START_TIMEOUT_S - 5
    meta = {**common.meta_key("run-dead"), "run_id": "run-dead", "status": "starting", "started_at": t,
            "records": 100, "index_sk": "2026-10-27T10:00:00.000+00:00#run-dead", "ttl": int(t + 3600)}
    index = {"pk": "RUNS", "sk": meta["index_sk"], "run_id": "run-dead", "status": "starting", "started_at": t}
    assert handler.admit(meta, index) == ("run-dead", None)
    out = handler.handler({"internal": "housekeeping"}, Ctx(600))
    assert out["actions"]["run-dead"] == "failed: start" and out["active_runs"] == []
    _, res = http(handler, "GET", "/results", run="run-dead")
    assert res["latest_run"]["status"] == "failed" and res["budget"]["records"] == 0


# --- split brain: concurrent runs -------------------------------------------------------------
def test_concurrent_runs_are_isolated(env):
    """Two runs with different settings at the same time: no record, counter or event crosses."""
    handler, common, worker, esm, threads = env
    http(handler, "POST", "/load")
    a = start(handler, reject_percent=0)                      # everything accepted on pass 1
    b = start(handler, reject_percent=90, kill_percent=10)    # many passes, crashes
    res_a, res_b = wait_complete(handler, a, timeout=240), wait_complete(handler, b, timeout=240)
    sa, sb = res_a["latest_run"]["stats"], res_b["latest_run"]["stats"]
    assert sa["closed"] == sb["closed"] == 100 and sa["open"] == sb["open"] == 0
    assert sa["avg_passes"] == 1.0 and sb["avg_passes"] > 2
    assert sa["kills_fired"] == 0 and sb["kills_fired"] > 0
    assert sa["races"]["double_commits"] == sb["races"]["double_commits"] == 0
    for run_id in (a, b):
        items = case_items(common, run_id)
        assert len(items) == 100 and all(i["run_id"] == run_id for i in items)
    # every message is grouped by its run, which is what makes the shared queues fair
    assert esm.groups and all(body_run == group for body_run, group in esm.groups)
    assert not any(esm.enabled.values()) and res_b["budget"]["runs"] == 0


def test_twenty_simultaneous_starts_admit_exactly_the_budget(env):
    handler, common, worker, esm, threads = env
    http(handler, "POST", "/load")
    results = []
    barrier = threading.Barrier(20)

    def hit():
        barrier.wait()
        results.append(http(handler, "POST", "/run", reject_percent=0))
    ts = [threading.Thread(target=hit) for _ in range(20)]
    [t.start() for t in ts]
    [t.join(60) for t in ts]
    accepted = [b["run_id"] for s, b in results if s == 202]
    refused = [b for s, b in results if s == 429]
    n = handler.MAX_ACTIVE_RUNS
    assert n > 2 and len(accepted) == n and len(refused) == 20 - n
    assert "budget is full" in refused[0]["error"]
    _, res = http(handler, "GET", "/results")
    assert sorted(r["run_id"] for r in res["recent_runs"]) == sorted(accepted)   # no orphans
    for run_id in accepted:
        assert wait_complete(handler, run_id, timeout=240)["latest_run"]["stats"]["closed"] == 100
    assert http(handler, "GET", "/results")[1]["budget"] == {"runs": 0, "max_runs": n, "records": 0,
                                                             "max_records": handler.RECORD_BUDGET}


def test_record_budget_is_shared_across_run_kinds(env, monkeypatch):
    handler, *_ = env
    monkeypatch.setattr(handler, "RECORD_BUDGET", 250)    # room for two 100-record runs, not three
    http(handler, "POST", "/load")
    start(handler, kind="chaos")
    start(handler, kind="integration")
    status, body = http(handler, "POST", "/run", kind="chaos")
    assert status == 429 and "200 of 250 records" in body["error"]


def test_cancelling_one_run_leaves_the_other_running(env):
    handler, common, worker, esm, threads = env
    http(handler, "POST", "/load")
    a = start(handler, reject_percent=90)
    b = start(handler, reject_percent=90)
    time.sleep(1.0)
    status, body = http(handler, "POST", "/cancel", run=a)
    assert status == 200 and body["status"] == "cancelled"
    assert any(esm.enabled.values())                          # b still needs the workers
    res_b = wait_complete(handler, b, timeout=240)
    assert res_b["latest_run"]["stats"]["closed"] == 100
    _, res_a = http(handler, "GET", "/results", run=a)
    assert res_a["latest_run"]["status"] == "cancelled" and res_a["latest_run"]["stats"]["closed"] < 100
    assert not any(esm.enabled.values())                      # last active run done: off
    # a stale tab cancelling a finished run is a clear no-op
    assert http(handler, "POST", "/cancel", run=b)[0] == 409
    assert http(handler, "POST", "/cancel", run=a)[0] == 409
    assert http(handler, "POST", "/cancel", run="run-nope")[0] == 404


def test_double_submit_with_one_client_token_starts_one_run(env):
    handler, common, *_ = env
    http(handler, "POST", "/load")
    out = []
    barrier = threading.Barrier(5)

    def hit():
        barrier.wait()
        out.append(http(handler, "POST", "/run", reject_percent=0, client_token="tab-1-click-1"))
    ts = [threading.Thread(target=hit) for _ in range(5)]
    [t.start() for t in ts]
    [t.join(30) for t in ts]
    run_ids = {b["run_id"] for s, b in out if s == 202}
    assert len(run_ids) == 1 and all(s == 202 for s, _ in out)
    assert http(handler, "GET", "/results")[1]["budget"]["runs"] == 1


def test_workers_drop_messages_of_cancelled_runs(env):
    handler, common, worker, esm, threads = env
    http(handler, "POST", "/load")
    a = start(handler, reject_percent=0)
    http(handler, "POST", "/cancel", run=a)
    record = {"messageId": "m", "receiptHandle": "r", "eventSourceARN": "arn:aws:sqs:us-east-1:1:effect-demo-rebooking",
              "body": json.dumps({"run_id": a, "case_id": "IROPS-MSP-0003", "pass": 1})}
    worker._control.clear()
    assert worker.process(record) is True                     # acknowledged and dropped
    assert all(i["claims"] == 0 or i["case_id"] != "IROPS-MSP-0003" for i in case_items(common, a))


def test_mappings_survive_a_run_finishing_while_another_starts(env, monkeypatch):
    """The last run turning workers off must not strand a run admitted in the same instant."""
    handler, common, worker, esm, threads = env
    t = time.time()

    def mk(run_id):
        meta = {**common.meta_key(run_id), "run_id": run_id, "status": "running", "started_at": t, "records": 1,
                "index_sk": f"x#{run_id}", "ttl": int(t + 3600)}
        return meta, {"pk": "RUNS", "sk": meta["index_sk"], "run_id": run_id}
    handler.admit(*mk("run-old"))
    handler.set_esms(True)
    real_set = handler.set_esms
    raced = {"done": False}

    def set_esms(enabled):
        out = real_set(enabled)
        if not enabled and not raced["done"]:           # run-new is admitted and turns workers on
            raced["done"] = True                         # just as run-old's "off" lands
            handler.admit(*mk("run-new"))
            real_set(True)
            real_set(False)                              # ...and run-old's late "off" wins the race
        return out
    monkeypatch.setattr(handler, "set_esms", set_esms)
    assert handler.transition("run-old", "complete", ["running"])
    out = handler.reconcile_mappings()
    assert out["wanted"] == "on (raced)" and all(esm.enabled.values())


def test_results_tolerate_kill_items_from_older_versions(env):
    handler, common, *_ = env
    t = time.time()
    common.ddb.put_item(TableName="experiment-runs", Item=common.to_ddb(
        {**common.meta_key("run-old"), "run_id": "run-old", "status": "complete", "started_at": t - 100,
         "finished_at": t - 20, "records": 1, "closed": 1, "index_sk": "a#run-old", "final": True}))
    common.ddb.put_item(TableName="experiment-runs", Item=common.to_ddb(
        {"pk": "RUN#run-old", "sk": "KILL#IROPS-MSP-0001", "case_id": "IROPS-MSP-0001", "status": "fired",
         "worker": "env-old", "claimed_at": t - 90, "killed_at": t - 89, "stage": "mid_work"}))
    status, res = http(handler, "GET", "/results", run="run-old")
    assert status == 200, res
    assert res["latest_run"]["kills"][0]["point"] == "before_commit"


# --- orphans and conflicts (found on AWS: a record left queued with no message anywhere) ---------
def test_orphaned_record_is_requeued_and_the_run_completes(env, monkeypatch):
    """A handoff lost with its message already acknowledged: the queues drain, one record stays
    open with nothing left to deliver it. The orphan re-queue must notice and finish the run."""
    handler, common, worker, esm, threads = env
    monkeypatch.setattr(handler, "ORPHAN_S", 3)
    monkeypatch.setattr(handler, "STALL_S", 3)
    real_send = worker.send_handoff
    lost = {"n": 0}

    def lossy(run_id, case_id, pass_no, kn, repair=False):
        if case_id == "IROPS-MSP-0005" and pass_no == 2 and not repair and not lost["n"]:
            lost["n"] += 1                            # the message vanishes, the incoming one is acked
            return
        return real_send(run_id, case_id, pass_no, kn, repair)
    monkeypatch.setattr(worker, "send_handoff", lossy)
    monkeypatch.setattr(worker, "customer_rejects", lambda run_id, case_id, pass_no, pct: case_id == "IROPS-MSP-0005" and pass_no == 1)
    http(handler, "POST", "/load")
    run_id = start(handler, reject_percent=0)
    res = wait_complete(handler, run_id, timeout=120)
    s = res["latest_run"]["stats"]
    assert lost["n"] == 1 and s["closed"] == 100 and s["open"] == 0
    assert s["degradation"]["orphans_requeued"] >= 1
    assert any(i["kind"] == "requeued" and i["case_id"] == "IROPS-MSP-0005" for i in res["latest_run"]["incidents"])


def test_sweeper_requeues_a_later_pass_instead_of_dropping_its_dead_letter(env):
    handler, common, worker, esm, threads = env
    import sweeper
    run_id, t = "run-dlq", time.time()
    common.ddb.put_item(TableName="experiment-runs", Item=common.to_ddb(
        {**common.case_key(run_id, "IROPS-MSP-0009"), "case_id": "IROPS-MSP-0009", "run_id": run_id,
         "status": "queued", "pass_no": 2, "queued_at": t - LEASE - 5, "history": [], "fencing_token": 1}))
    common.sqs.send_message(QueueUrl=common.DLQ, MessageBody=json.dumps(
        {"run_id": run_id, "case_id": "IROPS-MSP-0009", "pass": 1}))
    out = sweeper.handler({}, Ctx(60))
    assert out["requeued_later_pass"] == 1
    msgs = common.sqs.receive_message(QueueUrl=common.REROUTING_QUEUE, MaxNumberOfMessages=10)["Messages"]
    assert [json.loads(m["Body"])["pass"] for m in msgs] == [2]


def test_transaction_conflicts_are_retried_in_place(env, monkeypatch):
    handler, common, *_ = env
    from botocore.exceptions import ClientError
    calls = {"n": 0}
    real = common.ddb.transact_write_items

    def flaky(**kw):
        calls["n"] += 1
        if calls["n"] <= 2:
            err = ClientError({"Error": {"Code": "TransactionCanceledException", "Message": "conflict"},
                               "CancellationReasons": [{"Code": "TransactionConflict"}, {"Code": "None"}]},
                              "TransactWriteItems")
            err.__class__ = common.ddb.exceptions.TransactionCanceledException
            raise err
        return real(**kw)
    monkeypatch.setattr(common.ddb, "transact_write_items", flaky)
    common.transact([{"Put": {"TableName": "experiment-runs", "Item": {"pk": {"S": "x"}, "sk": {"S": "y"}}}}])
    assert calls["n"] == 3


# --- load runs ------------------------------------------------------------------------------
def wait_summary(handler, run_id, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        status, res = http(handler, "GET", "/results", run=run_id)
        assert status == 200, res
        run = res["latest_run"]
        if run.get("summary"):
            return run
        assert run["status"] not in ("failed", "cancelled"), run
        time.sleep(0.5)
    raise AssertionError(f"load run never scored: {json.dumps(run.get('stats'), indent=1)}")


def test_load_run_backpressure_and_handoff(env, monkeypatch):
    """Generated unique records, released under a backlog cap, across several producer handoffs."""
    handler, common, *_ = env
    monkeypatch.setattr(handler, "LOAD_RECORDS_PER_SEC", 400.0)     # far faster than the workers drain
    run_id = start(handler, kind="load", records=80, backlog_cap=10, chunk_s=2, rate=400)
    run = wait_summary(handler, run_id)
    s = run["summary"]
    assert s["passed"], s
    assert s["records"] == s["created"] == s["closed"] == 80 and s["lost"] == 0
    assert s["double_results"] == 0 and s["outside_rules"] == 0
    assert s["max_backlog"] <= 10 and s["create_skipped"] == 0
    assert s["backpressure_s"] > 0                              # the producer was held back
    assert s["handoffs"] >= 1                                    # the release outlived one chunk
    ids = {r["case_id"] for r in case_items(common, run_id)}
    assert len(ids) == 80 and all(i.startswith("LOAD-") for i in ids)
    assert run["records"] == [] and run["stats"]["released"] == 80    # the view never reads records


def test_load_producer_dies_and_is_restarted(env, monkeypatch):
    """A producer that dies between creating a block and advancing the cursor: the restart redoes it."""
    handler, common, *_ = env
    monkeypatch.setattr(handler, "LOAD_RECORDS_PER_SEC", 40.0)
    monkeypatch.setattr(handler, "PRODUCER_STALE_S", 1)
    real = handler.sqs.send_message_batch
    calls = {"n": 0}

    def dies_on_third(**kw):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("producer killed after creating block 3")
        return real(**kw)
    monkeypatch.setattr(handler.sqs, "send_message_batch", dies_on_third)
    run_id = start(handler, kind="load", records=50, backlog_cap=50, chunk_s=60, rate=40)
    end = time.time() + 30
    while time.time() < end and handler.get_meta(run_id, consistent=True).get("cursor", 0) < 20:
        time.sleep(0.2)
    time.sleep(1.5)
    meta = handler.get_meta(run_id, consistent=True)
    assert meta["status"] == "running" and int(meta["cursor"]) == 20     # stuck after block 2
    out = handler.housekeeping()
    assert out["actions"][run_id] == "restarted producer"
    s = wait_summary(handler, run_id)["summary"]
    assert s["passed"], s
    assert s["create_skipped"] == 10                     # block 3 was created once, then skipped
    assert s["double_results"] == 0 and s["lost"] == 0


def test_load_run_counts_against_budget(env):
    handler, *_ = env
    status, body = http(handler, "POST", "/run", kind="load", records=handler.RECORD_BUDGET + 1)
    assert status == 400 or status == 409, body


def test_throttled_start_invocations_are_adopted(env):
    """
    The workers can hold 8 of the account's 10 concurrent Lambdas, so simultaneous start
    invocations can sit throttled for minutes. The first one to run sets the others up too.
    """
    handler, common, *_ = env
    assert http(handler, "POST", "/load")[0] == 200
    real = handler.lam.invoke
    first = threading.Event()

    def throttled(FunctionName, InvocationType, Payload):
        ev = json.loads(Payload)
        if ev.get("internal") == "start_run" and first.is_set():
            def later():
                time.sleep(20)                  # Lambda retrying a throttled async invoke
                real(FunctionName=FunctionName, InvocationType=InvocationType, Payload=Payload)
            threading.Thread(target=later, daemon=True).start()
            return {"StatusCode": 202}
        first.set()
        return real(FunctionName=FunctionName, InvocationType=InvocationType, Payload=Payload)
    handler.lam.invoke = throttled
    ids = [start(handler, kind="integration", reject_percent=0, client_token=str(uuid.uuid4())) for _ in range(3)]
    time.sleep(0.2)
    end = time.time() + 15
    while time.time() < end and any(handler.get_meta(r, consistent=True)["status"] == "starting" for r in ids):
        time.sleep(0.2)
    metas = [handler.get_meta(r, consistent=True) for r in ids]
    assert all(m["status"] != "starting" for m in metas), [m["status"] for m in metas]
    for r in ids:
        res = wait_complete(handler, r, timeout=150)["latest_run"]
        assert res["stats"]["closed"] == res["stats"]["records"]
        assert not res["stats"]["races"]["double_commits"]
    time.sleep(21)                              # the late invocations arrive and find nothing to do
    for r in ids:
        ev = handler.query_all(KeyConditionExpression="pk = :p AND sk = :s",
                               ExpressionAttributeValues=common.to_ddb({":p": common.run_pk(r), ":s": "EVENTS"}))
        assert not (ev and ev[0].get("duplicates_sent")), "a run was set up twice"
