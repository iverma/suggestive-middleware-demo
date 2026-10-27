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
        self.envs = threading.local()
        self.threads = [threading.Thread(target=self.poll, args=(name,), daemon=True)
                        for name, (_, _, mx) in queues.items() for _ in range(mx)]
        worker.env_id = lambda: self.envs.id
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
                                            AttributeNames=["ApproximateReceiveCount"]).get("Messages", [])
            for m in msgs:
                event = {"Records": [{"messageId": m["MessageId"], "receiptHandle": m["ReceiptHandle"],
                                      "body": m["Body"], "eventSourceARN": arn,
                                      "attributes": {"ApproximateReceiveCount": m["Attributes"]["ApproximateReceiveCount"]}}]}
                try:
                    out = self.worker.handler(event, Ctx())
                except (Killed, self.worker.FunctionTimedOut):
                    self.kills += 1
                    self.envs.id = "env-" + uuid.uuid4().hex[:6]       # crashed env is replaced
                    continue                                           # message stays, reappears later
                except Exception:
                    self.faults += 1                                   # failed invoke: message stays
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
    import botocore.client
    lock = threading.Lock()
    real = botocore.client.BaseClient._make_api_call

    def call(self, operation, params):
        if self.meta.service_model.service_name == "dynamodb":
            with lock:
                return real(self, operation, params)
        return real(self, operation, params)
    monkeypatch.setattr(botocore.client.BaseClient, "_make_api_call", call)


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
                 "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "10"})}
        queues = {}
        for name, mx in (("rebooking", 5), ("rerouting", 3)):
            url = sqs.create_queue(QueueName=f"effect-demo-{name}", Attributes=attrs)["QueueUrl"]
            arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
            os.environ[f"{name.upper()}_QUEUE_URL"] = url
            queues[name] = (url, arn, mx)
        os.environ["DLQ_URL"] = dlq

        sys.path.insert(0, str(SRC))
        for m in ("common", "worker", "handler"):
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


def wait_complete(handler, timeout=150):
    end = time.time() + timeout
    while time.time() < end:
        status, res = http(handler, "GET", "/results")
        assert status == 200, res                   # /results must never fail mid-run
        run = res["latest_run"]
        if run and run["status"] == "complete":
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
    assert http(handler, "POST", "/run", reject_percent=101)[0] == 400
    assert http(handler, "POST", "/run", kill_percent=51)[0] == 400


def test_run_without_records_fails_cleanly(env):
    handler, _, _, esm, threads = env
    assert http(handler, "POST", "/run")[0] == 202
    for t in threads:
        t.join(5)
    _, res = http(handler, "GET", "/results")
    assert res["recent_runs"][0]["status"] == "failed"
    assert not any(esm.enabled.values())                      # mappings switched back off


def test_autoscaled_run_with_killed_workers_loses_nothing(env):
    handler, common, worker, esm, threads = env
    status, body = http(handler, "POST", "/load")
    assert status == 200 and body["loaded"] == 100
    assert not any(esm.enabled.values())                      # idle: no polling

    status, body = http(handler, "POST", "/run", reject_percent=50, kill_percent=15)
    assert status == 202, body
    assert http(handler, "POST", "/run")[0] == 409            # one run at a time

    res = wait_complete(handler)
    run, s = res["latest_run"], res["latest_run"]["stats"]
    print(json.dumps({k: s[k] for k in ("resolved", "human_review", "avg_passes", "workers_peak", "workers_used",
                                        "kills_fired", "takeovers_after_crash", "events", "duration_s")}, indent=1))
    assert s["records"] == 100 and s["open"] == 0
    assert s["resolved"] + s["human_review"] == 100            # nothing lost
    assert s["satisfactory"] == s["resolved"]                  # rules pass never leaves the golden set
    assert not any(r["unacceptable_offered"] for r in run["records"])
    # crashes on every pass, both modes, each one recovered the way its kill point predicts
    assert s["kills_fired"] >= 10 and s["kills_fired"] == esm.kills + esm.faults
    assert s["kills_recovered"] == s["kills_fired"], [k for k in run["kills"] if not k["recovered"]]
    assert any(int(k["pass"]) > 1 for k in run["kills"])
    assert sum(1 for w in run["workers"] if w["state"] == "killed") == esm.kills
    assert res["queues"]["dead_letter"]["visible"] == 0
    # the queue fanned work out across several concurrent workers, inside the mapping limits
    assert 3 <= s["workers_peak"] <= 8 + esm.kills     # a dead env's last moments can overlap its replacement
    assert max(run["timeline"]["rebooking"]) <= 5 and max(run["timeline"]["rerouting"]) <= 3
    assert s["workers_used"] >= 5
    assert not any(esm.enabled.values())                      # completion switched mappings off
    for r in run["records"]:
        assert r["fencing_token"] >= r["claims"] >= 1

    # a second run reuses the records; tokens keep increasing across runs
    before = {r["case_id"]: r["fencing_token"] for r in run["records"]}
    for t in threads:
        t.join(5)
    assert http(handler, "POST", "/run", reject_percent=0, kill_percent=0)[0] == 202
    res2 = wait_complete(handler)
    assert res2["latest_run"]["stats"]["avg_passes"] <= 1.1
    assert all(r["fencing_token"] > before[r["case_id"]] for r in res2["latest_run"]["records"])
    assert len(res2["recent_runs"]) == 2 and res2["recent_runs"][1]["final"]["records"] == 100

    status, body = http(handler, "POST", "/reset")
    assert status == 200 and body["deleted_records"] == 100
    _, res3 = http(handler, "GET", "/results")
    assert res3["records_in_table"] == 0 and res3["latest_run"] is None


def test_housekeeping_switches_off_abandoned_mappings(env):
    handler, _, _, esm, _ = env
    handler.set_esms(True)
    assert all(esm.enabled.values())
    out = handler.handler({"internal": "housekeeping"}, Ctx(600))
    assert out["disabled"] and not any(esm.enabled.values())


def test_every_kill_point_recovers_on_every_pass_in_both_modes(env):
    """Force each kill point x mode onto first and later passes; each must recover its own way."""
    handler, common, worker, esm, threads = env
    points, modes = common.KILL_POINTS, common.KILL_MODES

    def forced(run_id, case_id, pass_no, kill_percent):
        n = int(case_id[-4:])
        if n % 3:                                   # leave two thirds of records uncrashed
            return None
        i = n // 3 + pass_no
        return {"at": points[i % len(points)], "mode": modes[(i // len(points)) % 2]}
    common.crash_plan = forced                      # message_body looks it up at call time

    http(handler, "POST", "/load")
    assert http(handler, "POST", "/run", reject_percent=60, kill_percent=1)[0] == 202
    res = wait_complete(handler, timeout=240)
    run, s = res["latest_run"], res["latest_run"]["stats"]
    by_point = {k["point"]: k for k in s["kills_by_point"]}
    print(json.dumps(s["kills_by_point"], indent=1))
    assert s["open"] == 0 and s["resolved"] + s["human_review"] == 100
    assert s["kills_recovered"] == s["kills_fired"], [k for k in run["kills"] if not k["recovered"]]
    for p in points:
        assert by_point[p]["fired"] >= 1, p
    assert {int(k["pass"]) for k in run["kills"]} >= {1, 2}
    assert {k["mode"] for k in run["kills"]} == {"exit", "raise"}
    # the signatures each point should leave behind
    for k in run["kills"]:
        rec = next(r for r in run["records"] if r["case_id"] == k["case_id"])
        assert rec["status"] in ("resolved", "human_review"), k
        if k["point"] in ("after_claim", "during_offer", "during_customer_response", "before_commit"):
            assert k["recovered_by"] == "lease takeover" and k["recovery_s"] is not None, k
    assert s["events"]["repaired"] >= by_point["after_commit_before_handoff"]["fired"]
    assert s["events"]["stale"] >= 1                 # K7/K8: redelivered copies of finished work dropped
    assert res["queues"]["dead_letter"]["visible"] == 0


def test_slow_and_duplicate_workers_race_without_double_results(env):
    """
    Zombies (alive past their lease, racing a duplicate), workers hung past the function
    timeout, and plain duplicate deliveries, all at once. Every zombie must end fenced or
    uncontested, every timeout recovered, and no record may ever get two results for a pass.
    """
    handler, common, worker, esm, threads = env
    real_plan = common.race_plan

    def forced(run_id, case_id, pass_no, slow_percent=0, dup_percent=0):
        n = int(case_id[-4:])
        if pass_no == 1 and n in (10, 60):
            return "zombie", True                   # two first-pass zombies: leaves free workers to rescue
        if pass_no == 1 and n == 35:
            return "timeout", False                 # a hung first-pass worker (always happens)
        if pass_no == 2 and n % 20 == 5:
            return "timeout", False                 # hung re-route workers (if those records get rejected)
        return real_plan(run_id, case_id, pass_no, 0, dup_percent)
    common.race_plan = forced

    http(handler, "POST", "/load")
    assert http(handler, "POST", "/run", reject_percent=50, dup_percent=30)[0] == 202
    res = wait_complete(handler, timeout=240)
    run, s = res["latest_run"], res["latest_run"]["stats"]
    r = s["races"]
    print(json.dumps({k: v for k, v in r.items() if not k.endswith("_list")}, indent=1))
    print(json.dumps([(z["case_id"], z["outcome"], z.get("taken_over_by")) for z in r["zombie_list"]]))
    assert s["open"] == 0 and s["resolved"] + s["human_review"] == 100
    assert r["double_commits"] == 0, r["double_commit_list"]          # the core invariant
    assert r["zombies"] == 2 and r["zombies_fenced"] + r["zombies_uncontested"] == 2
    assert r["zombies_fenced"] >= 1                                    # fencing actually exercised
    # other late writes may be fenced too: any worker slower than its lease loses to a duplicate
    assert s["events"]["fenced"] >= r["zombies_fenced"]
    for z in r["zombie_list"]:
        rec = next(x for x in run["records"] if x["case_id"] == z["case_id"])
        if z["outcome"] == "fenced":                                  # the rescuer's result stands
            assert z["taken_over_by"] and z["taken_over_by"] != z["worker"]
    timeouts = next(k for k in s["kills_by_point"] if k["point"] == "stalled_past_timeout")
    assert timeouts["fired"] >= 1 and timeouts["recovered"] == timeouts["fired"]
    assert r["duplicates_sent"] >= 20                                 # ~30% of messages, plus the zombies'
    assert r["duplicates_held_back"] + r["duplicates_dropped"] >= 10
    assert res["queues"]["dead_letter"]["visible"] == 0


def test_crash_plan_is_deterministic_per_pass_and_never_copied(env):
    _, common, *_ = env
    plans = [common.crash_plan("run-x", f"IROPS-MSP-{i:04d}", p, 30) for i in range(1, 101) for p in (1, 2, 3)]
    assert plans == [common.crash_plan("run-x", f"IROPS-MSP-{i:04d}", p, 30) for i in range(1, 101) for p in (1, 2, 3)]
    hit = [p for p in plans if p]
    assert 60 <= len(hit) <= 120                                   # ~30% of 300
    assert {p["at"] for p in hit} == set(common.KILL_POINTS) and {p["mode"] for p in hit} == {"exit", "raise"}
    # a case crashing on pass 1 does not inherit that crash on pass 2: each pass is its own draw
    p1 = [common.crash_plan("run-x", f"IROPS-MSP-{i:04d}", 1, 30) for i in range(1, 101)]
    p2 = [common.crash_plan("run-x", f"IROPS-MSP-{i:04d}", 2, 30) for i in range(1, 101)]
    assert p1 != p2
    assert common.crash_plan("run-x", "IROPS-MSP-0001", 1, 0) is None
    body = json.loads(common.message_body("run-x", "IROPS-MSP-0001", 2, 0))
    assert "crash" not in body
    # duplicates: identical copies; a zombie is always duplicated so someone can race it
    kn = {"kill_percent": 30, "slow_percent": 20, "dup_percent": 25}
    for i in range(1, 101):
        copies = common.outgoing("run-x", f"IROPS-MSP-{i:04d}", 1, kn)
        assert len(copies) in (1, 2) and len(set(copies)) == 1
        b = json.loads(copies[0])
        if b.get("stall") == "zombie":
            assert len(copies) == 2


def test_crash_flag_fires_only_on_first_delivery(env):
    handler, common, worker, esm, _ = env
    body = {"run_id": "r", "case_id": "c", "pass": 1, "crash": {"at": "after_claim", "mode": "exit"}}
    assert worker.Crash({"attributes": {"ApproximateReceiveCount": "1"}}, body).at == "after_claim"
    assert worker.Crash({"attributes": {"ApproximateReceiveCount": "2"}}, body).at is None
    assert worker.Crash({"attributes": {"ApproximateReceiveCount": "1"}}, {**body, "crash": None}).at is None


def test_zombie_write_is_fenced(env):
    """A worker that stalls past its lease must not be able to commit over the takeover."""
    handler, common, worker, esm, _ = env
    http(handler, "POST", "/load")
    run_id = "run-zombie"
    common.ddb.put_item(TableName="experiment-runs", Item=common.to_ddb(
        {"pk": "CONTROL", "sk": "ACTIVE", "run_id": run_id, "status": "running", "reject_percent": 0,
         "expires_at": time.time() + 600}))
    common.ddb.update_item(TableName="customer-records-db", Key=common.to_ddb({"case_id": "IROPS-MSP-0001"}),
                           UpdateExpression="SET run_id = :r, #st = :q, pass_no = :one, history = :e, queued_at = :t",
                           ExpressionAttributeNames={"#st": "status"},
                           ExpressionAttributeValues=common.to_ddb({":r": run_id, ":q": "queued", ":one": 1,
                                                                    ":e": [], ":t": time.time()}))
    record = {"messageId": "m1", "receiptHandle": "rh", "eventSourceARN": "arn:aws:sqs:us-east-1:1:effect-demo-rebooking",
              "body": json.dumps({"run_id": run_id, "case_id": "IROPS-MSP-0001", "pass": 1})}
    ids = {"who": "zombie"}
    worker.env_id = lambda: ids["who"]
    real_work = worker.simulate_work

    def stall_then_rescue(*_):
        worker.simulate_work = real_work
        time.sleep(LEASE + 0.5)               # zombie "GC pause" past its lease
        ids["who"] = "rescuer"
        assert worker.process(record) is True  # redelivered copy: rescuer takes over and commits
        ids["who"] = "zombie"
    worker.simulate_work = stall_then_rescue
    try:
        done = worker.process(record)
    finally:
        worker.simulate_work = real_work
    assert done is False                      # zombie is fenced off and keeps its hands off the message
    rec = common.from_ddb(common.ddb.get_item(TableName="customer-records-db",
                                              Key=common.to_ddb({"case_id": "IROPS-MSP-0001"}))["Item"])
    assert rec["status"] == "resolved" and [h["w"] for h in rec["history"]] == ["rescuer"]
    assert [h.get("from") for h in rec["history"]] == ["zombie"]
    events = common.from_ddb(common.ddb.get_item(TableName="experiment-runs", Key=common.to_ddb(
        {"pk": f"RUN#{run_id}", "sk": "EVENTS"}))["Item"])
    assert events["fenced"] == 1


def test_results_tolerates_kill_items_from_older_versions(env):
    """A run recorded before the 8 kill points existed must still render (deploy smoke test)."""
    handler, common, *_ = env
    http(handler, "POST", "/load")
    t = time.time()
    common.ddb.put_item(TableName="experiment-runs", Item=common.to_ddb(
        {"pk": "RUN", "sk": "2026-10-27T09:00:00+00:00#run-old", "run_id": "run-old", "status": "complete",
         "started_at": t - 100, "finished_at": t - 20, "records": 1, "reject_percent": 50, "kill_workers": 3}))
    common.ddb.put_item(TableName="experiment-runs", Item=common.to_ddb(
        {"pk": "RUN#run-old", "sk": "KILL#IROPS-MSP-0001", "case_id": "IROPS-MSP-0001", "status": "fired",
         "worker": "env-old", "claimed_at": t - 90, "killed_at": t - 89, "stage": "mid_work"}))
    status, res = http(handler, "GET", "/results")
    assert status == 200, res
    assert res["latest_run"]["kills"][0]["point"] == "before_commit"
