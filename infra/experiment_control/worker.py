"""
Re-booking worker, invoked by SQS event source mappings (one message per invoke).

Lambda autoscales how many of these run at once from queue depth: up to the
mapping's maximum concurrency on the rebooking queue (pass 1) and on the
re-routing queue (pass 2+, rejected offers). Each concurrent copy runs in its
own execution environment; that environment is what the UI calls a "worker".

Per message: claim the customer record, make the next offer, simulate the
customer's answer, and commit the outcome.

Failure handling
- Claim = conditional UpdateItem that increments fencing_token and takes a
  lease. The lease is shorter than the queue's visibility timeout, so by the
  time SQS redelivers a crashed worker's message, its lease has expired and
  the record can be taken over.
- Commit is conditional on fencing_token and lease_owner still being ours. A
  worker that was presumed dead and wakes up late (a zombie) is fenced off.
- On rejection the record is committed as queued for pass+1 BEFORE the next
  message is sent; the current message is deleted (by Lambda) only after the
  invoke succeeds. If the worker dies in between, the old message comes back,
  and whoever gets it sees the record already queued for a later pass and
  re-sends the handoff (repair).
- Rejection decisions are seeded by (run, case, pass), so a retried pass makes
  the same decision as the attempt that crashed.
- Killed workers are an injected fault carried on the message itself: a
  message may say "crash": "mid_work" or "after_commit". On its first delivery
  the worker records the kill, then hard-exits its execution environment at
  that point, holding the lease and the undeleted message. No orchestrator
  decides who dies; the redelivered copy runs normally.

Return value uses ReportBatchItemFailures: a message listed there is NOT
deleted, so it is retried after its visibility timeout.
"""
import json
import os
import random
import time
import uuid

from common import (
    LEASE_SECONDS, RECORDS_TABLE, REBOOKING_QUEUE, REROUTING_QUEUE, RUNS_TABLE, TERMINAL,
    customer_rejects, ddb, from_ddb, now, offer_sequence, sqs, to_ddb,
)

WORK_MIN_S = float(os.environ.get("WORK_MIN_S", "0.6"))
WORK_MAX_S = float(os.environ.get("WORK_MAX_S", "1.8"))
CONTROL_CACHE_S = 2.0
CONTROL_KEY = {"pk": "CONTROL", "sk": "ACTIVE"}
COUNTERS = ("fenced", "repaired", "stale", "deferred")

# One id per execution environment (= one concurrent worker). Survives across warm invokes.
_ENV = {"id": "env-" + uuid.uuid4().hex[:6], "started": now()}
_control = {"t": 0.0, "v": {}}


def env_id():
    return _ENV["id"]


def simulate_work():
    """Artificial lag standing in for inventory search and pricing calls."""
    time.sleep(random.uniform(WORK_MIN_S, WORK_MAX_S))


def crash():
    """Simulated process death. Tests replace this so they don't exit the interpreter."""
    os._exit(137)


def control(force=False):
    if force or now() - _control["t"] > CONTROL_CACHE_S:
        _control["v"] = from_ddb(ddb.get_item(TableName=RUNS_TABLE, Key=to_ddb(CONTROL_KEY)).get("Item"))
        _control["t"] = now()
    return _control["v"]


def bump(run_id, counter):
    ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb({"pk": f"RUN#{run_id}", "sk": "EVENTS"}),
                    UpdateExpression="ADD #c :one", ExpressionAttributeNames={"#c": counter},
                    ExpressionAttributeValues=to_ddb({":one": 1}))


def queue_url(arn):
    name = (arn or "").rsplit(":", 1)[-1]
    return REROUTING_QUEUE if REROUTING_QUEUE.endswith("/" + name) else REBOOKING_QUEUE


def send_handoff(run_id, case_id, pass_no):
    sqs.send_message(QueueUrl=REROUTING_QUEUE, MessageBody=json.dumps(
        {"run_id": run_id, "case_id": case_id, "pass": pass_no}))


def crash_point(record, body):
    """
    The crash instruction travels on the message, set by whoever enqueued the work:
      "mid_work"     - die after claiming, before committing (exercises lease takeover)
      "after_commit" - die after committing, before the handoff / ack (exercises repair
                       of a lost handoff, or idempotent redelivery of a finished record)
    Obeyed only on the message's first delivery, so the redelivered copy is processed
    normally and each planned crash fires exactly once.
    """
    first = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1")) == 1
    return body.get("crash") if first else None


def die(run_id, case_id, claimed_at, stage):
    """Record the crash for the UI (telemetry, not coordination), then kill this environment."""
    ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb({
        "pk": f"RUN#{run_id}", "sk": f"KILL#{case_id}", "case_id": case_id, "status": "fired",
        "worker": env_id(), "claimed_at": round(claimed_at, 3), "killed_at": round(now(), 3), "stage": stage}))
    print(json.dumps({"worker": env_id(), "event": "killed", "case_id": case_id, "stage": stage}))
    crash()


def process(record, reject_percent=None):
    """Handle one SQS message. Returns True when the message is done (Lambda deletes it)."""
    body = json.loads(record["body"])
    run_id, case_id, pass_no = body["run_id"], body["case_id"], int(body["pass"])
    crash_at = crash_point(record, body)

    ctl = control()
    if ctl.get("run_id") != run_id:
        ctl = control(force=True)
    if ctl.get("run_id") != run_id or ctl.get("status") not in ("starting", "running"):
        return True                              # message from an old, finished or aborted run
    reject_percent = ctl.get("reject_percent", 50) if reject_percent is None else reject_percent

    t = now()
    try:
        resp = ddb.update_item(
            TableName=RECORDS_TABLE,
            Key=to_ddb({"case_id": case_id}),
            UpdateExpression=("SET fencing_token = fencing_token + :one, lease_owner = :w, "
                              "lease_expires = :exp, #st = :inprog, claimed_at = :t ADD claims :one"),
            ConditionExpression=("run_id = :run AND pass_no = :p AND "
                                 "(#st = :queued OR (#st = :inprog AND lease_expires < :t))"),
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=to_ddb({
                ":one": 1, ":w": env_id(), ":exp": round(t + LEASE_SECONDS, 3), ":inprog": "in_progress",
                ":queued": "queued", ":t": round(t, 3), ":run": run_id, ":p": pass_no}),
            ReturnValues="ALL_OLD",
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
    except ddb.exceptions.ConditionalCheckFailedException as e:
        return handle_unclaimable(record, run_id, case_id, pass_no, from_ddb(e.response.get("Item")), t)

    old = from_ddb(resp["Attributes"])
    token = int(old["fencing_token"]) + 1
    takeover = old.get("status") == "in_progress"

    # --- decide (pass 1: rules engine; pass 2+: walk the rules ranking) -------
    simulate_work()
    offers = offer_sequence(old)
    offer = offers[pass_no - 1] if pass_no <= len(offers) else "HUMAN_REVIEW"
    if offer == "HUMAN_REVIEW":
        outcome = "human_review"
    elif customer_rejects(run_id, case_id, pass_no, reject_percent):
        outcome = "reject"
    else:
        outcome = "accept"
    if crash_at == "mid_work":
        die(run_id, case_id, t, "mid_work")

    entry = {"p": pass_no, "o": offer, "r": outcome, "w": env_id(), "f": token, "c": round(t, 3), "t": round(now(), 3)}
    if takeover:
        entry["from"] = old.get("lease_owner")
    values = {":tok": token, ":w": env_id(), ":h": [entry], ":t": round(now(), 3), ":offer": offer}
    if outcome == "reject":
        update = ("SET #st = :queued, pass_no = :next, queued_at = :t, history = list_append(history, :h) "
                  "REMOVE lease_owner, lease_expires")
        values.update({":queued": "queued", ":next": pass_no + 1})
        values.pop(":offer")
    else:
        update = ("SET #st = :final, resolution = :offer, resolved_at = :t, "
                  "history = list_append(history, :h) REMOVE lease_owner, lease_expires")
        values[":final"] = "resolved" if outcome == "accept" else "human_review"
    try:
        ddb.update_item(
            TableName=RECORDS_TABLE, Key=to_ddb({"case_id": case_id}),
            UpdateExpression=update,
            ConditionExpression="fencing_token = :tok AND lease_owner = :w",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=to_ddb(values),
        )
    except ddb.exceptions.ConditionalCheckFailedException:
        # Someone took the record over after our lease lapsed; our result is discarded.
        # Keep the message (don't let Lambda delete it): the new owner holds the live delivery.
        bump(run_id, "fenced")
        return False

    if crash_at == "after_commit":
        die(run_id, case_id, t, "after_commit")     # committed, but the handoff / ack never happens
    if outcome == "reject":
        send_handoff(run_id, case_id, pass_no + 1)
    return True


def handle_unclaimable(record, run_id, case_id, pass_no, rec, t):
    if not rec or rec.get("run_id") != run_id or rec.get("status") in TERMINAL:
        bump(run_id, "stale")
        return True
    if int(rec.get("pass_no", 0)) > pass_no:
        if rec.get("status") == "queued":
            # The previous holder committed a rejection but may have died before sending
            # the next message. Re-send it; duplicates are harmless (claims are conditional).
            send_handoff(run_id, case_id, int(rec["pass_no"]))
            bump(run_id, "repaired")
        else:
            bump(run_id, "stale")
        return True
    if rec.get("status") == "in_progress":
        # Lease still live. Don't drop the message: if the holder is dead, this delivery is
        # how the record gets recovered. Hide it until the lease runs out, then retry.
        wait = max(1, int(float(rec.get("lease_expires", t)) - t) + 1)
        sqs.change_message_visibility(QueueUrl=queue_url(record.get("eventSourceARN")),
                                      ReceiptHandle=record["receiptHandle"], VisibilityTimeout=min(wait, 60))
        bump(run_id, "deferred")
        return False
    bump(run_id, "stale")
    return True


def handler(event, context):
    failures = []
    for record in event.get("Records", []):
        if not process(record):
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
