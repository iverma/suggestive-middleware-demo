"""
Dead-letter sweeper: separate control-plane infrastructure from the worker path.

A message lands in the dead-letter queue after failing every delivery (5 receives): bad
data, a bug, or a downstream that never recovered. Its record would otherwise stay open
forever, because nothing will deliver that message again. Every minute this function:

  1. checks the DLQ depth (one cheap call when it's empty, which is almost always),
  2. for each dead letter, reads the record and, if it is still waiting on that exact
     run and pass, routes it to HUMAN_REVIEW with a fenced write: the fencing token is
     bumped and the write only lands if the token hasn't moved since the read. A worker
     that somehow still holds the record can't overwrite the decision afterwards, and
     if a worker moved the record on in the meantime, the sweeper leaves it alone,
  3. deletes the dead letter (or leaves it for the next sweep if anything failed).

Cost: an EventBridge Scheduler tick per minute (~43k/month of 14M free), one SQS call per
empty tick (~43k/month of 1M free), and a sub-second Lambda run.
"""
import json
import os

from common import (
    DLQ, RUN_TTL_S, RUNS_TABLE, TERMINAL, cancellation, case_key, close_counters, ddb, dumps, from_ddb,
    now, run_pk, sqs, to_ddb,
)

MAX_PER_SWEEP = int(os.environ.get("SWEEP_MAX_MESSAGES", "50"))


def route_to_person(body, receive_count, t):
    """Fenced write: returns 'swept', 'already_moved_on' or 'contended'."""
    run_id, case_id, pass_no = body["run_id"], body["case_id"], int(body["pass"])
    rec = from_ddb(ddb.get_item(TableName=RUNS_TABLE, Key=to_ddb(case_key(run_id, case_id)),
                                ConsistentRead=True).get("Item"))
    if not rec or rec.get("status") in TERMINAL or int(rec.get("pass_no", 0)) != pass_no:
        return "already_moved_on"                # a duplicate or stale copy; the record is fine
    token = int(rec.get("fencing_token", 0))
    entry = {"p": pass_no, "o": "HUMAN_REVIEW", "r": "dead_lettered", "w": "sweeper", "f": token + 1,
             "c": round(t, 3), "t": round(t, 3), "deliveries": receive_count}
    try:
        # Closing the record and counting it toward its run's progress happen together.
        ddb.transact_write_items(TransactItems=[
            {"Update": {
                "TableName": RUNS_TABLE, "Key": to_ddb(case_key(run_id, case_id)),
                "UpdateExpression": ("SET #st = :hr, resolution = :hro, resolved_at = :t, fencing_token = :next, "
                                     "history = list_append(history, :h), swept = :true "
                                     "REMOVE lease_owner, lease_expires"),
                "ConditionExpression": "fencing_token = :tok AND pass_no = :p",
                "ExpressionAttributeNames": {"#st": "status"},
                "ExpressionAttributeValues": to_ddb({":hr": "human_review", ":hro": "HUMAN_REVIEW",
                                                     ":t": round(t, 3), ":next": token + 1, ":tok": token,
                                                     ":p": pass_no, ":h": [entry], ":true": True})}},
            close_counters(run_id, pass_no, "human_review", t)])
    except ddb.exceptions.TransactionCanceledException as e:
        if cancellation(e)[0] == "ConditionalCheckFailed":
            return "contended"                   # a worker touched it since the read; next sweep decides
        raise
    ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb({
        "pk": run_pk(run_id), "sk": f"SWEEP#{case_id}#{pass_no}", "case_id": case_id, "pass": pass_no,
        "swept_at": round(t, 3), "deliveries": receive_count, "poison": bool(body.get("poison")),
        "ttl": int(t + RUN_TTL_S)}))
    return "swept"


def sweep():
    depth = int(sqs.get_queue_attributes(QueueUrl=DLQ, AttributeNames=["ApproximateNumberOfMessages"])
                ["Attributes"]["ApproximateNumberOfMessages"])
    out = {"dlq_depth": depth, "swept": 0, "already_moved_on": 0, "contended": 0, "unreadable": 0, "errors": 0}
    seen = 0
    while depth and seen < MAX_PER_SWEEP:
        msgs = sqs.receive_message(QueueUrl=DLQ, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=60,
                                   AttributeNames=["ApproximateReceiveCount"]).get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            seen += 1
            try:
                body = json.loads(m["Body"])
                body["run_id"], body["case_id"], body["pass"]
            except (ValueError, KeyError, TypeError):
                out["unreadable"] += 1           # nothing to route; log it and drop it
                print(dumps({"sweeper": "unreadable", "body": m.get("Body", "")[:300]}))
                sqs.delete_message(QueueUrl=DLQ, ReceiptHandle=m["ReceiptHandle"])
                continue
            try:
                result = route_to_person(body, int(m.get("Attributes", {}).get("ApproximateReceiveCount", 0)), now())
            except Exception as e:               # leave it; it reappears for the next sweep
                out["errors"] += 1
                print(dumps({"sweeper": "error", "case_id": body.get("case_id"), "error": f"{type(e).__name__}: {e}"[:300]}))
                continue
            out[result] += 1
            if result != "contended":
                sqs.delete_message(QueueUrl=DLQ, ReceiptHandle=m["ReceiptHandle"])
            print(dumps({"sweeper": result, "case_id": body["case_id"], "pass": body["pass"], "run_id": body["run_id"]}))
    return out


def handler(event, context):
    try:
        out = sweep()
    except Exception as e:                       # never crash the schedule; the next tick retries
        out = {"error": f"{type(e).__name__}: {e}"[:300]}
    if out.get("dlq_depth") or out.get("error"):
        print(dumps({"sweep": out}))
    return out
