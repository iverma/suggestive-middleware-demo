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
- Crashes are an injected fault carried on the message itself:
  "crash": {"at": <kill point>, "mode": "exit" | "raise"}. The producer of each
  message (control for pass 1, the worker for a handoff or a repair) stamps it
  from a deterministic draw per (run, case, pass), so a re-sent copy carries the
  same instruction and the next pass gets its own draw; nothing is copied
  forward. Obeyed only on a message's first delivery, so each fires once.
  "exit" kills the execution environment (process death); "raise" throws out of
  the handler (a failing downstream call) and the environment survives. Either
  way the incoming message is not acknowledged. See common.KILL_POINTS.

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
    customer_rejects, ddb, from_ddb, message_body, now, offer_sequence, sqs, to_ddb,
)

WORK_MIN_S = float(os.environ.get("WORK_MIN_S", "0.6"))
WORK_MAX_S = float(os.environ.get("WORK_MAX_S", "1.8"))
OFFER_SHARE = 0.7          # of the simulated work: rules engine / inventory; the rest is the customer


class InjectedFault(Exception):
    """A planned crash in "raise" mode: escapes the handler like a failing downstream call."""
CONTROL_CACHE_S = 2.0
CONTROL_KEY = {"pk": "CONTROL", "sk": "ACTIVE"}
COUNTERS = ("fenced", "repaired", "stale", "deferred")

# One id per execution environment (= one concurrent worker). Survives across warm invokes.
_ENV = {"id": "env-" + uuid.uuid4().hex[:6], "started": now()}
_control = {"t": 0.0, "v": {}}


def env_id():
    return _ENV["id"]


def simulate_work(share=1.0):
    """Artificial lag: inventory search and pricing (offer), then the customer's answer."""
    time.sleep(random.uniform(WORK_MIN_S, WORK_MAX_S) * share)


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


def send_handoff(run_id, case_id, pass_no, kill_percent):
    """Next pass goes to the re-routing queue, with that pass's own crash draw (if any)."""
    sqs.send_message(QueueUrl=REROUTING_QUEUE, MessageBody=message_body(run_id, case_id, pass_no, kill_percent))


class Crash:
    """The crash instruction for one delivery, and the bookkeeping to fire it at most once."""

    def __init__(self, record, body):
        first = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1")) == 1
        plan = body.get("crash") if first else None
        if isinstance(plan, str):                      # older single-string form
            plan = {"at": plan, "mode": "exit"}
        self.at = plan["at"] if plan else None
        self.mode = plan.get("mode", "exit") if plan else None
        self.run_id, self.case_id, self.pass_no = body["run_id"], body["case_id"], int(body["pass"])
        self.queue = (record.get("eventSourceARN") or "").rsplit(":", 1)[-1]
        self.started_at, self.claimed_at = now(), None

    def at_point(self, point, fire_as=None):
        """Crash here if the plan says so. fire_as records where it actually happened, for
        plans whose point doesn't exist on this path (e.g. a handoff point on an accept)."""
        if self.at and self.at == point:
            self.fire(fire_as or point)

    def fire(self, actual):
        ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb({
            "pk": f"RUN#{self.run_id}", "sk": f"KILL#{self.case_id}#{self.pass_no}",
            "case_id": self.case_id, "pass": self.pass_no, "planned": self.at, "point": actual,
            "mode": self.mode, "queue": self.queue, "worker": env_id(),
            "started_at": round(self.started_at, 3),
            "claimed_at": round(self.claimed_at, 3) if self.claimed_at else None,
            "killed_at": round(now(), 3)}))
        print(json.dumps({"worker": env_id(), "event": "crash", "case_id": self.case_id,
                          "pass": self.pass_no, "point": actual, "mode": self.mode}))
        self.at = None
        if self.mode == "raise":
            raise InjectedFault(f"injected fault at {actual} ({self.case_id} pass {self.pass_no})")
        crash()


def process(record, reject_percent=None):
    """Handle one SQS message. Returns True when the message is done (Lambda deletes it)."""
    body = json.loads(record["body"])
    run_id, case_id, pass_no = body["run_id"], body["case_id"], int(body["pass"])
    crash = Crash(record, body)
    crash.at_point("on_receive")                                            # K1

    ctl = control()
    if ctl.get("run_id") != run_id:
        ctl = control(force=True)
    if ctl.get("run_id") != run_id or ctl.get("status") not in ("starting", "running"):
        return True                              # message from an old, finished or aborted run
    reject_percent = ctl.get("reject_percent", 50) if reject_percent is None else reject_percent
    kill_percent = ctl.get("kill_percent", 0)

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
        return handle_unclaimable(record, run_id, case_id, pass_no, from_ddb(e.response.get("Item")), t,
                                  kill_percent)

    crash.claimed_at = t
    old = from_ddb(resp["Attributes"])
    token = int(old["fencing_token"]) + 1
    takeover = old.get("status") == "in_progress"
    crash.at_point("after_claim")                                           # K2

    # --- decide (pass 1: rules engine; pass 2+: walk the rules ranking) -------
    simulate_work(OFFER_SHARE / 2)
    crash.at_point("during_offer")                                          # K3
    simulate_work(OFFER_SHARE / 2)
    offers = offer_sequence(old)
    offer = offers[pass_no - 1] if pass_no <= len(offers) else "HUMAN_REVIEW"
    if offer == "HUMAN_REVIEW":
        outcome = "human_review"
    else:
        # The offer goes out and the customer answers (downstream in real life).
        simulate_work((1 - OFFER_SHARE) / 2)
        crash.at_point("during_customer_response")                          # K4
        simulate_work((1 - OFFER_SHARE) / 2)
        outcome = "reject" if customer_rejects(run_id, case_id, pass_no, reject_percent) else "accept"
    # A human-review pass has no customer step; a plan for K4 fires at the same place as K5.
    crash.at_point("during_customer_response", fire_as="before_commit")
    crash.at_point("before_commit")                                         # K5

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

    if outcome == "reject":
        crash.at_point("after_commit_before_handoff")                       # K6
        send_handoff(run_id, case_id, pass_no + 1, kill_percent)
        crash.at_point("after_handoff_before_ack")                          # K7
        # A plan for K8 (final commit) on a rejection fires at the equivalent point here.
        crash.at_point("after_final_commit_before_ack", fire_as="after_handoff_before_ack")
    else:
        crash.at_point("after_final_commit_before_ack")                     # K8
        # Handoff plans on a final outcome (nothing to hand off) fire at the equivalent point.
        crash.at_point("after_commit_before_handoff", fire_as="after_final_commit_before_ack")
        crash.at_point("after_handoff_before_ack", fire_as="after_final_commit_before_ack")
    return True


def handle_unclaimable(record, run_id, case_id, pass_no, rec, t, kill_percent=0):
    if not rec or rec.get("run_id") != run_id or rec.get("status") in TERMINAL:
        bump(run_id, "stale")
        return True
    if int(rec.get("pass_no", 0)) > pass_no:
        if rec.get("status") == "queued":
            # The previous holder committed a rejection but may have died before sending
            # the next message. Re-send it; duplicates are harmless (claims are conditional).
            send_handoff(run_id, case_id, int(rec["pass_no"]), kill_percent)
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
