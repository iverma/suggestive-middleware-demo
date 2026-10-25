"""
Re-booking worker. Several run in parallel during an experiment run.

Loop: take a message from the re-routing queue (rejected offers, pass 2+) or
the rebooking queue (pass 1), claim the customer record, make the next offer,
simulate the customer's answer, and commit the outcome.

Failure handling
- Claim = conditional UpdateItem that increments fencing_token and takes a
  lease (lease length == SQS visibility timeout). Only one worker can hold a
  record; a record whose holder died becomes claimable when the lease expires,
  which is exactly when SQS redelivers its message.
- Commit is conditional on fencing_token and lease_owner still being ours. A
  worker that was presumed dead and wakes up late (a zombie) is fenced off.
- On rejection the record is committed as queued for pass+1 BEFORE the next
  message is sent, and the current message is deleted last. If the worker dies
  between those steps, the old message comes back, and whoever gets it sees the
  record is already queued for a later pass and re-sends the handoff (repair).
- Rejection decisions are seeded by (run, case, pass), so a retried pass makes
  the same decision as the attempt that crashed.
- Killed workers are an injected fault: the worker records that it was killed,
  then hard-exits the process mid-record, holding its lease and its message.
"""
import json
import os
import random
import time

from common import (
    LEASE_SECONDS, RECORDS_TABLE, REBOOKING_QUEUE, REROUTING_QUEUE, RUNS_TABLE, TERMINAL,
    WORKER_FUNCTION, customer_rejects, ddb, from_ddb, lam, now, offer_sequence, sqs, to_ddb,
)

WORK_MIN_S = float(os.environ.get("WORK_MIN_S", "0.6"))
WORK_MAX_S = float(os.environ.get("WORK_MAX_S", "1.8"))
IDLE_EXIT_S = float(os.environ.get("IDLE_EXIT_S", str(LEASE_SECONDS + 15)))
CONTROL_CHECK_S = 3.0
HEARTBEAT_S = 4.0
CCF = "ConditionalCheckFailedException"


def crash():
    """Simulated process death. Tests replace this so they don't exit the interpreter."""
    os._exit(137)


class Worker:
    def __init__(self, run_id, worker_id, reject_percent, kill=None, deadline=None):
        self.run_id, self.id = run_id, worker_id
        self.reject_percent = float(reject_percent)
        self.kill = kill or {}
        self.deadline = deadline or now() + 840
        self.stats = {k: 0 for k in ("claims", "completed", "accepted", "rejected", "human_review",
                                     "takeovers", "fenced", "stale", "deferred", "repaired")}
        self._last_control = 0.0
        self._active = True
        self._last_beat = 0.0

    # --- bookkeeping in the runs table ---------------------------------------
    def save(self, status, force=False):
        t = now()
        if not force and t - self._last_beat < HEARTBEAT_S:
            return
        self._last_beat = t
        item = {"pk": f"RUN#{self.run_id}", "sk": f"WORKER#{self.id}", "worker_id": self.id,
                "status": status, "heartbeat_at": round(t, 3), **self.stats}
        if self.kill:
            item["kill_plan"] = f"after claim #{self.kill['after_claims']} ({self.kill['point']})"
        ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb(item))

    def run_still_active(self):
        t = now()
        if t - self._last_control >= CONTROL_CHECK_S:
            self._last_control = t
            ctl = from_ddb(ddb.get_item(TableName=RUNS_TABLE,
                                        Key=to_ddb({"pk": "CONTROL", "sk": "ACTIVE"})).get("Item"))
            self._active = ctl.get("run_id") == self.run_id and ctl.get("status") != "aborted"
        return self._active

    def maybe_die(self, point):
        if self.kill and self.kill["point"] == point and self.stats["claims"] >= self.kill["after_claims"]:
            self.save("killed", force=True)
            print(json.dumps({"worker": self.id, "event": "killed", "point": point, **self.stats}))
            crash()

    # --- queues --------------------------------------------------------------
    def receive(self):
        for q, wait in ((REROUTING_QUEUE, 1), (REBOOKING_QUEUE, 2)):
            resp = sqs.receive_message(QueueUrl=q, MaxNumberOfMessages=1, WaitTimeSeconds=wait)
            for m in resp.get("Messages", []):
                return q, m
        return None, None

    def drained(self):
        total = 0
        for q in (REROUTING_QUEUE, REBOOKING_QUEUE):
            a = sqs.get_queue_attributes(QueueUrl=q, AttributeNames=[
                "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed"])["Attributes"]
            total += sum(int(v) for v in a.values())
        return total == 0

    def send_handoff(self, case_id, pass_no):
        sqs.send_message(QueueUrl=REROUTING_QUEUE, MessageBody=json.dumps(
            {"run_id": self.run_id, "case_id": case_id, "pass": pass_no}))

    # --- one message ---------------------------------------------------------
    def process(self, q, msg):
        body = json.loads(msg["Body"])
        receipt = msg["ReceiptHandle"]
        if body["run_id"] != self.run_id:
            # Message from an older run: drop it. One from a newer run: give it back.
            if self.run_still_active():
                sqs.delete_message(QueueUrl=q, ReceiptHandle=receipt)
                self.stats["stale"] += 1
            else:
                sqs.change_message_visibility(QueueUrl=q, ReceiptHandle=receipt, VisibilityTimeout=0)
                self._active = False
            return

        case_id, pass_no = body["case_id"], int(body["pass"])
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
                    ":one": 1, ":w": self.id, ":exp": round(t + LEASE_SECONDS, 3), ":inprog": "in_progress",
                    ":queued": "queued", ":t": round(t, 3), ":run": self.run_id, ":p": pass_no}),
                ReturnValues="ALL_OLD",
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
        except ddb.exceptions.ConditionalCheckFailedException as e:
            self.handle_unclaimable(q, receipt, case_id, pass_no, from_ddb(e.response.get("Item")), t)
            return

        old = from_ddb(resp["Attributes"])
        token = int(old["fencing_token"]) + 1
        takeover = old.get("status") == "in_progress"
        self.stats["claims"] += 1
        if takeover:
            self.stats["takeovers"] += 1
        self.maybe_die("after_claim")

        # --- decide (pass 1: rules engine; pass 2+: walk the rules ranking) ---
        time.sleep(random.uniform(WORK_MIN_S, WORK_MAX_S))   # simulated inventory search / pricing
        offers = offer_sequence(old)
        offer = offers[pass_no - 1] if pass_no <= len(offers) else "HUMAN_REVIEW"
        if offer == "HUMAN_REVIEW":
            outcome = "human_review"
        elif customer_rejects(self.run_id, case_id, pass_no, self.reject_percent):
            outcome = "reject"
        else:
            outcome = "accept"

        entry = {"p": pass_no, "o": offer, "r": outcome, "w": self.id, "f": token, "t": round(now(), 3)}
        if takeover:
            entry["from"] = old.get("lease_owner")
        values = {":tok": token, ":w": self.id, ":h": [entry], ":t": round(now(), 3), ":offer": offer}
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
            # Someone took the record over after our lease lapsed. Our result is discarded,
            # and we leave the message alone: the new owner holds the live delivery.
            self.stats["fenced"] += 1
            return

        self.stats["completed"] += 1
        self.stats["accepted" if outcome == "accept" else "rejected" if outcome == "reject" else "human_review"] += 1
        if outcome == "reject":
            self.maybe_die("after_commit")          # dies with the handoff not yet sent
            self.send_handoff(case_id, pass_no + 1)
        sqs.delete_message(QueueUrl=q, ReceiptHandle=receipt)

    def handle_unclaimable(self, q, receipt, case_id, pass_no, rec, t):
        if not rec or rec.get("run_id") != self.run_id or rec.get("status") in TERMINAL:
            sqs.delete_message(QueueUrl=q, ReceiptHandle=receipt)
            self.stats["stale"] += 1
        elif int(rec.get("pass_no", 0)) > pass_no:
            if rec.get("status") == "queued":
                # The previous holder committed a rejection but may have died before sending
                # the next message. Re-send it; duplicates are harmless (claims are conditional).
                self.send_handoff(case_id, int(rec["pass_no"]))
                self.stats["repaired"] += 1
            else:
                self.stats["stale"] += 1
            sqs.delete_message(QueueUrl=q, ReceiptHandle=receipt)
        elif rec.get("status") == "in_progress":
            # Lease still live (clock edge vs. SQS redelivery). Don't drop the message: if the
            # holder is dead, this delivery is how the record gets recovered.
            wait = max(1, int(float(rec.get("lease_expires", t)) - t) + 1)
            sqs.change_message_visibility(QueueUrl=q, ReceiptHandle=receipt, VisibilityTimeout=min(wait, 60))
            self.stats["deferred"] += 1
        else:
            sqs.delete_message(QueueUrl=q, ReceiptHandle=receipt)
            self.stats["stale"] += 1

    # --- main loop -----------------------------------------------------------
    def run(self):
        self.save("running", force=True)
        idle_since = None
        reason = "deadline"
        while now() < self.deadline:
            if not self.run_still_active():
                reason = "run_inactive"
                break
            q, msg = self.receive()
            if msg is None:
                idle_since = idle_since or now()
                if now() - idle_since >= IDLE_EXIT_S and self.drained():
                    reason = "drained"
                    break
                self.save("running")
                continue
            idle_since = None
            self.process(q, msg)
            self.save("running")
        successor = None
        if reason == "deadline" and self.run_still_active() and not self.drained():
            successor = self.hand_over()
        self.save("finished", force=True)
        print(json.dumps({"worker": self.id, "event": "exit", "reason": reason, "successor": successor,
                          **self.stats}))
        return {"reason": reason, "successor": successor, **self.stats}

    def hand_over(self):
        """Out of Lambda time with work left: start a fresh worker so the run keeps going."""
        base, _, gen = self.id.partition("-r")
        successor = f"{base}-r{int(gen or 1) + 1}"
        lam.invoke(FunctionName=WORKER_FUNCTION, InvocationType="Event", Payload=json.dumps(
            {"run_id": self.run_id, "worker_id": successor, "reject_percent": self.reject_percent}).encode())
        return successor


def handler(event, context):
    remaining = context.get_remaining_time_in_millis() / 1000 if context else 840
    w = Worker(event["run_id"], event["worker_id"], event["reject_percent"],
               kill=event.get("kill"), deadline=now() + remaining - 45)
    return w.run()
