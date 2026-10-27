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

External side effects are at-least-once. Everything this worker writes to
DynamoDB is guarded (conditional claim, fenced commit), so no pass is ever
recorded twice. But anything sent outside the system (the offer to the
customer at K4, a booking, a voucher) happens before the commit, so a crash
between sending it and committing (K4/K5) means the worker that takes over
sends it again. Exactly-once delivery isn't possible; production makes those
calls idempotent instead: each carries a key that is stable across takeovers,
(case_id, pass, action), never the fencing token, and the receiving service
ignores a key it has already seen (an idempotency-key API, or a "sent" ledger
written with a conditional put). This demo assumes that is in place.

Errors: every message is handled inside a catch-all, so one bad message can't
take the others in a batch down with it. Throttling is retried in the SDK with
exponential backoff and jitter; if that runs out, the message itself backs off
exponentially (by its receive count) before it is retried. Anything else is
retried after the normal visibility timeout, and a message that keeps failing
ends in the dead-letter queue, where the sweeper routes its record to a person.

Return value uses ReportBatchItemFailures: a message listed there is NOT
deleted, so it is retried after its visibility timeout.
"""
import json
import os
import random
import time
import uuid

import throttle
from common import (
    ACTIVE, RUN_TTL_S, cancellation, case_key, close_counters, is_throttle, meta_key, run_pk,
    LEASE_SECONDS, REBOOKING_QUEUE, REROUTING_QUEUE, RUNS_TABLE, TERMINAL,
    VISIBILITY_SECONDS, customer_rejects, ddb, from_ddb, knobs, now, offer_sequence, outgoing, sqs, to_ddb,
)

WORK_MIN_S = float(os.environ.get("WORK_MIN_S", "0.6"))
WORK_MAX_S = float(os.environ.get("WORK_MAX_S", "1.8"))
OFFER_SHARE = 0.7          # of the simulated work: rules engine / inventory; the rest is the customer


# A zombie stops waiting for a takeover this long before its invoke would time out.
ZOMBIE_MARGIN_S = float(os.environ.get("ZOMBIE_MARGIN_S", "1.5"))
DEFAULT_REMAINING_S = 20.0


class InjectedFault(Exception):
    """A planned crash in "raise" mode: escapes the handler like a failing downstream call."""


class PoisonMessage(Exception):
    """A message that fails on every delivery (bad data, a bug): retrying never helps."""


class FunctionTimedOut(BaseException):
    """Reached only outside Lambda (tests): in Lambda the runtime is killed at the timeout first."""


CONTROL_CACHE_S = 2.0
COUNTERS = ("fenced", "repaired", "stale", "deferred")

MAX_BACKOFF_S = 300
throttle.install(ddb)                    # injected 400s only fire while a run asks for them
throttle.install(sqs)

# One id per execution environment (= one concurrent worker). Survives across warm invokes.
_ENV = {"id": "env-" + uuid.uuid4().hex[:6], "started": now()}
_control = {}                                     # run_id -> {"t": fetched at, "v": META}


def env_id():
    return _ENV["id"]


def simulate_work(share=1.0):
    """Artificial lag: inventory search and pricing (offer), then the customer's answer."""
    time.sleep(random.uniform(WORK_MIN_S, WORK_MAX_S) * share)


def crash():
    """Simulated process death. Tests replace this so they don't exit the interpreter."""
    os._exit(137)


def run_meta(run_id, force=False):
    """A run's settings and status, from its META item, cached per run for a couple of seconds."""
    c = _control.setdefault(run_id, {"t": 0.0, "v": {}})
    if force or now() - c["t"] > CONTROL_CACHE_S:
        c["v"] = from_ddb(ddb.get_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id))).get("Item"))
        c["t"] = now()
    if len(_control) > 50:                       # warm workers see many runs; keep the cache small
        for k in sorted(_control, key=lambda k: _control[k]["t"])[:25]:
            _control.pop(k, None)
    return c["v"]


def telemetry(fn):
    """Bookkeeping for the UI must never break the record path: log and carry on."""
    def wrapped(*a, **kw):
        try:
            return fn(*a, **kw)
        except Exception as e:
            print(json.dumps({"worker": env_id(), "event": "telemetry_failed", "what": fn.__name__,
                              "error": f"{type(e).__name__}: {e}"[:300]}))
    wrapped.__name__ = fn.__name__
    return wrapped


@telemetry
def bump(run_id, counter, n=1):
    bump_many(run_id, {counter: n})


@telemetry
def bump_many(run_id, counts):
    counts = {k: v for k, v in counts.items() if v}
    if not counts:
        return
    names = {f"#c{i}": k for i, k in enumerate(counts)}
    ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb({"pk": run_pk(run_id), "sk": "EVENTS"}),
                    UpdateExpression=("SET #ttl = if_not_exists(#ttl, :ttl) ADD "
                                      + ", ".join(f"#c{i} :v{i}" for i in range(len(counts)))),
                    ExpressionAttributeNames={**names, "#ttl": "ttl"},
                    ExpressionAttributeValues=to_ddb({**{f":v{i}": v for i, v in enumerate(counts.values())},
                                                      ":ttl": int(now() + RUN_TTL_S)}))


def queue_url(arn):
    name = (arn or "").rsplit(":", 1)[-1]
    return REROUTING_QUEUE if REROUTING_QUEUE.endswith("/" + name) else REBOOKING_QUEUE


def send_handoff(run_id, case_id, pass_no, kn, repair=False):
    """
    Next pass goes to the re-routing queue, carrying that pass's own fault draws. A planned
    duplicate is sent as two copies; a repair re-send is itself a duplicate, so it sends one.
    """
    bodies = outgoing(run_id, case_id, pass_no, kn)
    if repair:
        bodies = bodies[:1]
    for b in bodies:
        # Fair queues: grouping by run stops one run's backlog from starving the others.
        sqs.send_message(QueueUrl=REROUTING_QUEUE, MessageBody=b, MessageGroupId=run_id)
    if len(bodies) > 1:
        bump(run_id, "duplicates_sent")


class Crash:
    """The crash instruction for one delivery, and the bookkeeping to fire it at most once."""

    def __init__(self, record, body):
        first = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1")) == 1
        plan = body.get("crash") if first else None
        if isinstance(plan, str):                      # older single-string form
            plan = {"at": plan, "mode": "exit"}
        self.at = plan["at"] if plan else None
        self.mode = plan.get("mode", "exit") if plan else None
        self.stall = body.get("stall") if first else None
        self.run_id, self.case_id, self.pass_no = body["run_id"], body["case_id"], int(body["pass"])
        self.queue = (record.get("eventSourceARN") or "").rsplit(":", 1)[-1]
        self.started_at, self.claimed_at = now(), None

    def at_point(self, point, fire_as=None):
        """Crash here if the plan says so. fire_as records where it actually happened, for
        plans whose point doesn't exist on this path (e.g. a handoff point on an accept)."""
        if self.at and self.at == point:
            self.fire(fire_as or point)

    def fire(self, actual):
        self._record(actual)
        print(json.dumps({"worker": env_id(), "event": "crash", "case_id": self.case_id,
                          "pass": self.pass_no, "point": actual, "mode": self.mode}))
        self.at = None
        if self.mode == "raise":
            raise InjectedFault(f"injected fault at {actual} ({self.case_id} pass {self.pass_no})")
        crash()

    @telemetry
    def _record(self, actual):
        ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb({
            "pk": f"RUN#{self.run_id}", "sk": f"KILL#{self.case_id}#{self.pass_no}#{env_id()}", "ttl": int(now() + RUN_TTL_S),
            "case_id": self.case_id, "pass": self.pass_no, "planned": self.at, "point": actual,
            "mode": self.mode, "queue": self.queue, "worker": env_id(),
            "started_at": round(self.started_at, 3),
            "claimed_at": round(self.claimed_at, 3) if self.claimed_at else None,
            "killed_at": round(now(), 3)}))


def stall_past_timeout(crash, deadline):
    """Slow worker: hangs past the function timeout, so Lambda kills it mid-record."""
    crash.at, crash.mode = "stalled_past_timeout", "timeout"
    telemetry(ddb.put_item)(TableName=RUNS_TABLE, Item=to_ddb({
        "pk": f"RUN#{crash.run_id}", "sk": f"KILL#{crash.case_id}#{crash.pass_no}#{env_id()}", "ttl": int(now() + RUN_TTL_S),
        "case_id": crash.case_id, "pass": crash.pass_no, "planned": "stalled_past_timeout",
        "point": "stalled_past_timeout", "mode": "timeout", "queue": crash.queue, "worker": env_id(),
        "started_at": round(crash.started_at, 3), "claimed_at": round(crash.claimed_at, 3),
        "killed_at": round(now(), 3), "timeout_at": round(deadline, 3)}))   # killed_at = when it hung
    print(json.dumps({"worker": env_id(), "event": "stall_past_timeout", "case_id": crash.case_id,
                      "pass": crash.pass_no}))
    time.sleep(max(0.0, deadline - now()) + 1.0)
    raise FunctionTimedOut(f"{crash.case_id} pass {crash.pass_no} ran past the function timeout")


def stall_past_lease(run_id, case_id, token, claimed_at, deadline):
    """
    Zombie: still alive, but stalled until its lease is gone and, if possible, until another
    worker has taken the record over (a duplicate of this message is in flight for exactly
    that). Returns who took over, or None if nobody did before this invoke had to move on.
    """
    time.sleep(max(0.0, min(claimed_at + LEASE_SECONDS + 0.2, deadline) - now()))
    while now() < deadline:
        rec = from_ddb(ddb.get_item(TableName=RUNS_TABLE, Key=to_ddb(case_key(run_id, case_id)),
                                    ConsistentRead=True, ProjectionExpression="fencing_token, lease_owner")
                       .get("Item"))
        if int(rec.get("fencing_token", token)) != token:
            return rec.get("lease_owner") or "another worker"
        time.sleep(0.4)
    return None


@telemetry
def record_zombie(run_id, case_id, pass_no, claimed_at, woke_at, outcome, taken_over_by):
    ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb({
        "pk": f"RUN#{run_id}", "sk": f"ZOMBIE#{case_id}#{pass_no}#{env_id()}", "case_id": case_id, "ttl": int(now() + RUN_TTL_S),
        "pass": pass_no, "worker": env_id(), "claimed_at": round(claimed_at, 3), "woke_at": round(woke_at, 3),
        "outcome": outcome, "taken_over_by": taken_over_by}))
    print(json.dumps({"worker": env_id(), "event": "zombie", "case_id": case_id, "pass": pass_no,
                      "outcome": outcome, "taken_over_by": taken_over_by}))


def process(record, reject_percent=None, context=None):
    """Handle one SQS message. Returns True when the message is done (Lambda deletes it)."""
    body = json.loads(record["body"])
    run_id, case_id, pass_no = body["run_id"], body["case_id"], int(body["pass"])
    crash = Crash(record, body)
    remaining = context.get_remaining_time_in_millis() / 1000 if context else DEFAULT_REMAINING_S
    deadline = now() + remaining                                            # when Lambda kills this invoke
    crash.at_point("on_receive")                                            # K1
    if body.get("poison"):
        raise PoisonMessage(f"{case_id} pass {pass_no}: unprocessable message")

    ctl = run_meta(run_id)
    if ctl.get("status") not in ACTIVE:
        ctl = run_meta(run_id, force=True)       # a brand-new run's META may not be cached yet
    if ctl.get("status") not in ACTIVE:
        return True                              # message from a finished, cancelled or unknown run
    reject_percent = ctl.get("reject_percent", 50) if reject_percent is None else reject_percent
    kn = knobs(ctl)
    throttle.configure(kn.get("throttle_percent", 0))                       # this run's injected 400s

    t = now()
    try:
        resp = ddb.update_item(
            TableName=RUNS_TABLE,
            Key=to_ddb(case_key(run_id, case_id)),
            UpdateExpression=("SET fencing_token = fencing_token + :one, lease_owner = :w, "
                              "lease_expires = :exp, #st = :inprog, claimed_at = :t ADD claims :one"),
            ConditionExpression=("attribute_exists(pk) AND pass_no = :p AND "
                                 "(#st = :queued OR (#st = :inprog AND lease_expires < :t))"),
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=to_ddb({
                ":one": 1, ":w": env_id(), ":exp": round(t + LEASE_SECONDS, 3), ":inprog": "in_progress",
                ":queued": "queued", ":t": round(t, 3), ":p": pass_no}),
            ReturnValues="ALL_OLD",
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
    except ddb.exceptions.ConditionalCheckFailedException as e:
        current = from_ddb(e.response.get("Item"))
        if (current.get("status") == "in_progress" and current.get("lease_owner") == env_id()
                and int(current.get("pass_no", 0)) == pass_no
                and abs(float(current.get("claimed_at", 0)) - round(t, 3)) < 0.01):
            # Our own claim: the first attempt landed but its response was lost, and the SDK's
            # retry then failed the condition we had just satisfied. Carry on as the owner.
            resp = {"Attributes": e.response["Item"], "own_retry": True}
        else:
            return handle_unclaimable(record, run_id, case_id, pass_no, current, t, kn)

    crash.claimed_at = t
    old = from_ddb(resp["Attributes"])
    if resp.get("own_retry"):                   # we only have the state after our claim
        token = int(old["fencing_token"])
        takeover = False
    else:
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
        # The offer goes out and the customer answers (downstream in real life). Sending it is a
        # side effect outside DynamoDB, so it is at-least-once: a takeover after K4/K5 sends it
        # again. Production sends it with an idempotency key of (case_id, pass, "offer"), which
        # stays the same across takeovers, and the messaging service drops a repeated key.
        simulate_work((1 - OFFER_SHARE) / 2)
        crash.at_point("during_customer_response")                          # K4
        simulate_work((1 - OFFER_SHARE) / 2)
        outcome = "reject" if customer_rejects(run_id, case_id, pass_no, reject_percent) else "accept"
    # A human-review pass has no customer step; a plan for K4 fires at the same place as K5.
    crash.at_point("during_customer_response", fire_as="before_commit")
    crash.at_point("before_commit")                                         # K5

    # Planned slowness, right before the commit (a slow downstream call or a long pause).
    zombie_rival = None
    if takeover:
        crash.stall = None          # a worker rescuing a record never plays the slow one itself
    if crash.stall == "timeout":
        stall_past_timeout(crash, deadline)
    elif crash.stall == "zombie":
        zombie_rival = stall_past_lease(run_id, case_id, token, t, deadline - ZOMBIE_MARGIN_S)

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
    record_update = {"TableName": RUNS_TABLE, "Key": to_ddb(case_key(run_id, case_id)),
                     "UpdateExpression": update,
                     "ConditionExpression": "fencing_token = :tok AND lease_owner = :w",
                     "ExpressionAttributeNames": {"#st": "status"},
                     "ExpressionAttributeValues": to_ddb(values),
                     "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}
    current = None
    try:
        if outcome == "reject":
            ddb.update_item(**record_update)
        else:
            # Closing the record and counting it toward the run's progress happen together.
            ddb.transact_write_items(TransactItems=[
                {"Update": record_update},
                close_counters(run_id, pass_no, values[":final"], now())])
    except ddb.exceptions.ConditionalCheckFailedException as e:
        current = from_ddb(e.response.get("Item"))
    except ddb.exceptions.TransactionCanceledException as e:
        code, current = cancellation(e)
        if code != "ConditionalCheckFailed":
            raise                                # a conflict or throttle: the catch-all retries it
    if current is not None:
        mine = any(int(h.get("f", -1)) == token and h.get("w") == env_id() and int(h.get("p", 0)) == pass_no
                   for h in current.get("history", []))
        if not mine:
            # Someone took the record over after our lease lapsed; our result is discarded.
            # Keep the message (don't let Lambda delete it): the new owner holds the live delivery.
            bump(run_id, "fenced")
            if crash.stall == "zombie":
                record_zombie(run_id, case_id, pass_no, t, now(), "fenced", zombie_rival)
            return False
        # Our own commit: it landed, but the response was lost and the SDK's retry failed the
        # condition the first attempt had already consumed. The pass is saved (and, for a close,
        # counted: the transaction is all-or-nothing); carry on.
        bump(run_id, "own_write_recognized")
    if crash.stall == "zombie":
        # Nobody took over while it stalled: a late but uncontested commit is legitimate.
        record_zombie(run_id, case_id, pass_no, t, now(), "uncontested", None)

    if outcome == "reject":
        crash.at_point("after_commit_before_handoff")                       # K6
        send_handoff(run_id, case_id, pass_no + 1, kn)
        crash.at_point("after_handoff_before_ack")                          # K7
        # A plan for K8 (final commit) on a rejection fires at the equivalent point here.
        crash.at_point("after_final_commit_before_ack", fire_as="after_handoff_before_ack")
    else:
        crash.at_point("after_final_commit_before_ack")                     # K8
        # Handoff plans on a final outcome (nothing to hand off) fire at the equivalent point.
        crash.at_point("after_commit_before_handoff", fire_as="after_final_commit_before_ack")
        crash.at_point("after_handoff_before_ack", fire_as="after_final_commit_before_ack")
    return True


def handle_unclaimable(record, run_id, case_id, pass_no, rec, t, kn=None):
    if not rec or rec.get("run_id") != run_id or rec.get("status") in TERMINAL:
        bump(run_id, "stale")
        return True
    if int(rec.get("pass_no", 0)) > pass_no:
        waiting = t - float(rec.get("queued_at", t))
        if rec.get("status") == "queued" and waiting >= LEASE_SECONDS:
            # Queued for the next pass for a while with nobody picking it up: the previous
            # holder committed a rejection but died before sending the next message. Re-send
            # it; a duplicate is harmless (claims are conditional).
            send_handoff(run_id, case_id, int(rec["pass_no"]), kn or {}, repair=True)
            bump(run_id, "repaired")
        else:
            # Just handed off (this is a duplicate copy of finished work), or already picked up.
            bump(run_id, "stale")
        return True
    if rec.get("status") == "in_progress":
        # Lease still live. Don't drop the message: if the holder is dead, this delivery is
        # how the record gets recovered. Hide it until the lease runs out, then retry.
        wait = max(1, int(float(rec.get("lease_expires", t)) - t) + 1)
        wait = min(wait, VISIBILITY_SECONDS)
        sqs.change_message_visibility(QueueUrl=queue_url(record.get("eventSourceARN")),
                                      ReceiptHandle=record["receiptHandle"], VisibilityTimeout=min(wait, 60))
        bump(run_id, "deferred")
        return False
    bump(run_id, "stale")
    return True


def receive_count(record):
    try:
        return int(record.get("attributes", {}).get("ApproximateReceiveCount", "1"))
    except (TypeError, ValueError):
        return 1


def backoff_seconds(n):
    """Exponential backoff with jitter for a message whose SDK retries ran out: 20s, 40s, 80s..."""
    return int(min(MAX_BACKOFF_S, VISIBILITY_SECONDS * 2 ** (max(1, n) - 1)) * random.uniform(0.8, 1.2))


def on_error(record, exc):
    """Log, count, and decide when the failed message should be retried."""
    kind = ("throttled" if is_throttle(exc) else "poison" if isinstance(exc, PoisonMessage)
            else "injected" if isinstance(exc, InjectedFault) else "error")
    try:
        body = json.loads(record.get("body") or "{}")
    except ValueError:
        body = {}
    n = receive_count(record)
    delay = backoff_seconds(n) if kind == "throttled" else None
    print(json.dumps({"worker": env_id(), "event": "message_failed", "kind": kind, "case_id": body.get("case_id"),
                      "pass": body.get("pass"), "receive_count": n, "backoff_s": delay,
                      "error": f"{type(exc).__name__}: {exc}"[:300]}))
    if delay:
        try:
            sqs.change_message_visibility(QueueUrl=queue_url(record.get("eventSourceARN")),
                                          ReceiptHandle=record["receiptHandle"], VisibilityTimeout=delay)
        except Exception as e:                   # message just comes back after its normal timeout
            print(json.dumps({"worker": env_id(), "event": "backoff_failed", "error": str(e)[:200]}))
    if body.get("run_id"):
        throttle.pause()                         # bookkeeping itself isn't throttled on purpose
        bump_many(body["run_id"], {f"failed_{kind}": 1, "backoffs": 1 if delay else 0})


def handler(event, context):
    failures = []
    for record in event.get("Records", []):
        run_id = None
        try:
            run_id = json.loads(record["body"]).get("run_id")
            done = process(record, context=context)
        except Exception as e:                   # catch-all: one message never fails the batch
            on_error(record, e)
            done = False
        finally:
            throttle.pause()
            c = throttle.counters()
            throttle.configure(0)
            if run_id and (c["throttles_injected"] or c["sdk_retries"]):
                bump_many(run_id, c)
        if not done:
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
