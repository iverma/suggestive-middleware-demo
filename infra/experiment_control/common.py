"""Shared config, AWS clients, DynamoDB helpers, pacing and the pass-1 rules engine."""
import csv
import hashlib
import json
import os
import time
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.config import Config

RECORDS_TABLE = os.environ["TABLE_NAME"]
RUNS_TABLE = os.environ.get("RUNS_TABLE", "")
TABLE_WCU = float(os.environ.get("TABLE_WCU", "10"))
DEFAULT_WCU_PER_SEC = float(os.environ.get("DEFAULT_WCU_PER_SEC", "8"))
REBOOKING_QUEUE = os.environ.get("REBOOKING_QUEUE_URL", "")
REROUTING_QUEUE = os.environ.get("REROUTING_QUEUE_URL", "")
DLQ = os.environ.get("DLQ_URL", "")
WORKER_FUNCTION = os.environ.get("WORKER_FUNCTION", "")
# Lease < queue visibility timeout: when SQS redelivers a dead worker's message, its lease is over.
LEASE_SECONDS = int(os.environ.get("LEASE_SECONDS", "15"))
VISIBILITY_SECONDS = int(os.environ.get("VISIBILITY_SECONDS", "20"))
# Event source mappings (queue -> worker): uuid and max concurrency, "rebooking" and "rerouting".
ESMS = {
    "rebooking": {"uuid": os.environ.get("REBOOKING_ESM_ID", ""),
                  "max": int(os.environ.get("REBOOKING_MAX_WORKERS", "5"))},
    "rerouting": {"uuid": os.environ.get("REROUTING_ESM_ID", ""),
                  "max": int(os.environ.get("REROUTING_MAX_WORKERS", "3"))},
}
CSV_PATH = os.environ.get("CSV_PATH", os.path.join(os.path.dirname(__file__), "golden_set_irops_msp.csv"))
STOP_AT = "acceptable_resolutions"
KEY = "case_id"
NUMERIC = {
    "original_stops", "party_size", "adults", "children",
    "next_same_route_seat_hrs", "split_party_first_seats_hrs", "alt_route_stops",
    "alt_route_hrs", "partner_airline_hrs", "alt_airport_hrs", "ground_transport_hrs",
}
TERMINAL = {"resolved", "human_review"}

# Throttling and transient errors are retried inside the SDK with exponential backoff and full
# jitter ("standard" mode: sleep random(0, 1) * 2^attempt, capped). Workers get 3 retries (worst
# case ~7s of sleeping) and short timeouts, so a retry storm can never outlive the 20s invoke
# while holding a 15s lease; past that, the message itself backs off exponentially (see
# worker.backoff_seconds). "adaptive" mode was tried and rejected: its client-side rate limiter
# outlives the burst in a warm worker and slows every later record unpredictably.
# The control function serves people and can afford to retry longer.
WORKER_CFG = Config(retries={"mode": "standard", "max_attempts": 3}, connect_timeout=2, read_timeout=5)
CONTROL_CFG = Config(retries={"mode": "standard", "max_attempts": 6}, connect_timeout=3, read_timeout=20)
_cfg = WORKER_CFG if os.environ.get("ROLE") == "worker" else CONTROL_CFG
ddb = boto3.client("dynamodb", config=_cfg)
sqs = boto3.client("sqs", config=_cfg)
lam = boto3.client("lambda", config=_cfg)

THROTTLE_CODES = {"ProvisionedThroughputExceededException", "ThrottlingException", "Throttling",
                  "RequestThrottled", "RequestLimitExceeded", "TooManyRequestsException",
                  "RequestThrottledException", "LimitExceededException"}


def is_throttle(exc):
    return getattr(exc, "response", {}).get("Error", {}).get("Code") in THROTTLE_CODES

_ser, _deser = TypeSerializer(), TypeDeserializer()


def now():
    return time.time()


def _dec(v):
    if isinstance(v, float):
        return Decimal(str(v))
    if isinstance(v, dict):
        return {k: _dec(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_dec(x) for x in v]
    return v


def to_ddb(d):
    return {k: _ser.serialize(_dec(v)) for k, v in d.items()}


def from_ddb(item):
    return {k: _deser.deserialize(v) for k, v in (item or {}).items()}


def plain(o):
    """Decimal-safe JSON conversion."""
    if isinstance(o, Decimal):
        return int(o) if o == o.to_integral_value() else float(o)
    if isinstance(o, dict):
        return {k: plain(v) for k, v in o.items()}
    if isinstance(o, (list, tuple, set)):
        return [plain(v) for v in o]
    return o


def dumps(o):
    return json.dumps(plain(o), default=str)


def scan_all(table, **kw):
    items, start = [], None
    while True:
        if start:
            kw["ExclusiveStartKey"] = start
        resp = ddb.scan(TableName=table, **kw)
        items += [from_ddb(i) for i in resp.get("Items", [])]
        start = resp.get("LastEvaluatedKey")
        if not start:
            return items


class WcuPacer:
    """Keeps cumulative consumed WCU at or below rate * elapsed seconds."""

    def __init__(self, rate, clock=time.monotonic, sleep=time.sleep):
        self.rate, self.clock, self.sleep = rate, clock, sleep
        self.start, self.used, self.slept = clock(), 0.0, 0.0

    def record(self, units):
        self.used += units
        ahead = self.used / self.rate - (self.clock() - self.start)
        if ahead > 0:
            self.sleep(ahead)
            self.slept += ahead

    def elapsed(self):
        return self.clock() - self.start


def pace_rate(requested=None):
    rate = min(float(requested or DEFAULT_WCU_PER_SEC), TABLE_WCU)
    if rate <= 0:
        raise ValueError("wcu_per_sec must be > 0")
    return rate


def consumed(resp):
    return resp.get("ConsumedCapacity", {}).get("CapacityUnits", 1.0)


# --- Golden set CSV ----------------------------------------------------------
def read_csv(path=CSV_PATH):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def input_rows(path=CSV_PATH):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        cols = header[: header.index(STOP_AT)]
        for raw in reader:
            yield dict(zip(cols, raw[: len(cols)]))


_golden = None


def golden():
    """case_id -> {acceptable: [...], unacceptable: set}. Used only for scoring."""
    global _golden
    if _golden is None:
        _golden = {
            r[KEY]: {
                "acceptable": r["acceptable_resolutions"].split("|"),
                "unacceptable": set(r["unacceptable_resolutions"].split("|")) - {"none"},
            }
            for r in read_csv()
        }
    return _golden


# --- Pass-1 rules engine -----------------------------------------------------
# Produces the ordered list of offers to make for a customer record, using only
# the record and its inventory snapshot. Passes 2+ currently walk this list; that
# is the seam where agent reasoning over customer history will plug in.
DEADLINE_HRS = {"critical": 12, "urgent": 24, "flexible": 72}
SENSITIVE = {"infant_in_arms", "pregnancy_third_trimester", "medical_condition",
             "wheelchair_assistance", "elderly_passenger", "service_animal"}
NO_GROUND = {"medical_condition", "pregnancy_third_trimester", "wheelchair_assistance", "unaccompanied_minor"}
NO_PARTNER = {"traveling_with_pet", "unaccompanied_minor"}


def _num(r, k):
    v = r.get(k)
    return None if v in (None, "") else float(v)


def offer_sequence(r):
    concerns = set() if r.get("travel_concerns", "none") == "none" else set(r["travel_concerns"].split(";"))
    urg = r["urgency"]
    deadline = DEADLINE_HRS[urg]
    sensitive = bool(concerns & SENSITIVE)
    options = []

    def consider(name, hrs, blocked=False):
        if hrs is not None and not blocked and hrs <= deadline:
            options.append((name, hrs))

    alt_stops = _num(r, "alt_route_stops")
    consider("REBOOK_SAME_ROUTE", _num(r, "next_same_route_seat_hrs"))
    consider("REBOOK_ALTERNATE_AIRPORT", _num(r, "alt_airport_hrs"))
    consider("REBOOK_PARTNER_AIRLINE", _num(r, "partner_airline_hrs"), bool(concerns & NO_PARTNER))
    consider("REBOOK_ALTERNATE_ROUTE", _num(r, "alt_route_hrs"),
             sensitive and alt_stops is not None and alt_stops >= 2)
    consider("ALTERNATE_GROUND_TRANSPORT", _num(r, "ground_transport_hrs"), bool(concerns & NO_GROUND))
    consider("SPLIT_PARTY_REBOOK", _num(r, "split_party_first_seats_hrs"),
             (_num(r, "children") or 0) > 0 or sensitive)

    options.sort(key=lambda x: x[1])
    same = [o for o in options if o[0] == "REBOOK_SAME_ROUTE"]
    if same and same[0][1] <= options[0][1] + 4:
        options.remove(same[0])
        options.insert(0, same[0])
    offers = [o[0] for o in options]

    if r["cabin"] != "economy" and r.get("premium_cabin_available") == "N" and urg != "flexible":
        offers.append("CABIN_DOWNGRADE_WITH_FARE_DIFF_REFUND")
    if urg == "flexible":
        offers.append("TRAVEL_CREDIT")
    offers.append("FULL_REFUND")

    mandatory_human = ("unaccompanied_minor" in concerns
                       or ("medical_condition" in concerns and urg == "critical")
                       or (not options and urg == "critical"))
    return ["HUMAN_REVIEW"] + offers if mandatory_human else offers + ["HUMAN_REVIEW"]


# --- Crash injection ---------------------------------------------------------
# Every place in the worker's path where a crash leaves the record in a different state.
# Ordered as the worker reaches them; the comment is the recovery each one should trigger.
KILL_POINTS = [
    "on_receive",                      # K1 nothing touched yet          -> plain redelivery
    "after_claim",                     # K2 leased, nothing decided      -> lease takeover
    "during_offer",                    # K3 rules engine running         -> lease takeover
    "during_customer_response",        # K4 offer out, awaiting customer -> lease takeover
    "before_commit",                   # K5 answer known, not written    -> lease takeover
    "after_commit_before_handoff",     # K6 pass+1 written, no message   -> handoff repair
    "after_handoff_before_ack",        # K7 next message sent, incoming not acked -> duplicate dropped
    "after_final_commit_before_ack",   # K8 final status written, not acked       -> finished duplicate dropped
]
KILL_MODES = ["exit", "raise"]         # process death vs. an exception escaping the handler


def _h(*parts):
    return int(hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:12], 16)


def crash_plan(run_id, case_id, pass_no, kill_percent):
    """
    Whether the message for (run, case, pass) carries a crash, and where. Deterministic, so
    every producer of that message (first enqueue, handoff, repair re-send) stamps the same
    instruction, and each pass gets its own independent draw: nothing is copied forward.
    """
    if not kill_percent or (_h("crash?", run_id, case_id, pass_no) % 10000) / 100.0 >= float(kill_percent):
        return None
    return {"at": KILL_POINTS[_h("crash@", run_id, case_id, pass_no) % len(KILL_POINTS)],
            "mode": KILL_MODES[_h("crash-mode", run_id, case_id, pass_no) % len(KILL_MODES)]}


# Race faults: slow workers and duplicate deliveries.
#   stall "zombie"  - the worker stalls past its lease while still alive, then tries to commit.
#                     Always sent with a duplicate, so another worker can take the record over
#                     meanwhile: the late write must be fenced off.
#   stall "timeout" - the worker stalls past the function timeout; Lambda kills it mid-record.
#   dup             - the message is enqueued twice (SQS standard queues deliver at least once).
STALLS = ["zombie", "timeout"]


def race_plan(run_id, case_id, pass_no, slow_percent=0, dup_percent=0):
    stall = None
    if slow_percent and (_h("stall?", run_id, case_id, pass_no) % 10000) / 100.0 < float(slow_percent):
        stall = STALLS[_h("stall-kind", run_id, case_id, pass_no) % len(STALLS)]
    dup = stall == "zombie" or bool(
        dup_percent and (_h("dup?", run_id, case_id, pass_no) % 10000) / 100.0 < float(dup_percent))
    return stall, dup


FAULT_KNOBS = ("kill_percent", "slow_percent", "dup_percent", "poison_percent", "throttle_percent")


def knobs(ctl):
    """The fault settings of a run, as stored on its control item."""
    return {k: ctl.get(k, 0) for k in FAULT_KNOBS}


def poison_plan(run_id, case_id, pass_no, poison_percent=0):
    """A poison message fails on every delivery (bad data, a bug): it can only end in the DLQ."""
    return bool(poison_percent) and (_h("poison?", run_id, case_id, pass_no) % 10000) / 100.0 < float(poison_percent)


def outgoing(run_id, case_id, pass_no, kn):
    """
    The message(s) to enqueue for (run, case, pass): one body, or two identical copies when a
    duplicate is planned. Every producer (first enqueue, handoff, repair) calls this, and every
    draw is deterministic per (run, case, pass), so all copies agree and nothing is inherited
    from the previous pass.
    """
    body = {"run_id": run_id, "case_id": case_id, "pass": pass_no}
    plan = crash_plan(run_id, case_id, pass_no, kn.get("kill_percent", 0))
    if plan:
        body["crash"] = plan
    stall, dup = race_plan(run_id, case_id, pass_no, kn.get("slow_percent", 0), kn.get("dup_percent", 0))
    if stall:
        body["stall"] = stall
    if dup:
        body["dup"] = True
    if poison_plan(run_id, case_id, pass_no, kn.get("poison_percent", 0)):
        body["poison"] = True
    text = json.dumps(body)
    return [text, text] if dup else [text]


def message_body(run_id, case_id, pass_no, kill_percent):
    """Single-copy body with only the crash draw (kept for callers and tests that use it)."""
    return outgoing(run_id, case_id, pass_no, {"kill_percent": kill_percent})[0]


def customer_rejects(run_id, case_id, pass_no, reject_percent):
    """Deterministic per (run, case, pass): a retry after a crash sees the same answer."""
    h = int(hashlib.sha256(f"{run_id}|{case_id}|{pass_no}".encode()).hexdigest()[:8], 16)
    return (h % 10000) / 100.0 < float(reject_percent)
