"""
Experiment control Lambda for the IROPS re-booking demo.

Served publicly through a Lambda Function URL:
  GET  /                     UI
  GET  /results[?run=<id>]   one run in detail (latest if not given), active runs, recent runs
  POST /run                  start a run: ?kind=chaos|integration&reject_percent=50&kill_percent=0
                             &slow_percent=0&dup_percent=0&poison_percent=0&throttle_percent=0
  POST /cancel?run=<id>      cancel one run; every other run carries on
  POST /load                 paced load of the golden-set inputs into customer-records-db
  POST /reset?confirm=everything   cancel every run, empty the queues, delete all run data

Concurrency. Several runs can be active at once (people, tabs, experiments, integration
checks, later load tests). Each run works on its own copy of the records, in its own
partition of experiment-runs, so runs never share a row. Admission control keeps the total
inside one budget: MAX_ACTIVE_RUNS runs and RECORD_BUDGET records in flight.

Workers are the worker Lambda, invoked by SQS event source mappings that Lambda autoscales
from queue depth; the mappings are shared by all runs. They are on while any run is active
and off otherwise (an enabled mapping long-polls around the clock, which costs SQS requests
while idle). Every change of the active set bumps an epoch, and reconcile_mappings re-checks
it after switching off, so a run finishing and another starting can't race the mappings into
the wrong state.

Also invoked by CloudFormation on deploy (loads the golden set), by itself asynchronously
(second half of /run), and by EventBridge Scheduler every 5 minutes (housekeeping).
Mutating routes are POST-only so link previews and crawlers can't trigger them.
"""
import json
import os
import random
import threading
import time
import zlib
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

import agent
import generator
from analysis import (
    EVENT_COUNTERS, attribute_kills, incidents, kills_by_point, normalize_kill, race_results, score,
    worker_activity,
)
from common import (
    LEGACY_RUN_FIELDS, input_columns,
    ACTIVE, ADMISSION_KEY, DLQ, ESMS, FINISHED, INDEX_PK, KEY, LEASE_SECONDS, RECORD_BUDGET,
    MAX_ACTIVE_RUNS, NUMERIC, RECORDS_TABLE, REBOOKING_QUEUE, REROUTING_QUEUE, RUN_KINDS, RUNS_TABLE, TERMINAL,
    VISIBILITY_SECONDS, WcuPacer, cancellation, case_key, plain_body, transact, consumed, ddb, dumps, from_ddb, golden,
    input_rows, knobs, lam, meta_key, now, offer_sequence, outgoing, pace_rate, run_pk, scan_all, sqs, to_ddb, ttl_for,
)

MAX_KILL_PERCENT = 50     # every message crashing once doubles a run's work; keep runs sane
MAX_SLOW_PERCENT = 20     # each slow pass holds a worker for ~15-20s
MAX_DUP_PERCENT = 100
MAX_POISON_PERCENT = 10   # each poisoned record waits out 5 failed deliveries, then the sweeper
MAX_THROTTLE_PERCENT = 30 # share of worker AWS calls answered with a 400 throttle
START_TIMEOUT_S = 120     # a run still "starting" after this never finished its start: fail it
SETUP_CLAIM_S = 60        # a set-up claim older than this belongs to a dead invocation
RUN_MAX_S = 30 * 60       # a run still active after this has stalled: fail it, free its budget
STALL_S = LEASE_SECONDS + VISIBILITY_SECONDS + 30
# Orphans: open records whose message is gone (every recovery path for them failed). When the
# shared queues are completely empty, nothing can still deliver them, so after ORPHAN_S re-send
# their message. With a backlog in the queues (a load test) only records waiting longer than
# ORPHAN_BACKLOG_S qualify, so a busy queue doesn't trigger mass re-sends. Duplicates are harmless.
ORPHAN_S = 90
ORPHAN_BACKLOG_S = 20 * 60
GOLDEN_READ_PER_SEC = 4.0 # read units/s spent copying the golden set (its table has 5)
RESET_SETTLE_S = float(os.environ.get("RESET_SETTLE_S", "4"))
ESM_RETRY_S = float(os.environ.get("ESM_RETRY_S", "45"))
UI_PATH = os.path.join(os.path.dirname(__file__), "ui.html")

# Load tests: generated unique records released at a rate that keeps experiment-runs under its
# capacity alarm, never letting the rebooking backlog pass a cap, and handing the producer off
# to a fresh invocation every LOAD_CHUNK_S seconds (a 1,000-record release outlasts one Lambda).
LOAD_MAX_RECORDS = int(os.environ.get("LOAD_MAX_RECORDS", "1000"))
LOAD_RECORDS_PER_SEC = float(os.environ.get("LOAD_RECORDS_PER_SEC", "2.0"))
LOAD_BACKLOG_CAP = int(os.environ.get("LOAD_BACKLOG_CAP", "500"))
LOAD_CHUNK_S = int(os.environ.get("LOAD_CHUNK_S", "540"))
LOAD_BLOCK = 10                 # records created and enqueued per step (one SendMessageBatch)
# Customers with a preference take ~2.6 passes and 10-13 WCU per record (measured live), and the
# rules and agent arms run side by side: 0.7 records/s each keeps the pair under 16 WCU/s (80%).
PREFERENCE_RECORDS_PER_SEC = 0.7
PRODUCER_STALE_S = 120          # no producer heartbeat for this long: housekeeping restarts it
FINALIZE_READ_PER_SEC = 8.0     # read units/s spent scoring a finished load run
SNAPSHOT_READ_PER_SEC = 4.0     # read units/s spent refreshing a running load run's view
FINALIZE_RETRY_S = 180          # a finished load run still unscored after this: score it again
SNAPSHOT_S = 60                 # a load run's full view is rebuilt this often while it runs
SNAPSHOT_RECORDS = 200          # records shown in a load run's view (the most notable ones)
WATCH_TICK_S = 5


class HttpError(Exception):
    def __init__(self, status, msg):
        super().__init__(msg)
        self.status, self.msg = status, msg


# --- event source mappings ---------------------------------------------------
def set_esms(enabled):
    """Switch both queue->worker mappings on or off. Retries while a previous change settles."""
    out = {}
    for name, esm in ESMS.items():
        if not esm["uuid"]:
            continue
        end = now() + ESM_RETRY_S
        while True:
            try:
                out[name] = lam.update_event_source_mapping(UUID=esm["uuid"], Enabled=enabled)["State"]
                break
            except lam.exceptions.ResourceInUseException:
                if now() > end:
                    out[name] = "busy"
                    break
                time.sleep(2)
    return out


def esm_states():
    out = {}
    for name, esm in ESMS.items():
        state = "unknown"
        if esm["uuid"]:
            try:
                state = lam.get_event_source_mapping(UUID=esm["uuid"])["State"]
            except Exception as e:                   # don't fail /results over a status read
                state = f"unknown ({type(e).__name__})"
        out[name] = {"state": state, "max_workers": esm["max"]}
    return out


def admission():
    item = from_ddb(ddb.get_item(TableName=RUNS_TABLE, Key=to_ddb(ADMISSION_KEY), ConsistentRead=True).get("Item"))
    return {"active": set(item.get("active", set())), "records": int(item.get("active_records", 0)),
            "epoch": int(item.get("epoch", 0))}


def reconcile_mappings():
    """
    Workers on if any run is active, off if none. Switching off re-reads the admission epoch
    afterwards: if a run was admitted meanwhile (and switched them on just before our 'off'
    landed), switch them back on. Safe to call from anywhere, any number of times.
    """
    adm = admission()
    want = bool(adm["active"])
    states = {k: v["state"] for k, v in esm_states().items()}
    on = {"Enabled", "Enabling", "Updating"}
    if want and not all(s in on for s in states.values()):
        return {"wanted": "on", "set": set_esms(True)}
    if not want and any(s not in ("Disabled", "Disabling") for s in states.values()):
        result = set_esms(False)
        again = admission()
        if again["active"] or again["epoch"] != adm["epoch"]:
            return {"wanted": "on (raced)", "set": set_esms(True)}
        return {"wanted": "off", "set": result}
    return {"wanted": "on" if want else "off", "set": None}


def safe_reconcile():
    try:
        return reconcile_mappings()
    except Exception as e:                          # housekeeping and the next caller retry
        print(dumps({"reconcile": "failed", "error": f"{type(e).__name__}: {e}"[:300]}))
        return {"error": type(e).__name__}


# --- golden set: load into customer-records-db --------------------------------
def build_load_update(row):
    names, values, sets, removes = {}, {}, [], []
    for i, (col, val) in enumerate(row.items()):
        if col == KEY:
            continue
        names[f"#c{i}"] = col
        if val == "":
            removes.append(f"#c{i}")
        else:
            values[f":v{i}"] = {"N": str(Decimal(val))} if col in NUMERIC else {"S": val}
            sets.append(f"#c{i} = :v{i}")
    for j, f in enumerate(LEGACY_RUN_FIELDS):      # runs now work on their own copies
        names[f"#l{j}"] = f
        removes.append(f"#l{j}")
    expr = "SET " + ", ".join(sets) + (" REMOVE " + ", ".join(removes) if removes else "")
    return {"TableName": RECORDS_TABLE, "Key": {KEY: {"S": row[KEY]}}, "UpdateExpression": expr,
            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values,
            "ReturnConsumedCapacity": "TOTAL"}


def load(wcu_per_sec=None, if_missing=False):
    """Upserts the customer records from the CSV. Safe while runs are active: runs work on copies."""
    if if_missing:
        _golden_count.clear()
        have, want = golden_count(), sum(1 for _ in input_rows())
        if have >= want:                        # every Run button calls /load first: usually a no-op
            return {"loaded": 0, "already_loaded": have, "table": RECORDS_TABLE}
    rate = pace_rate(wcu_per_sec)
    pacer = WcuPacer(rate)
    n = 0
    for row in input_rows():
        pacer.record(consumed(ddb.update_item(**build_load_update(row))))
        n += 1
    secs = pacer.elapsed()
    _golden_count.clear()
    result = {"loaded": n, "wcu_consumed": round(pacer.used, 2), "wcu_per_sec_budget": rate,
              "wcu_per_sec_actual": round(pacer.used / secs, 2) if secs else None,
              "seconds": round(secs, 1), "table": RECORDS_TABLE}
    print(dumps(result))
    return result


def read_golden():
    """The customer records, read from customer-records-db at a paced read rate."""
    items, start, t0, used = [], None, time.monotonic(), 0.0
    while True:
        kw = {"TableName": RECORDS_TABLE, "Limit": 25, "ReturnConsumedCapacity": "TOTAL"}
        if start:
            kw["ExclusiveStartKey"] = start
        resp = ddb.scan(**kw)
        items += [from_ddb(i) for i in resp.get("Items", [])]
        used += consumed(resp)
        ahead = used / GOLDEN_READ_PER_SEC - (time.monotonic() - t0)
        if ahead > 0:
            time.sleep(ahead)
        start = resp.get("LastEvaluatedKey")
        if not start:
            return sorted(items, key=lambda r: r[KEY])


_golden_count = {}


def golden_count():
    """How many customer records exist; cached a minute (a COUNT scan still reads every item)."""
    if _golden_count.get("t", 0) < now() - 60:
        total, start = 0, None
        while True:
            kw = {"TableName": RECORDS_TABLE, "Select": "COUNT"}
            if start:
                kw["ExclusiveStartKey"] = start
            resp = ddb.scan(**kw)
            total += resp.get("Count", 0)
            start = resp.get("LastEvaluatedKey")
            if not start:
                break
        _golden_count.update(t=now(), n=total)
    return _golden_count["n"]


# --- run lifecycle -----------------------------------------------------------
def _int_param(params, name, default, lo, hi):
    raw = params.get(name, default)
    try:
        v = int(raw)
    except (TypeError, ValueError):
        raise HttpError(400, f"{name} must be an integer")
    if not lo <= v <= hi:
        raise HttpError(400, f"{name} must be between {lo} and {hi}")
    return v


def get_meta(run_id, consistent=False):
    return from_ddb(ddb.get_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id)),
                                 ConsistentRead=consistent).get("Item"))


def admit(meta, index, client_token=None):
    """
    Admit a run in ONE transaction: claim its share of the admission budget, create its META,
    create its index entry (and remember the caller's idempotency token, if any). Either all
    of it happens or none, so a crash here can never leave a budget claimed with no run behind
    it. Returns (run_id or None, why not). A repeated client_token returns the run it started.
    """
    n = int(meta["records"])
    items = [
        {"Update": {
            "TableName": RUNS_TABLE, "Key": to_ddb(ADMISSION_KEY),
            "UpdateExpression": "ADD active :id, active_records :n, epoch :one",
            "ConditionExpression": ("(attribute_not_exists(active) OR size(active) < :maxruns) AND "
                                    "(attribute_not_exists(active_records) OR active_records <= :room)"),
            "ExpressionAttributeValues": to_ddb({":id": {meta["run_id"]}, ":n": n, ":one": 1,
                                                 ":maxruns": MAX_ACTIVE_RUNS, ":room": RECORD_BUDGET - n})}},
        {"Put": {"TableName": RUNS_TABLE, "Item": to_ddb(meta), "ConditionExpression": "attribute_not_exists(pk)"}},
        {"Put": {"TableName": RUNS_TABLE, "Item": to_ddb(index), "ConditionExpression": "attribute_not_exists(pk)"}},
    ]
    token_key = {"pk": "TOKEN", "sk": str(client_token)[:80]} if client_token else None
    if token_key:
        items.append({"Put": {"TableName": RUNS_TABLE, "ConditionExpression": "attribute_not_exists(pk)",
                              "Item": to_ddb({**token_key, "run_id": meta["run_id"], "ttl": meta["ttl"]})}})
    try:
        transact(items)
        return meta["run_id"], None
    except ddb.exceptions.TransactionCanceledException as e:
        reasons = [r.get("Code") for r in e.response.get("CancellationReasons", [])]
        if token_key and len(reasons) > 3 and reasons[3] == "ConditionalCheckFailed":
            prior = from_ddb(ddb.get_item(TableName=RUNS_TABLE, Key=to_ddb(token_key), ConsistentRead=True).get("Item"))
            return prior.get("run_id"), "duplicate"
        if reasons and reasons[0] == "ConditionalCheckFailed":
            adm = admission()
            holders = []
            for r in sorted(adm["active"]):
                m = get_meta(r) or {}
                arm = {"1": " agent arm", "0": " rules arm"}.get(str(m.get("agent")), "") if m.get("customer") == "preference" else ""
                holders.append(f"{m.get('kind', 'run')}{arm} …{r[-6:]} ({m.get('records', '?')})")
            return None, (f"not enough room: this run needs {meta['records']:,} records, and {adm['records']:,} of "
                          f"{RECORD_BUDGET:,} are in use ({len(adm['active'])} of {MAX_ACTIVE_RUNS} runs"
                          f"{': ' + ', '.join(holders) if holders else ''}). Try again when one finishes, or cancel one")
        raise


def transition(run_id, status, expect, **extra):
    """
    Move a run to a new status: META (conditionally, only from `expect`) and its index entry
    change together, and a finishing run gives its budget back in the same transaction. Returns
    False if another caller got there first, so side effects run exactly once.
    """
    meta = get_meta(run_id, consistent=True)
    if not meta:
        return False
    names = {"#st": "status", **{f"#{k}": k for k in extra}}
    values = {":s": status, **{f":{k}": v for k, v in extra.items()}}
    sets = "SET #st = :s" + "".join(f", #{k} = :{k}" for k in extra)
    expect_vals = {f":e{i}": e for i, e in enumerate(expect)}
    items = [
        {"Update": {"TableName": RUNS_TABLE, "Key": to_ddb(meta_key(run_id)), "UpdateExpression": sets,
                    "ConditionExpression": "#st IN (" + ", ".join(expect_vals) + ")",
                    "ExpressionAttributeNames": names,
                    "ExpressionAttributeValues": to_ddb({**values, **expect_vals})}},
        {"Update": {"TableName": RUNS_TABLE, "Key": to_ddb({"pk": INDEX_PK, "sk": meta["index_sk"]}),
                    "UpdateExpression": sets, "ExpressionAttributeNames": names,
                    "ExpressionAttributeValues": to_ddb(values)}},
    ]
    if status in FINISHED:
        items.append({"Update": {
            "TableName": RUNS_TABLE, "Key": to_ddb(ADMISSION_KEY),
            "UpdateExpression": "DELETE active :id ADD active_records :minus, epoch :one",
            "ExpressionAttributeValues": to_ddb({":id": {run_id}, ":minus": -int(meta.get("records", 0)), ":one": 1})}})
    try:
        transact(items)
    except ddb.exceptions.TransactionCanceledException as e:
        if cancellation(e)[0] == "ConditionalCheckFailed":
            return False
        raise
    _cache.clear()
    return True


def finish(run_id, status, **extra):
    """complete / failed / cancelled: exactly one caller wins, then the workers are reconciled."""
    won = transition(run_id, status, list(ACTIVE), finished_at=round(now(), 3), **extra)
    if won:
        safe_reconcile()
        if get_meta(run_id).get("kind") == "load":
            invoke_self({"internal": "finalize", "run_id": run_id})
    return won


def invoke_self(payload):
    lam.invoke(FunctionName=os.environ["AWS_LAMBDA_FUNCTION_NAME"], InvocationType="Event",
               Payload=json.dumps(payload).encode())


def start_run(params):
    kind = params.get("kind", "chaos")
    if kind not in RUN_KINDS:
        raise HttpError(400, f"kind must be one of {', '.join(RUN_KINDS)}")
    reject = _int_param(params, "reject_percent", 30 if kind == "load" else 50, 0, 100)
    kn = {"kill_percent": _int_param(params, "kill_percent", 0, 0, MAX_KILL_PERCENT),
          "slow_percent": _int_param(params, "slow_percent", 0, 0, MAX_SLOW_PERCENT),
          "dup_percent": _int_param(params, "dup_percent", 0, 0, MAX_DUP_PERCENT),
          "poison_percent": _int_param(params, "poison_percent", 0, 0, MAX_POISON_PERCENT),
          "throttle_percent": _int_param(params, "throttle_percent", 0, 0, MAX_THROTTLE_PERCENT)}
    agent_on = _int_param(params, "agent", 0, 0, 1)
    customer = params.get("customer", "random")
    if customer not in ("random", "preference"):
        raise HttpError(400, "customer must be random or preference")
    if customer == "preference" and kind != "load":
        raise HttpError(400, "customers with a preference come with generated load records (kind=load)")
    load_fields = {}
    if kind == "load":
        # A clean throughput run: generated unique records, no injected faults.
        kn = {k: 0 for k in kn}
        records = _int_param(params, "records", LOAD_MAX_RECORDS, 10, LOAD_MAX_RECORDS)
        load_fields = {
            "seed": generator.LOAD_SEED, "cursor": 0, "produced": 0, "create_skipped": 0, "handoffs": 0,
            "backpressure_s": 0,
            "max_backlog": 0, "producer_beat": round(now(), 3),
            "backlog_cap": _int_param(params, "backlog_cap", LOAD_BACKLOG_CAP, LOAD_BLOCK, LOAD_BACKLOG_CAP),
            "chunk_s": _int_param(params, "chunk_s", LOAD_CHUNK_S, 1, LOAD_CHUNK_S),
            "rate": min(float(params.get("rate") or LOAD_RECORDS_PER_SEC), LOAD_RECORDS_PER_SEC)}
        if customer == "preference":
            # Picky customers take more passes, so more writes per record: release them slower.
            load_fields["rate"] = min(load_fields["rate"], PREFERENCE_RECORDS_PER_SEC)
    else:
        records = golden_count()
        if not records:
            raise HttpError(409, "no customer records loaded; POST /load first")
    t = now()
    stamp = datetime.fromtimestamp(t, timezone.utc)
    run_id = f"run-{stamp:%Y%m%d-%H%M%S}-{random.randint(0, 0xffffff):06x}"
    index_sk = f"{stamp.isoformat(timespec='milliseconds')}#{run_id}"
    max_workers = sum(e["max"] for e in ESMS.values())
    common_fields = {"run_id": run_id, "kind": kind, "status": "starting", "started_at": round(t, 3),
                     "reject_percent": reject, **kn, "records": records, "max_workers": max_workers,
                     "agent": agent_on, "customer": customer, "ttl": ttl_for(t)}
    meta = {**meta_key(run_id), **common_fields, **load_fields, "index_sk": index_sk, "closed": 0, "resolved": 0,
            "human_review": 0, "passes_total": 0, "closed_by_pass": {}, "last_progress_at": round(t, 3)}
    index = {"pk": INDEX_PK, "sk": index_sk, **common_fields}
    token = params.get("client_token")
    admitted, why = admit(meta, index, token)
    if why and why != "duplicate":
        housekeeping()                          # frees budget held by runs that already ended
        admitted, why = admit(meta, index, token)
    if why == "duplicate":                      # a retried or double-submitted request
        prior = get_meta(admitted, consistent=True)
        return {"run_id": admitted, "kind": prior.get("kind"), "status": prior.get("status"), "duplicate": True,
                "records": prior.get("records"), "reject_percent": prior.get("reject_percent"), **knobs(prior)}
    if why:
        raise HttpError(429, why)
    try:
        lam.invoke(FunctionName=os.environ["AWS_LAMBDA_FUNCTION_NAME"], InvocationType="Event",
                   Payload=json.dumps({"internal": "start_run", "run_id": run_id}).encode())
    except Exception as e:
        finish(run_id, "failed", error=f"could not start the run: {type(e).__name__}")
        raise HttpError(503, "could not start the run; it was rolled back, try again")
    return {"run_id": run_id, "kind": kind, "status": "starting", "reject_percent": reject, **kn, **load_fields,
            "agent": agent_on, "customer": customer,
            "records": records, "workers": f"autoscaled by SQS, up to {max_workers}, shared by all runs"}


def run_async(ev):
    """Second half of /run, invoked asynchronously. Any failure fails the run (others carry on)."""
    try:
        return _run_async(ev["run_id"])
    except Exception as e:
        # Only a run still "starting" has failed to start. A load run's producer can die after the
        # run is under way; housekeeping restarts it from its cursor.
        if transition(ev["run_id"], "failed", ["starting"], finished_at=round(now(), 3),
                      error=f"run start failed: {type(e).__name__}: {e}"[:300]):
            safe_reconcile()
            return {"run_id": ev["run_id"], "status": "failed"}
        print(dumps({"producer": "failed", "run_id": ev["run_id"], "error": f"{type(e).__name__}: {e}"[:300]}))
        return {"run_id": ev["run_id"], "error": type(e).__name__}


def _run_async(run_id):
    meta = get_meta(run_id, consistent=True)
    if meta.get("status") != "starting":
        return {"run_id": run_id, "status": meta.get("status")}
    if meta.get("kind") == "load":
        return produce(run_id)
    if not claim_setup(run_id):
        return {"run_id": run_id, "status": "set up by another invocation"}
    out = setup_run(run_id, meta)
    adopt_starting(run_id)
    return out


def claim_setup(run_id):
    """
    One invocation sets a run up. The account allows 10 concurrent Lambdas and the workers may
    hold 8, so several start invocations can sit throttled (Lambda retries them for minutes).
    Whichever control invocation gets to a waiting run first claims it; a stale claim (its
    invocation died) can be taken over.
    """
    t = now()
    try:
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id)),
                        UpdateExpression="SET setup_at = :t",
                        ConditionExpression="#st = :starting AND (attribute_not_exists(setup_at) OR setup_at < :stale)",
                        ExpressionAttributeNames={"#st": "status"},
                        ExpressionAttributeValues=to_ddb({":t": round(t, 3), ":starting": "starting",
                                                          ":stale": round(t - SETUP_CLAIM_S, 3)}))
        return True
    except ddb.exceptions.ConditionalCheckFailedException:
        return False


def adopt_starting(done_run_id):
    """
    After setting up its own run, a start invocation sets up any other run still waiting for
    its own (throttled) start invocation, so simultaneous starts don't wait on Lambda retries.
    """
    done = {done_run_id}
    while True:                                 # re-scan: more runs may have been admitted meanwhile
        adopted = False
        for run_id in sorted(admission()["active"] - done):
            done.add(run_id)
            meta = get_meta(run_id, consistent=True)
            if meta.get("status") != "starting" or meta.get("kind") == "load" or not claim_setup(run_id):
                continue
            adopted = True
            try:
                setup_run(run_id, meta)
                print(dumps({"adopted_start": run_id}))
            except Exception as e:
                finish(run_id, "failed", error=f"run start failed: {type(e).__name__}: {e}"[:300])
        if not adopted:
            return


def setup_run(run_id, meta):
    safe_reconcile()                            # workers on early: overlaps with copying records
    t = float(meta["started_at"])
    pacer = WcuPacer(pace_rate(os.environ.get("RUN_INIT_WCU_PER_SEC")))
    cases = []
    columns = set(input_columns()) - {KEY}
    for rec in read_golden()[: int(meta["records"])]:
        case_id = rec[KEY]
        item = {**{k: v for k, v in rec.items() if k in columns}, **case_key(run_id, case_id), KEY: case_id,
                "run_id": run_id, "status": "queued", "pass_no": 1, "queued_at": round(now(), 3), "history": [],
                "claims": 0, "fencing_token": 0, "ttl": ttl_for(t)}
        try:
            resp = ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb(item), ReturnConsumedCapacity="TOTAL",
                                ConditionExpression="attribute_not_exists(pk)")   # a retried start can't reset one
            pacer.record(consumed(resp))
        except ddb.exceptions.ConditionalCheckFailedException:
            pacer.record(1)
        cases.append(case_id)
    if len(cases) != int(meta["records"]):      # the golden set shrank since admission
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id)), UpdateExpression="SET records = :n",
                        ExpressionAttributeValues=to_ddb({":n": len(cases)}))

    # Faults travel on the messages. Each producer stamps its own messages from the same
    # deterministic draws per (run, case, pass): here pass 1, in the worker every later pass.
    kn = knobs(meta)
    entries = [b for c in cases for b in outgoing(run_id, c, 1, kn)]
    # MessageGroupId = run: the queues are standard queues used as SQS *fair queues*, so a run
    # with a big backlog (a load test) can't starve another run's messages behind it.
    for i in range(0, len(entries), 10):
        sqs.send_message_batch(QueueUrl=REBOOKING_QUEUE, Entries=[
            {"Id": str(j), "MessageBody": b, "MessageGroupId": run_id} for j, b in enumerate(entries[i:i + 10])])
    if len(entries) > len(cases):
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb({"pk": run_pk(run_id), "sk": "EVENTS"}),
                        UpdateExpression="SET #ttl = if_not_exists(#ttl, :ttl) ADD duplicates_sent :n",
                        ExpressionAttributeNames={"#ttl": "ttl"},
                        ExpressionAttributeValues=to_ddb({":n": len(entries) - len(cases), ":ttl": ttl_for(t)}))
    # Only now is the run "running": if this function died while sending, the run is still
    # "starting", and housekeeping fails it after START_TIMEOUT_S.
    transition(run_id, "running", ["starting"], queued_at=round(now(), 3), records=len(cases))
    safe_reconcile()
    print(dumps({"run_id": run_id, "records": len(cases), "messages": len(entries), **kn}))
    return {"run_id": run_id, "records": len(cases)}


def backlog():
    a = sqs.get_queue_attributes(QueueUrl=REBOOKING_QUEUE, AttributeNames=["ApproximateNumberOfMessages"])
    return int(a["Attributes"]["ApproximateNumberOfMessages"])


def produce_async(ev):
    try:
        return produce(ev["run_id"])
    except Exception as e:                      # housekeeping restarts a producer that stops beating
        print(dumps({"producer": "failed", "run_id": ev["run_id"], "error": f"{type(e).__name__}: {e}"[:300]}))
        return {"run_id": ev["run_id"], "error": type(e).__name__}


def produce(run_id):
    """
    Release a load run's generated records: create each record's run copy and enqueue its
    first-pass message, LOAD_BLOCK at a time, at no more than `rate` records/s, and never while
    the rebooking backlog would pass `backlog_cap` (backpressure). After `chunk_s` seconds it
    hands off to a fresh invocation of itself.

    Safe to run again from any point: records are created with a conditional put (never twice)
    and the cursor only advances by compare-and-set after a block's messages are sent. A
    producer that dies mid-block leaves the cursor behind, so the next one redoes that block:
    the creates are skipped (counted in create_skipped) and its messages are sent again, which
    the workers treat as ordinary duplicates.
    """
    meta = get_meta(run_id, consistent=True)
    if meta.get("status") not in ACTIVE:
        return {"run_id": run_id, "status": meta.get("status")}
    if meta.get("status") == "starting":
        safe_reconcile()                        # workers on before the first message
    n, rate, cap = int(meta["records"]), float(meta["rate"]), int(meta["backlog_cap"])
    seed, t_start = meta["seed"], float(meta["started_at"])
    deadline = now() + int(meta["chunk_s"])
    columns = [c for c in input_columns() if c != KEY]
    kn = knobs(meta)                            # all zero for load runs: one plain message per record
    cursor = int(meta["cursor"])
    t0, released, max_seen, last_beat, waited = now(), 0, int(meta.get("max_backlog", 0)), 0.0, 0
    max_release = int(meta.get("max_release_depth", 0))
    snaps = Snapshotter(run_id)
    try:
        return _drive(run_id, meta, snaps, n, rate, cap, seed, t_start, deadline, columns, kn, cursor,
                      t0, released, max_seen, last_beat, waited, max_release)
    finally:
        snaps.join()                            # never leave a snapshot half-written across a freeze


def _drive(run_id, meta, snaps, n, rate, cap, seed, t_start, deadline, columns, kn, cursor,
           t0, released, max_seen, last_beat, waited, max_release):
    prefer = meta.get("customer") == "preference"
    while cursor < n and now() < deadline:
        depth = backlog()
        max_seen = max(max_seen, depth)
        if depth + LOAD_BLOCK > cap:            # backpressure: let the workers catch up
            time.sleep(1)
            waited += 1
            if now() - last_beat > 10:
                ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id)),
                                UpdateExpression="SET producer_beat = :t, max_backlog = :m ADD backpressure_s :w",
                                ExpressionAttributeValues=to_ddb({":t": round(now(), 3), ":m": max_seen, ":w": waited}))
                last_beat, waited = now(), 0
            t0 += 1                             # time spent held back doesn't count toward the pace
            continue
        block = range(cursor + 1, min(cursor + LOAD_BLOCK, n) + 1)
        skipped = 0
        for i in block:
            case_id = f"LOAD-{i:05d}"
            item = {**generator.make_inputs(i, seed, columns), **case_key(run_id, case_id), KEY: case_id,
                    "run_id": run_id, "status": "queued", "pass_no": 1, "queued_at": round(now(), 3),
                    "history": [], "claims": 0, "fencing_token": 0, "ttl": ttl_for(t_start)}
            if prefer:
                # Hidden preference (only the simulated customer reads it) and the note from
                # their earlier call (the only hint the agent gets).
                item["pref"], item["customer_note"] = generator.preference(i, seed, offer_sequence(item))
            try:
                ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb(item), ConditionExpression="attribute_not_exists(pk)")
            except ddb.exceptions.ConditionalCheckFailedException:
                skipped += 1                    # created by a producer that died before advancing
        release_depth = depth + len(block)      # what this release can take the backlog to, as seen
        bodies = [b for i in block for b in outgoing(run_id, f"LOAD-{i:05d}", 1, kn)]
        sqs.send_message_batch(QueueUrl=REBOOKING_QUEUE, Entries=[
            {"Id": str(j), "MessageBody": b, "MessageGroupId": run_id} for j, b in enumerate(bodies)])
        new = block[-1]
        try:
            ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id)),
                            UpdateExpression=("SET #c = :new, producer_beat = :t, max_backlog = :m, "
                                              "max_release_depth = :rd "
                                              "ADD produced :n, create_skipped :s, backpressure_s :w"),
                            ConditionExpression="#c = :old",
                            ExpressionAttributeNames={"#c": "cursor"},
                            ExpressionAttributeValues=to_ddb({":new": new, ":old": cursor, ":t": round(now(), 3),
                                                              ":m": max_seen, ":n": len(block), ":s": skipped,
                                                              ":w": waited, ":rd": max_release}))
        except ddb.exceptions.ConditionalCheckFailedException:
            # Another producer moved the cursor (a restart raced a slow one): continue from there.
            cursor = int(get_meta(run_id, consistent=True)["cursor"])
            continue
        cursor, waited = new, 0
        max_release = max(max_release, release_depth)
        released += len(block)
        if released == len(block) and meta.get("status") == "starting":
            transition(run_id, "running", ["starting"], queued_at=round(now(), 3))
        ahead = released / rate - (now() - t0)  # pacing: never faster than `rate` records/s
        if ahead > 0:
            time.sleep(ahead)
        snaps.tick()
    # Released: keep watching until every record is closed, refreshing the view.
    while cursor >= n and now() < deadline:
        m = get_meta(run_id, consistent=True)
        if m.get("status") not in ACTIVE:
            return {"run_id": run_id, "status": m.get("status")}
        if int(m.get("closed", 0)) >= n:
            finish(run_id, "complete")          # the finalizer builds the final view
            return {"run_id": run_id, "done": True}
        snaps.tick()
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id)), UpdateExpression="SET producer_beat = :t",
                        ExpressionAttributeValues=to_ddb({":t": round(now(), 3)}))
        time.sleep(WATCH_TICK_S)
    if get_meta(run_id).get("status") in ACTIVE:
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id)), UpdateExpression="ADD handoffs :one",
                        ExpressionAttributeValues=to_ddb({":one": 1}))
        invoke_self({"internal": "produce", "run_id": run_id})
    return {"run_id": run_id, "cursor": cursor, "handed_off": True}


class Snapshotter:
    """Refreshes a running load run's view in a background thread, so release pacing never waits on it."""

    def __init__(self, run_id):
        self.run_id, self.last, self.th = run_id, now(), None

    def tick(self):
        if (self.th is None or not self.th.is_alive()) and now() - self.last > SNAPSHOT_S:
            self.last = now()
            self.th = threading.Thread(target=self._run, daemon=True)
            self.th.start()

    def _run(self):
        try:
            snapshot_load(self.run_id, SNAPSHOT_READ_PER_SEC)
        except Exception as e:                  # a missed refresh is not worth stopping the run for
            print(dumps({"snapshot": "failed", "run_id": self.run_id, "error": f"{type(e).__name__}: {e}"[:300]}))

    def join(self):
        if self.th is not None:
            self.th.join()


def paced_partition(run_id, read_per_sec=FINALIZE_READ_PER_SEC):
    """Every item of a run's partition, read at no more than `read_per_sec` read units/s."""
    items, start, t0, used = [], None, time.monotonic(), 0.0
    while True:
        kw = {"TableName": RUNS_TABLE, "KeyConditionExpression": "pk = :p",
              "ExpressionAttributeValues": to_ddb({":p": run_pk(run_id)}),
              "ReturnConsumedCapacity": "TOTAL", "Limit": 100}
        if start:
            kw["ExclusiveStartKey"] = start
        resp = ddb.query(**kw)
        items += [from_ddb(i) for i in resp.get("Items", []) if i["sk"]["S"] != "SNAPSHOT"]
        used += consumed(resp)
        ahead = used / read_per_sec - (time.monotonic() - t0)
        if ahead > 0:
            time.sleep(ahead)
        start = resp.get("LastEvaluatedKey")
        if not start:
            return items, used


_load_gold = {}


def load_gold(seed):
    """case_id -> {acceptable, unacceptable} for a load set, regenerated from its seed."""
    def lookup(case_id):
        key = (seed, case_id)
        if key not in _load_gold:
            rec = generator.make_record(int(case_id.split("-")[1]), seed)
            _load_gold[key] = {"acceptable": rec["acceptable_resolutions"].split("|"),
                               "unacceptable": set(rec["unacceptable_resolutions"].split("|")) - {"none"}}
        return _load_gold[key]
    return lookup


def notable(rec):
    """Sort key: the records worth looking at in a 1,000-record run come first."""
    return (rec["status"] in TERMINAL, not rec["swept"], not rec["unacceptable_offered"], -rec["crashes_here"],
            -rec["takeovers"], -int(rec.get("pass_no") or 0), rec["case_id"])


def snapshot_load(run_id, read_per_sec=FINALIZE_READ_PER_SEC):
    """
    Rebuild a load run's full view (workers over time, incidents, degraded dependencies, crash
    and race results, passes, workers, the most notable records) from one paced read of its
    partition, and store it compressed on one item that every viewer reads instead.
    """
    items, used = paced_partition(run_id, read_per_sec)
    meta = next(i for i in items if i["sk"] == "META")
    t = now()
    detail, _, _ = build_detail(meta, items, t, load_gold(meta.get("seed", generator.LOAD_SEED)))
    table = sorted(detail["records"], key=notable)
    detail["records"] = table[:SNAPSHOT_RECORDS]
    detail["records_total"] = len(table)
    detail["snapshot_at"] = round(t, 3)
    detail["snapshot_read_units"] = round(used, 1)
    blob = zlib.compress(dumps(detail).encode(), 9)
    ddb.put_item(TableName=RUNS_TABLE, Item={
        "pk": {"S": run_pk(run_id)}, "sk": {"S": "SNAPSHOT"}, "data": {"B": blob},
        "at": {"N": str(round(t, 3))}, "ttl": {"N": str(ttl_for(float(meta["started_at"])))}})
    _cache.pop(("snapshot", run_id), None)
    return meta, items, used


def read_snapshot(run_id):
    key = ("snapshot", run_id)
    hit = _cache.get(key)
    if hit and hit["t"] > now() - 10:
        return hit["v"]
    item = ddb.get_item(TableName=RUNS_TABLE, Key={"pk": {"S": run_pk(run_id)}, "sk": {"S": "SNAPSHOT"}}).get("Item")
    v = json.loads(zlib.decompress(item["data"]["B"])) if item else None
    _cache[key] = {"t": now(), "v": v}
    return v


def finalize_load(run_id):
    """
    Score a finished load run once, reading its records a single time at a paced rate: lost
    records, records with two results for one pass, outcomes outside each record's acceptable
    list (regenerated from its seed), and the worker concurrency it reached. The same read
    builds the run's final view.
    """
    meta, items, used = snapshot_load(run_id)
    rows = [i for i in items if i["sk"].startswith("CASE#")]
    seed = meta.get("seed", generator.LOAD_SEED)
    double = outside = 0
    for r in rows:
        hist = r.get("history", [])
        if len({int(h["p"]) for h in hist}) != len(hist):
            double += 1
        rec = generator.make_record(int(r[KEY].split("-")[1]), seed)
        acceptable = rec["acceptable_resolutions"].split("|")
        unacceptable = set(rec["unacceptable_resolutions"].split("|")) - {"none"}
        if (r.get("status") == "resolved" and r.get("resolution") not in acceptable) or \
                any(h["o"] in unacceptable for h in hist):
            outside += 1
    end = float(meta.get("finished_at") or now())
    activity = worker_activity(rows, [], float(meta["started_at"]), end, now())
    closed = int(meta.get("closed", 0))
    summary = {"records": int(meta["records"]), "created": len(rows), "closed": closed,
               "lost": int(meta["records"]) - closed, "double_results": double, "outside_rules": outside,
               "resolved": int(meta.get("resolved", 0)), "human_review": int(meta.get("human_review", 0)),
               "avg_passes": round(int(meta.get("passes_total", 0)) / max(1, closed), 2),
               "duration_s": round(end - float(meta["started_at"]), 1), "peak_workers": activity["peak"],
               "max_backlog": int(meta.get("max_backlog", 0)), "backlog_cap": int(meta.get("backlog_cap", 0)),
               "handoffs": int(meta.get("handoffs", 0)), "create_skipped": int(meta.get("create_skipped", 0)),
               "backpressure_s": int(meta.get("backpressure_s", 0)), "read_units_spent": round(used, 1)}
    # The queues are shared by every tenant, so the backlog peak (max_backlog) includes other runs'
    # messages and SQS's approximate, lagging counts. What the producer controls, and what is
    # checked, is that it only ever released a block into a backlog that stayed within the cap.
    summary["max_release_depth"] = int(meta.get("max_release_depth", 0))
    # Agent comparison: how fast each record closed and what the model calls cost.
    calls_h = [h for r in rows for h in r.get("history", []) if "tk" in h]
    spans = sorted(float(r["history"][-1]["t"]) - float(r["history"][0]["c"]) for r in rows if r.get("history"))
    rerouted = [r for r in rows if len(r.get("history", [])) >= 2]
    calls = len(calls_h)                        # committed passes that called the model
    t_in, t_out = sum(int(h["tk"][0]) for h in calls_h), sum(int(h["tk"][1]) for h in calls_h)
    summary.update({
        "agent": int(meta.get("agent", 0)), "customer": meta.get("customer", "random"),
        "avg_close_s": round(sum(spans) / max(1, len(spans)), 1),
        "p90_close_s": round(spans[int(0.9 * (len(spans) - 1))], 1) if spans else 0,
        "closed_first_reroute": sum(1 for r in rerouted if len(r["history"]) == 2 and r.get("status") == "resolved"),
        "rerouted": len(rerouted),
        "agent_calls": calls, "agent_fallbacks": sum(1 for h in calls_h if h.get("fb")),
        "agent_tokens_in": t_in, "agent_tokens_out": t_out, "agent_cost_usd": agent.cost(t_in, t_out)})
    summary["passed"] = (summary["lost"] == 0 and double == 0 and outside == 0 and len(rows) == summary["records"]
                         and summary["max_release_depth"] <= summary["backlog_cap"])
    for key in (meta_key(run_id), {"pk": INDEX_PK, "sk": meta["index_sk"]}):
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(key), UpdateExpression="SET #f = :f",
                        ExpressionAttributeNames={"#f": "final"}, ExpressionAttributeValues=to_ddb({":f": summary}))
    _cache.clear()
    print(dumps({"load_summary": run_id, **summary}))
    return summary


def load_detail(meta, queues, t):
    """
    A load run's view. Live counters come from META; everything else (workers over time,
    incidents, crash and race results, workers, notable records) from the latest snapshot,
    rebuilt about once a minute by one paced read. Viewers never read the 1,000 records.
    """
    run_id = meta["run_id"]
    status = meta["status"]
    records, closed = int(meta.get("records", 0)), int(meta.get("closed", 0))
    if status == "running" and records and closed >= records:
        finish(run_id, "complete")
        meta = get_meta(run_id, consistent=True)
        status = meta["status"]
    started = float(meta["started_at"])
    end = float(meta["finished_at"]) if meta.get("finished_at") else t
    final = meta.get("final") if isinstance(meta.get("final"), dict) else None
    if status in FINISHED and not final and t - end > FINALIZE_RETRY_S:
        # The finalizer is an async invoke with no retries: if it died, the next viewer restarts it
        # (at most once per FINALIZE_RETRY_S, claimed with a conditional write).
        try:
            ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id)), UpdateExpression="SET finalize_at = :t",
                            ConditionExpression="attribute_not_exists(finalize_at) OR finalize_at < :old",
                            ExpressionAttributeValues=to_ddb({":t": round(t, 3), ":old": round(t - FINALIZE_RETRY_S, 3)}))
            invoke_self({"internal": "finalize", "run_id": run_id})
        except ddb.exceptions.ConditionalCheckFailedException:
            pass
    stats = {"records": records, "closed": closed, "open": records - closed, "released": int(meta.get("cursor", 0)),
             "resolved": int(meta.get("resolved", 0)), "human_review": int(meta.get("human_review", 0)),
             "backlog_now": queues["rebooking"]["visible"], "max_backlog": int(meta.get("max_backlog", 0)),
             "backlog_cap": int(meta.get("backlog_cap", 0)), "handoffs": int(meta.get("handoffs", 0)),
             "backpressure_s": int(meta.get("backpressure_s", 0)),
             "max_release_depth": int(meta.get("max_release_depth", 0)), "duration_s": round(end - started, 1)}
    keep = {k: v for k, v in meta.items() if k not in ("pk", "sk", "closed_by_pass", "final")}
    snap = read_snapshot(run_id)
    if not snap:                                # first refresh comes about a minute in
        return {**keep, "status": status, "stats": stats, "summary": final, "records": [], "kills": [],
                "workers": [], "incidents": [], "sweeps": [], "timeline": None, "snapshot_at": None}
    # The full view from the latest snapshot, with live counters laid over it.
    busy = sum(q["in_flight"] for name, q in queues.items() if name != "dead_letter")
    live = {**stats, "status_counts": {**snap["stats"].get("status_counts", {})},
            "workers_busy_now": busy if status in ACTIVE else 0}
    return {**keep, **snap, "status": status, "stats": {**snap["stats"], **live}, "summary": final}


def cancel(params):
    run_id = params.get("run")
    if not run_id:
        raise HttpError(400, "cancel which run? pass ?run=<run_id>")
    meta = get_meta(run_id, consistent=True)
    if not meta:
        raise HttpError(404, f"no run {run_id}")
    if meta.get("status") in FINISHED:
        raise HttpError(409, f"run {run_id} already {meta['status']}")
    if not finish(run_id, "cancelled", error="cancelled by a user"):
        raise HttpError(409, f"run {run_id} finished while it was being cancelled")
    # Its messages stay in the shared queues; workers drop them on sight (run not active).
    return {"run_id": run_id, "status": "cancelled"}


# --- reset everything (admin) ------------------------------------------------
def paced_delete(table, keys, pacer):
    for k in keys:
        pacer.record(consumed(ddb.delete_item(TableName=table, Key=to_ddb(k), ReturnConsumedCapacity="TOTAL")))


def reset_all(params):
    """Cancel every run, empty the shared queues, delete all run data. The golden set stays."""
    if params.get("confirm") != "everything":
        raise HttpError(400, "this cancels every run for everyone; confirm with ?confirm=everything")
    t0 = now()
    cancelled = [r for r in admission()["active"] if finish(r, "cancelled", error="reset by a user")]
    ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb({**ADMISSION_KEY, "active_records": 0,
                                                    "epoch": admission()["epoch"] + 1}))
    esm = set_esms(False)
    time.sleep(RESET_SETTLE_S)                  # in-flight worker invokes finish or see the cancel
    purged = []
    for q in (REBOOKING_QUEUE, REROUTING_QUEUE, DLQ):
        try:
            sqs.purge_queue(QueueUrl=q)
            purged.append(q.rsplit("/", 1)[-1])
        except sqs.exceptions.PurgeQueueInProgress:
            purged.append(q.rsplit("/", 1)[-1] + " (purge already in progress)")
    pacer = WcuPacer(pace_rate(params.get("wcu_per_sec") or 15))
    keys = [{"pk": i["pk"], "sk": i["sk"]} for i in scan_all(RUNS_TABLE, ProjectionExpression="pk, sk")
            if (i["pk"], i["sk"]) != (ADMISSION_KEY["pk"], ADMISSION_KEY["sk"])]
    paced_delete(RUNS_TABLE, keys, pacer)
    _cache.clear()
    return {"cancelled_runs": cancelled, "deleted_run_items": len(keys), "purged_queues": purged,
            "worker_mappings": esm, "wcu_consumed": round(pacer.used, 2), "seconds": round(now() - t0, 1)}


# --- results -----------------------------------------------------------------
_cache = {}
CACHE_S = 2.0


def queue_depths():
    out = {}
    for name, q in (("rebooking", REBOOKING_QUEUE), ("rerouting", REROUTING_QUEUE), ("dead_letter", DLQ)):
        a = sqs.get_queue_attributes(QueueUrl=q, AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        out[name] = {"visible": int(a["ApproximateNumberOfMessages"]),
                     "in_flight": int(a["ApproximateNumberOfMessagesNotVisible"])}
    return out


def query_all(**kw):
    items, start = [], None
    while True:
        if start:
            kw["ExclusiveStartKey"] = start
        resp = ddb.query(TableName=RUNS_TABLE, **kw)
        items += [from_ddb(i) for i in resp.get("Items", [])]
        start = resp.get("LastEvaluatedKey")
        if not start:
            return items


def progress(meta):
    keep = ("run_id", "kind", "status", "started_at", "records", "closed", "resolved", "human_review",
            "reject_percent", "last_progress_at", "finished_at", "error", "agent", "customer") + tuple(knobs({}).keys())
    return {k: meta.get(k) for k in keep}


def results(run_id=None, force=False):
    key = run_id or "latest"
    hit = _cache.get(key)
    if not force and hit and hit["t"] > now() - CACHE_S:
        return hit["v"]
    t = now()
    recent = [from_ddb(i) for i in ddb.query(
        TableName=RUNS_TABLE, KeyConditionExpression="pk = :p",
        ExpressionAttributeValues=to_ddb({":p": INDEX_PK}), ScanIndexForward=False, Limit=12)["Items"]]
    queues = queue_depths()
    run_id = run_id or (recent[0]["run_id"] if recent else None)
    detail = run_detail(run_id, queues, t) if run_id else None   # may finish the run: read budget after
    adm = admission()
    active_metas = [m for m in (get_meta(r) for r in sorted(adm["active"])) if m]
    out = {"generated_at": round(t, 3), "records_in_table": golden_count(), "recent_runs": recent,
           "active_runs": [progress(m) for m in active_metas],
           "budget": {"runs": len(adm["active"]), "max_runs": MAX_ACTIVE_RUNS,
                      "records": adm["records"], "max_records": RECORD_BUDGET},
           "queues": queues, "worker_mappings": esm_states(), "latest_run": detail}
    if detail and detail["status"] == "complete":
        for r in out["recent_runs"]:                # the index row read above predates the finish
            if r["run_id"] == run_id and r.get("status") in ACTIVE:
                r["status"] = "complete"
    _cache[key] = {"t": t, "v": out}
    return out


def run_detail(run_id, queues, t):
    head = get_meta(run_id)
    if head.get("kind") == "load":
        return load_detail(head, queues, t)
    if not head:
        return None
    # Completion comes from the exactly-once counters, not from reading every record.
    if head["status"] in ACTIVE and head["status"] != "starting" and \
            int(head.get("records", 0)) and int(head.get("closed", 0)) >= int(head.get("records", 0)):
        finish(run_id, "complete")
    items = query_all(KeyConditionExpression="pk = :p", ExpressionAttributeValues=to_ddb({":p": run_pk(run_id)}))
    meta = next((i for i in items if i["sk"] == "META"), None)
    if not meta:
        return None
    detail, open_rows, last_activity = build_detail(meta, items, t, golden())
    status, stats = meta["status"], detail["stats"]
    if status == "complete" and "final" not in meta:
        # Keep the summary on the index entry so the recent-runs table doesn't re-read records.
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb({"pk": INDEX_PK, "sk": meta["index_sk"]}),
                        UpdateExpression="SET #f = :f", ExpressionAttributeNames={"#f": "final"},
                        ExpressionAttributeValues=to_ddb({":f": json.loads(dumps(stats))}))
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id)),
                        UpdateExpression="SET #f = :true", ExpressionAttributeNames={"#f": "final"},
                        ExpressionAttributeValues=to_ddb({":true": True}))
    shown = status
    if status == "running" and open_rows and t - last_activity > STALL_S:
        if requeue_orphans(run_id, open_rows, queues, t):
            shown = "recovering"                # orphans found and re-sent just now
        elif not any(q["visible"] + q["in_flight"] for q in queues.values()):
            shown = "stalled"                   # open records but nothing happening: shown, not hidden
    meta_out = {k: v for k, v in meta.items() if k not in ("pk", "sk", "closed_by_pass")}
    return {**meta_out, **detail, "status": shown}


def build_detail(meta, items, t, gold):
    """
    Everything the run view shows, computed from a run's partition: per-record table, crash and
    race results, degraded dependencies, worker intervals and the concurrency timeline.
    `gold` maps case_id -> {acceptable, unacceptable} for scoring. Returns (detail, open rows,
    last activity time).
    """
    rows = [i for i in items if i["sk"].startswith("CASE#")]
    kills = [normalize_kill(i) for i in items if i["sk"].startswith("KILL#")]
    sweeps = sorted((i for i in items if i["sk"].startswith("SWEEP#")), key=lambda i: float(i["swept_at"]))
    requeues = [i for i in items if i["sk"].startswith("REQUEUE#")]
    events = next((i for i in items if i["sk"] == "EVENTS"), {})
    started = float(meta["started_at"])
    status_counts, passes, table = {}, {}, []
    satisfactory = takeovers = 0
    last_activity = started
    for r in sorted(rows, key=lambda x: x[KEY]):
        st = r.get("status", "unknown")
        status_counts[st] = status_counts.get(st, 0) + 1
        hist = r.get("history", [])
        g = gold(r[KEY]) if callable(gold) else gold.get(r[KEY], {"acceptable": [], "unacceptable": set()})
        ok, bad = score(r, g)
        satisfactory += ok
        takeovers += sum(1 for h in hist if "from" in h)
        last_activity = max([last_activity, float(r.get("claimed_at", 0))] + [float(h["t"]) for h in hist])
        if st in TERMINAL:
            p = int(r.get("pass_no", 0))
            passes[p] = passes.get(p, 0) + 1
        table.append({
            "case_id": r[KEY], "status": st, "pass_no": r.get("pass_no"), "resolution": r.get("resolution"),
            "offers": [f"{h['o']}:{h['r']}" for h in hist], "fencing_token": r.get("fencing_token"),
            "claims": r.get("claims"), "takeovers": sum(1 for h in hist if "from" in h),
            "lease_owner": r.get("lease_owner"), "satisfactory": ok, "unacceptable_offered": bad,
            "urgency": r.get("urgency"), "cabin": r.get("cabin"), "destination": r.get("destination"),
            "party_size": r.get("party_size"), "travel_concerns": r.get("travel_concerns"),
            "queued_at": r.get("queued_at"), "resolved_at": r.get("resolved_at"), "swept": bool(r.get("swept")),
            "history": [{k: h.get(k) for k in ("p", "o", "r", "w", "c", "t", "from", "why", "fb")} for h in hist],
            "crashes_here": sum(1 for k in kills if k["case_id"] == r[KEY]),
        })
    open_rows = [r for r in rows if r.get("status") not in TERMINAL]
    queued = [r for r in open_rows if r.get("status") == "queued"]

    def oldest(rs):
        if not rs:
            return None
        o = min(rs, key=lambda r: float(r.get("queued_at", t)))
        return {"case_id": o[KEY], "status": o.get("status"), "pass_no": o.get("pass_no"),
                "waiting_s": round(t - float(o.get("queued_at", t)), 1)}

    records = int(meta.get("records", 0))
    closed = int(meta.get("closed", 0))
    status = meta["status"]
    finished_at = float(meta["finished_at"]) if meta.get("finished_at") else None
    end = finished_at or (t if status in ACTIVE else last_activity)
    activity = worker_activity(rows, kills, started, end, t)
    fired = attribute_kills(kills, {r[KEY]: r for r in rows})
    stats = {
        "records": records, "closed": closed, "status_counts": status_counts,
        "resolved": status_counts.get("resolved", 0), "human_review": status_counts.get("human_review", 0),
        "satisfactory": satisfactory, "open": len(open_rows) if rows else records - closed,
        "passes_to_close": {str(k): passes[k] for k in sorted(passes)},
        "avg_passes": round(sum(k * v for k, v in passes.items()) / max(1, sum(passes.values())), 2),
        "oldest_unpicked": oldest(queued), "oldest_unresolved": oldest(open_rows),
        "takeovers_after_crash": takeovers,
        "kills_fired": len(fired), "kills_recovered": sum(1 for k in fired if k["recovered"]),
        "kills_by_point": kills_by_point(fired),
        "events": {k: int(events.get(k, 0)) for k in EVENT_COUNTERS},
        "races": race_results(items, rows, events),
        "degradation": {
            "throttles_injected": int(events.get("throttles_injected", 0)),
            "sdk_retries": int(events.get("sdk_retries", 0)),
            "throttled_after_retries": int(events.get("failed_throttled", 0)),
            "message_backoffs": int(events.get("backoffs", 0)),
            "poison_failures": int(events.get("failed_poison", 0)),
            "other_errors": int(events.get("failed_error", 0)),
            "own_writes_recognized": int(events.get("own_write_recognized", 0)),
            "dead_lettered_swept": len(sweeps),
            "orphans_requeued": int(events.get("orphans_requeued", 0)),
            "conflicts_after_retries": int(events.get("failed_conflict", 0)),
        },
        "workers_busy_now": activity["busy_now"], "workers_peak": activity["peak"],
        "workers_used": activity["used"], "first_claim_s": activity["first_claim_s"],
        "queued_s": round(float(meta["queued_at"]) - started, 1) if meta.get("queued_at") else None,
        "duration_s": round(end - started, 1),
    }
    detail = {"stats": stats, "workers": activity["workers"],
              "incidents": incidents(started, rows, fired, stats["races"]["zombie_list"], sweeps, requeues),
              "sweeps": sweeps, "timeline": activity["timeline"], "kills": fired, "records": table}
    return detail, open_rows, last_activity


def requeue_orphans(run_id, rows, queues, t):
    """Re-send the message for open records nothing can deliver any more. At most once a minute per run."""
    # A dead letter still counts as "a message left": its record belongs to the sweeper, which
    # sends it to a person. Re-sending it here would quietly heal a poison message instead
    # (found live: a poison run whose other records finished early lost all its sweeps).
    empty = not any(q["visible"] + q["in_flight"] for q in queues.values())
    limit = ORPHAN_S if empty else ORPHAN_BACKLOG_S
    orphans = []
    for r in rows:
        st, waited = r.get("status"), None
        if st == "queued":
            waited = t - float(r.get("queued_at", t))
        elif st == "in_progress":
            waited = t - float(r.get("lease_expires", t))         # lease long gone, nobody took over
        if waited is not None and waited > limit:
            orphans.append(r)
    if not orphans:
        return 0
    try:                                         # one caller per minute per run (UI tabs, housekeeping)
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(meta_key(run_id)),
                        UpdateExpression="SET last_requeue_at = :t",
                        ConditionExpression="attribute_not_exists(last_requeue_at) OR last_requeue_at < :cut",
                        ExpressionAttributeValues=to_ddb({":t": round(t, 3), ":cut": round(t - 60, 3)}))
    except ddb.exceptions.ConditionalCheckFailedException:
        return 0
    for r in orphans:
        p = int(r.get("pass_no", 1))
        sqs.send_message(QueueUrl=REBOOKING_QUEUE if p == 1 else REROUTING_QUEUE, MessageGroupId=run_id,
                         MessageBody=plain_body(run_id, r[KEY], p, "orphan: no message left for this record"))
        ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb({
            "pk": run_pk(run_id), "sk": f"REQUEUE#{r[KEY]}#{p}#{int(t)}", "case_id": r[KEY], "pass": p,
            "requeued_at": round(t, 3), "status_was": r.get("status"), "ttl": int(t + 86400)}))
    ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb({"pk": run_pk(run_id), "sk": "EVENTS"}),
                    UpdateExpression="SET #ttl = if_not_exists(#ttl, :ttl) ADD orphans_requeued :n",
                    ExpressionAttributeNames={"#ttl": "ttl"},
                    ExpressionAttributeValues=to_ddb({":n": len(orphans), ":ttl": int(t + 86400)}))
    print(dumps({"requeued_orphans": [(r[KEY], r.get("pass_no"), r.get("status")) for r in orphans], "run_id": run_id}))
    return len(orphans)


# --- housekeeping --------------------------------------------------------------
def reconcile_budget():
    """
    The admitted-record counter is kept with ADD/subtract in the same transactions that admit and
    finish runs. Found live: it drifted 1,000 records high with no run holding them, which
    blocked new runs. Recompute it from the runs actually in the active set, and write it only if
    no admission or finish happened in between (epoch unchanged). Returns the correction.
    """
    adm = admission()
    actual = sum(int(m.get("records", 0)) for m in (get_meta(r, consistent=True) for r in adm["active"])
                 if m and m.get("status") in ACTIVE)
    if actual == adm["records"]:
        return 0
    values = {":n": actual, ":one": 1}
    if adm["epoch"]:
        cond, values[":e"] = "epoch = :e", adm["epoch"]
    else:
        cond = "attribute_not_exists(epoch)"
    try:
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(ADMISSION_KEY),
                        UpdateExpression="SET active_records = :n ADD epoch :one",
                        ConditionExpression=cond, ExpressionAttributeValues=to_ddb(values))
    except ddb.exceptions.ConditionalCheckFailedException:
        print(dumps({"budget_reconcile": "skipped: admissions changed meanwhile", "counted": adm["records"],
                     "actual": actual}))
        return 0                                # the next tick checks again
    print(dumps({"budget_reconciled": {"was": adm["records"], "now": actual, "active": sorted(adm["active"])}}))
    return actual - adm["records"]


def housekeeping():
    """Every few minutes: finish or fail unwatched runs, free their budget, reconcile workers."""
    try:
        return _housekeeping()
    except Exception as e:                      # log loudly; the next tick tries again
        print(dumps({"housekeeping": "failed", "error": f"{type(e).__name__}: {e}"[:300]}))
        return {"error": type(e).__name__}


def _housekeeping():
    t = now()
    adm = admission()
    actions, oldest_progress = {}, 0.0
    for run_id in sorted(adm["active"]):
        meta = get_meta(run_id, consistent=True)
        if not meta or meta.get("status") in FINISHED:
            # Budget still held by a run that is gone or already over (e.g. TTL'd): release it.
            ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb(ADMISSION_KEY),
                            UpdateExpression="DELETE active :id ADD active_records :minus, epoch :one",
                            ExpressionAttributeValues=to_ddb({":id": {run_id}, ":one": 1,
                                                              ":minus": -int((meta or {}).get("records", 0))}))
            actions[run_id] = "released"
            continue
        started = float(meta["started_at"])
        if meta["status"] == "starting" and t - started > START_TIMEOUT_S:
            finish(run_id, "failed", error="run start did not finish (control function died?)")
            actions[run_id] = "failed: start"
        elif t - started > RUN_MAX_S:
            finish(run_id, "failed", error="run did not finish within its time limit")
            actions[run_id] = "failed: time limit"
        elif int(meta.get("closed", 0)) >= int(meta.get("records", 0)) > 0 and meta["status"] == "running":
            finish(run_id, "complete")
            actions[run_id] = "complete"
        elif meta.get("kind") == "load" and t - float(meta.get("producer_beat", started)) > PRODUCER_STALE_S:
            invoke_self({"internal": "produce", "run_id": run_id})     # cursor CAS makes this safe
            actions[run_id] = "restarted producer"
        else:
            oldest_progress = max(oldest_progress, t - float(meta.get("last_progress_at", started)))
            if meta["status"] == "running" and t - float(meta.get("last_progress_at", started)) > ORPHAN_S:
                open_rows = [r for r in query_all(
                    KeyConditionExpression="pk = :p AND begins_with(sk, :c)",
                    ProjectionExpression="case_id, #st, pass_no, queued_at, lease_expires",
                    ExpressionAttributeNames={"#st": "status"},
                    ExpressionAttributeValues=to_ddb({":p": run_pk(run_id), ":c": "CASE#"}))
                    if r.get("status") not in TERMINAL]
                n = requeue_orphans(run_id, open_rows, queue_depths(), t)
                if n:
                    actions[run_id] = f"requeued {n} orphans"
    drift = reconcile_budget()
    if drift:
        actions["budget"] = f"record count corrected by {drift:+d}"
    mappings = safe_reconcile()
    # Custom metric for the stuck-run alarm, via CloudWatch embedded metric format (a log line,
    # no API call): seconds since any active run last closed a record. 0 when nothing is active.
    print(json.dumps({"_aws": {"Timestamp": int(t * 1000), "CloudWatchMetrics": [{
        "Namespace": "IropsDemo", "Dimensions": [[]],
        "Metrics": [{"Name": "SecondsSinceRunProgress", "Unit": "Seconds"},
                    {"Name": "ActiveRuns", "Unit": "Count"}]}]},
        "SecondsSinceRunProgress": round(oldest_progress, 1), "ActiveRuns": len(admission()["active"])}))
    out = {"active_runs": sorted(admission()["active"]), "actions": actions, "mappings": mappings}
    print(dumps({"housekeeping": out}))
    return out


# --- entry points ------------------------------------------------------------
def http_response(status, body, ctype="application/json"):
    return {"statusCode": status, "headers": {"content-type": ctype, "cache-control": "no-store"},
            "body": body if isinstance(body, str) else dumps(body)}


def route(event):
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    path = (event.get("rawPath") or "/").rstrip("/") or "/"
    params = event.get("queryStringParameters") or {}
    if event.get("body"):
        try:
            params = {**json.loads(event["body"]), **params}
        except (ValueError, TypeError):
            pass
    if path == "/" and method == "GET":
        with open(UI_PATH) as f:
            return http_response(200, f.read(), "text/html; charset=utf-8")
    if path == "/results" and method == "GET":
        return http_response(200, results(params.get("run")))
    mutating = {"/reset": lambda: reset_all(params),
                "/load": lambda: load(params.get("wcu_per_sec"), params.get("if_missing") in ("1", "true", True)),
                "/run": lambda: start_run(params),
                "/cancel": lambda: cancel(params)}
    if path in mutating:
        if method != "POST":
            return http_response(405, {"error": f"use POST {path}"})
        return http_response(202 if path == "/run" else 200, mutating[path]())
    return http_response(404, {"error": "not found", "routes": [
        "GET /", "GET /results?run=", "POST /run", "POST /cancel?run=", "POST /load", "POST /reset?confirm=everything"]})


def respond_cfn(event, context, status, data=None, reason=""):
    body = json.dumps({
        "Status": status, "Reason": reason or f"See CloudWatch log stream {context.log_stream_name}",
        "PhysicalResourceId": event.get("PhysicalResourceId", f"experiment-load-{RECORDS_TABLE}"),
        "StackId": event["StackId"], "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"], "Data": data or {},
    }).encode()
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT",
                                 headers={"Content-Type": "", "Content-Length": str(len(body))})
    urllib.request.urlopen(req, timeout=10)


def on_cloudformation(event, context):
    try:
        if event["RequestType"] == "Delete":
            respond_cfn(event, context, "SUCCESS")
            return
        r = load()
        respond_cfn(event, context, "SUCCESS", {
            "Loaded": str(r["loaded"]), "WcuConsumed": str(r["wcu_consumed"]),
            "WcuPerSecActual": str(r["wcu_per_sec_actual"]), "Seconds": str(r["seconds"])})
    except Exception as e:  # always answer CloudFormation, or the stack hangs for an hour
        print(f"load failed: {e!r}")
        respond_cfn(event, context, "FAILED", reason=str(e)[:500])


def handler(event, context):
    if "RequestType" in event and "ResponseURL" in event:
        return on_cloudformation(event, context)
    if event.get("internal") == "start_run":
        return run_async(event)
    if event.get("internal") == "housekeeping":
        return housekeeping()
    if event.get("internal") == "produce":
        return produce_async(event)
    if event.get("internal") == "finalize":
        return finalize_load(event["run_id"])
    if "requestContext" in event:
        try:
            return route(event)
        except HttpError as e:
            return http_response(e.status, {"error": e.msg})
        except Exception as e:
            print(f"error: {e!r}")
            return http_response(500, {"error": type(e).__name__, "detail": str(e)[:300]})
    # direct console invocation: {"action": "load" | "reset" | "results" | "run" | "cancel", ...}
    actions = {"load": lambda: load(event.get("wcu_per_sec")), "reset": lambda: reset_all(event),
               "results": lambda: results(event.get("run"), force=True), "run": lambda: start_run(event),
               "cancel": lambda: cancel(event), "housekeeping": housekeeping}
    if event.get("action") not in actions:
        return {"error": f"unknown action {event.get('action')!r}", "actions": sorted(actions)}
    return json.loads(dumps(actions[event["action"]]()))
