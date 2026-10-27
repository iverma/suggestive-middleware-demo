"""
Experiment control Lambda for the IROPS re-booking demo.

Served publicly through a Lambda Function URL:
  GET  /                 UI
  GET  /results          latest run: outcomes, passes per record, oldest unpicked, workers, recent runs
  POST /reset            stop any run, purge queues, delete every item in both tables
  POST /load             paced load of the golden-set inputs (?wcu_per_sec=N)
  POST /run              start a run (?reject_percent=50&kill_percent=0&slow_percent=0&dup_percent=0
                         &poison_percent=0&throttle_percent=0)

Workers are not launched by this function. They are the worker Lambda, invoked
by SQS event source mappings that Lambda autoscales from queue depth. This
function only switches those mappings on for a run and off when it ends: an
enabled mapping long-polls its queue around the clock, which would cost SQS
requests while idle.

Also invoked by CloudFormation on deploy (loads the golden set), by itself
asynchronously (second half of /run), and by EventBridge Scheduler every few
minutes (housekeeping: finish runs nobody is watching, switch mappings off).
Mutating routes are POST-only so link previews and crawlers can't trigger them.
"""
import json
import os
import random
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

from common import (
    KILL_POINTS, knobs, outgoing,
    DLQ, ESMS, KEY, LEASE_SECONDS, NUMERIC, RECORDS_TABLE, REBOOKING_QUEUE, REROUTING_QUEUE, RUNS_TABLE,
    TERMINAL, VISIBILITY_SECONDS, WcuPacer, consumed, ddb, dumps, from_ddb, golden, input_rows, lam, now,
    pace_rate, scan_all, sqs, to_ddb,
)

MAX_KILL_PERCENT = 50     # every message crashing once doubles a run's work; keep runs sane
MAX_SLOW_PERCENT = 20     # each slow pass holds a worker for ~15-20s
MAX_DUP_PERCENT = 100
MAX_POISON_PERCENT = 10   # each poisoned record waits out 5 failed deliveries, then the sweeper
MAX_THROTTLE_PERCENT = 30 # share of worker AWS calls answered with a 400 throttle
START_TIMEOUT_S = 120     # a run still "starting" after this never finished its start: roll it back
RUN_LOCK_S = 30 * 60
STALL_S = LEASE_SECONDS + VISIBILITY_SECONDS + 30
RESET_SETTLE_S = float(os.environ.get("RESET_SETTLE_S", "4"))
ESM_RETRY_S = float(os.environ.get("ESM_RETRY_S", "45"))
UI_PATH = os.path.join(os.path.dirname(__file__), "ui.html")
CONTROL_KEY = {"pk": "CONTROL", "sk": "ACTIVE"}
ACTIVE = ("starting", "running")


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


# --- load --------------------------------------------------------------------
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
    names["#ft"] = "fencing_token"
    values[":zero"] = {"N": "0"}
    sets.append("#ft = if_not_exists(#ft, :zero)")
    expr = "SET " + ", ".join(sets) + (" REMOVE " + ", ".join(removes) if removes else "")
    return {"TableName": RECORDS_TABLE, "Key": {KEY: {"S": row[KEY]}}, "UpdateExpression": expr,
            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values,
            "ReturnConsumedCapacity": "TOTAL"}


def load(wcu_per_sec=None):
    """Customer fields come from the CSV; an existing fencing_token is never reset."""
    rate = pace_rate(wcu_per_sec)
    pacer = WcuPacer(rate)
    n = 0
    for row in input_rows():
        pacer.record(consumed(ddb.update_item(**build_load_update(row))))
        n += 1
    secs = pacer.elapsed()
    result = {"loaded": n, "wcu_consumed": round(pacer.used, 2), "wcu_per_sec_budget": rate,
              "wcu_per_sec_actual": round(pacer.used / secs, 2) if secs else None,
              "seconds": round(secs, 1), "table": RECORDS_TABLE}
    print(dumps(result))
    return result


# --- reset -------------------------------------------------------------------
def paced_delete(table, keys, pacer):
    for k in keys:
        pacer.record(consumed(ddb.delete_item(TableName=table, Key=to_ddb(k), ReturnConsumedCapacity="TOTAL")))


def reset(wcu_per_sec=None):
    t0 = now()
    esm = set_esms(False)
    ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb({**CONTROL_KEY, "status": "aborted", "run_id": "",
                                                     "expires_at": round(now() + 60, 3)}))
    time.sleep(RESET_SETTLE_S)          # in-flight worker invokes finish or see the abort
    purged = []
    for q in (REBOOKING_QUEUE, REROUTING_QUEUE, DLQ):
        try:
            sqs.purge_queue(QueueUrl=q)
            purged.append(q.rsplit("/", 1)[-1])
        except sqs.exceptions.PurgeQueueInProgress:
            purged.append(q.rsplit("/", 1)[-1] + " (purge already in progress)")
    pacer = WcuPacer(pace_rate(wcu_per_sec))
    rec_keys = [{KEY: i[KEY]} for i in scan_all(RECORDS_TABLE, ProjectionExpression=KEY)]
    paced_delete(RECORDS_TABLE, rec_keys, pacer)
    run_keys = [{"pk": i["pk"], "sk": i["sk"]} for i in scan_all(RUNS_TABLE, ProjectionExpression="pk, sk")]
    paced_delete(RUNS_TABLE, run_keys, pacer)
    _cache.clear()
    return {"deleted_records": len(rec_keys), "deleted_run_items": len(run_keys), "purged_queues": purged,
            "worker_mappings": esm, "wcu_consumed": round(pacer.used, 2), "seconds": round(now() - t0, 1)}


# --- run ---------------------------------------------------------------------
def _int_param(params, name, default, lo, hi):
    raw = params.get(name, default)
    try:
        v = int(raw)
    except (TypeError, ValueError):
        raise HttpError(400, f"{name} must be an integer")
    if not lo <= v <= hi:
        raise HttpError(400, f"{name} must be between {lo} and {hi}")
    return v


def acquire_lock(summary, t):
    """
    Take the run lock and create the run's summary in ONE transaction: either both exist or
    neither does, so a crash here can never leave a lock with no run behind it (which would
    block /run for the lock's lifetime). The lock is free when there is none, it expired, or
    its run has finished.
    """
    lock = {**CONTROL_KEY, "run_id": summary["run_id"], "summary_sk": summary["sk"], "status": "starting",
            "reject_percent": summary["reject_percent"], **knobs(summary), "started_at": round(t, 3),
            "expires_at": round(t + RUN_LOCK_S, 3)}
    try:
        ddb.transact_write_items(TransactItems=[
            {"Put": {"TableName": RUNS_TABLE, "Item": to_ddb(lock),
                     "ConditionExpression": "attribute_not_exists(pk) OR expires_at < :t OR NOT (#st IN (:s, :r))",
                     "ExpressionAttributeNames": {"#st": "status"},
                     "ExpressionAttributeValues": to_ddb({":t": round(t, 3), ":s": "starting", ":r": "running"})}},
            {"Put": {"TableName": RUNS_TABLE, "Item": to_ddb(summary),
                     "ConditionExpression": "attribute_not_exists(pk)"}},
        ])
        return True
    except ddb.exceptions.TransactionCanceledException:
        return False


def start_run(params):
    reject = _int_param(params, "reject_percent", 50, 0, 100)
    kn = {"kill_percent": _int_param(params, "kill_percent", 0, 0, MAX_KILL_PERCENT),
          "slow_percent": _int_param(params, "slow_percent", 0, 0, MAX_SLOW_PERCENT),
          "dup_percent": _int_param(params, "dup_percent", 0, 0, MAX_DUP_PERCENT),
          "poison_percent": _int_param(params, "poison_percent", 0, 0, MAX_POISON_PERCENT),
          "throttle_percent": _int_param(params, "throttle_percent", 0, 0, MAX_THROTTLE_PERCENT)}
    t = now()
    stamp = datetime.fromtimestamp(t, timezone.utc)
    run_id = f"run-{stamp:%Y%m%d-%H%M%S}-{random.randint(0, 0xffff):04x}"
    sk = f"{stamp.isoformat(timespec='seconds')}#{run_id}"
    max_workers = sum(e["max"] for e in ESMS.values())
    summary = {"pk": "RUN", "sk": sk, "run_id": run_id, "status": "starting", "started_at": round(t, 3),
               "reject_percent": reject, **kn, "max_workers": max_workers}
    if not acquire_lock(summary, t):
        results(force=True)                     # completes the current run if it is done
        if not acquire_lock(summary, now()):
            raise HttpError(409, "a run is already in progress; wait for it to finish or POST /reset")
    try:
        lam.invoke(FunctionName=os.environ["AWS_LAMBDA_FUNCTION_NAME"], InvocationType="Event",
                   Payload=json.dumps({"internal": "start_run", **summary}).encode())
    except Exception as e:
        rollback(run_id, sk, f"could not start the run: {type(e).__name__}")
        raise HttpError(503, "could not start the run; it was rolled back, try again")
    _cache.clear()
    return {"run_id": run_id, "status": "starting", "reject_percent": reject, **kn,
            "workers": f"autoscaled by SQS, up to {max_workers}"}


def set_run_status(run_id, sk, status, expect=None, **extra):
    """
    Move a run to a new status: its summary and the lock change together in one transaction.
    With expect, only from those statuses; returns False if another caller got there first
    (e.g. two /results calls both noticing the run is done), so side effects run only once.
    """
    names = {"#st": "status", **{f"#{k}": k for k in extra}}
    values = {":s": status, **{f":{k}": v for k, v in extra.items()}}
    summary = {"TableName": RUNS_TABLE, "Key": to_ddb({"pk": "RUN", "sk": sk}),
               "UpdateExpression": "SET #st = :s" + "".join(f", #{k} = :{k}" for k in extra),
               "ExpressionAttributeNames": names, "ExpressionAttributeValues": to_ddb(values)}
    if expect:
        summary["ConditionExpression"] = "#st IN (" + ", ".join(f":e{i}" for i in range(len(expect))) + ")"
        summary["ExpressionAttributeValues"] = to_ddb({**values, **{f":e{i}": e for i, e in enumerate(expect)}})
    lock = {"TableName": RUNS_TABLE, "Key": to_ddb(CONTROL_KEY), "UpdateExpression": "SET #st = :s",
            "ConditionExpression": "run_id = :r", "ExpressionAttributeNames": {"#st": "status"},
            "ExpressionAttributeValues": to_ddb({":s": status, ":r": run_id})}
    try:
        ddb.transact_write_items(TransactItems=[{"Update": summary}, {"Update": lock}])
        return True
    except ddb.exceptions.TransactionCanceledException as e:
        reasons = [r.get("Code") for r in e.response.get("CancellationReasons", [])]
        if reasons and reasons[0] == "ConditionalCheckFailed":
            return False                        # someone else already moved this run on
        # The lock belongs to a newer run or a reset: update just this run's summary.
        try:
            ddb.update_item(**summary)
            return True
        except ddb.exceptions.ConditionalCheckFailedException:
            return False


def rollback(run_id, sk, reason):
    """A run that failed to start cleanly: mark it failed, stop the workers, drop its messages."""
    print(dumps({"rollback": run_id, "reason": reason}))
    if not set_run_status(run_id, sk, "failed", expect=["starting", "running"], error=reason):
        return False
    ctl = from_ddb(ddb.get_item(TableName=RUNS_TABLE, Key=to_ddb(CONTROL_KEY)).get("Item"))
    if ctl.get("run_id") == run_id:             # only touch shared infra if no newer run owns it
        set_esms(False)
        for q in (REBOOKING_QUEUE, REROUTING_QUEUE):
            try:
                sqs.purge_queue(QueueUrl=q)
            except sqs.exceptions.PurgeQueueInProgress:
                pass
    _cache.clear()
    return True


def run_async(ev):
    """Second half of /run, invoked asynchronously. Any failure rolls the run back."""
    try:
        return _run_async(ev)
    except Exception as e:
        rollback(ev["run_id"], ev["sk"], f"run start failed: {type(e).__name__}: {e}"[:300])
        return {"run_id": ev["run_id"], "status": "failed"}


def _run_async(ev):
    run_id, sk = ev["run_id"], ev["sk"]
    esm = set_esms(True)                       # takes a few seconds to apply; overlaps record prep
    pacer = WcuPacer(pace_rate(os.environ.get("RUN_INIT_WCU_PER_SEC")))
    t = now()
    cases = []
    for row in input_rows():
        try:
            resp = ddb.update_item(
                TableName=RECORDS_TABLE, Key=to_ddb({KEY: row[KEY]}),
                UpdateExpression=("SET run_id = :r, #st = :q, pass_no = :one, queued_at = :t, history = :empty, "
                                  "claims = :zero REMOVE resolution, resolved_at, lease_owner, lease_expires, claimed_at"),
                ConditionExpression="attribute_exists(case_id)",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues=to_ddb({":r": run_id, ":q": "queued", ":one": 1, ":t": round(t, 3),
                                                  ":empty": [], ":zero": 0}),
                ReturnConsumedCapacity="TOTAL")
            pacer.record(consumed(resp))
            cases.append(row[KEY])
        except ddb.exceptions.ConditionalCheckFailedException:
            pacer.record(1)
    if not cases:
        rollback(run_id, sk, "no customer records found; POST /load first")
        return {"run_id": run_id, "status": "failed"}

    # Faults travel on the messages. Each producer stamps its own messages from the same
    # deterministic draws per (run, case, pass): here pass 1, in the worker every later pass.
    kn = knobs(ev)
    entries = [b for c in cases for b in outgoing(run_id, c, 1, kn)]
    for i in range(0, len(entries), 10):
        sqs.send_message_batch(QueueUrl=REBOOKING_QUEUE, Entries=[
            {"Id": str(j), "MessageBody": b} for j, b in enumerate(entries[i:i + 10])])
    if len(entries) > len(cases):
        ddb.update_item(TableName=RUNS_TABLE, Key=to_ddb({"pk": f"RUN#{run_id}", "sk": "EVENTS"}),
                        UpdateExpression="ADD duplicates_sent :n",
                        ExpressionAttributeValues=to_ddb({":n": len(entries) - len(cases)}))
    # Only now is the run "running": if this function died while sending, the run is still
    # "starting", and housekeeping rolls it back after START_TIMEOUT_S.
    if not set_run_status(run_id, sk, "running", expect=["starting"], records=len(cases),
                          queued_at=round(now(), 3), worker_mappings=esm):
        return {"run_id": run_id, "status": "superseded"}
    print(dumps({"run_id": run_id, "records": len(cases), "messages": len(entries), **kn, "esm": esm}))
    return {"run_id": run_id, "records": len(cases)}


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


def score(row, g):
    offered = [h["o"] for h in row.get("history", [])]
    bad = [o for o in offered if o in g["unacceptable"]]
    ok = (row.get("status") == "resolved" and row.get("resolution") in g["acceptable"] and not bad)
    return ok, bad


def worker_activity(rows, kills, start, end, t):
    """
    Reconstruct what the autoscaled workers did from the records themselves: every pass
    a worker handles leaves a [claim, commit] interval in the record's history, a record
    being worked right now has a live lease, and a killed worker left a KILL item.
    """
    killed_envs = {k["worker"] for k in kills if k.get("worker") and k.get("mode", "exit") in ("exit", "timeout")}
    faulted = {(k["worker"], k["case_id"], int(k.get("pass", 1))) for k in kills}   # their leases are abandoned
    intervals = []   # (start, end, env, pool)
    for r in rows:
        for h in r.get("history", []):
            if "c" in h:
                intervals.append((float(h["c"]), float(h["t"]), h["w"], "rebooking" if int(h["p"]) == 1 else "rerouting"))
        if (r.get("status") == "in_progress" and r.get("lease_owner") not in killed_envs
                and (r.get("lease_owner"), r[KEY], int(r.get("pass_no", 1))) not in faulted
                and float(r.get("lease_expires", 0)) > t):
            pool = "rebooking" if int(r.get("pass_no", 1)) == 1 else "rerouting"
            intervals.append((float(r.get("claimed_at", t)), t, r["lease_owner"], pool))
    for k in kills:
        if k.get("worker"):
            began = float(k.get("claimed_at") or k.get("started_at") or k["killed_at"])
            pool = "rebooking" if int(k.get("pass", 1)) == 1 else "rerouting"
            intervals.append((began, float(k["killed_at"]), k["worker"], pool))
    # One claim = one interval. A worker killed after committing appears in both the record's
    # history and its kill entry; keep the longer of the two.
    by_claim = {}
    for a, b, env, pool in intervals:
        key = (env, round(a, 3))
        if key not in by_claim or b > by_claim[key][1]:
            by_claim[key] = (a, b, env, pool)
    intervals = list(by_claim.values())

    timeline = concurrency_timeline(intervals, start, end)
    envs = {}
    for a, b, env, pool in intervals:
        e = envs.setdefault(env, {"worker_id": env, "first_seen": a, "last_seen": b, "passes": 0,
                                  "pools": set(), "takeovers": 0, "faults": 0, "killed": env in killed_envs})
        e["first_seen"], e["last_seen"] = min(e["first_seen"], a), max(e["last_seen"], b)
        e["pools"].add(pool)
    for r in rows:
        for h in r.get("history", []):
            if h.get("w") in envs:
                envs[h["w"]]["passes"] += 1
                envs[h["w"]]["takeovers"] += 1 if "from" in h else 0
    for k in kills:
        if k.get("worker") in envs and k.get("mode") == "raise":
            envs[k["worker"]]["faults"] += 1
    busy_now = {r["lease_owner"] for r in rows if r.get("status") == "in_progress"
                and r.get("lease_owner") not in killed_envs
                and (r.get("lease_owner"), r[KEY], int(r.get("pass_no", 1))) not in faulted
                and float(r.get("lease_expires", 0)) > t}
    workers = []
    for e in sorted(envs.values(), key=lambda x: x["first_seen"]):
        state = "killed" if e["killed"] else ("busy" if e["worker_id"] in busy_now else "idle")
        workers.append({**e, "pools": sorted(e["pools"]), "state": state,
                        "first_seen_s": round(e["first_seen"] - start, 1), "last_seen_s": round(e["last_seen"] - start, 1)})
    first_claim = min((a for a, *_ in intervals), default=None)
    return {
        "busy_now": len(busy_now), "peak": max(timeline["total"], default=0), "used": len(envs),
        "first_claim_s": round(first_claim - start, 1) if first_claim else None,
        "timeline": timeline, "workers": workers,
    }


def concurrency_timeline(intervals, start, end):
    """Per 1s bin: the most workers that were busy at the same instant, per pool and in total.
    An exact sweep over [claim, commit] intervals, so a crashed worker and its replacement
    are never counted as concurrent just because they fall in the same second."""
    n_bins = max(1, int((end - start) // 1) + 1)
    events = sorted([(a, 1, p) for a, b, _, p in intervals] + [(b, -1, p) for a, b, _, p in intervals],
                    key=lambda e: (e[0], e[1]))          # at a tie, ends before starts
    cur = {"rebooking": 0, "rerouting": 0}
    out = {"bin_s": 1, "rebooking": [0] * n_bins, "rerouting": [0] * n_bins, "total": [0] * n_bins}
    i = 0
    for b in range(n_bins):
        lo, hi = start + b, start + b + 1
        peak = {k: v for k, v in cur.items()}
        peak_total = sum(cur.values())
        while i < len(events) and events[i][0] < hi:
            t_, d, p = events[i]
            cur[p] += d
            if t_ >= lo or d > 0:
                peak[p] = max(peak[p], cur[p])
                peak_total = max(peak_total, sum(cur.values()))
            i += 1
        out["rebooking"][b], out["rerouting"][b], out["total"][b] = peak["rebooking"], peak["rerouting"], peak_total
    return out


RECOVERY = {
    "on_receive": "redelivered",
    "after_claim": "lease takeover", "during_offer": "lease takeover",
    "during_customer_response": "lease takeover", "before_commit": "lease takeover",
    "after_commit_before_handoff": "handoff repaired",
    "after_handoff_before_ack": "duplicate dropped",
    "after_final_commit_before_ack": "finished duplicate dropped",
    "stalled_past_timeout": "lease takeover",
}
TIMEOUT_POINT = "stalled_past_timeout"


LEGACY_STAGES = {"mid_work": "before_commit", "after_commit": "after_commit_before_handoff",
                 "before_handoff": "after_commit_before_handoff"}


def normalize_kill(k):
    """Kill items written before the 8 kill points existed carried only a coarse stage."""
    if "point" not in k:
        k = {**k, "point": LEGACY_STAGES.get(k.get("stage"), "before_commit")}
    return {"pass": 1, "mode": "exit", **k}


def attribute_kills(kills, rows):
    """
    For each crash, check the record for the recovery that crash point should have caused,
    and how long it took. Recovered = the record moved past the crashed pass (or finished)
    and, for crashes holding a lease, the next claim of that pass shows a takeover from it.
    """
    out = []
    for k in sorted(map(normalize_kill, kills), key=lambda k: float(k["killed_at"])):
        r = rows.get(k["case_id"], {})
        p = int(k.get("pass", 1))
        hist = r.get("history", [])
        moved_on = r.get("status") in TERMINAL or int(r.get("pass_no", 0)) > p
        how = RECOVERY.get(k["point"], "?")
        nxt = None
        if how in ("redelivered", "lease takeover"):
            done = [h for h in hist if int(h["p"]) == p]
            nxt = done[0] if done else None
            recovered = bool(nxt) and (how == "redelivered" or nxt.get("from") == k["worker"])
            if how == "redelivered" and nxt and nxt.get("from"):
                how = "redelivered (after a takeover)"
        elif how == "handoff repaired":
            later = [h for h in hist if int(h["p"]) == p + 1]
            nxt = later[0] if later else None
            recovered = moved_on and (bool(nxt) or r.get("status") in TERMINAL)
        else:                                    # the work itself was done before the crash
            recovered = moved_on
        out.append({**k, "recovered": bool(recovered), "recovered_by": how,
                    "recovery_s": round(float(nxt["c"]) - float(k["killed_at"]), 1) if nxt and "c" in nxt else None})
    return out


EVENT_COUNTERS = ("fenced", "repaired", "stale", "deferred", "duplicates_sent", "throttles_injected",
                  "sdk_retries", "failed_throttled", "failed_poison", "failed_injected", "failed_error", "backoffs",
                  "own_write_recognized")


def incidents(start, rows, kills, zombies, sweeps, limit=60):
    """Everything that went wrong and how it was handled, newest first, for the incident feed."""
    out = []
    for k in kills:
        out.append({"t": float(k["killed_at"]), "kind": "crash", "worker": k["worker"], "case_id": k["case_id"],
                    "pass": int(k.get("pass", 1)), "point": k["point"], "mode": k.get("mode"),
                    "recovered": k.get("recovered"), "recovered_by": k.get("recovered_by")})
    for r in rows:
        for h in r.get("history", []):
            if h.get("from") and "c" in h:
                out.append({"t": float(h["c"]), "kind": "takeover", "worker": h["w"], "from": h["from"],
                            "case_id": r[KEY], "pass": int(h["p"])})
    for z in zombies:
        out.append({"t": float(z["woke_at"]), "kind": "zombie", "worker": z["worker"], "case_id": z["case_id"],
                    "pass": int(z["pass"]), "outcome": z.get("outcome"), "rival": z.get("taken_over_by")})
    for w in sweeps:
        out.append({"t": float(w["swept_at"]), "kind": "swept", "case_id": w["case_id"], "pass": int(w["pass"]),
                    "deliveries": int(w.get("deliveries", 0))})
    for e in out:
        e["at_s"] = round(e.pop("t") - start, 1)
    return sorted(out, key=lambda e: e["at_s"], reverse=True)[:limit]


def race_results(run_items, rows, events):
    """Slow-worker and duplicate-delivery outcomes, and the invariant duplicates must never break."""
    zombies = [i for i in run_items if i["sk"].startswith("ZOMBIE#")]
    # Exactly one committed result per (record, pass), however many copies or workers touched it.
    double = [{"case_id": r[KEY], "passes": sorted({int(h["p"]) for h in r.get("history", [])
                                                    if [int(x["p"]) for x in r["history"]].count(int(h["p"])) > 1})}
              for r in rows if len({int(h["p"]) for h in r.get("history", [])}) != len(r.get("history", []))]
    return {
        "zombies": len(zombies),
        "zombies_fenced": sum(1 for z in zombies if z.get("outcome") == "fenced"),
        "zombies_uncontested": sum(1 for z in zombies if z.get("outcome") == "uncontested"),
        "zombie_list": sorted(zombies, key=lambda z: float(z["claimed_at"])),
        "duplicates_sent": int(events.get("duplicates_sent", 0)),
        "duplicates_held_back": int(events.get("deferred", 0)),
        "duplicates_dropped": int(events.get("stale", 0)),
        "double_commits": len(double), "double_commit_list": double,
    }


def kills_by_point(fired):
    rows = []
    for point in KILL_POINTS + [TIMEOUT_POINT]:
        ks = [k for k in fired if k["point"] == point]
        waits = [k["recovery_s"] for k in ks if k["recovery_s"] is not None]
        rows.append({"point": point, "fired": len(ks),
                     "exit": sum(1 for k in ks if k.get("mode") == "exit"),
                     "raise": sum(1 for k in ks if k.get("mode") == "raise"),
                     "timeout": sum(1 for k in ks if k.get("mode") == "timeout"),
                     "recovered": sum(1 for k in ks if k["recovered"]),
                     "recovered_by": RECOVERY[point],
                     "passes": sorted({int(k.get("pass", 1)) for k in ks}),
                     "avg_recovery_s": round(sum(waits) / len(waits), 1) if waits else None})
    return rows


def results(force=False):
    if not force and _cache.get("t", 0) > now() - CACHE_S:
        return _cache["v"]
    t = now()
    runs = [from_ddb(i) for i in ddb.query(
        TableName=RUNS_TABLE, KeyConditionExpression="pk = :p",
        ExpressionAttributeValues=to_ddb({":p": "RUN"}), ScanIndexForward=False, Limit=10)["Items"]]
    records = scan_all(RECORDS_TABLE)
    out = {"generated_at": round(t, 3), "records_in_table": len(records), "recent_runs": runs,
           "queues": queue_depths(), "worker_mappings": esm_states(), "latest_run": None}
    if not runs:
        _cache.update(t=t, v=out)
        return out

    run = runs[0]
    run_id = run["run_id"]
    started = float(run["started_at"])
    rows = [r for r in records if r.get("run_id") == run_id]
    run_items = [from_ddb(i) for i in ddb.query(
        TableName=RUNS_TABLE, KeyConditionExpression="pk = :p",
        ExpressionAttributeValues=to_ddb({":p": f"RUN#{run_id}"}))["Items"]]
    kills = [normalize_kill(i) for i in run_items if i["sk"].startswith("KILL#")]
    sweeps = sorted((i for i in run_items if i["sk"].startswith("SWEEP#")), key=lambda i: float(i["swept_at"]))
    events = next((i for i in run_items if i["sk"] == "EVENTS"), {})
    gold = golden()

    status_counts, passes, table = {}, {}, []
    satisfactory = takeovers = 0
    last_activity = started
    for r in sorted(rows, key=lambda x: x[KEY]):
        st = r.get("status", "unknown")
        status_counts[st] = status_counts.get(st, 0) + 1
        hist = r.get("history", [])
        g = gold.get(r[KEY], {"acceptable": [], "unacceptable": set()})
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
            "history": [{k: h.get(k) for k in ("p", "o", "r", "w", "c", "t", "from")} for h in hist],
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

    complete = bool(rows) and not open_rows and len(rows) == int(run.get("records", len(rows)))
    finished_at = max((float(r.get("resolved_at", 0)) for r in rows), default=None) if complete else None
    if run.get("finished_at"):
        finished_at = float(run["finished_at"])
    live = run["status"] in ACTIVE and t - started < RUN_LOCK_S
    end = finished_at or (t if live else last_activity)
    activity = worker_activity(rows, kills, started, end, t)
    fired = attribute_kills(kills, {r[KEY]: r for r in rows})
    stats = {
        "records": len(rows), "status_counts": status_counts,
        "resolved": status_counts.get("resolved", 0), "human_review": status_counts.get("human_review", 0),
        "satisfactory": satisfactory, "open": len(open_rows),
        "passes_to_close": {str(k): passes[k] for k in sorted(passes)},
        "avg_passes": round(sum(k * v for k, v in passes.items()) / max(1, sum(passes.values())), 2),
        "oldest_unpicked": oldest(queued), "oldest_unresolved": oldest(open_rows),
        "takeovers_after_crash": takeovers,
        "kills_fired": len(fired), "kills_recovered": sum(1 for k in fired if k["recovered"]),
        "kills_by_point": kills_by_point(fired),
        "events": {k: int(events.get(k, 0)) for k in EVENT_COUNTERS},
        "races": race_results(run_items, rows, events),
        "degradation": {
            "throttles_injected": int(events.get("throttles_injected", 0)),
            "sdk_retries": int(events.get("sdk_retries", 0)),
            "throttled_after_retries": int(events.get("failed_throttled", 0)),
            "message_backoffs": int(events.get("backoffs", 0)),
            "poison_failures": int(events.get("failed_poison", 0)),
            "other_errors": int(events.get("failed_error", 0)),
            "own_writes_recognized": int(events.get("own_write_recognized", 0)),
            "dead_lettered_swept": len(sweeps),
        },
        "workers_busy_now": activity["busy_now"], "workers_peak": activity["peak"],
        "workers_used": activity["used"], "first_claim_s": activity["first_claim_s"],
        "queued_s": round(float(run["queued_at"]) - started, 1) if run.get("queued_at") else None,
        "duration_s": round(end - started, 1),
    }
    status = run["status"]
    if status in ACTIVE and complete:
        status = "complete"
        won = set_run_status(run_id, run["sk"], "complete", expect=list(ACTIVE),
                             finished_at=round(finished_at, 3), final=json.loads(dumps(stats)))
        run.update(status="complete", finished_at=finished_at)
        if won:                                 # exactly one caller switches the workers off
            try:
                set_esms(False)                 # idle mappings would keep polling SQS
            except Exception as e:              # housekeeping retries
                print(f"could not disable worker mappings: {e!r}")
    elif (status == "running" and open_rows and t - last_activity > STALL_S
          and not any(q["visible"] + q["in_flight"] for q in out["queues"].values())):
        status = "stalled"                      # open records but nothing happening: shown, not hidden
    out["latest_run"] = {**run, "status": status, "stats": stats, "workers": activity["workers"],
                         "incidents": incidents(started, rows, fired, stats["races"]["zombie_list"], sweeps),
                         "sweeps": sweeps,
                         "timeline": activity["timeline"], "kills": fired, "records": table}
    _cache.update(t=t, v=out)
    return out


def housekeeping():
    """Every few minutes: finish or roll back unwatched runs; never leave mappings polling idle."""
    try:
        return _housekeeping()
    except Exception as e:                      # log loudly; the next tick tries again
        print(dumps({"housekeeping": "failed", "error": f"{type(e).__name__}: {e}"[:300]}))
        return {"error": type(e).__name__}


def _housekeeping():
    ctl = from_ddb(ddb.get_item(TableName=RUNS_TABLE, Key=to_ddb(CONTROL_KEY)).get("Item"))
    t = now()
    rolled_back = None
    if ctl.get("status") == "starting" and t - float(ctl.get("started_at", t)) > START_TIMEOUT_S:
        rolled_back = ctl["run_id"]
        rollback(ctl["run_id"], ctl["summary_sk"], "run start did not finish (control function died?)")
    elif ctl.get("status") in ACTIVE and float(ctl.get("expires_at", 0)) < t:
        rolled_back = ctl["run_id"]
        rollback(ctl["run_id"], ctl["summary_sk"], "run lock expired before the run finished")
    elif ctl.get("status") in ACTIVE:
        results(force=True)                     # completes it if every record is done
    ctl = from_ddb(ddb.get_item(TableName=RUNS_TABLE, Key=to_ddb(CONTROL_KEY)).get("Item"))
    active = ctl.get("status") in ACTIVE
    states = {k: v["state"] for k, v in esm_states().items()}
    changed = None
    if not active and any(s not in ("Disabled", "Disabling") for s in states.values()):
        changed = set_esms(False)
    print(dumps({"housekeeping": True, "active_run": ctl.get("run_id") if active else None,
                 "rolled_back": rolled_back, "mappings": states, "disabled": changed}))
    return {"active_run": active, "rolled_back": rolled_back, "mappings": states, "disabled": changed}


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
        return http_response(200, results())
    mutating = {"/reset": lambda: reset(params.get("wcu_per_sec")),
                "/load": lambda: load(params.get("wcu_per_sec")),
                "/run": lambda: start_run(params)}
    if path in mutating:
        if method != "POST":
            return http_response(405, {"error": f"use POST {path}"})
        return http_response(202 if path == "/run" else 200, mutating[path]())
    return http_response(404, {"error": "not found",
                               "routes": ["GET /", "GET /results", "POST /reset", "POST /load", "POST /run"]})


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
    if "requestContext" in event:
        try:
            return route(event)
        except HttpError as e:
            return http_response(e.status, {"error": e.msg})
        except Exception as e:
            print(f"error: {e!r}")
            return http_response(500, {"error": type(e).__name__, "detail": str(e)[:300]})
    # direct console invocation: {"action": "load" | "reset" | "results" | "run", ...}
    actions = {"load": lambda: load(event.get("wcu_per_sec")), "reset": lambda: reset(event.get("wcu_per_sec")),
               "results": lambda: results(force=True), "run": lambda: start_run(event)}
    if event.get("action") not in actions:
        return {"error": f"unknown action {event.get('action')!r}", "actions": sorted(actions)}
    return json.loads(dumps(actions[event["action"]]()))
