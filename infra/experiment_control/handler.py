"""
Experiment control Lambda for the IROPS re-booking demo.

Served publicly through a Lambda Function URL:
  GET  /                 UI
  GET  /results          latest run: outcomes, passes per record, oldest unpicked, workers, recent runs
  POST /reset            stop any run, purge queues, delete every item in both tables
  POST /load             paced load of the golden-set inputs (?wcu_per_sec=N)
  POST /run              start a run (?workers=4&reject_percent=50&kill_workers=0)

Also invoked by CloudFormation on deploy (loads the golden set) and by itself,
asynchronously, to start a run without holding the HTTP request open.
Mutating routes are POST-only so link previews and crawlers can't trigger them.
"""
import json
import os
import random
import time
import urllib.request
from datetime import datetime, timezone

from common import (
    KEY, LEASE_SECONDS, RECORDS_TABLE, REBOOKING_QUEUE, REROUTING_QUEUE, DLQ, RUNS_TABLE,
    TERMINAL, WORKER_FUNCTION, WcuPacer, consumed, ddb, dumps, from_ddb, golden, input_rows,
    lam, now, pace_rate, scan_all, sqs, to_ddb,
)
from common import NUMERIC  # noqa: F401  (re-exported for tests)
from decimal import Decimal

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))
RUN_LOCK_S = 16 * 60                 # > worker Lambda max lifetime
RESET_SETTLE_S = float(os.environ.get("RESET_SETTLE_S", "4"))
UI_PATH = os.path.join(os.path.dirname(__file__), "ui.html")
CCF = "ConditionalCheckFailedException"
CONTROL_KEY = {"pk": "CONTROL", "sk": "ACTIVE"}


class HttpError(Exception):
    def __init__(self, status, msg):
        super().__init__(msg)
        self.status, self.msg = status, msg


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
    ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb({**CONTROL_KEY, "status": "aborted", "run_id": "",
                                                     "expires_at": round(now() + 60, 3)}))
    time.sleep(RESET_SETTLE_S)          # workers notice the abort within CONTROL_CHECK_S and exit
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
            "wcu_consumed": round(pacer.used, 2), "seconds": round(now() - t0, 1)}


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


def acquire_lock(run_id, sk, t):
    ddb.put_item(
        TableName=RUNS_TABLE,
        Item=to_ddb({**CONTROL_KEY, "run_id": run_id, "summary_sk": sk, "status": "starting",
                     "expires_at": round(t + RUN_LOCK_S, 3)}),
        ConditionExpression="attribute_not_exists(pk) OR expires_at < :t OR #st IN (:c, :a, :f)",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues=to_ddb({":t": round(t, 3), ":c": "complete", ":a": "aborted", ":f": "failed"}),
    )


def start_run(params):
    workers = _int_param(params, "workers", 4, 1, MAX_WORKERS)
    reject = _int_param(params, "reject_percent", 50, 0, 100)
    kills = _int_param(params, "kill_workers", 0, 0, max(0, workers - 1))
    t = now()
    stamp = datetime.fromtimestamp(t, timezone.utc)
    run_id = f"run-{stamp:%Y%m%d-%H%M%S}-{random.randint(0, 0xffff):04x}"
    sk = f"{stamp.isoformat(timespec='seconds')}#{run_id}"
    try:
        acquire_lock(run_id, sk, t)
    except ddb.exceptions.ConditionalCheckFailedException:
        results(force=True)                     # marks the current run complete if it is
        try:
            acquire_lock(run_id, sk, now())
        except ddb.exceptions.ConditionalCheckFailedException:
            raise HttpError(409, "a run is already in progress; wait for it to finish or POST /reset")
    summary = {"pk": "RUN", "sk": sk, "run_id": run_id, "status": "starting", "started_at": round(t, 3),
               "workers": workers, "reject_percent": reject, "kill_workers": kills}
    ddb.put_item(TableName=RUNS_TABLE, Item=to_ddb(summary))
    lam.invoke(FunctionName=os.environ["AWS_LAMBDA_FUNCTION_NAME"], InvocationType="Event",
               Payload=json.dumps({"internal": "start_run", **summary}).encode())
    _cache.clear()
    return {"run_id": run_id, "status": "starting", "workers": workers, "reject_percent": reject,
            "kill_workers": kills}


def set_run_status(run_id, sk, status, **extra):
    ddb.update_item(
        TableName=RUNS_TABLE, Key=to_ddb({"pk": "RUN", "sk": sk}),
        UpdateExpression="SET #st = :s" + "".join(f", #{k} = :{k}" for k in extra),
        ExpressionAttributeNames={"#st": "status", **{f"#{k}": k for k in extra}},
        ExpressionAttributeValues=to_ddb({":s": status, **{f":{k}": v for k, v in extra.items()}}))
    try:
        ddb.update_item(
            TableName=RUNS_TABLE, Key=to_ddb(CONTROL_KEY), UpdateExpression="SET #st = :s",
            ConditionExpression="run_id = :r", ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=to_ddb({":s": status, ":r": run_id}))
    except ddb.exceptions.ConditionalCheckFailedException:
        pass                                   # a newer run (or a reset) owns the lock now


def run_async(ev):
    """Second half of /run, invoked asynchronously so the HTTP call returns right away."""
    run_id, sk = ev["run_id"], ev["sk"]
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
        set_run_status(run_id, sk, "failed", error="no customer records found; POST /load first")
        return {"run_id": run_id, "status": "failed"}

    for i in range(0, len(cases), 10):
        sqs.send_message_batch(QueueUrl=REBOOKING_QUEUE, Entries=[
            {"Id": str(j), "MessageBody": json.dumps({"run_id": run_id, "case_id": c, "pass": 1})}
            for j, c in enumerate(cases[i:i + 10])])

    victims = set(random.sample(range(ev["workers"]), ev["kill_workers"]))
    launched = []
    for i in range(ev["workers"]):
        wid = f"w{i + 1:02d}"
        payload = {"run_id": run_id, "worker_id": wid, "reject_percent": ev["reject_percent"]}
        if i in victims:
            payload["kill"] = {"after_claims": random.randint(2, 8),
                               "point": random.choice(["after_claim", "after_commit"])}
        lam.invoke(FunctionName=WORKER_FUNCTION, InvocationType="Event", Payload=json.dumps(payload).encode())
        launched.append(payload)
    set_run_status(run_id, sk, "running", records=len(cases), queued_at=round(t, 3))
    print(dumps({"run_id": run_id, "records": len(cases), "workers": launched}))
    return {"run_id": run_id, "records": len(cases), "workers": launched}


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


def results(force=False):
    if not force and _cache.get("t", 0) > now() - CACHE_S:
        return _cache["v"]
    t = now()
    runs = [from_ddb(i) for i in ddb.query(
        TableName=RUNS_TABLE, KeyConditionExpression="pk = :p",
        ExpressionAttributeValues=to_ddb({":p": "RUN"}), ScanIndexForward=False, Limit=10)["Items"]]
    records = scan_all(RECORDS_TABLE)
    out = {"generated_at": round(t, 3), "records_in_table": len(records), "recent_runs": runs,
           "queues": queue_depths(), "latest_run": None}
    if not runs:
        _cache.update(t=t, v=out)
        return out

    run = runs[0]
    run_id = run["run_id"]
    rows = [r for r in records if r.get("run_id") == run_id]
    workers = [from_ddb(i) for i in ddb.query(
        TableName=RUNS_TABLE, KeyConditionExpression="pk = :p",
        ExpressionAttributeValues=to_ddb({":p": f"RUN#{run_id}"}))["Items"]]
    gold = golden()

    status_counts, passes, table = {}, {}, []
    satisfactory = takeovers = 0
    for r in sorted(rows, key=lambda x: x[KEY]):
        st = r.get("status", "unknown")
        status_counts[st] = status_counts.get(st, 0) + 1
        hist = r.get("history", [])
        g = gold.get(r[KEY], {"acceptable": [], "unacceptable": set()})
        ok, bad = score(r, g)
        satisfactory += ok
        takeovers += sum(1 for h in hist if "from" in h)
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
            "queued_at": r.get("queued_at"), "resolved_at": r.get("resolved_at"),
        })

    open_rows = [r for r in rows if r.get("status") not in TERMINAL]
    queued = [r for r in open_rows if r.get("status") == "queued"]

    def oldest(rs):
        if not rs:
            return None
        o = min(rs, key=lambda r: float(r.get("queued_at", t)))
        return {"case_id": o[KEY], "status": o.get("status"), "pass_no": o.get("pass_no"),
                "waiting_s": round(t - float(o.get("queued_at", t)), 1)}

    alive = [w for w in workers if w.get("status") == "running" and t - float(w.get("heartbeat_at", 0)) < 15]
    worker_totals = {k: sum(int(w.get(k, 0)) for w in workers)
                     for k in ("claims", "completed", "takeovers", "fenced", "stale", "deferred", "repaired")}
    complete = bool(rows) and not open_rows and len(rows) == int(run.get("records", len(rows)))
    finished_at = max((float(r.get("resolved_at", 0)) for r in rows), default=None) if complete else None
    stats = {
        "records": len(rows), "status_counts": status_counts,
        "resolved": status_counts.get("resolved", 0), "human_review": status_counts.get("human_review", 0),
        "satisfactory": satisfactory, "open": len(open_rows),
        "passes_to_close": {str(k): passes[k] for k in sorted(passes)},
        "avg_passes": round(sum(k * v for k, v in passes.items()) / max(1, sum(passes.values())), 2),
        "oldest_unpicked": oldest(queued), "oldest_unresolved": oldest(open_rows),
        "takeovers_after_crash": takeovers, "workers_alive": len(alive), "worker_totals": worker_totals,
        "duration_s": round(finished_at - float(run["started_at"]), 1) if finished_at else
        round(t - float(run["started_at"]), 1),
    }
    status = run["status"]
    if status in ("starting", "running") and complete:
        status = "complete"
        set_run_status(run_id, run["sk"], "complete", finished_at=round(finished_at, 3),
                       final=json.loads(dumps(stats)))
        run["status"] = "complete"
    elif status == "running" and not alive and t - float(run["started_at"]) > LEASE_SECONDS + 30:
        status = "stalled"                         # open records, no live workers: shown, not hidden
    out["latest_run"] = {**run, "status": status, "stats": stats,
                         "workers": sorted(workers, key=lambda w: w.get("worker_id", "")), "records": table}
    _cache.update(t=t, v=out)
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
