"""
Experiment control Lambda for the IROPS re-booking demo.

Today it does one thing: load the golden-set CSV into customer-records-db,
paced so the load never consumes more than a chosen number of write capacity
units per second. Later experiment actions (reset, start run, ...) belong here.

Pacing is driven by the ConsumedCapacity DynamoDB reports for each write, not
by row count, so it stays correct if items ever grow past 1 KB.

Invocation:
  - Automatically on deploy (CloudFormation custom resource, Create/Update).
  - By hand, e.g. Lambda console "Test" with:
        {"action": "load", "wcu_per_sec": 5}
    wcu_per_sec is optional (default DEFAULT_WCU_PER_SEC) and is capped at the
    table's provisioned write capacity.

Each row is written with UpdateItem: customer fields are refreshed from the
CSV, but an existing fencing_token is never reset, so a load can't clobber a
record a worker currently holds. Columns from acceptable_resolutions onward
are ground truth for tests and are never loaded.
"""
import csv
import json
import os
import time
import urllib.request
from decimal import Decimal

import boto3
from botocore.config import Config

TABLE = os.environ["TABLE_NAME"]
TABLE_WCU = float(os.environ["TABLE_WCU"])
DEFAULT_WCU_PER_SEC = float(os.environ.get("DEFAULT_WCU_PER_SEC", "8"))
CSV_PATH = os.path.join(os.path.dirname(__file__), "golden_set_irops_msp.csv")
STOP_AT = "acceptable_resolutions"
KEY = "case_id"
NUMERIC = {
    "original_stops", "party_size", "adults", "children",
    "next_same_route_seat_hrs", "split_party_first_seats_hrs", "alt_route_stops",
    "alt_route_hrs", "partner_airline_hrs", "alt_airport_hrs", "ground_transport_hrs",
}

# Retries stay on as a safety net; pacing should mean they never fire.
ddb = boto3.client("dynamodb", config=Config(retries={"mode": "standard", "max_attempts": 10}))


# --- CSV -> DynamoDB ---------------------------------------------------------
def to_attr(col, val):
    return {"N": str(Decimal(val))} if col in NUMERIC else {"S": val}


def read_rows(path=CSV_PATH):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        cols = header[: header.index(STOP_AT)]
        for raw in reader:
            yield dict(zip(cols, raw[: len(cols)]))


def build_update(row):
    names, values, sets, removes = {}, {}, [], []
    for i, (col, val) in enumerate(row.items()):
        if col == KEY:
            continue
        names[f"#c{i}"] = col
        if val == "":
            removes.append(f"#c{i}")
        else:
            values[f":v{i}"] = to_attr(col, val)
            sets.append(f"#c{i} = :v{i}")
    names["#ft"] = "fencing_token"
    values[":zero"] = {"N": "0"}
    sets.append("#ft = if_not_exists(#ft, :zero)")
    expr = "SET " + ", ".join(sets)
    if removes:
        expr += " REMOVE " + ", ".join(removes)
    return {
        "TableName": TABLE,
        "Key": {KEY: {"S": row[KEY]}},
        "UpdateExpression": expr,
        "ExpressionAttributeNames": names,
        "ExpressionAttributeValues": values,
        "ReturnConsumedCapacity": "TOTAL",
    }


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


def load(wcu_per_sec=None):
    rate = min(float(wcu_per_sec or DEFAULT_WCU_PER_SEC), TABLE_WCU)
    if rate <= 0:
        raise ValueError("wcu_per_sec must be > 0")
    pacer = WcuPacer(rate)
    n = 0
    for row in read_rows():
        resp = ddb.update_item(**build_update(row))
        pacer.record(resp.get("ConsumedCapacity", {}).get("CapacityUnits", 1.0))
        n += 1
    secs = pacer.elapsed()
    result = {
        "loaded": n,
        "wcu_consumed": round(pacer.used, 2),
        "wcu_per_sec_budget": rate,
        "wcu_per_sec_actual": round(pacer.used / secs, 2) if secs else None,
        "seconds": round(secs, 1),
        "table": TABLE,
    }
    print(json.dumps(result))
    return result


# --- Entry points ------------------------------------------------------------
def respond(event, context, status, data=None, reason=""):
    body = json.dumps({
        "Status": status,
        "Reason": reason or f"See CloudWatch log stream {context.log_stream_name}",
        "PhysicalResourceId": event.get("PhysicalResourceId", f"experiment-load-{TABLE}"),
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": data or {},
    }).encode()
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT",
                                 headers={"Content-Type": "", "Content-Length": str(len(body))})
    urllib.request.urlopen(req, timeout=10)


def on_cloudformation(event, context):
    try:
        if event["RequestType"] == "Delete":
            respond(event, context, "SUCCESS")  # table lifecycle is owned by the stack
            return
        r = load()
        respond(event, context, "SUCCESS",
                {"Loaded": str(r["loaded"]), "WcuConsumed": str(r["wcu_consumed"]),
                 "WcuPerSecActual": str(r["wcu_per_sec_actual"]), "Seconds": str(r["seconds"])})
    except Exception as e:  # always answer CloudFormation, or the stack hangs for an hour
        print(f"load failed: {e!r}")
        respond(event, context, "FAILED", reason=str(e)[:500])


ACTIONS = {"load": lambda ev: load(ev.get("wcu_per_sec"))}


def handler(event, context):
    if "RequestType" in event and "ResponseURL" in event:
        return on_cloudformation(event, context)
    action = event.get("action")
    if action not in ACTIONS:
        return {"error": f"unknown action {action!r}", "actions": sorted(ACTIONS)}
    return ACTIONS[action](event)
