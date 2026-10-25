"""
CloudFormation custom resource that loads the golden-set CSV into the
customer records table.

Idempotent: each row is written with UpdateItem. Customer fields are
refreshed from the CSV, but an existing fencing_token is never reset,
so re-deploying can't clobber the state of a record a worker holds.
Columns from acceptable_resolutions onward are ground truth for tests and
are never loaded.
"""
import csv
import json
import os
import urllib.request
from decimal import Decimal

import boto3
from botocore.config import Config

TABLE = os.environ["TABLE_NAME"]
CSV_PATH = os.path.join(os.path.dirname(__file__), "golden_set_irops_msp.csv")
STOP_AT = "acceptable_resolutions"
KEY = "case_id"
NUMERIC = {
    "original_stops", "party_size", "adults", "children",
    "next_same_route_seat_hrs", "split_party_first_seats_hrs", "alt_route_stops",
    "alt_route_hrs", "partner_airline_hrs", "alt_airport_hrs", "ground_transport_hrs",
}

# Provisioned capacity is small (free tier), so lean on SDK retries for throttles.
ddb = boto3.client("dynamodb", config=Config(retries={"mode": "adaptive", "max_attempts": 25}))


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
    }


def load():
    n = 0
    for row in read_rows():
        ddb.update_item(**build_update(row))
        n += 1
    return n


def respond(event, context, status, data=None, reason=""):
    body = json.dumps({
        "Status": status,
        "Reason": reason or f"See CloudWatch log stream {context.log_stream_name}",
        "PhysicalResourceId": event.get("PhysicalResourceId", f"golden-set-load-{TABLE}"),
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": data or {},
    }).encode()
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT",
                                 headers={"Content-Type": "", "Content-Length": str(len(body))})
    urllib.request.urlopen(req, timeout=10)


def handler(event, context):
    print(json.dumps({k: event.get(k) for k in ("RequestType", "LogicalResourceId")}))
    try:
        if event["RequestType"] == "Delete":
            respond(event, context, "SUCCESS")  # table lifecycle is owned by the stack
            return
        loaded = load()
        print(f"loaded {loaded} records into {TABLE}")
        respond(event, context, "SUCCESS", {"Loaded": str(loaded)})
    except Exception as e:  # always answer CloudFormation, or the stack hangs for an hour
        print(f"load failed: {e!r}")
        respond(event, context, "FAILED", reason=str(e)[:500])
