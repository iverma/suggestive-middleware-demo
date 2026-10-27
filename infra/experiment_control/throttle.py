"""
Throttle injection: a free stand-in for AWS Fault Injection Service.

FIS costs $0.10 per action-minute, and its API throttle/error actions only target EC2 and
Kinesis, not DynamoDB or SQS. Instead, this hooks the SDK's HTTP layer ("before-send"):
for a share of calls it returns a real HTTP 400 throttling response, in the exact wire
format each service uses, instead of sending the request. Everything after that is the
genuine code path: botocore parses the error, classifies it as throttling, retries with
exponential backoff and jitter, adaptive mode starts rate-limiting the client, and if
retries run out the exception reaches the worker, which backs the message off.

Per-invoke counters record what was injected and how many retries the SDK spent.
"""
import json
import random
import threading
import uuid

from botocore.awsrequest import AWSResponse

_state = threading.local()


def _s():
    if not hasattr(_state, "percent"):
        _state.percent, _state.injected, _state.retries = 0.0, 0, 0
    return _state


def configure(percent):
    """Set this invoke's injection rate (0 turns it off) and reset its counters."""
    s = _s()
    s.percent, s.injected, s.retries = float(percent or 0), 0, 0


def pause():
    """Stop injecting for the rest of this invoke but keep the counters (for bookkeeping writes)."""
    _s().percent = 0.0


def counters():
    s = _s()
    return {"throttles_injected": s.injected, "sdk_retries": s.retries}


class _Raw:
    def __init__(self, body):
        self._body = body

    def stream(self, **_):
        yield self._body

    def read(self, *_):
        return self._body


def _throttle_response(request, model, service):
    rid = str(uuid.uuid4())
    if service == "dynamodb":
        body = json.dumps({"__type": "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
                           "message": "The level of configured provisioned throughput for the table was exceeded."})
        headers = {"x-amzn-RequestId": rid, "Content-Type": "application/x-amz-json-1.0"}
    elif model.service_model.protocol == "query":                  # older SQS wire format
        body = ("<ErrorResponse><Error><Type>Sender</Type><Code>Throttling</Code>"
                "<Message>Rate exceeded</Message></Error><RequestId>%s</RequestId></ErrorResponse>" % rid)
        headers = {"x-amzn-RequestId": rid, "Content-Type": "text/xml"}
    else:                                                         # SQS JSON protocol
        body = json.dumps({"__type": "com.amazonaws.sqs#ThrottlingException", "message": "Rate exceeded"})
        headers = {"x-amzn-RequestId": rid, "Content-Type": "application/x-amz-json-1.0",
                   "x-amzn-query-error": "Throttling;Sender"}
    return AWSResponse(request.url, 400, headers, _Raw(body.encode()))


def install(client):
    """Attach the hooks to a boto3 client (DynamoDB or SQS). Safe to call once per client."""
    service = client.meta.service_model.service_name

    def before_send(request, **kw):
        s = _s()
        if s.percent and random.random() * 100 < s.percent:
            s.injected += 1
            return _throttle_response(request, kw.get("model") or _Model(client), service)
        return None

    def after_call(http_response=None, parsed=None, **_):
        if isinstance(parsed, dict):
            _s().retries += int(parsed.get("ResponseMetadata", {}).get("RetryAttempts", 0))

    client.meta.events.register_first(f"before-send.{service}", before_send)
    client.meta.events.register(f"after-call.{service}", after_call)


class _Model:
    def __init__(self, client):
        self.service_model = client.meta.service_model
