"""
Reroute agent: one Claude Haiku call on Amazon Bedrock per reroute pass (pass 2+).

The worker hands over the customer's record, the note from their earlier call, the offers
they already rejected and the offers still allowed. The model picks the next offer and says why
in one line. The pick must be one of the allowed offers; anything else (an error, a timeout,
an unknown offer, unparseable output) returns None and the worker falls back to the rules
engine's order for that pass, so a slow or failing model never stalls a record.

Cost: about 400 input and 40 output tokens per call. Claude Haiku 5.5 via the global
inference profile is $0.10 / $0.50 per million tokens, so about $0.00006 a call.
"""
import json
import os
import re

import boto3
from botocore.config import Config

MODEL_ID = os.environ.get("AGENT_MODEL_ID", "global.anthropic.claude-haiku-5-5")
PRICE_IN, PRICE_OUT = 0.10, 0.50              # USD per million tokens (global profile)
_client = None

OFFER_TEXT = {
    "REBOOK_SAME_ROUTE": "a later flight on the original route",
    "REBOOK_ALTERNATE_AIRPORT": "a flight into a nearby alternate airport",
    "REBOOK_PARTNER_AIRLINE": "a seat on a partner airline",
    "REBOOK_ALTERNATE_ROUTE": "a different routing with extra connections",
    "ALTERNATE_GROUND_TRANSPORT": "ground transport (bus or car service) to the destination",
    "SPLIT_PARTY_REBOOK": "splitting the party across different flights",
    "CABIN_DOWNGRADE_WITH_FARE_DIFF_REFUND": "a lower cabin with the fare difference refunded",
    "TRAVEL_CREDIT": "travel credit for a future trip",
    "FULL_REFUND": "a full refund",
}
SYSTEM = ("You help an airline re-booking desk after a weather cancellation out of MSP. Pick the single "
          "next offer this customer is most likely to accept, using what they said and what they already "
          "turned down. Choose only from the allowed offers. Reply with JSON only: "
          '{"offer": "<OFFER_CODE>", "why": "<one short sentence>"}')
FIELDS = ("destination", "cabin", "party_size", "adults", "children", "travel_concerns", "urgency")


def client():
    global _client
    if _client is None:
        _client = boto3.client("bedrock-runtime", config=Config(
            connect_timeout=2, read_timeout=6, retries={"max_attempts": 2, "mode": "standard"}))
    return _client


def prompt(record, rejected, allowed):
    lines = ["Customer: " + ", ".join(f"{k}={record.get(k)}" for k in FIELDS if record.get(k) not in (None, "")),
             f"What they said on an earlier call: \"{record.get('customer_note', '(nothing on file)')}\"",
             "Already offered and rejected: " + (", ".join(rejected) or "nothing yet"),
             "Allowed offers:"]
    lines += [f"- {o}: {OFFER_TEXT.get(o, o)}" for o in allowed]
    return "\n".join(lines)


def recommend(record, rejected, allowed):
    """
    Returns (offer or None, why, usage). usage = {"in": tokens, "out": tokens, "error": str|None}.
    Never raises: the caller falls back to the rules order on None.
    """
    usage = {"in": 0, "out": 0, "error": None}
    try:
        resp = client().converse(
            modelId=MODEL_ID, system=[{"text": SYSTEM}],
            messages=[{"role": "user", "content": [{"text": prompt(record, rejected, allowed)}]}],
            inferenceConfig={"maxTokens": 120, "temperature": 0})
        u = resp.get("usage", {})
        usage.update({"in": int(u.get("inputTokens", 0)), "out": int(u.get("outputTokens", 0))})
        text = "".join(c.get("text", "") for c in resp["output"]["message"]["content"])
        m = re.search(r"\{.*\}", text, re.S)
        pick = json.loads(m.group(0)) if m else {}
        offer = str(pick.get("offer", "")).strip()
        if offer not in allowed:
            usage["error"] = f"offer not allowed: {offer[:60]}"
            return None, "", usage
        return offer, str(pick.get("why", ""))[:160], usage
    except Exception as e:                        # throttled, no model access, timeout, bad output
        usage["error"] = f"{type(e).__name__}: {e}"[:200]
        return None, "", usage


def cost(tokens_in, tokens_out):
    return round((tokens_in * PRICE_IN + tokens_out * PRICE_OUT) / 1e6, 4)
