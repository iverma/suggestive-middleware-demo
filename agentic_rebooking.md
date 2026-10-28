# Agentic rebooking: a live model on the reroute path

When a customer turns down an offer, the record goes back on the queue for a reroute. In agent
runs, each reroute asks **Claude Haiku 4.5 on Amazon Bedrock** which offer to make next, instead of
taking the next one in the rules engine's fixed order. The goal of the demo is to show that a
reasoning model, reading what the customer said, closes records in fewer reroutes than the rules
engine, without giving up any of the system's safety guarantees.

## Flow

```
pass 1:  rules engine ──► first offer ──► customer accepts? ── yes ──► resolved
                                              │ no
                                              ▼
pass 2+: worker ──► Bedrock (Claude Haiku 4.5) ──► next offer ──► customer accepts? ── yes ──► resolved
           │            │                                         │ no
           │            └─ error / timeout / invalid pick         ▼
           │               → next offer in rules order        reroute again (until the list is
           │                                                  used up → human review)
           └─ record, customer's note, offers already rejected, offers still allowed
```

- **Pass 1 always comes from the rules engine.** It's instant and free, and it's right for many customers.
- **Pass 2 and later, in agent runs:** the worker sends the model one short prompt with:
  - the customer's record: destination, cabin, party, children, travel concerns, urgency;
  - the note from their earlier call;
  - the offers they already rejected;
  - the offers still allowed, each with a one-line description.

  The model replies with JSON naming the next offer and a one-sentence reason.
- **Guardrail:** the pick must be one of the allowed offers. The model can reorder the menu but never add to it.
- **Fallback:** an error, a timeout (about 6s), throttling, a denied call or an unparseable or invalid pick all fall back to the next offer in the rules order for that pass. The fallback is counted, and its reason is saved on the record. A slow or unavailable model never stalls a record.

## How it fits the existing safety design

| Concern | What happens |
|---|---|
| A worker crashes after the model call | The lease expires and another worker retakes the record and calls the model again. The pass commits once, under the fencing token, so there's never a double result. The cost of a repeated call is a fraction of a cent. |
| Model output can't be trusted | It's validated against the allowed offers before it's used. Anything else is a fallback, not an error. |
| Cost and write capacity | The call's token counts and the model's reason ride on the pass's history entry, so there are no extra database writes. Run totals are summed when the run is scored. |
| Bedrock throttling or no model access | It falls back to the rules order. This was seen live: before model access was granted, all 1,593 calls were denied, and both 1,000-record arms still closed every record with 0 lost and 0 double results. |
| The model call is the work | On agent passes the worker skips its simulated "inventory search" delay; the real model call takes that time instead. |

## The demo scenario (synthetic, built to make the effect visible)

The 100 golden records close in 2–3 reroutes no matter what, so the comparison uses generated
load customers instead (`customer=preference`):

- **Hidden preference:** each customer will accept exactly one offer from their list, never the rules engine's first choice. These are the customers the rules engine ranks wrong. Neither the rules engine nor the model sees the preference.
- **Customer note:** about 70% of customers hinted at their preference on an earlier call ("I don't care which airline it is, I just need to get there today"). The other 30% left a vague note ("This is the second cancellation this month, I'm frustrated").
- **Rules arm:** walks the offers in the rules engine's order.
- **Agent arm:** reads the note and what was already rejected, and jumps ahead.
- **Same customers in both arms:** both use the same generated customers (same seed), so the arms differ only in how reroutes are chosen.

**Expected result** (from the mocked suite, with a stand-in model that follows the hints, not a live model): the agent closes about 80% of rerouted records on their first reroute, against about 45% for rules, at about 2.1 passes per record against 2.5. The live numbers depend on how well the model reads the notes.

## Running it

- **UI:** start "Run rules" and "Run agent" (500 customers each). They run side by side as two tenants. The "Rules vs agent" panel compares:
  - average passes per record;
  - the share closed on the first reroute;
  - the share sent to a person;
  - average and p90 time to close a record;
  - model calls and fallbacks;
  - tokens and estimated cost.

  Each record's detail shows the model's one-line reason for every offer it picked.
- **API:** `POST /run?kind=load&customer=preference&agent=1&records=500`. Use `agent=0` for the rules arm.
- **GitHub Actions:** the "Agent compare" workflow (manual only) runs both arms side by side on the deployed stack and prints both summaries.

## Capacity and timing

- **Release rate:** customers with a preference need about 2.6 passes and 10–13 write units per record (measured live). Each arm releases 0.7 records/s, so the two arms together stay under 80% of the runs table's 20 WCU. A pair of 500-record arms takes about 12–15 minutes.
- **Lambda concurrency:** workers are capped at 6 (3 first-pass, 3 reroute) of the account's 10 concurrent Lambdas. That leaves room for the two arms' producers, the UI and the sweeper. With 8 workers, the live runs used all 10, and the UI's requests got throttled ("Too Many Requests"). The UI now also waits and retries quietly when that happens.

## Cost (the only part of the demo outside the free tier)

| Item | Rate | Per 500-record agent arm |
|---|---|---|
| Claude Haiku 4.5, global inference profile | $1 per M input, $5 per M output tokens | about 400 in + 40 out tokens per call ≈ $0.0006; 500–800 calls ≈ **$0.30–0.50** |
| Rules arm | none | $0 |
| DynamoDB, SQS, Lambda | within the free tier | $0 |

Claude Haiku 5.5 would be ten times cheaper ($0.10 / $0.50 per M tokens), but it isn't available to this account. Switching models is one setting (`AGENT_MODEL_ID` in the stack).

## Production version (not built)

- **Customer history from the customer DB:** past trips, past offers accepted and declined, and complaints, read by `customer_id`.
- **Semantic search over past cases:** DynamoDB vector search (on-demand tables only) over embedded past complaints and what finally resolved them, for example with Titan Text Embeddings V2. The top matches go into the prompt as "what worked for customers with similar concerns".
- **A/B split per customer:** split by hashing the customer id, with both arms visible side by side, ramped toward the agent as it proves itself, and with a kill switch.
- **Golden-set eval:** score the model's picks against each golden record's ordered acceptable outcomes before every model or prompt change.
- **Prompt caching** for the fixed instructions and offer descriptions, plus a per-run cap on model calls and a monthly budget alert.
- **Idempotency key** of `(case_id, pass)` on the offer sent to the customer, since a takeover can re-send it.
