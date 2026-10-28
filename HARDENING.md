# Hardening: concurrency, scalability, resiliency

This page covers what the demo does to stay correct when workers crash, messages repeat, and
several runs share one deployment. It also lists how each part is tested and what is left for
later. The whole system runs inside the AWS free tier, and some limits below exist because of that.

## Architecture at a glance

```
UI / API (Lambda URL) ──► experiment-control ──► SQS rebooking ─┐
                              │                  SQS rerouting ─┼─► workers (Lambda, autoscaled)
                              │                  SQS DLQ ───────┴─► sweeper (every minute)
                              ▼
                   DynamoDB: customer-records-db (inputs), experiment-runs (one partition per run)
                   EventBridge Scheduler: housekeeping (every 5 min), sweeper (every 1 min)
                   CloudWatch: 10 alarms → SNS email
```

## Multi-tenancy

Every run is a tenant. Chaos, integration and load runs, from any number of browser tabs or
clients, share one deployment: the same queues, workers, Lambda concurrency and DynamoDB
capacity. Each tenant still gets its own data, settings and results.

| Guarantee | How |
|---|---|
| **Own data** | Each run copies the records into its own partition (`RUN#<id>`) and works only there. Runs never read or overwrite each other's records, and a finished run's data can't be changed by a later one. |
| **Own settings** | Rejection, crash, slow, duplicate, poison and throttle settings travel on each run's own messages, so one run's faults never apply to another. |
| **Fair share of workers** | Messages carry the run id as their group (SQS fair queues), so a big tenant (a 1,000-record load test) can't starve a small one. |
| **Shared budget** | Admission control caps active runs (6) and records (1,300) across all tenants. Requests over the cap get a 429. The budget is released when a run finishes, fails or is cancelled. |
| **Scoped actions** | Cancel stops only its own run. A repeated start with the same client token returns the first run, and a stale client gets a 409 for a run that already finished. |
| **Scoped recovery** | The sweeper, orphan re-queue and lost-handoff repair write only into the partition of the run that owns the record. |
| **Visible in the UI** | The multi-tenancy panel shows each active run as a tenant: its settings, progress, share of the record budget and closing rate, next to the shared meters. |

How it's tested: the mocked suite covers 20 simultaneous starts, budget limits, cancel isolation, double submit and cancelled-run message drops. The live split-brain check runs five clients at once (see the latest live results).

## Concurrency

| Feature | What it protects against |
|---|---|
| **Leases + fencing tokens** on every record | Two workers acting on one record. A worker that comes back after its lease expired (a "zombie") has its write rejected. |
| **Lease (15s) < visibility timeout (20s) = worker timeout** | A message reappearing while its worker could still be writing. |
| **Exactly-once counters** (record close + run counters in one DynamoDB transaction) | Progress being counted twice, or a record closing without being counted. |
| **Own-write recognition** | A write that landed but whose response was lost being treated as a failure. |
| **Idempotent run starts** (`client_token`) | Double clicks and client retries starting duplicate runs. |
| **Per-run partitions** (`RUN#<id>`) and fair queues (`MessageGroupId = run`) | Runs reading or writing each other's records, and one big run starving the others. |
| **Admission control**: at most 6 active runs and 1,300 records, shared by chaos, integration and load runs | Too much work admitted for the provisioned capacity. |
| **Conditional status transitions** | Cancel racing finish, two finishers, or stale clients changing a finished run. |
| **Run set-up adoption** | Throttled start invocations. Workers can hold 8 of the account's 10 Lambdas, so with several simultaneous starts some start invocations wait minutes for Lambda's retries. Set-up is claimed per run, and the first control invocation that runs sets up every waiting run, one after another within the write budget. This was found by the live five-client check. |

## Scalability

| Feature | Notes |
|---|---|
| **Workers autoscale from queue depth** (SQS event source mappings, 3 first-pass + 3 reroute) | Capped well below the account's 10 concurrent Lambdas, leaving room for load producers, the UI and the sweeper. With 8 workers, two side-by-side load runs used all 10, and the UI's requests were throttled ("Too Many Requests", found live). The UI now also waits and retries quietly on a 429. |
| **Paced writers** | Run setup, golden loads and the load producer each stay under a write-unit budget, so the provisioned 25 WCU / 25 RCU (free tier) are never exceeded. |
| **Load producer with backpressure** | Releases at most 2 records/s and holds back while the backlog is above a cap. SQS depth is approximate, so the cap holds to within one 10-record block. It hands off to a fresh invocation every chunk, so a run can outlast one Lambda's 15-minute limit. A producer that dies is restarted from its cursor. |
| **Load runs never read their records per poll** | Live counters come from the run's summary item. The full view (workers over time, incidents, crash and race tables, workers, the 200 most notable records) is rebuilt about once a minute by one paced background read and stored compressed (about 11 KB). Every viewer reads that one item, so the cost of viewing doesn't grow with the run's size or the number of viewers. |
| **TTL (24h) on all run data** | Storage stays flat no matter how many runs are made. |

## Resiliency

| Feature | Failure handled |
|---|---|
| **SQS redelivery + lease takeover** | A worker crashing at any of 8 points in its work. |
| **Retries with backoff** (botocore standard mode; message-level backoff on throttles) | DynamoDB/SQS throttling and transient errors. |
| **Dead-letter queue + sweeper** | Poison messages: after 5 deliveries the record goes to human review with a fenced write, in its own run's partition. |
| **Lost-handoff repair and orphan re-queue** | A record whose next message never got sent, or disappeared. A record whose message is in the dead-letter queue is left to the sweeper, so poison is never quietly re-sent. That gap was found live. |
| **Housekeeping** | Runs stuck starting, runs past their time limit, budget held by finished runs, dead load producers, worker mappings out of step, and a drifted record budget. The admitted-record counter is recounted from the runs actually active and corrected (found live: it drifted 1,000 high and refused new runs). |
| **Alarms (CloudWatch → email)** | Backlog, message age, DLQ depth, DynamoDB capacity and throttles, Lambda errors, and a stuck-run heartbeat. |
| **Backups** | None in the demo. Point-in-time recovery isn't in the free tier (see production-level enhancements). |

## Testing

| Layer | What runs | Where |
|---|---|---|
| **Mocked end-to-end** (~30 tests, moto + simulated SQS pollers) | Crashes at every kill point, zombies, duplicates, poison, injected throttles, lost handoffs, orphans, 20 simultaneous starts, budget limits, cancel isolation, double submit, load producer backpressure, handoff and restart. | Every push (`Deploy` workflow, before deploy) |
| **Infrastructure template tests** (CDK assertions) | Alarm wiring and thresholds, capacity within free tier, ESM concurrency below the account limit, lease < visibility, retention, schedules. | Every push |
| **Live chaos check** | A real run with crashes, slow workers, duplicates, poison and throttles. Fails on any lost or open record. | `Live check` workflow |
| **Live split-brain check** | Five clients at once with different settings, a mid-flight cancel, poison swept only in its own run, a double submit, a stale cancel, and budget released afterwards. | `Live check` workflow |
| **Live load check** | 1,000 unique generated records. Checks for no loss, no double results, outcomes within the rules, backlog under its cap, producer handoffs, no alarm fired, DynamoDB under 80% of capacity and no throttles. | `Load check` workflow |

## Latest live results (deployed stack, us-west-2)

- **Chaos run**: 100/100 closed. 21 crashes at all 8 kill points recovered, 68 injected throttles absorbed by retries, 0 records with two results.
- **Load run**: 1,000/1,000 closed in about 9 minutes, with the full view refreshing about once a minute. 0 lost, 0 double results, 0 outcomes outside the rules. The producer only released into a backlog within its cap (10), was held back 20s, and handed off 8 times. DynamoDB peaked at 14.7 WCU/s (the alarm threshold is 16), and no alarm fired.
- **Split-brain (five clients)**: all five runs completed. The run cancelled mid-flight stopped. Poison was swept only within its own runs (5 and 11 records), with none in the runs without poison. A double submit started exactly one run, and a stale cancel got a 409. Every run had its records queued within 27s and closed its first record within 79s of queueing.

## Reroute agent (Claude Haiku on Bedrock)

On reroute passes, runs started with `agent=1` ask Claude Haiku 4.5 on Amazon Bedrock which offer to make next. The pick is validated against the allowed offers. Any error, timeout, throttle or invalid pick falls back to the rules order, so the agent never stalls a record and never weakens the fencing or exactly-once guarantees. The architecture, demo scenario, cost and production plan are in [agentic_rebooking.md](agentic_rebooking.md).

## Production-level enhancements (not in the demo because they cost money)

| Enhancement | Why it's needed in production | Rough cost |
|---|---|---|
| **Multi-region**: DynamoDB global tables, SQS queues and workers in a second region, with failover | A regional outage shouldn't stop re-booking | Replicated writes, a second stack, cross-region traffic |
| **Point-in-time recovery** on both tables, plus on-demand backups before schema changes | Recovery from bad writes or deletes, to any second in the last 35 days | $0.20/GB-month (under a cent for this data) |
| **On-demand or auto-scaled DynamoDB capacity** | The runs table's 20 WCU caps throughput at about 2 records/s (about 7 WCU per record), which is why 1,000 records take about 9 minutes | On-demand is billed per request |
| **Higher Lambda concurrency quota and reserved concurrency** for the control plane | The account's 10 concurrent executions are shared by workers and control functions; the start-adoption fix works around this | Quota increase, no direct charge |
| **AWS Budgets alert** on monthly spend | Catch unexpected charges early | Little or none for a single spend budget |
| **Deploy during a run**: Lambda versions and aliases with gradual rollout | Ship without pausing work in progress | Mostly no added cost |

## Later work (no added cost)

- A load test where backpressure always engages, which needs workers slower than the release rate (a fault knob for load runs, or a lower worker cap).
- Idempotency keys on any real external side effect (customer notifications), since messages can be delivered more than once.
