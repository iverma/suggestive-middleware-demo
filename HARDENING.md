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
| **Workers autoscale from queue depth** (SQS event source mappings, 5 rebooking + 3 rerouting) | Capped below the account's 10-execution Lambda limit so the control functions can always run. |
| **Paced writers** | Run setup, golden loads and the load producer each stay under a write-unit budget, so the provisioned 25 WCU / 25 RCU (free tier) are never exceeded. |
| **Load producer with backpressure** | Releases at most 2 records/s and holds back while the backlog is above a cap. SQS depth is approximate, so the cap holds to within one 10-record block. It hands off to a fresh invocation every chunk, so a run can outlast one Lambda's 15-minute limit. A producer that dies is restarted from its cursor. |
| **Load runs never read their records per poll** | The UI and API read counters only. A paced finalizer scores the run once. |
| **TTL (24h) on all run data** | Storage stays flat no matter how many runs are made. |

## Resiliency

| Feature | Failure handled |
|---|---|
| **SQS redelivery + lease takeover** | A worker crashing at any of 8 points in its work. |
| **Retries with backoff** (botocore standard mode; message-level backoff on throttles) | DynamoDB/SQS throttling and transient errors. |
| **Dead-letter queue + sweeper** | Poison messages: after 5 deliveries the record goes to human review with a fenced write, in its own run's partition. |
| **Lost-handoff repair and orphan re-queue** | A record whose next message never got sent, or disappeared. A record whose message is in the dead-letter queue is left to the sweeper, so poison is never quietly re-sent. That gap was found live. |
| **Housekeeping** | Runs stuck starting, runs past their time limit, budget held by finished runs, dead load producers, worker mappings out of step. |
| **Alarms (CloudWatch → email)** | Backlog, message age, DLQ depth, DynamoDB capacity and throttles, Lambda errors, and a stuck-run heartbeat. |
| **Backups** | Point-in-time recovery is not enabled yet, because it isn't in the free tier (see later work). |

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
- **Load run**: 1,000/1,000 closed in about 9 minutes. 0 lost, 0 double results, 0 outcomes outside the rules. Backpressure held the producer back for 24s, and the producer handed off 8 times. DynamoDB peaked at 14 WCU/s (the alarm threshold is 16), and no alarm fired.
- **Split-brain (five clients)**: all five runs completed. The mid-flight cancel stopped its run, a double submit started exactly one run, and a stale cancel got a 409. After the start-adoption fix every run had its records queued within 28s. Re-running after the dead-letter fix.

## Later work

**Resiliency**
- **Multi-region**: DynamoDB global tables plus SQS queues and workers in a second region, with failover. This is left out of the demo because replicated writes and cross-region traffic aren't free.
- **Point-in-time recovery** on both tables (about $0.20/GB-month, under a cent for this data) and on-demand backups before schema changes.
- **AWS Budgets alert** when monthly spend passes $1.
- **Deploy during a run** (rolling Lambda versions with aliases). Judged low value for the demo.

**Scalability**
- On-demand or auto-scaled DynamoDB capacity, and a higher Lambda concurrency quota, to go past the free-tier ceilings. Throughput is currently bound by 20 WCU on the runs table, at about 7 WCU per record.
- A load test where backpressure always engages, which needs workers slower than the release rate (a fault knob for load runs, or a lower worker cap).

**Product**
- Later resolution passes that use an agent reasoning over customer booking history, after the rules-engine first pass.
- Idempotency keys on any real external side effect (customer notifications), since messages can be delivered more than once.
