import hashlib
import shutil
from pathlib import Path

from aws_cdk import (
    CfnOutput, CustomResource, Duration, RemovalPolicy, Stack,
    aws_dynamodb as dynamodb,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as sources,
    aws_logs as logs,
    aws_scheduler as scheduler,
    aws_scheduler_targets as scheduler_targets,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_sqs as sqs,
)
from constructs import Construct

ROOT = Path(__file__).resolve().parents[2]
CSV = ROOT / "golden_set" / "golden_set_irops_msp.csv"
SRC = ROOT / "infra" / "experiment_control"
BUILD = ROOT / "infra" / ".build" / "experiment_control"
RUNTIME_FILES = ["common.py", "handler.py", "analysis.py", "worker.py", "sweeper.py", "throttle.py", "generator.py", "agent.py", "ui.html"]

# Free tier: 25 provisioned RCU + 25 provisioned WCU per account, across ALL tables.
# Provisioned capacity is what's billed, so these numbers ARE the cost ceiling: no
# workload can push DynamoDB past them, it gets throttled instead. Total here: 20 / 20.
# customer-records-db only holds the customer inputs now (written by /load, read when a run
# starts); every run works on its own copy in experiment-runs, which takes the rest. 25 / 25.
RECORDS_RCU, RECORDS_WCU = 5, 5
RUNS_RCU, RUNS_WCU = 20, 20
LOAD_WCU_PER_SEC = 4           # /load into customer-records-db (5)
RUN_INIT_WCU_PER_SEC = 12      # copying records into a run (experiment-runs, 20)

# Admission budget shared by every kind of run (chaos, integration, later load tests).
MAX_ACTIVE_RUNS = 6
RECORD_BUDGET = 1300           # records in flight: one 1,000-record load test + three 100-record runs
RUN_TTL_S = 24 * 3600          # DynamoDB deletes run data (free) a day after the run started

# Load tests: generated, unique records. The producer releases them at a rate that keeps the
# experiment-runs table under the capacity alarm: ~7 write units per record (create 1, claim and
# commit per pass, and a transactional close that writes 2 items at 2x), so 2 records/s ~ 14-15
# of the 20 provisioned, under the 80% (16) alarm line. It also never lets the rebooking backlog
# grow past LOAD_BACKLOG_CAP, well under the backlog alarm.
LOAD_MAX_RECORDS = 1000
LOAD_RECORDS_PER_SEC = 2.0
LOAD_BACKLOG_CAP = 500
LOAD_CHUNK_S = 540             # a producer invocation hands off to a fresh one before its 10-min timeout
QUEUE_RETENTION_DAYS = 1       # was 1 hour: a backlog drained at ~2 records/s must never expire

ALARM_EMAIL = "verma.esha@gmail.com"
# Alarm thresholds derive from the limits above, so changing a limit moves its alarm.
ALARM_BACKLOG = 1000           # messages waiting in the rebooking queue
ALARM_MESSAGE_AGE_S = 600      # oldest message in a work queue (longest planned backoff is 300s)
ALARM_CAPACITY_SHARE = 0.8     # of the experiment-runs table's provisioned capacity
ALARM_NO_PROGRESS_S = 600      # an active run that closed nothing for this long is stuck

# Worker timing: a worker invoke handles one message in ~1-2s and can never outlive WORKER_TIMEOUT.
# The lease must be shorter than the visibility timeout, so that when SQS redelivers a crashed
# worker's message its lease has already expired; Lambda requires visibility >= function timeout.
# Timeout == visibility (20s) > lease (15s) leaves a 15-20s window where a worker is still alive
# but has lost its lease: that's where a zombie can exist, and where fencing has to stop it.
WORKER_TIMEOUT_S = 20
LEASE_SECONDS = 15
VISIBILITY_SECONDS = 20

# Autoscaling ceiling, per queue. The account's Lambda concurrency limit is 10 (new-account
# default) and is shared by every function, so workers get 8 and the UI/API keeps 2.
# After a quota increase, raise these: e.g. 20 and 10.
AGENT_MODEL_ID = "global.anthropic.claude-haiku-5-5"   # cheapest Claude on Bedrock ($0.10/$0.50 per M tokens)
LAMBDA_ACCOUNT_CONCURRENCY = 10   # this account's quota (see the AWS limits workflow)
REBOOKING_MAX_WORKERS = 5
REROUTING_MAX_WORKERS = 3
CONTROL_FN = "effect-demo-experiment-control"
LOAD_SCHEMA_VERSION = "2: inputs only; run fields live on each run's copy"
SWEEPER_FN = "effect-demo-sweeper"
# Deliveries before a message is dead-lettered (crashes, held-back duplicates and backoffs all
# count). 5 x 20s visibility puts a poison message in the DLQ within ~2 minutes.
MAX_RECEIVES = 5
WORKER_FN = "effect-demo-worker"


def stage_runtime():
    """Bundle the code with the CSV so the Lambda asset is self-contained."""
    if BUILD.exists():
        shutil.rmtree(BUILD)
    BUILD.mkdir(parents=True)
    for f in RUNTIME_FILES:
        shutil.copy(SRC / f, BUILD / f)
    shutil.copy(CSV, BUILD / CSV.name)
    # The schema version is part of the hash, so a change to what /load writes reloads on deploy.
    return hashlib.sha256(CSV.read_bytes() + LOAD_SCHEMA_VERSION.encode()).hexdigest()[:16]


def provisioned_table(scope, cid, name, pk, sk=None, rcu=5, wcu=5, ttl=None):
    return dynamodb.Table(
        scope, cid, table_name=name,
        partition_key=dynamodb.Attribute(name=pk, type=dynamodb.AttributeType.STRING),
        sort_key=dynamodb.Attribute(name=sk, type=dynamodb.AttributeType.STRING) if sk else None,
        billing_mode=dynamodb.BillingMode.PROVISIONED,   # on-demand is billed per request
        read_capacity=rcu, write_capacity=wcu,           # no auto scaling: it could exceed 25/25
        point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
            point_in_time_recovery_enabled=False),         # PITR is billed per GB
        encryption=dynamodb.TableEncryption.DEFAULT,      # AWS-owned key, no KMS charge
        removal_policy=RemovalPolicy.DESTROY,             # demo: tearing down leaves nothing billing
        time_to_live_attribute=ttl,                       # TTL deletes are free
    )


class CustomerRecordsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        records = provisioned_table(self, "CustomerRecordsDb", "customer-records-db", "case_id",
                                    rcu=RECORDS_RCU, wcu=RECORDS_WCU)
        runs = provisioned_table(self, "ExperimentRuns", "experiment-runs", "pk", "sk",
                                 rcu=RUNS_RCU, wcu=RUNS_WCU, ttl="ttl")

        # SQS: 1M requests/month always free. Queue-managed encryption is free.
        dlq = sqs.Queue(self, "DeadLetter", queue_name="effect-demo-dead-letter",
                        retention_period=Duration.days(1), encryption=sqs.QueueEncryption.SQS_MANAGED)
        redrive = sqs.DeadLetterQueue(queue=dlq, max_receive_count=MAX_RECEIVES)
        queue_props = dict(visibility_timeout=Duration.seconds(VISIBILITY_SECONDS),
                           retention_period=Duration.days(QUEUE_RETENTION_DAYS), dead_letter_queue=redrive,
                           encryption=sqs.QueueEncryption.SQS_MANAGED)
        rebooking = sqs.Queue(self, "Rebooking", queue_name="effect-demo-rebooking", **queue_props)
        rerouting = sqs.Queue(self, "Rerouting", queue_name="effect-demo-rerouting", **queue_props)

        data_hash = stage_runtime()
        code = lambda_.Code.from_asset(str(BUILD))
        env = {
            "TABLE_NAME": records.table_name, "RUNS_TABLE": runs.table_name,
            "TABLE_WCU": str(RUNS_WCU), "DEFAULT_WCU_PER_SEC": str(LOAD_WCU_PER_SEC),
            "RUN_INIT_WCU_PER_SEC": str(RUN_INIT_WCU_PER_SEC), "LEASE_SECONDS": str(LEASE_SECONDS),
            "MAX_ACTIVE_RUNS": str(MAX_ACTIVE_RUNS), "RECORD_BUDGET": str(RECORD_BUDGET),
            "RUN_TTL_S": str(RUN_TTL_S),
            "LOAD_MAX_RECORDS": str(LOAD_MAX_RECORDS), "LOAD_RECORDS_PER_SEC": str(LOAD_RECORDS_PER_SEC),
            "LOAD_BACKLOG_CAP": str(LOAD_BACKLOG_CAP), "LOAD_CHUNK_S": str(LOAD_CHUNK_S),
            "VISIBILITY_SECONDS": str(VISIBILITY_SECONDS),
            "REBOOKING_QUEUE_URL": rebooking.queue_url, "REROUTING_QUEUE_URL": rerouting.queue_url,
            "DLQ_URL": dlq.queue_url,
            "REBOOKING_MAX_WORKERS": str(REBOOKING_MAX_WORKERS), "REROUTING_MAX_WORKERS": str(REROUTING_MAX_WORKERS),
        }
        def fn(cid, name, handler, timeout, description, environment):
            log_group = logs.LogGroup(self, f"{cid}Logs", log_group_name=f"/aws/lambda/{name}",
                                      retention=logs.RetentionDays.ONE_WEEK, removal_policy=RemovalPolicy.DESTROY)
            f = lambda_.Function(
                self, cid, function_name=name, description=description,
                runtime=lambda_.Runtime.PYTHON_3_12, architecture=lambda_.Architecture.ARM_64,
                handler=handler, code=code, memory_size=256, timeout=timeout,
                environment=environment, log_group=log_group,
                retry_attempts=0,                 # async invokes are never retried
            )
            records.grant_read_write_data(f)
            runs.grant_read_write_data(f)
            for q in (rebooking, rerouting, dlq):
                q.grant_send_messages(f)
                q.grant_consume_messages(f)
            return f

        worker = fn("Worker", WORKER_FN, "worker.handler", Duration.seconds(WORKER_TIMEOUT_S),
                    "Re-booking worker: invoked by SQS, claims records with fencing tokens",
                    {**env, "ROLE": "worker", "AGENT_MODEL_ID": AGENT_MODEL_ID})
        # Reroute agent (runs started with agent=1): one Claude Haiku call per reroute pass, via the
        # global cross-region inference profile. Not free tier: about $0.00006 a call.
        worker.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=[f"arn:{self.partition}:bedrock:{self.region}:{self.account}:inference-profile/{AGENT_MODEL_ID}",
                       f"arn:{self.partition}:bedrock:*::foundation-model/{AGENT_MODEL_ID.split('.', 1)[1]}",
                       f"arn:{self.partition}:bedrock:::foundation-model/{AGENT_MODEL_ID.split('.', 1)[1]}"]))
        # A model's first call in an account subscribes to it through AWS Marketplace (no charge
        # for the subscription itself; usage is billed per token).
        worker.add_to_role_policy(iam.PolicyStatement(
            actions=["aws-marketplace:ViewSubscriptions", "aws-marketplace:Subscribe"], resources=["*"]))

        # Lambda autoscales the workers from queue depth, one message per invoke. Created switched
        # OFF: an enabled mapping long-polls its queue around the clock (~650k SQS requests/month
        # per queue at idle, against 1M free), so the control function enables them for a run only.
        mappings = {}
        for name, q, mx in (("REBOOKING", rebooking, REBOOKING_MAX_WORKERS),
                            ("REROUTING", rerouting, REROUTING_MAX_WORKERS)):
            src = sources.SqsEventSource(q, batch_size=1, enabled=False, max_concurrency=mx,
                                         report_batch_item_failures=True)
            worker.add_event_source(src)
            mappings[name] = src

        control_env = {**env, **{f"{n}_ESM_ID": m.event_source_mapping_id for n, m in mappings.items()}}
        control = fn("ExperimentControl", CONTROL_FN, "handler.handler", Duration.minutes(10),
                     "Experiment control: UI + /reset /load /run /results; switches worker scaling on/off",
                     control_env)
        for q in (rebooking, rerouting, dlq):
            q.grant_purge(control)
        control.add_to_role_policy(iam.PolicyStatement(
            actions=["lambda:UpdateEventSourceMapping", "lambda:GetEventSourceMapping"],
            resources=[m.event_source_mapping_arn for m in mappings.values()]))
        # Async self-invoke for the second half of /run. ARN from the name avoids a dependency cycle.
        control.add_to_role_policy(iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[f"arn:{self.partition}:lambda:{self.region}:{self.account}:function:{CONTROL_FN}"]))

        # Dead-letter sweeper: separate control-plane function with its own narrow role. Every
        # minute it routes records whose message was dead-lettered to HUMAN_REVIEW (fenced write).
        sweeper_logs = logs.LogGroup(self, "SweeperLogs", log_group_name=f"/aws/lambda/{SWEEPER_FN}",
                                     retention=logs.RetentionDays.ONE_WEEK, removal_policy=RemovalPolicy.DESTROY)
        sweeper = lambda_.Function(
            self, "Sweeper", function_name=SWEEPER_FN,
            description="Routes dead-lettered records to HUMAN_REVIEW with a fenced write",
            runtime=lambda_.Runtime.PYTHON_3_12, architecture=lambda_.Architecture.ARM_64,
            handler="sweeper.handler", code=code, memory_size=256, timeout=Duration.seconds(60),
            environment=env, log_group=sweeper_logs, retry_attempts=0,
        )
        dlq.grant_consume_messages(sweeper)
        runs.grant_read_write_data(sweeper)      # run copies live in experiment-runs
        scheduler.Schedule(
            self, "SweepDeadLetters", schedule_name="effect-demo-sweep-dead-letters",
            schedule=scheduler.ScheduleExpression.rate(Duration.minutes(1)),
            target=scheduler_targets.LambdaInvoke(sweeper, retry_attempts=0),
            description="Route dead-lettered records to a person (~43k invocations/month of 14M free)",
        )

        # Safety net: every 5 minutes, finish runs nobody is watching and switch idle mappings off.
        # EventBridge Scheduler: 14M invocations/month free; this uses ~8,600.
        scheduler.Schedule(
            self, "Housekeeping", schedule_name="effect-demo-housekeeping",
            schedule=scheduler.ScheduleExpression.rate(Duration.minutes(5)),
            target=scheduler_targets.LambdaInvoke(
                control, input=scheduler.ScheduleTargetInput.from_object({"internal": "housekeeping"}),
                retry_attempts=0),
            description="Finish unwatched runs; never leave worker mappings polling idle queues",
        )

        # Public HTTPS endpoint for the UI and API. Function URLs have no charge of their
        # own; each request is a normal (free-tier) Lambda invocation.
        url = control.add_function_url(auth_type=lambda_.FunctionUrlAuthType.NONE)

        # New logical ID: a custom resource can't switch to a different Lambda in place.
        load = CustomResource(
            self, "ExperimentGoldenSetLoad",
            service_token=control.function_arn,
            resource_type="Custom::ExperimentGoldenSetLoad",
            properties={"DataHash": data_hash},   # CSV change -> paced reload on next deploy
        )
        load.node.add_dependency(records)

        CfnOutput(self, "DemoUrl", value=url.url)
        CfnOutput(self, "TableName", value=records.table_name)
        CfnOutput(self, "RunsTableName", value=runs.table_name)
        CfnOutput(self, "ExperimentControlFunction", value=control.function_name)
        CfnOutput(self, "ExperimentControlArn", value=control.function_arn)
        CfnOutput(self, "WorkerFunction", value=worker.function_name)
        CfnOutput(self, "SweeperFunction", value=sweeper.function_name)
        CfnOutput(self, "MaxWorkers", value=str(REBOOKING_MAX_WORKERS + REROUTING_MAX_WORKERS))

        CfnOutput(self, "RecordsLoaded", value=load.get_att_string("Loaded"))
        CfnOutput(self, "LoadWcuConsumed", value=load.get_att_string("WcuConsumed"))
        CfnOutput(self, "LoadWcuPerSecActual", value=load.get_att_string("WcuPerSecActual"))
        CfnOutput(self, "LoadSeconds", value=load.get_att_string("Seconds"))
        CfnOutput(self, "GoldenSetHash", value=data_hash)

        self.add_alarms(records, runs, rebooking, rerouting, dlq, worker, sweeper, control)

    def add_alarms(self, records, runs, rebooking, rerouting, dlq, worker, sweeper, control):
        """
        Ten alarms (the CloudWatch free tier), each on a single metric, emailing through one SNS
        topic (email is free up to 1,000 a month). AWS emails a confirmation link first.
        """
        topic = sns.Topic(self, "Alarms", topic_name="effect-demo-alarms",
                          display_name="IROPS demo alarms")
        topic.add_subscription(subs.EmailSubscription(ALARM_EMAIL))
        minute, five = Duration.minutes(1), Duration.minutes(5)
        per_min_writes = RUNS_WCU * 60 * ALARM_CAPACITY_SHARE
        per_min_reads = RUNS_RCU * 60 * ALARM_CAPACITY_SHARE

        def alarm(cid, metric, threshold, periods, what, op=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD):
            a = cw.Alarm(self, cid, alarm_name=f"effect-demo-{cid}", metric=metric, threshold=threshold,
                         evaluation_periods=periods, comparison_operator=op, alarm_description=what,
                         treat_missing_data=cw.TreatMissingData.NOT_BREACHING)
            a.add_alarm_action(cw_actions.SnsAction(topic))
            a.add_ok_action(cw_actions.SnsAction(topic))
            return a

        alarm("dead-letters-waiting",
              dlq.metric_approximate_number_of_messages_visible(period=minute, statistic="Maximum"), 1, 5,
              "Dead letters for 5 minutes: the sweeper isn't routing them to a person")
        alarm("rebooking-stuck-messages",
              rebooking.metric_approximate_age_of_oldest_message(period=minute, statistic="Maximum"),
              ALARM_MESSAGE_AGE_S, 3, "A first-pass message has waited too long: workers stuck or off")
        alarm("rerouting-stuck-messages",
              rerouting.metric_approximate_age_of_oldest_message(period=minute, statistic="Maximum"),
              ALARM_MESSAGE_AGE_S, 3, "A re-route message has waited too long: workers stuck or off")
        alarm("rebooking-backlog",
              rebooking.metric_approximate_number_of_messages_visible(period=minute, statistic="Maximum"),
              ALARM_BACKLOG, 3, "Backlog above the producer's cap: backpressure isn't holding")
        alarm("runs-table-write-capacity",
              runs.metric_consumed_write_capacity_units(period=minute, statistic="Sum"), per_min_writes, 5,
              f"experiment-runs writes above {int(ALARM_CAPACITY_SHARE * 100)}% of {RUNS_WCU} WCU for 5 minutes")
        alarm("runs-table-read-capacity",
              runs.metric_consumed_read_capacity_units(period=minute, statistic="Sum"), per_min_reads, 5,
              f"experiment-runs reads above {int(ALARM_CAPACITY_SHARE * 100)}% of {RUNS_RCU} RCU for 5 minutes")
        alarm("runs-table-throttled",
              cw.Metric(namespace="AWS/DynamoDB", metric_name="WriteThrottleEvents",
                        dimensions_map={"TableName": runs.table_name}, period=minute, statistic="Sum"),
              1, 1, "DynamoDB itself throttled writes to experiment-runs (real, not injected)")
        alarm("worker-throttled",
              worker.metric_throttles(period=five, statistic="Sum"), 1, 1,
              "Worker invocations throttled: the account's Lambda concurrency limit was hit")
        alarm("worker-errors",
              worker.metric_errors(period=five, statistic="Sum"), 20, 1,
              "20+ worker invocations failed in 5 minutes (expected during heavy chaos tests)")
        alarm("run-not-progressing",
              cw.Metric(namespace="IropsDemo", metric_name="SecondsSinceRunProgress", period=five,
                        statistic="Maximum"),
              ALARM_NO_PROGRESS_S, 1, "An active run closed no record for 10 minutes: stuck records")
        CfnOutput(self, "AlarmTopic", value=topic.topic_arn)
