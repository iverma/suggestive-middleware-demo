import hashlib
import shutil
from pathlib import Path

from aws_cdk import (
    CfnOutput, CustomResource, Duration, RemovalPolicy, Stack,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as sources,
    aws_logs as logs,
    aws_scheduler as scheduler,
    aws_scheduler_targets as scheduler_targets,
    aws_sqs as sqs,
)
from constructs import Construct

ROOT = Path(__file__).resolve().parents[2]
CSV = ROOT / "golden_set" / "golden_set_irops_msp.csv"
SRC = ROOT / "infra" / "experiment_control"
BUILD = ROOT / "infra" / ".build" / "experiment_control"
RUNTIME_FILES = ["common.py", "handler.py", "worker.py", "ui.html"]

# Free tier: 25 provisioned RCU + 25 provisioned WCU per account, across ALL tables.
# Provisioned capacity is what's billed, so these numbers ARE the cost ceiling: no
# workload can push DynamoDB past them, it gets throttled instead. Total here: 20 / 20.
RECORDS_RCU, RECORDS_WCU = 15, 15
RUNS_RCU, RUNS_WCU = 5, 5
PACE_WCU_PER_SEC = 12          # loads, resets and run start stay under the records table's 15

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
REBOOKING_MAX_WORKERS = 5
REROUTING_MAX_WORKERS = 3
CONTROL_FN = "effect-demo-experiment-control"
WORKER_FN = "effect-demo-worker"


def stage_runtime():
    """Bundle the code with the CSV so the Lambda asset is self-contained."""
    if BUILD.exists():
        shutil.rmtree(BUILD)
    BUILD.mkdir(parents=True)
    for f in RUNTIME_FILES:
        shutil.copy(SRC / f, BUILD / f)
    shutil.copy(CSV, BUILD / CSV.name)
    return hashlib.sha256(CSV.read_bytes()).hexdigest()[:16]


def provisioned_table(scope, cid, name, pk, sk=None, rcu=5, wcu=5):
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
    )


class CustomerRecordsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        records = provisioned_table(self, "CustomerRecordsDb", "customer-records-db", "case_id",
                                    rcu=RECORDS_RCU, wcu=RECORDS_WCU)
        runs = provisioned_table(self, "ExperimentRuns", "experiment-runs", "pk", "sk",
                                 rcu=RUNS_RCU, wcu=RUNS_WCU)

        # SQS: 1M requests/month always free. Queue-managed encryption is free.
        dlq = sqs.Queue(self, "DeadLetter", queue_name="effect-demo-dead-letter",
                        retention_period=Duration.days(1), encryption=sqs.QueueEncryption.SQS_MANAGED)
        redrive = sqs.DeadLetterQueue(queue=dlq, max_receive_count=10)
        queue_props = dict(visibility_timeout=Duration.seconds(VISIBILITY_SECONDS),
                           retention_period=Duration.hours(1), dead_letter_queue=redrive,
                           encryption=sqs.QueueEncryption.SQS_MANAGED)
        rebooking = sqs.Queue(self, "Rebooking", queue_name="effect-demo-rebooking", **queue_props)
        rerouting = sqs.Queue(self, "Rerouting", queue_name="effect-demo-rerouting", **queue_props)

        data_hash = stage_runtime()
        code = lambda_.Code.from_asset(str(BUILD))
        env = {
            "TABLE_NAME": records.table_name, "RUNS_TABLE": runs.table_name,
            "TABLE_WCU": str(RECORDS_WCU), "DEFAULT_WCU_PER_SEC": str(PACE_WCU_PER_SEC),
            "RUN_INIT_WCU_PER_SEC": str(PACE_WCU_PER_SEC), "LEASE_SECONDS": str(LEASE_SECONDS),
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
                    "Re-booking worker: invoked by SQS, claims records with fencing tokens", env)

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
        CfnOutput(self, "MaxWorkers", value=str(REBOOKING_MAX_WORKERS + REROUTING_MAX_WORKERS))
        CfnOutput(self, "RecordsLoaded", value=load.get_att_string("Loaded"))
        CfnOutput(self, "LoadWcuConsumed", value=load.get_att_string("WcuConsumed"))
        CfnOutput(self, "LoadWcuPerSecActual", value=load.get_att_string("WcuPerSecActual"))
        CfnOutput(self, "LoadSeconds", value=load.get_att_string("Seconds"))
        CfnOutput(self, "GoldenSetHash", value=data_hash)
