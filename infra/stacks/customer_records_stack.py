import hashlib
import shutil
from pathlib import Path

from aws_cdk import (
    CfnOutput, CustomResource, Duration, RemovalPolicy, Stack,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_logs as logs,
)
from constructs import Construct

ROOT = Path(__file__).resolve().parents[2]
CSV = ROOT / "golden_set" / "golden_set_irops_msp.csv"
CONTROL_SRC = ROOT / "infra" / "experiment_control"
CONTROL_BUILD = ROOT / "infra" / ".build" / "experiment_control"

# Free tier: 25 provisioned RCU + 25 provisioned WCU per account, across ALL tables.
# Keep this table well under so later tables (queue, ledger) still fit.
READ_CAPACITY = 10
WRITE_CAPACITY = 10
# Loads are paced below the table's write capacity so concurrent workers keep headroom
# and the load itself never gets throttled. Provisioned capacity is what's billed, so
# this is about clean experiments, not cost: the table can never exceed 10 WCU.
LOAD_WCU_PER_SEC = 8


def stage_experiment_control():
    """Bundle the handler with the CSV so the Lambda asset is self-contained."""
    if CONTROL_BUILD.exists():
        shutil.rmtree(CONTROL_BUILD)
    CONTROL_BUILD.mkdir(parents=True)
    shutil.copy(CONTROL_SRC / "handler.py", CONTROL_BUILD / "handler.py")
    shutil.copy(CSV, CONTROL_BUILD / CSV.name)
    return hashlib.sha256(CSV.read_bytes()).hexdigest()[:16]


class CustomerRecordsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        table = dynamodb.Table(
            self, "CustomerRecordsDb",
            table_name="customer-records-db",
            partition_key=dynamodb.Attribute(name="case_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PROVISIONED,   # on-demand is billed per request
            read_capacity=READ_CAPACITY,
            write_capacity=WRITE_CAPACITY,
            # no auto scaling: it could raise capacity past the free 25/25
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=False),         # PITR is billed per GB
            encryption=dynamodb.TableEncryption.DEFAULT,      # AWS-owned key, no KMS charge
            removal_policy=RemovalPolicy.DESTROY,             # demo: tearing down leaves nothing billing
        )

        data_hash = stage_experiment_control()

        log_group = logs.LogGroup(
            self, "ExperimentControlLogs",
            log_group_name="/aws/lambda/effect-demo-experiment-control",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        control = lambda_.Function(
            self, "ExperimentControl",
            function_name="effect-demo-experiment-control",
            description="Experiment control: paced golden-set load (more actions later)",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(CONTROL_BUILD)),
            memory_size=256,
            timeout=Duration.minutes(10),   # 500 rows at 8 WCU/s is ~63s; plenty of room
            environment={
                "TABLE_NAME": table.table_name,
                "TABLE_WCU": str(WRITE_CAPACITY),
                "DEFAULT_WCU_PER_SEC": str(LOAD_WCU_PER_SEC),
            },
            log_group=log_group,
        )
        table.grant_write_data(control)

        # New logical ID: a custom resource can't switch to a different Lambda in place.
        load = CustomResource(
            self, "ExperimentGoldenSetLoad",
            service_token=control.function_arn,
            resource_type="Custom::ExperimentGoldenSetLoad",
            properties={"DataHash": data_hash},   # CSV change -> paced reload on next deploy
        )
        load.node.add_dependency(table)

        CfnOutput(self, "TableName", value=table.table_name)
        CfnOutput(self, "ExperimentControlFunction", value=control.function_name)
        CfnOutput(self, "RecordsLoaded", value=load.get_att_string("Loaded"))
        CfnOutput(self, "LoadWcuConsumed", value=load.get_att_string("WcuConsumed"))
        CfnOutput(self, "LoadWcuPerSecActual", value=load.get_att_string("WcuPerSecActual"))
        CfnOutput(self, "LoadSeconds", value=load.get_att_string("Seconds"))
        CfnOutput(self, "GoldenSetHash", value=data_hash)
