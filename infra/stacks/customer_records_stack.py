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
LOADER_SRC = ROOT / "infra" / "loader"
LOADER_BUILD = ROOT / "infra" / ".build" / "loader"

# Free tier: 25 provisioned RCU + 25 provisioned WCU per account, across ALL tables.
# Keep this table well under so later tables (queue, ledger) still fit.
READ_CAPACITY = 10
WRITE_CAPACITY = 10


def stage_loader():
    """Bundle the handler with the CSV so the Lambda asset is self-contained."""
    if LOADER_BUILD.exists():
        shutil.rmtree(LOADER_BUILD)
    LOADER_BUILD.mkdir(parents=True)
    shutil.copy(LOADER_SRC / "handler.py", LOADER_BUILD / "handler.py")
    shutil.copy(CSV, LOADER_BUILD / CSV.name)
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

        data_hash = stage_loader()

        log_group = logs.LogGroup(
            self, "LoaderLogs",
            log_group_name="/aws/lambda/effect-demo-records-loader",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        loader = lambda_.Function(
            self, "Loader",
            function_name="effect-demo-records-loader",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LOADER_BUILD)),
            memory_size=256,
            timeout=Duration.minutes(5),
            environment={"TABLE_NAME": table.table_name},
            log_group=log_group,
        )
        table.grant_write_data(loader)

        load = CustomResource(
            self, "GoldenSetLoad",
            service_token=loader.function_arn,
            resource_type="Custom::GoldenSetLoad",
            properties={"DataHash": data_hash},   # CSV change -> reload on next deploy
        )
        load.node.add_dependency(table)

        CfnOutput(self, "TableName", value=table.table_name)
        CfnOutput(self, "RecordsLoaded", value=load.get_att_string("Loaded"))
        CfnOutput(self, "GoldenSetHash", value=data_hash)
