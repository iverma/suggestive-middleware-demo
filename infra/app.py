#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.customer_records_stack import CustomerRecordsStack

app = cdk.App()
CustomerRecordsStack(
    app, "effect-demo-customer-records",
    env=cdk.Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")),
    description="IROPS re-booking demo: customer records table + golden set loader",
)
cdk.Tags.of(app).add("project", "suggestive-middleware-demo")
app.synth()
