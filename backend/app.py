#!/usr/bin/env python3
import os

import aws_cdk as cdk

from backend.storage_stack import StorageStack


app = cdk.App()
StorageStack(
    app, "SmoothFyStorageStack",
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    ),
)

app.synth()
