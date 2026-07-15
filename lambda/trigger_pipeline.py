"""
Lambda handler that fires on new object creation in the S3 raw zone and kicks off
the processing job.

In production this would submit an EMR step (via boto3 emr client) or start a Glue
job run; this handler is written for the EMR path since that mirrors the batch
PySpark job in run_pipeline.py. It is not deployed/invoked as part of this repo's
CI -- it's included as the reference implementation for the "event-driven
orchestration" piece of the architecture, and is exercised by
tests/test_lambda_trigger.py using a mocked boto3 client (moto), not a live AWS call.

Deploy with the trigger configured as: S3 raw-zone bucket -> ObjectCreated:Put ->
this Lambda, filtered to the `raw/` prefix and `.json` suffix.
"""
import json
import os
from urllib.parse import unquote_plus

import boto3

EMR_CLUSTER_ID = os.environ.get("EMR_CLUSTER_ID", "")
SPARK_STEP_JAR = "command-runner.jar"


def build_spark_submit_step(s3_input_path: str, s3_output_path: str) -> dict:
    """Build the EMR step definition that runs run_pipeline.py against the new file."""
    return {
        "Name": f"clickstream-etl-{os.path.basename(s3_input_path)}",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": SPARK_STEP_JAR,
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "s3://your-code-bucket/scripts/run_pipeline.py",
                "--input", s3_input_path,
                "--output", s3_output_path,
            ],
        },
    }


def handler(event, context):
    """
    S3 ObjectCreated event handler. Extracts the bucket/key of the newly landed
    file and submits an EMR step to process it.
    """
    emr_client = boto3.client("emr", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        s3_input_path = f"s3://{bucket}/{key}"
        s3_output_path = f"s3://{bucket}/processed/"

        step = build_spark_submit_step(s3_input_path, s3_output_path)

        if EMR_CLUSTER_ID:
            response = emr_client.add_job_flow_steps(
                JobFlowId=EMR_CLUSTER_ID,
                Steps=[step],
            )
            results.append({"input": s3_input_path, "step_id": response["StepIds"][0]})
        else:
            # No cluster configured (e.g. local test) -- just report what would run.
            results.append({"input": s3_input_path, "step": step, "submitted": False})

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": results}),
    }
