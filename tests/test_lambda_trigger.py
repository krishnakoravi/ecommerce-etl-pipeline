"""Tests for the Lambda trigger's step-building logic and event parsing.
Uses a fake S3 ObjectCreated event payload -- no real AWS call is made since
EMR_CLUSTER_ID is left unset, which routes the handler into its dry-run branch.

Note: source lives in lambda/trigger_pipeline.py. "lambda" is a Python reserved
keyword, so it can't be imported as a package with a normal `import` statement --
this loads the module directly from its file path instead, which also matches how
AWS actually packages/deploys the handler (as a standalone file, not a package)."""
import importlib.util
import json
from pathlib import Path

_module_path = Path(__file__).resolve().parent.parent / "lambda" / "trigger_pipeline.py"
_spec = importlib.util.spec_from_file_location("trigger_pipeline", _module_path)
trigger_pipeline = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(trigger_pipeline)

build_spark_submit_step = trigger_pipeline.build_spark_submit_step
handler = trigger_pipeline.handler


def test_build_spark_submit_step_includes_paths():
    step = build_spark_submit_step("s3://bucket/raw/events.json", "s3://bucket/processed/")
    args = step["HadoopJarStep"]["Args"]
    assert "s3://bucket/raw/events.json" in args
    assert "s3://bucket/processed/" in args
    assert step["ActionOnFailure"] == "CONTINUE"


def _s3_event(bucket="my-bucket", key="raw/clickstream_events.json"):
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                }
            }
        ]
    }


def test_handler_dry_run_without_cluster_id(monkeypatch):
    """With no EMR_CLUSTER_ID set, the handler should report what it WOULD submit
    rather than calling AWS -- this is what lets the test run without credentials."""
    monkeypatch.delenv("EMR_CLUSTER_ID", raising=False)
    # reload module-level constant behavior via direct call; handler reads env at call time
    trigger_pipeline.EMR_CLUSTER_ID = ""

    response = handler(_s3_event(), context=None)
    body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert body["processed"][0]["input"] == "s3://my-bucket/raw/clickstream_events.json"
    assert body["processed"][0]["submitted"] is False
