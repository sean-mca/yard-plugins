"""Expanded validation tests for trigger-specific validation rules.

Tests that trigger structure validation catches missing required fields
and rejects invalid airflow_version values.
"""

import json
import os
import subprocess


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PLUGIN_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "yard-plugin-airflow.py",
)


def run_plugin(request_dict):
    """Spawn the plugin script with a JSON request on stdin."""
    input_data = json.dumps(request_dict) + "\n"
    result = subprocess.run(
        ["python3", PLUGIN_SCRIPT],
        input=input_data,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result.stdout, result.stderr, result.returncode


def get_validate_errors(job_config):
    """Run validate and return the errors list."""
    stdout, stderr, rc = run_plugin({
        "operation": "validate",
        "job_name": "test-dag",
        "job_config": job_config,
    })
    assert rc == 0, "validate failed: {}".format(stderr)
    lines = stdout.strip().split("\n")
    assert len(lines) == 2, "expected 2 stdout lines, got {}".format(len(lines))
    response = json.loads(lines[1])
    return response["errors"]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_validate_s3_trigger_missing_bucket():
    """S3 trigger with empty bucket should produce a validation error."""
    errors = get_validate_errors({
        "dags_bucket": "b",
        "trigger": {"s3": {"bucket": "", "prefix": "x"}},
    })
    s3_errors = [e for e in errors if "s3" in e.get("field", "").lower()
                 or "bucket" in e.get("message", "").lower()]
    assert len(s3_errors) > 0, (
        "should report error on empty S3 bucket; errors: {}".format(errors)
    )


def test_validate_sqs_trigger_missing_queue_url():
    """SQS trigger with empty queue_url should produce a validation error."""
    errors = get_validate_errors({
        "dags_bucket": "b",
        "trigger": {"sqs": {"queue_url": ""}},
    })
    sqs_errors = [e for e in errors if "sqs" in e.get("field", "").lower()
                  or "queue_url" in e.get("message", "").lower()]
    assert len(sqs_errors) > 0, (
        "should report error on empty SQS queue_url; errors: {}".format(errors)
    )


def test_validate_invalid_airflow_version():
    """airflow_version=4 should produce a validation error."""
    errors = get_validate_errors({
        "dags_bucket": "b",
        "airflow_version": 4,
    })
    version_errors = [e for e in errors if "airflow_version" in e.get("field", "")]
    assert len(version_errors) > 0, (
        "should report error on airflow_version=4; errors: {}".format(errors)
    )


def test_validate_valid_trigger_config():
    """Valid S3 trigger with dags_bucket should produce no errors."""
    errors = get_validate_errors({
        "dags_bucket": "b",
        "trigger": {"s3": {"bucket": "b", "prefix": "p/"}},
    })
    assert len(errors) == 0, (
        "valid trigger config should produce no errors; got: {}".format(errors)
    )
