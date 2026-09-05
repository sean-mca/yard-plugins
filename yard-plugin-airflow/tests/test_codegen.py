"""Codegen tests for all 6 trigger types via the subprocess protocol.

Each test spawns the plugin script with a trigger configuration and asserts
on the generated DAG script content. Follows the same subprocess pattern
as test_protocol.py.
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


def get_script(job_name, job_config):
    """Run codegen and return the generated script string."""
    stdout, stderr, rc = run_plugin({
        "operation": "codegen",
        "job_name": job_name,
        "job_config": job_config,
    })
    assert rc == 0, "codegen failed: {}".format(stderr)
    lines = stdout.strip().split("\n")
    assert len(lines) == 2, "expected 2 stdout lines, got {}".format(len(lines))
    response = json.loads(lines[1])
    return response["script"]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_codegen_schedule_only_dag():
    """Schedule-only DAG with @daily cron, one bash task."""
    script = get_script("sched_dag", {
        "dags_bucket": "b",
        "schedule": "@daily",
        "tasks": [{"task_id": "etl", "task_type": "bash", "command": "echo hello"}],
    })
    assert "schedule=" in script
    assert '"@daily"' in script
    assert "BashOperator" in script
    # No sensor tasks in a schedule-only DAG
    assert "S3KeySensor" not in script
    assert "SqsSensor" not in script


def test_codegen_dataset_trigger():
    """Dataset-triggered DAG with default v3 uses Asset class."""
    script = get_script("ds_dag", {
        "dags_bucket": "b",
        "trigger": {"dataset": {"uri": "s3://events/data"}},
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert "Asset" in script, "default v3 should use Asset class"
    assert "schedule=" in script
    assert "Asset(" in script
    assert "s3://events/data" in script
    # No sensor tasks for dataset triggers
    assert "S3KeySensor" not in script
    assert "SqsSensor" not in script
    # Version banner should be present for triggered DAGs
    assert "Airflow version contract" in script


def test_codegen_s3_trigger():
    """S3-triggered DAG produces S3KeySensor with default knob values."""
    script = get_script("s3_dag", {
        "dags_bucket": "b",
        "trigger": {"s3": {"bucket": "my-bucket", "prefix": "incoming/"}},
        "tasks": [{"task_id": "etl", "task_type": "bash", "command": "echo hi"}],
    })
    assert "S3KeySensor" in script
    assert "bucket_name=" in script
    assert "poke_interval=60" in script
    assert "timeout=86400" in script
    assert "deferrable=True" in script
    # Sensor edge to the root task
    assert "_yard_wait_s3 >> t_etl" in script


def test_codegen_sqs_trigger():
    """SQS-triggered DAG produces SqsSensor with default knob values."""
    script = get_script("sqs_dag", {
        "dags_bucket": "b",
        "trigger": {"sqs": {"queue_url": "https://sqs.us-east-1.amazonaws.com/123/queue"}},
        "tasks": [{"task_id": "proc", "task_type": "bash", "command": "echo hi"}],
    })
    assert "SqsSensor" in script
    assert "sqs_queue=" in script
    assert "wait_time_seconds=20" in script
    assert "deferrable=True" in script
    # Sensor edge to the root task
    assert "_yard_wait_sqs >> t_proc" in script


def test_codegen_api_trigger():
    """API-triggered DAG produces schedule=None and header with curl example."""
    script = get_script("api_dag", {
        "dags_bucket": "b",
        "trigger": {"api": {"description": "Manual ETL trigger"}},
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert "schedule=None" in script
    assert "Trigger: API" in script
    assert "curl -X POST" in script
    # No sensor tasks for API triggers
    assert "S3KeySensor" not in script
    assert "SqsSensor" not in script


def test_codegen_composite_all_datasets():
    """Composite all-datasets trigger produces alpha-sorted & chain."""
    script = get_script("comp_dag", {
        "dags_bucket": "b",
        "trigger": {"all": [
            {"dataset": {"uri": "s3://b"}},
            {"dataset": {"uri": "s3://a"}},
        ]},
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    # Alpha-sorted: s3://a appears before s3://b in the schedule expression
    a_pos = script.index("s3://a")
    b_pos = script.index("s3://b")
    assert a_pos < b_pos, "s3://a should appear before s3://b (alpha-sorted)"
    assert " & " in script, "all-datasets should use & separator"


def test_codegen_composite_heterogeneous_all():
    """Heterogeneous-all trigger: dataset at schedule level + S3 sensor."""
    script = get_script("hetero_dag", {
        "dags_bucket": "b",
        "trigger": {"all": [
            {"dataset": {"uri": "s3://x"}},
            {"s3": {"bucket": "b", "prefix": "p/"}},
        ]},
        "tasks": [
            {"task_id": "t1", "task_type": "bash", "command": "echo hi"},
            {"task_id": "t2", "task_type": "bash", "command": "echo bye"},
        ],
    })
    # Dataset at schedule level
    assert "Asset(" in script or "Dataset(" in script
    # S3 sensor task present
    assert "S3KeySensor" in script
    # With only 1 sensor, should wire directly to roots (no _yard_join)
    assert "_yard_join" not in script
    # Both sensor and dataset imports should be present
    assert "S3KeySensor" in script
    assert "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor" in script


def test_codegen_max_active_runs_triggered():
    """Triggered DAG defaults to max_active_runs=1 (CONC-01)."""
    # Default: max_active_runs=1 for triggered DAGs
    script1 = get_script("mar_dag", {
        "dags_bucket": "b",
        "trigger": {"dataset": {"uri": "s3://x"}},
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert "max_active_runs=1" in script1, "triggered DAG should default to max_active_runs=1"

    # User override: max_active_runs=5 wins over CONC-01 default
    script2 = get_script("mar_dag", {
        "dags_bucket": "b",
        "trigger": {"dataset": {"uri": "s3://x"}},
        "max_active_runs": 5,
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert "max_active_runs=5" in script2, "user override should win over CONC-01 default"


def test_codegen_schedule_only_no_max_active_runs():
    """Schedule-only DAG does NOT emit max_active_runs (Airflow default preserved)."""
    script = get_script("sched_no_mar", {
        "dags_bucket": "b",
        "schedule": "@daily",
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert "max_active_runs" not in script, (
        "schedule-only DAG should not emit max_active_runs"
    )


def test_codegen_s3_trigger_default_aws_conn_id():
    """D-12: S3 trigger falls back to aws_conn_id='aws_default' when no connection configured."""
    script = get_script("s3_default_conn", {
        "dags_bucket": "b",
        "trigger": {"s3": {"bucket": "b", "prefix": "p/"}},
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert 'aws_conn_id="aws_default"' in script, (
        "S3 sensor should fall back to aws_default when no connection configured (D-12)"
    )


def test_codegen_sqs_trigger_default_aws_conn_id():
    """D-12: SQS trigger falls back to aws_conn_id='aws_default' when no connection configured."""
    script = get_script("sqs_default_conn", {
        "dags_bucket": "b",
        "trigger": {"sqs": {"queue_url": "https://sqs.us-east-1.amazonaws.com/123/q"}},
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert 'aws_conn_id="aws_default"' in script, (
        "SQS sensor should fall back to aws_default when no connection configured (D-12)"
    )
