"""Version-aware output tests for Airflow v2 and v3 codegen.

Tests that airflow_version=2 produces Dataset/core imports while
airflow_version=3 (or default) produces Asset/providers-standard imports.
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


def test_version_v3_default_uses_asset():
    """Codegen with dataset trigger, no airflow_version -> Asset (v3 default)."""
    script = get_script("v3_dag", {
        "dags_bucket": "b",
        "trigger": {"dataset": {"uri": "s3://events/data"}},
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert "Asset" in script, "default version should use Asset"
    assert "from airflow.sdk import Asset" in script, (
        "default version should use v3 Asset import"
    )


def test_version_v2_explicit_uses_dataset():
    """Codegen with dataset trigger, airflow_version=2 -> Dataset."""
    script = get_script("v2_dag", {
        "dags_bucket": "b",
        "airflow_version": 2,
        "trigger": {"dataset": {"uri": "s3://events/data"}},
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert "Dataset" in script, "v2 should use Dataset class"
    assert "from airflow.datasets import Dataset" in script, (
        "v2 should use core Dataset import path"
    )


def test_version_v3_bash_import():
    """Codegen with bash task, airflow_version=3 -> providers-standard import."""
    script = get_script("v3_bash", {
        "dags_bucket": "b",
        "airflow_version": 3,
        "schedule": "@daily",
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert (
        "from airflow.providers.standard.operators.bash import BashOperator"
        in script
    ), "v3 should use providers-standard BashOperator import"


def test_version_v2_bash_import():
    """Codegen with bash task, airflow_version=2 -> core import."""
    script = get_script("v2_bash", {
        "dags_bucket": "b",
        "airflow_version": 2,
        "schedule": "@daily",
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert (
        "from airflow.operators.bash import BashOperator" in script
    ), "v2 should use core BashOperator import"


def test_version_v2_banner_contains_airflow_2_9():
    """Triggered DAG with airflow_version=2 has banner with apache-airflow >= 2.9."""
    script = get_script("v2_banner", {
        "dags_bucket": "b",
        "airflow_version": 2,
        "trigger": {"dataset": {"uri": "s3://x"}},
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert "apache-airflow >= 2.9" in script, (
        "v2 banner should reference airflow >= 2.9"
    )


def test_version_v3_banner_contains_airflow_3_0():
    """Triggered DAG with airflow_version=3 has banner with apache-airflow >= 3.0."""
    script = get_script("v3_banner", {
        "dags_bucket": "b",
        "airflow_version": 3,
        "trigger": {"dataset": {"uri": "s3://x"}},
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert "apache-airflow >= 3.0" in script, (
        "v3 banner should reference airflow >= 3.0"
    )
    assert "apache-airflow-providers-standard" in script, (
        "v3 banner should include providers-standard"
    )


def test_schedule_only_no_banner():
    """Schedule-only DAG should NOT contain version banner."""
    script = get_script("no_banner", {
        "dags_bucket": "b",
        "schedule": "@daily",
        "tasks": [{"task_id": "t", "task_type": "bash", "command": "echo hi"}],
    })
    assert "Airflow version contract" not in script, (
        "schedule-only DAG should not have version banner"
    )
