"""Protocol integration tests for yard-plugin-airflow.

Mirrors the test patterns from yard-plugin-glue/tests/protocol.rs, adapted
for pytest + subprocess.run. Each test spawns the plugin script, sends a JSON
request on stdin, and asserts on the handshake and response lines.
"""

import json
import os
import subprocess


# ---------------------------------------------------------------------------
# Helpers (not fixtures -- plain functions usable anywhere in this module)
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


def parse_protocol_output(stdout_text):
    """Parse stdout into exactly 2 lines (handshake + response).

    Returns (handshake_dict, response_dict). Mirrors the Rust
    parse_protocol_output from protocol.rs.
    """
    lines = stdout_text.strip().split("\n")
    assert len(lines) == 2, (
        "expected exactly 2 stdout lines (handshake + response), got {}".format(
            len(lines)
        )
    )
    handshake = json.loads(lines[0])
    response = json.loads(lines[1])
    return handshake, response


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_handshake_contains_plugin_identity():
    """Send schema request. Assert handshake has protocol_version, name, capabilities."""
    stdout, stderr, rc = run_plugin({"operation": "schema"})
    assert rc == 0, "process exited with error: {}".format(stderr)

    handshake, _ = parse_protocol_output(stdout)
    assert handshake["protocol_version"] == 1
    assert handshake["name"] == "yard-plugin-airflow"
    assert handshake["version"].startswith("0.")
    assert isinstance(handshake["capabilities"], list)
    assert len(handshake["capabilities"]) > 0


def test_validate_catches_missing_dags_bucket():
    """Send validate with empty job_config. Assert dags_bucket error is returned."""
    stdout, stderr, rc = run_plugin({
        "operation": "validate",
        "job_name": "test-dag",
        "job_config": {},
    })
    assert rc == 0, "process exited with error: {}".format(stderr)

    _, response = parse_protocol_output(stdout)
    errors = response["errors"]
    assert len(errors) > 0, "empty config should produce validation errors"
    assert any(
        e["field"] == "dags_bucket" for e in errors
    ), "should report missing dags_bucket; errors: {}".format(errors)


def test_validate_catches_trigger_schedule_mutual_exclusion():
    """Send validate with both schedule and trigger. Assert mutual exclusion error."""
    stdout, stderr, rc = run_plugin({
        "operation": "validate",
        "job_name": "test-dag",
        "job_config": {
            "dags_bucket": "b",
            "schedule": "@daily",
            "trigger": {"dataset": {"uri": "s3://x"}},
        },
    })
    assert rc == 0, "process exited with error: {}".format(stderr)

    _, response = parse_protocol_output(stdout)
    errors = response["errors"]
    assert any(
        "mutual" in e["message"].lower() or "exclusive" in e["message"].lower()
        for e in errors
    ), "should report mutual exclusion; errors: {}".format(errors)


def test_validate_accepts_valid_config():
    """Send validate with valid config. Assert errors array is empty."""
    stdout, stderr, rc = run_plugin({
        "operation": "validate",
        "job_name": "test-dag",
        "job_config": {
            "dags_bucket": "my-bucket",
            "schedule": "@daily",
            "tasks": [],
        },
    })
    assert rc == 0, "process exited with error: {}".format(stderr)

    _, response = parse_protocol_output(stdout)
    errors = response["errors"]
    assert len(errors) == 0, "valid config should produce no errors; got: {}".format(
        errors
    )


def test_codegen_generates_dag_script():
    """Send codegen with schedule-only config. Assert script contains DAG markers."""
    stdout, stderr, rc = run_plugin({
        "operation": "codegen",
        "job_name": "my_dag",
        "job_config": {
            "dags_bucket": "b",
            "schedule": "@daily",
            "tasks": [
                {
                    "task_id": "etl",
                    "task_type": "bash",
                    "command": "echo hello",
                }
            ],
        },
    })
    assert rc == 0, "codegen failed: {}".format(stderr)

    _, response = parse_protocol_output(stdout)
    script = response["script"]
    assert isinstance(script, str) and len(script) > 0, "script should be a non-empty string"
    assert "dag_id=" in script, "script should contain dag_id="
    assert "my_dag" in script, "script should contain the DAG name"
    assert "schedule=" in script, "script should contain schedule="
    assert "BashOperator" in script, "script should contain BashOperator"


def test_schema_returns_field_definitions():
    """Send schema request. Assert fields array with dags_bucket required."""
    stdout, stderr, rc = run_plugin({"operation": "schema"})
    assert rc == 0, "process exited with error: {}".format(stderr)

    _, response = parse_protocol_output(stdout)
    fields = response["fields"]
    assert isinstance(fields, list), "fields should be a list"
    assert len(fields) > 0, "fields should not be empty"

    # dags_bucket is required
    dags_bucket = next((f for f in fields if f["name"] == "dags_bucket"), None)
    assert dags_bucket is not None, "should have a dags_bucket field"
    assert dags_bucket["required"] is True, "dags_bucket should be required"


def test_stdout_contains_only_json():
    """Send schema request. Assert every non-empty stdout line is valid JSON."""
    stdout, _, rc = run_plugin({"operation": "schema"})
    assert rc == 0

    for i, line in enumerate(stdout.strip().split("\n")):
        if not line.strip():
            continue
        try:
            json.loads(line)
        except json.JSONDecodeError:
            assert False, "stdout line {} is not valid JSON: {}".format(i, line)


def test_stderr_receives_no_json():
    """Send schema request. Assert stderr does not contain protocol JSON."""
    stdout, stderr, rc = run_plugin({"operation": "schema"})
    assert rc == 0

    # stderr may contain log lines, but should not contain protocol JSON
    if stderr.strip():
        for line in stderr.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                assert "protocol_version" not in obj, (
                    "handshake JSON found on stderr: {}".format(line)
                )
            except json.JSONDecodeError:
                # Not JSON -- that's fine for log lines
                pass
