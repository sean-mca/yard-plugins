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


# ---------------------------------------------------------------------------
# Deploy / Destroy / Verify lifecycle tests (Plan 3)
# Mirrors yard-plugin-glue/tests/protocol.rs lines 136-288: spawn with
# AWS_EC2_METADATA_DISABLED=true, accept exit 0 or 1, verify handshake
# was written before any error.
# ---------------------------------------------------------------------------


def _run_plugin_no_aws(request_dict):
    """Spawn the plugin with AWS credentials disabled and extended timeout."""
    env = {**os.environ, "AWS_EC2_METADATA_DISABLED": "true"}
    input_data = json.dumps(request_dict) + "\n"
    result = subprocess.run(
        ["python3", PLUGIN_SCRIPT],
        input=input_data,
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )
    return result.stdout, result.stderr, result.returncode


def test_deploy_processes_request_without_crash():
    """Send deploy request. Assert handshake + graceful exit (0 or 1)."""
    stdout, stderr, rc = _run_plugin_no_aws({
        "operation": "deploy",
        "job_name": "test-dag",
        "job_config": {
            "dags_bucket": "test-bucket",
            "dags_prefix": "dags/",
        },
        "artifact": "# Generated DAG\nfrom airflow import DAG",
    })
    assert rc in (0, 1), "unexpected exit code {}: {}".format(rc, stderr)

    lines = stdout.strip().split("\n")
    assert len(lines) >= 1, "no stdout lines — handshake missing"
    handshake = json.loads(lines[0])
    assert handshake["name"] == "yard-plugin-airflow"

    if rc == 0:
        assert len(lines) == 2, "expected 2 stdout lines on success"
        response = json.loads(lines[1])
        assert "resources" in response, "deploy response should have resources"
        assert isinstance(response["resources"], list)


def test_destroy_processes_request_without_crash():
    """Send destroy request. Assert handshake + graceful exit (0 or 1)."""
    stdout, stderr, rc = _run_plugin_no_aws({
        "operation": "destroy",
        "job_name": "test-dag",
        "resources": [
            {
                "type": "s3_object",
                "id": "s3://test-bucket/dags/test-dag.py",
                "provider": "airflow",
            }
        ],
    })
    assert rc in (0, 1), "unexpected exit code {}: {}".format(rc, stderr)

    lines = stdout.strip().split("\n")
    assert len(lines) >= 1, "no stdout lines — handshake missing"
    handshake = json.loads(lines[0])
    assert handshake["name"] == "yard-plugin-airflow"

    if rc == 0:
        assert len(lines) == 2, "expected 2 stdout lines on success"
        response = json.loads(lines[1])
        assert isinstance(response, dict)


def test_verify_processes_request_without_crash():
    """Send verify request. Assert handshake + graceful exit (0 or 1)."""
    stdout, stderr, rc = _run_plugin_no_aws({
        "operation": "verify",
        "job_name": "test-dag",
        "resources": [
            {
                "type": "s3_object",
                "id": "s3://test-bucket/dags/test-dag.py",
                "provider": "airflow",
            }
        ],
    })
    assert rc in (0, 1), "unexpected exit code {}: {}".format(rc, stderr)

    lines = stdout.strip().split("\n")
    assert len(lines) >= 1, "no stdout lines — handshake missing"
    handshake = json.loads(lines[0])
    assert handshake["name"] == "yard-plugin-airflow"

    if rc == 0:
        assert len(lines) == 2, "expected 2 stdout lines on success"
        response = json.loads(lines[1])
        assert "statuses" in response, "verify response should have statuses"
        assert isinstance(response["statuses"], list)


def test_codegen_then_deploy_roundtrip():
    """Codegen a DAG script, then deploy it. Assert handshake on deploy."""
    # Step 1: codegen
    stdout1, stderr1, rc1 = run_plugin({
        "operation": "codegen",
        "job_name": "roundtrip_dag",
        "job_config": {
            "dags_bucket": "rt-bucket",
            "schedule": "@daily",
            "tasks": [
                {"task_id": "step1", "task_type": "bash", "command": "echo ok"},
            ],
        },
    })
    assert rc1 == 0, "codegen failed: {}".format(stderr1)
    _, codegen_resp = parse_protocol_output(stdout1)
    script = codegen_resp["script"]
    assert "roundtrip_dag" in script

    # Step 2: deploy with the generated script as artifact
    stdout2, stderr2, rc2 = _run_plugin_no_aws({
        "operation": "deploy",
        "job_name": "roundtrip_dag",
        "job_config": {
            "dags_bucket": "rt-bucket",
        },
        "artifact": script,
    })
    assert rc2 in (0, 1), "deploy crashed: {}".format(stderr2)

    lines2 = stdout2.strip().split("\n")
    assert len(lines2) >= 1, "no handshake on deploy"
    handshake2 = json.loads(lines2[0])
    assert handshake2["name"] == "yard-plugin-airflow"


def test_validate_codegen_schema_work_without_boto3():
    """Verify validate, codegen, schema do not require boto3 at import time.

    Sends each request in a separate subprocess and asserts exit 0. This is a
    regression guard for the lazy-import pattern, not a proof of isolation.
    """
    configs = [
        {
            "operation": "validate",
            "job_name": "no-boto-test",
            "job_config": {"dags_bucket": "b"},
        },
        {
            "operation": "codegen",
            "job_name": "no-boto-test",
            "job_config": {
                "dags_bucket": "b",
                "schedule": "@daily",
                "tasks": [
                    {"task_id": "t", "task_type": "bash", "command": "echo x"},
                ],
            },
        },
        {"operation": "schema"},
    ]

    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}

    for request in configs:
        input_data = json.dumps(request) + "\n"
        result = subprocess.run(
            ["python3", PLUGIN_SCRIPT],
            input=input_data,
            capture_output=True,
            text=True,
            timeout=10,
            env=env,
        )
        op = request.get("operation", "?")
        assert result.returncode == 0, (
            "{} failed (exit {}): {}".format(op, result.returncode, result.stderr)
        )
        lines = result.stdout.strip().split("\n")
        assert len(lines) == 2, (
            "{} expected 2 stdout lines, got {}".format(op, len(lines))
        )
        handshake = json.loads(lines[0])
        assert handshake["name"] == "yard-plugin-airflow"
        response = json.loads(lines[1])
        assert isinstance(response, dict), (
            "{} response is not a dict".format(op)
        )
