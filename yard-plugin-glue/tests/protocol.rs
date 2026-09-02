//! Integration tests for the Glue plugin stdio protocol.
//!
//! Each test spawns the `yard-plugin-glue` binary via `assert_cmd`,
//! sends a JSON request on stdin, and asserts on the handshake and
//! response lines emitted on stdout.

use assert_cmd::Command;
use serde_json::json;

/// Parse stdout into exactly two lines (handshake + response) and return
/// both as `serde_json::Value`.
fn parse_protocol_output(stdout: &[u8]) -> (serde_json::Value, serde_json::Value) {
    let text = String::from_utf8(stdout.to_vec()).unwrap();
    let lines: Vec<&str> = text.trim().lines().collect();
    assert_eq!(lines.len(), 2, "expected exactly 2 stdout lines (handshake + response), got {}", lines.len());
    let handshake: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    let response: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    (handshake, response)
}

#[test]
fn handshake_contains_plugin_identity() {
    let request = json!({ "operation": "schema" });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    assert!(output.status.success(), "process exited with error: {:?}", output.status);

    let (handshake, _) = parse_protocol_output(&output.stdout);
    assert_eq!(handshake["protocol_version"], 1);
    assert_eq!(handshake["name"], "yard-plugin-glue");
    assert!(handshake["capabilities"].is_array(), "capabilities should be a JSON array");

    let caps = handshake["capabilities"].as_array().unwrap();
    assert!(!caps.is_empty(), "capabilities should not be empty");
}

#[test]
fn validate_returns_empty_errors() {
    let request = json!({
        "operation": "validate",
        "job_name": "test-job",
        "job_config": {}
    });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    assert!(output.status.success());

    let (_, response) = parse_protocol_output(&output.stdout);
    let errors = response["errors"].as_array().unwrap();
    assert!(errors.is_empty(), "stub validate should return empty errors");
}

#[test]
fn codegen_returns_null_script() {
    let request = json!({
        "operation": "codegen",
        "job_name": "test-job",
        "job_config": {}
    });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    assert!(output.status.success());

    let (_, response) = parse_protocol_output(&output.stdout);
    assert!(response["script"].is_null(), "stub codegen should return null script");
}

#[test]
fn deploy_returns_empty_resources() {
    let request = json!({
        "operation": "deploy",
        "job_name": "test-job",
        "job_config": {},
        "artifact": "s3://bucket/script.py"
    });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    assert!(output.status.success());

    let (_, response) = parse_protocol_output(&output.stdout);
    let resources = response["resources"].as_array().unwrap();
    assert!(resources.is_empty(), "stub deploy should return empty resources");
}

#[test]
fn destroy_returns_empty_object() {
    let request = json!({
        "operation": "destroy",
        "job_name": "test-job",
        "resources": []
    });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    assert!(output.status.success());

    let (_, response) = parse_protocol_output(&output.stdout);
    assert!(response.is_object(), "destroy response should be a JSON object");
}

#[test]
fn verify_returns_empty_statuses() {
    let request = json!({
        "operation": "verify",
        "job_name": "test-job",
        "resources": []
    });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    assert!(output.status.success());

    let (_, response) = parse_protocol_output(&output.stdout);
    let statuses = response["statuses"].as_array().unwrap();
    assert!(statuses.is_empty(), "stub verify should return empty statuses");
}

#[test]
fn schema_returns_default_response() {
    let request = json!({ "operation": "schema" });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    assert!(output.status.success());

    let (_, response) = parse_protocol_output(&output.stdout);
    let fields = response["fields"].as_array().unwrap();
    assert!(fields.is_empty(), "stub schema should return empty fields");
}
