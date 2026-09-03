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
fn validate_catches_missing_role_and_script_bucket() {
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
    assert!(!errors.is_empty(), "empty config should produce validation errors");

    // Should flag missing role
    let has_role_error = errors.iter().any(|e| e["field"] == "role");
    assert!(has_role_error, "should report missing role; errors: {errors:?}");

    // Should flag missing script_bucket
    let has_bucket_error = errors.iter().any(|e| e["field"] == "glue.script_bucket");
    assert!(has_bucket_error, "should report missing script_bucket; errors: {errors:?}");
}

#[test]
fn validate_accepts_valid_config() {
    let request = json!({
        "operation": "validate",
        "job_name": "test-job",
        "job_config": {
            "role": "arn:aws:iam::123:role/Test",
            "glue": {
                "script_bucket": "my-bucket"
            }
        }
    });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    assert!(output.status.success());

    let (_, response) = parse_protocol_output(&output.stdout);
    let errors = response["errors"].as_array().unwrap();
    assert!(errors.is_empty(), "valid config should produce no errors; got: {errors:?}");
}

#[test]
fn codegen_generates_pyspark_script() {
    let request = json!({
        "operation": "codegen",
        "job_name": "test-job",
        "job_config": {
            "sources": [{
                "name": "src",
                "source_type": "s3",
                "path": "s3://bucket/input/",
                "format": "parquet"
            }],
            "sink": {
                "sink_type": "s3",
                "path": "s3://bucket/output/",
                "format": "parquet"
            }
        }
    });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    assert!(output.status.success(), "codegen failed: {:?}", String::from_utf8_lossy(&output.stderr));

    let (_, response) = parse_protocol_output(&output.stdout);
    let script = response["script"].as_str()
        .expect("codegen should return a non-null script string");
    assert!(!script.is_empty(), "script should not be empty");
    assert!(
        script.contains("GlueContext") || script.contains("SparkSession"),
        "script should contain PySpark markers; got: {}", &script[..200.min(script.len())]
    );
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
fn schema_returns_full_field_definitions() {
    let request = json!({ "operation": "schema" });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    assert!(output.status.success());

    let (_, response) = parse_protocol_output(&output.stdout);

    // Exactly 12 field entries
    let fields = response["fields"].as_array().unwrap();
    assert_eq!(fields.len(), 12, "schema should return 12 fields; got {}", fields.len());

    // 5 supported source types
    let source_types = response["supported_source_types"].as_array().unwrap();
    assert_eq!(source_types.len(), 5, "should have 5 source types; got {}", source_types.len());

    // 4 supported sink types
    let sink_types = response["supported_sink_types"].as_array().unwrap();
    assert_eq!(sink_types.len(), 4, "should have 4 sink types; got {}", sink_types.len());

    // script_bucket is required
    let bucket_field = fields.iter().find(|f| f["name"] == "script_bucket").unwrap();
    assert_eq!(bucket_field["required"], true, "script_bucket should be required");

    // region is not required
    let region_field = fields.iter().find(|f| f["name"] == "region").unwrap();
    assert_eq!(region_field["required"], false, "region should not be required");
}
