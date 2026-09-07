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
fn deploy_processes_request_without_crash() {
    let request = json!({
        "operation": "deploy",
        "job_name": "test-job",
        "job_config": {
            "role": "arn:aws:iam::123:role/Test",
            "glue": {
                "script_bucket": "test-bucket",
                "script_prefix": "scripts/"
            }
        },
        "artifact": "print('hello')"
    });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        // Dummy credentials plus an unroutable endpoint keep this test
        // fully offline. The runtime bridge now completes AWS I/O, so
        // without these the spawned binary would reach whatever account
        // the developer's ambient credentials resolve to. Port 1 has no
        // listener, so the connection is refused immediately.
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_ENDPOINT_URL", "http://127.0.0.1:1")
        .env("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
        .env("AWS_CONFIG_FILE", "/dev/null")
        .timeout(std::time::Duration::from_secs(30))
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    // Without AWS credentials the handler errors and the binary exits 1.
    // The key assertion: no crash (panic/signal), and the protocol
    // handshake was written before the error.
    let exit_code = output.status.code()
        .expect("process should exit with a code, not a signal");
    assert!(
        exit_code == 0 || exit_code == 1,
        "unexpected exit code {exit_code}; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let text = String::from_utf8(output.stdout.to_vec()).unwrap();
    let lines: Vec<&str> = text.trim().lines().collect();
    assert!(!lines.is_empty(), "stdout should contain at least the handshake line");

    // First line is always the handshake
    let handshake: serde_json::Value = serde_json::from_str(lines[0])
        .expect("handshake line should be valid JSON");
    assert_eq!(handshake["name"], "yard-plugin-glue");

    // If the handler succeeded (unlikely without creds), verify response
    if exit_code == 0 {
        assert_eq!(lines.len(), 2, "successful response should have 2 lines");
        let response: serde_json::Value = serde_json::from_str(lines[1])
            .expect("response line should be valid JSON");
        if let Some(resources) = response.get("resources") {
            assert!(resources.is_array(), "resources should be an array");
        }
    }
}

#[test]
fn destroy_processes_request_without_crash() {
    let request = json!({
        "operation": "destroy",
        "job_name": "test-job",
        "resources": [{
            "type": "glue_job",
            "id": "test-job",
            "provider": "glue"
        }]
    });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        // Dummy credentials plus an unroutable endpoint keep this test
        // fully offline. The runtime bridge now completes AWS I/O, so
        // without these the spawned binary would reach whatever account
        // the developer's ambient credentials resolve to. Port 1 has no
        // listener, so the connection is refused immediately.
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_ENDPOINT_URL", "http://127.0.0.1:1")
        .env("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
        .env("AWS_CONFIG_FILE", "/dev/null")
        .timeout(std::time::Duration::from_secs(30))
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    // Without AWS credentials the handler errors and the binary exits 1.
    let exit_code = output.status.code()
        .expect("process should exit with a code, not a signal");
    assert!(
        exit_code == 0 || exit_code == 1,
        "unexpected exit code {exit_code}; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let text = String::from_utf8(output.stdout.to_vec()).unwrap();
    let lines: Vec<&str> = text.trim().lines().collect();
    assert!(!lines.is_empty(), "stdout should contain at least the handshake line");

    // Handshake is always first
    let handshake: serde_json::Value = serde_json::from_str(lines[0])
        .expect("handshake line should be valid JSON");
    assert_eq!(handshake["name"], "yard-plugin-glue");

    // On success (exit 0), verify the response structure
    if exit_code == 0 {
        assert_eq!(lines.len(), 2, "successful response should have 2 lines");
        let response: serde_json::Value = serde_json::from_str(lines[1])
            .expect("response line should be valid JSON");
        assert!(response.is_object(), "destroy response should be a JSON object");
    }
}

#[test]
fn verify_processes_request_without_crash() {
    let request = json!({
        "operation": "verify",
        "job_name": "test-job",
        "resources": [
            {
                "type": "s3_object",
                "id": "s3://test-bucket/scripts/test-job.py",
                "provider": "glue"
            },
            {
                "type": "glue_job",
                "id": "test-job",
                "provider": "glue"
            }
        ]
    });

    let output = Command::cargo_bin("yard-plugin-glue")
        .unwrap()
        // Dummy credentials plus an unroutable endpoint keep this test
        // fully offline. The runtime bridge now completes AWS I/O, so
        // without these the spawned binary would reach whatever account
        // the developer's ambient credentials resolve to. Port 1 has no
        // listener, so the connection is refused immediately.
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_ENDPOINT_URL", "http://127.0.0.1:1")
        .env("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
        .env("AWS_CONFIG_FILE", "/dev/null")
        .timeout(std::time::Duration::from_secs(30))
        .write_stdin(format!("{}\n", request))
        .output()
        .unwrap();

    // Without AWS credentials the handler errors and the binary exits 1.
    let exit_code = output.status.code()
        .expect("process should exit with a code, not a signal");
    assert!(
        exit_code == 0 || exit_code == 1,
        "unexpected exit code {exit_code}; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let text = String::from_utf8(output.stdout.to_vec()).unwrap();
    let lines: Vec<&str> = text.trim().lines().collect();
    assert!(!lines.is_empty(), "stdout should contain at least the handshake line");

    // Handshake is always first
    let handshake: serde_json::Value = serde_json::from_str(lines[0])
        .expect("handshake line should be valid JSON");
    assert_eq!(handshake["name"], "yard-plugin-glue");

    // On success (exit 0), verify the response structure
    if exit_code == 0 {
        assert_eq!(lines.len(), 2, "successful response should have 2 lines");
        let response: serde_json::Value = serde_json::from_str(lines[1])
            .expect("response line should be valid JSON");
        assert!(response.is_object(), "verify response should be a JSON object");
    }
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
