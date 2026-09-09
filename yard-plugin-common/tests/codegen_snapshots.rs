//! Snapshot tests for the codegen pipeline.
//!
//! Each test loads a YAML fixture, parses it to `serde_json::Value`,
//! calls `generate_pyspark`, and uses `insta::assert_snapshot!` to lock
//! the output. Run `cargo insta review` to approve new/changed snapshots.

#![allow(clippy::unwrap_used)]

use insta::assert_snapshot;

fn codegen(fixture: &str) -> String {
    let config: serde_json::Value = serde_yaml::from_str(fixture).unwrap();
    yard_plugin_common::codegen::generate_pyspark("test_job", &config).unwrap()
}

#[test]
fn codegen_s3_simple() {
    assert_snapshot!(codegen(include_str!("fixtures/s3_simple.yaml")));
}

#[test]
fn codegen_jdbc_secret() {
    assert_snapshot!(codegen(include_str!("fixtures/jdbc_secret.yaml")));
}

#[test]
fn codegen_multi_transform() {
    assert_snapshot!(codegen(include_str!("fixtures/multi_transform.yaml")));
}

#[test]
fn codegen_pii_masking() {
    assert_snapshot!(codegen(include_str!("fixtures/pii_masking.yaml")));
}

#[test]
fn codegen_iceberg_fill_nulls() {
    assert_snapshot!(codegen(include_str!("fixtures/iceberg_fill_nulls.yaml")));
}

#[test]
fn codegen_body_override() {
    assert_snapshot!(codegen(include_str!("fixtures/body_override.yaml")));
}

#[test]
fn codegen_kafka_source() {
    assert_snapshot!(codegen(include_str!("fixtures/kafka_source.yaml")));
}

#[test]
fn codegen_api_source() {
    assert_snapshot!(codegen(include_str!("fixtures/api_source.yaml")));
}
