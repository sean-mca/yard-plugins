//! Ministack-gated lifecycle suite for the Glue plugin.
//!
//! Every test here spawns the real `yard-plugin-glue` binary and reaches a
//! local ministack emulator through environment variables alone. The suite is
//! gated on `YARD_TEST_AWS_ENDPOINT`: with it unset every test returns early
//! with a skip note on stderr, so the offline suite stays green.

mod common;

// Task 1 scaffold. It exists so `--test lifecycle` resolves a target while the
// harness is landed, and drives the gate, `unique_name`, `Clients`, the RAII
// `BucketFixture`, and the `run_plugin` hygiene assertions in one pass. Task 2
// removes it once the real lifecycle tests exercise the same helpers.
#[tokio::test]
async fn harness_smoke_test_runs_a_schema_request() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    let clients = common::clients(&endpoint).await;
    let bucket = common::unique_name("smoke");
    let _fixture = common::BucketFixture::create(&clients.s3, &bucket).await;

    // A schema request reaches no AWS service, so this exercises the spawn
    // helper without depending on the emulator's Glue or S3 gateways.
    let run = common::run_plugin(&endpoint, &serde_json::json!({ "operation": "schema" }));

    assert_eq!(run.exit_code, 0, "schema request failed; stderr: {}", run.stderr);
}
