//! Ministack-gated lifecycle suite for the Glue plugin.
//!
//! Every test here spawns the real `yard-plugin-glue` binary over stdio and
//! reaches a local ministack emulator through environment variables alone — no
//! production code knows the emulator exists. Assertions read back real stored
//! state with the Glue and S3 SDKs rather than trusting the plugin's own
//! response.
//!
//! The suite is gated on `YARD_TEST_AWS_ENDPOINT`: with it unset every test
//! returns early with a skip note on stderr, so `cargo test --workspace` stays
//! green on a machine with no emulator running. `make test-integration` sets
//! the variable; it can also be exported by hand to target an already-running
//! container.

mod common;

use serde_json::json;

/// The Glue execution role every fixture declares. Ministack does not validate
/// it, and the zeroed account id makes clear it names nothing real.
const TEST_ROLE: &str = "arn:aws:iam::000000000000:role/GlueRole";

#[tokio::test]
async fn deploy_creates_new_glue_job() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    let clients = common::clients(&endpoint).await;
    let job = common::unique_name("create");
    let bucket = common::unique_name("create-bkt");
    let fixture = common::BucketFixture::create(&clients.s3, &bucket).await;
    let script_key = format!("scripts/{job}.py");
    fixture.track_key(&script_key);

    let run = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "deploy",
            "job_name": job,
            "job_config": {
                "role": TEST_ROLE,
                "glue": {
                    "region": "us-east-1",
                    "script_bucket": bucket,
                    "script_prefix": "scripts/",
                    "worker_type": "G.1X",
                    "glue_version": "4.0",
                    "number_of_workers": 2
                }
            },
            "artifact": "print('lifecycle v1')"
        }),
    );

    assert_eq!(run.exit_code, 0, "deploy failed; stderr: {}", run.stderr);

    let resources = run.response["resources"]
        .as_array()
        .expect("deploy response should carry a resources array");
    assert_eq!(resources.len(), 2, "expected s3_object then glue_job; got {resources:?}");
    assert_eq!(resources[0]["type"], "s3_object");
    assert_eq!(resources[0]["id"], format!("s3://{bucket}/{script_key}"));
    assert_eq!(resources[0]["provider"], "glue");
    assert_eq!(resources[1]["type"], "glue_job");
    assert_eq!(resources[1]["id"], job);
    assert_eq!(resources[1]["provider"], "glue");

    let stored = clients
        .glue
        .get_job()
        .job_name(&job)
        .send()
        .await
        .expect("get_job should find the deployed job");
    let stored = stored.job().expect("get_job should return a job body");
    let expected_location = format!("s3://{bucket}/{script_key}");
    assert_eq!(stored.role(), Some(TEST_ROLE));
    assert_eq!(
        stored.command().and_then(|c| c.script_location()),
        Some(expected_location.as_str())
    );
    assert_eq!(stored.worker_type().map(|w| w.as_str()), Some("G.1X"));
    assert_eq!(stored.glue_version(), Some("4.0"));
    assert_eq!(stored.number_of_workers(), Some(2));
}

#[tokio::test]
async fn deploy_updates_existing_glue_job() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    let clients = common::clients(&endpoint).await;
    let job = common::unique_name("update");
    let bucket = common::unique_name("update-bkt");
    let fixture = common::BucketFixture::create(&clients.s3, &bucket).await;
    let script_key = format!("scripts/{job}.py");
    fixture.track_key(&script_key);

    // The precondition is arranged by the plugin's own deploy, not by a
    // hand-rolled CreateJob, so the update path starts from real prior state.
    let first = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "deploy",
            "job_name": job,
            "job_config": {
                "role": TEST_ROLE,
                "glue": {
                    "region": "us-east-1",
                    "script_bucket": bucket,
                    "script_prefix": "scripts/",
                    "worker_type": "G.1X",
                    "glue_version": "4.0",
                    "number_of_workers": 2
                }
            },
            "artifact": "print('lifecycle v1')"
        }),
    );
    assert_eq!(first.exit_code, 0, "first deploy failed; stderr: {}", first.stderr);

    let second_artifact = "print('lifecycle v2 -- updated')";
    let run = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "deploy",
            "job_name": job,
            "job_config": {
                "role": TEST_ROLE,
                "glue": {
                    "region": "us-east-1",
                    "script_bucket": bucket,
                    "script_prefix": "scripts/",
                    "worker_type": "G.2X",
                    "glue_version": "4.0",
                    "number_of_workers": 5
                }
            },
            "artifact": second_artifact
        }),
    );

    assert_eq!(run.exit_code, 0, "second deploy failed; stderr: {}", run.stderr);

    let stored = clients
        .glue
        .get_job()
        .job_name(&job)
        .send()
        .await
        .expect("get_job should find the updated job");
    let stored = stored.job().expect("get_job should return a job body");
    assert_eq!(stored.worker_type().map(|w| w.as_str()), Some("G.2X"));
    assert_eq!(stored.number_of_workers(), Some(5));

    let object = clients
        .s3
        .get_object()
        .bucket(&bucket)
        .key(&script_key)
        .send()
        .await
        .expect("the script object should exist after the second deploy");
    let body = object
        .body
        .collect()
        .await
        .expect("script body should stream")
        .into_bytes();
    assert_eq!(
        body.as_ref(),
        second_artifact.as_bytes(),
        "the stored script should be the second artifact verbatim"
    );
}
