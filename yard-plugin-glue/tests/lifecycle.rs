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

// Variant A. An absent `script_prefix` does NOT yield a bare `<job>.py` key:
// `GlueConfig::script_prefix` carries a serde default of `yard-scripts/`, so
// that default is what lands in S3. Do not "correct" this expectation.
#[tokio::test]
async fn deploy_uses_default_script_prefix() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    let clients = common::clients(&endpoint).await;
    let job = common::unique_name("pfx-default");
    let bucket = common::unique_name("pfx-default-bkt");
    let fixture = common::BucketFixture::create(&clients.s3, &bucket).await;
    let expected_key = format!("yard-scripts/{job}.py");
    fixture.track_key(&expected_key);

    let run = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "deploy",
            "job_name": job,
            "job_config": {
                "role": TEST_ROLE,
                "glue": {
                    "region": "us-east-1",
                    "script_bucket": bucket
                }
            },
            "artifact": "print('default prefix')"
        }),
    );

    assert_eq!(run.exit_code, 0, "deploy failed; stderr: {}", run.stderr);
    let script_resource = run.response["resources"][0].clone();
    assert_eq!(script_resource["id"], format!("s3://{bucket}/{expected_key}"));

    clients
        .s3
        .head_object()
        .bucket(&bucket)
        .key(&expected_key)
        .send()
        .await
        .expect("the script should exist at the default-prefixed key");

    let verify = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "verify",
            "job_name": job,
            "resources": [script_resource]
        }),
    );

    assert_eq!(verify.exit_code, 0, "verify failed; stderr: {}", verify.stderr);
    let statuses = verify.response["statuses"]
        .as_array()
        .expect("verify response should carry a statuses array");
    assert_eq!(statuses.len(), 1, "expected one status; got {statuses:?}");
    assert_eq!(
        statuses[0]["exists"], true,
        "verify should report the uploaded script as present"
    );
}

#[tokio::test]
async fn deploy_honors_explicit_script_prefix() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    let clients = common::clients(&endpoint).await;
    let job = common::unique_name("pfx-explicit");
    let bucket = common::unique_name("pfx-explicit-bkt");
    let fixture = common::BucketFixture::create(&clients.s3, &bucket).await;
    let expected_key = format!("scripts/{job}.py");
    fixture.track_key(&expected_key);

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
                    "script_prefix": "scripts/"
                }
            },
            "artifact": "print('explicit prefix')"
        }),
    );

    assert_eq!(run.exit_code, 0, "deploy failed; stderr: {}", run.stderr);
    let script_resource = run.response["resources"][0].clone();
    assert_eq!(script_resource["id"], format!("s3://{bucket}/{expected_key}"));

    clients
        .s3
        .head_object()
        .bucket(&bucket)
        .key(&expected_key)
        .send()
        .await
        .expect("the script should exist under the explicit prefix");

    let verify = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "verify",
            "job_name": job,
            "resources": [script_resource]
        }),
    );

    assert_eq!(verify.exit_code, 0, "verify failed; stderr: {}", verify.stderr);
    let statuses = verify.response["statuses"]
        .as_array()
        .expect("verify response should carry a statuses array");
    assert_eq!(statuses.len(), 1, "expected one status; got {statuses:?}");
    assert_eq!(
        statuses[0]["exists"], true,
        "verify should report the uploaded script as present"
    );
}

#[tokio::test]
async fn deploy_with_empty_prefix_writes_to_bucket_root() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    let clients = common::clients(&endpoint).await;
    let job = common::unique_name("pfx-empty");
    let bucket = common::unique_name("pfx-empty-bkt");
    let fixture = common::BucketFixture::create(&clients.s3, &bucket).await;
    let expected_key = format!("{job}.py");
    fixture.track_key(&expected_key);

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
                    "script_prefix": ""
                }
            },
            "artifact": "print('empty prefix')"
        }),
    );

    assert_eq!(run.exit_code, 0, "deploy failed; stderr: {}", run.stderr);
    let script_resource = run.response["resources"][0].clone();
    assert_eq!(script_resource["id"], format!("s3://{bucket}/{expected_key}"));

    clients
        .s3
        .head_object()
        .bucket(&bucket)
        .key(&expected_key)
        .send()
        .await
        .expect("the script should exist at the bucket root");

    let verify = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "verify",
            "job_name": job,
            "resources": [script_resource]
        }),
    );

    assert_eq!(verify.exit_code, 0, "verify failed; stderr: {}", verify.stderr);
    let statuses = verify.response["statuses"]
        .as_array()
        .expect("verify response should carry a statuses array");
    assert_eq!(statuses.len(), 1, "expected one status; got {statuses:?}");
    assert_eq!(
        statuses[0]["exists"], true,
        "verify should report the uploaded script as present"
    );
}

// This test proves idempotent destroy end to end (INTG-01, D-11). It does
// NOT cover the `EntityNotFoundException` arm in `delete_glue_job`: ministack
// and real AWS both return success for `DeleteJob` against a job that does
// not exist, so the SDK never produces the error that arm classifies, and
// this test passes identically whether or not the arm is present. D-10's
// verification is code review plus `cargo clippy -D warnings`, recorded in
// 08-VALIDATION.md under Manual-Only Verifications. Do not read this test as
// coverage for it.
#[tokio::test]
async fn destroying_an_already_deleted_job_succeeds() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    // Arrange: the precondition is a real deploy, per D-08 -- the plugin's own
    // operation creates the state the destroy then removes.
    let clients = common::clients(&endpoint).await;
    let job = common::unique_name("destroy-twice");
    let bucket = common::unique_name("destroy-twice-bkt");
    let fixture = common::BucketFixture::create(&clients.s3, &bucket).await;
    let script_key = format!("scripts/{job}.py");
    fixture.track_key(&script_key);

    let deploy = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "deploy",
            "job_name": job,
            "job_config": {
                "role": TEST_ROLE,
                "glue": {
                    "region": "us-east-1",
                    "script_bucket": bucket,
                    "script_prefix": "scripts/"
                }
            },
            "artifact": "print('to be destroyed')"
        }),
    );
    assert_eq!(deploy.exit_code, 0, "deploy failed; stderr: {}", deploy.stderr);
    let deployed_resources = deploy.response["resources"].clone();

    // Act: the same destroy request, twice. destroy resolves its region from
    // the environment, which run_plugin sets, so it carries no glue config.
    let destroy_request = json!({
        "operation": "destroy",
        "job_name": job,
        "resources": deployed_resources
    });
    let first = common::run_plugin(&endpoint, &destroy_request);
    let second = common::run_plugin(&endpoint, &destroy_request);

    // Assert
    assert_eq!(first.exit_code, 0, "first destroy failed; stderr: {}", first.stderr);
    assert!(
        first.response.is_object(),
        "first destroy should answer with a JSON object; got {}",
        first.response
    );
    assert_eq!(
        second.exit_code, 0,
        "second destroy failed; stderr: {}",
        second.stderr
    );
    assert!(
        second.response.is_object(),
        "second destroy should answer with a JSON object; got {}",
        second.response
    );

    // Existence is probed with GetJob only: ministack light does not implement
    // the Glue job-listing action, and a sweep could observe state this test
    // does not own.
    let error = clients
        .glue
        .get_job()
        .job_name(&job)
        .send()
        .await
        .expect_err("the job should be absent after destroy");
    assert!(
        error
            .as_service_error()
            .is_some_and(|se| se.is_entity_not_found_exception()),
        "expected an entity-not-found service error; got {error:?}"
    );
}

#[tokio::test]
async fn destroy_with_no_resources_succeeds() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    // Arrange
    let clients = common::clients(&endpoint).await;
    let job = common::unique_name("destroy-empty");
    let bucket = common::unique_name("destroy-empty-bkt");
    let fixture = common::BucketFixture::create(&clients.s3, &bucket).await;
    let script_key = format!("scripts/{job}.py");
    fixture.track_key(&script_key);

    let deploy = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "deploy",
            "job_name": job,
            "job_config": {
                "role": TEST_ROLE,
                "glue": {
                    "region": "us-east-1",
                    "script_bucket": bucket,
                    "script_prefix": "scripts/"
                }
            },
            "artifact": "print('survives an empty destroy')"
        }),
    );
    assert_eq!(deploy.exit_code, 0, "deploy failed; stderr: {}", deploy.stderr);

    // Act
    let destroy = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "destroy",
            "job_name": job,
            "resources": []
        }),
    );

    // Assert: exit 0 with a JSON object, and nothing mutated -- proven by
    // reading the stored state back rather than by trusting the response.
    assert_eq!(destroy.exit_code, 0, "destroy failed; stderr: {}", destroy.stderr);
    assert!(
        destroy.response.is_object(),
        "destroy should answer with a JSON object; got {}",
        destroy.response
    );

    clients
        .glue
        .get_job()
        .job_name(&job)
        .send()
        .await
        .expect("the deployed job should survive a destroy with no resources");

    clients
        .s3
        .head_object()
        .bucket(&bucket)
        .key(&script_key)
        .send()
        .await
        .expect("the deployed script should survive a destroy with no resources");
}

#[tokio::test]
async fn destroy_succeeds_when_s3_cleanup_fails() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    // Arrange: a real deployed job, plus a bucket name that is never created.
    // Deriving the bogus name from unique_name guarantees it is absent and
    // cannot collide with a bucket another test in this process owns.
    let clients = common::clients(&endpoint).await;
    let job = common::unique_name("s3-fail");
    let bucket = common::unique_name("s3-fail-bkt");
    let fixture = common::BucketFixture::create(&clients.s3, &bucket).await;
    let script_key = format!("scripts/{job}.py");
    fixture.track_key(&script_key);

    let deploy = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "deploy",
            "job_name": job,
            "job_config": {
                "role": TEST_ROLE,
                "glue": {
                    "region": "us-east-1",
                    "script_bucket": bucket,
                    "script_prefix": "scripts/"
                }
            },
            "artifact": "print('s3 cleanup will fail')"
        }),
    );
    assert_eq!(deploy.exit_code, 0, "deploy failed; stderr: {}", deploy.stderr);

    let missing_bucket = common::unique_name("never-created-bkt");

    // Act: the real job first, then an s3_object in a bucket that does not
    // exist. DeleteObject raises a no-such-bucket service error, which is what
    // drives the handler's non-fatal branch -- and the ordering mirrors the
    // handler's own two passes, fatal Glue deletes before S3 cleanup.
    let destroy = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "destroy",
            "job_name": job,
            "resources": [
                {"type": "glue_job", "id": job, "provider": "glue"},
                {
                    "type": "s3_object",
                    "id": format!("s3://{missing_bucket}/{script_key}"),
                    "provider": "glue"
                }
            ]
        }),
    );

    // Assert: the S3 failure is reported but does not fail the operation.
    assert_eq!(destroy.exit_code, 0, "destroy failed; stderr: {}", destroy.stderr);
    assert!(
        destroy.response.is_object(),
        "destroy should answer with a JSON object; got {}",
        destroy.response
    );

    // Only the prefix is matched: the full line interpolates the resource id
    // and the SDK's own error rendering, neither of which is a stable contract.
    assert!(
        destroy.stderr.contains("Non-fatal"),
        "expected a non-fatal S3 cleanup warning on stderr; got: {}",
        destroy.stderr
    );

    // The fatal Glue delete still ran to completion before the S3 step failed.
    let error = clients
        .glue
        .get_job()
        .job_name(&job)
        .send()
        .await
        .expect_err("the job should be absent after destroy");
    assert!(
        error
            .as_service_error()
            .is_some_and(|se| se.is_entity_not_found_exception()),
        "expected an entity-not-found service error; got {error:?}"
    );
}

#[tokio::test]
async fn verify_reports_false_for_missing_glue_job() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    // Arrange: a real deploy provides the s3_object half of the mixed request;
    // the glue_job half names a job that was never created.
    let clients = common::clients(&endpoint).await;
    let job = common::unique_name("verify-mixed");
    let bucket = common::unique_name("verify-mixed-bkt");
    let fixture = common::BucketFixture::create(&clients.s3, &bucket).await;
    let script_key = format!("scripts/{job}.py");
    fixture.track_key(&script_key);

    let deploy = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "deploy",
            "job_name": job,
            "job_config": {
                "role": TEST_ROLE,
                "glue": {
                    "region": "us-east-1",
                    "script_bucket": bucket,
                    "script_prefix": "scripts/"
                }
            },
            "artifact": "print('verify me')"
        }),
    );
    assert_eq!(deploy.exit_code, 0, "deploy failed; stderr: {}", deploy.stderr);

    // Taken from the deploy response rather than rebuilt by hand: that is what
    // proves deploy and verify agree on resource identity.
    let script_resource = deploy.response["resources"][0].clone();
    let missing_job = common::unique_name("never-created-job");
    let missing_resource = json!({
        "type": "glue_job",
        "id": missing_job,
        "provider": "glue"
    });

    // Act: one mixed request, real resource first.
    let verify = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "verify",
            "job_name": job,
            "resources": [script_resource.clone(), missing_resource.clone()]
        }),
    );

    // Assert
    assert_eq!(verify.exit_code, 0, "verify failed; stderr: {}", verify.stderr);
    let statuses = verify.response["statuses"]
        .as_array()
        .expect("verify response should carry a statuses array");
    assert_eq!(statuses.len(), 2, "expected two statuses; got {statuses:?}");

    assert_eq!(
        statuses[0]["exists"], true,
        "the deployed script should be reported present"
    );
    assert_eq!(
        statuses[1]["exists"], false,
        "a job that was never created should be reported absent"
    );

    // Each status echoes the original resource, in request order.
    assert_eq!(statuses[0]["resource"]["type"], script_resource["type"]);
    assert_eq!(statuses[0]["resource"]["id"], script_resource["id"]);
    assert_eq!(statuses[0]["resource"]["provider"], script_resource["provider"]);
    assert_eq!(statuses[1]["resource"]["type"], "glue_job");
    assert_eq!(statuses[1]["resource"]["id"], missing_job);
    assert_eq!(statuses[1]["resource"]["provider"], "glue");
}

#[tokio::test]
async fn verify_reports_true_after_successful_deploy() {
    let Some(endpoint) = common::ministack_endpoint() else {
        return;
    };

    // Arrange
    let clients = common::clients(&endpoint).await;
    let job = common::unique_name("verify-true");
    let bucket = common::unique_name("verify-true-bkt");
    let fixture = common::BucketFixture::create(&clients.s3, &bucket).await;
    let script_key = format!("scripts/{job}.py");
    fixture.track_key(&script_key);

    let deploy = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "deploy",
            "job_name": job,
            "job_config": {
                "role": TEST_ROLE,
                "glue": {
                    "region": "us-east-1",
                    "script_bucket": bucket,
                    "script_prefix": "scripts/"
                }
            },
            "artifact": "print('all resources present')"
        }),
    );
    assert_eq!(deploy.exit_code, 0, "deploy failed; stderr: {}", deploy.stderr);
    let deployed_resources = deploy.response["resources"].clone();

    // Act: verify exactly the resources the deploy reported.
    let verify = common::run_plugin(
        &endpoint,
        &json!({
            "operation": "verify",
            "job_name": job,
            "resources": deployed_resources
        }),
    );

    // Assert
    assert_eq!(verify.exit_code, 0, "verify failed; stderr: {}", verify.stderr);
    let statuses = verify.response["statuses"]
        .as_array()
        .expect("verify response should carry a statuses array");
    assert_eq!(statuses.len(), 2, "expected two statuses; got {statuses:?}");
    for status in statuses {
        assert_eq!(
            status["exists"], true,
            "every resource a successful deploy returned should exist: {status}"
        );
    }
}
