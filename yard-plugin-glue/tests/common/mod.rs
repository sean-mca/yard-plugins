//! Shared fixtures for the ministack-backed lifecycle suite.
//!
//! Holds the opt-in endpoint gate, per-test unique naming, the in-process
//! AWS client fixture, the RAII bucket fixture, and the spawn helper that
//! enforces the plugin's two-line stdout contract. Consumed only by
//! `tests/lifecycle.rs`, which is the single place that declares
//! `mod common;`.

// Every file under `tests/` compiles as its own crate, and a helper used by
// only some lifecycle tests would otherwise trip unused-code diagnostics and
// fail `cargo clippy --all-targets -- -D warnings`.
#![allow(dead_code)]

use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, Once};

use yard_plugin_common::aws::S3Client;

/// Region every lifecycle test pins, for both the spawned binary and the
/// in-process SDK clients.
const TEST_REGION: &str = "us-east-1";

/// Resolve the ministack endpoint, or `None` when the opt-in variable is unset.
///
/// Never panics and never probes a port to decide — the gate is explicit
/// opt-in, so `cargo test` stays green on a machine with no emulator.
pub fn ministack_endpoint() -> Option<String> {
    match std::env::var("YARD_TEST_AWS_ENDPOINT") {
        Ok(value) if !value.is_empty() => Some(value),
        _ => {
            // Written to the stderr handle explicitly. stdout is the plugin
            // protocol channel, so nothing in this harness may reach it.
            let _ = writeln!(
                std::io::stderr(),
                "SKIP: set YARD_TEST_AWS_ENDPOINT (e.g. http://127.0.0.1:4566) \
                 to run the ministack lifecycle tests -- try `make test-integration`"
            );
            None
        }
    }
}

/// Guards the one-time publication of the AWS settings into this process.
static INIT_ENV: Once = Once::new();

/// Publish the ministack AWS settings into this process's environment so the
/// in-process SDK clients resolve exactly the way the spawned binary does.
///
/// `Once` — not a mutex — because it gives the write-before-read ordering
/// without serializing the lifecycle tests against each other.
fn init_process_env(endpoint: &str) {
    INIT_ENV.call_once(|| {
        std::env::set_var("AWS_ENDPOINT_URL", endpoint);
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_DEFAULT_REGION", TEST_REGION);
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    });
}

/// The in-process AWS clients a lifecycle test reads stored state with.
pub struct Clients {
    /// Glue client, used for `get_job` assertions.
    pub glue: aws_sdk_glue::Client,
    /// S3 client, used for `head_object` / `get_object` assertions.
    pub s3: S3Client,
}

/// Build both clients through the production credential/region loader.
///
/// Reusing `yard_plugin_common::aws::aws_config` is deliberate: it makes the
/// tests prove the real loader honours `AWS_ENDPOINT_URL`, and it keeps the
/// signature free of any type from `aws-config` or `aws-sdk-s3`, neither of
/// which is a direct dependency of this crate.
pub async fn clients(endpoint: &str) -> Clients {
    init_process_env(endpoint);

    let sdk_config = yard_plugin_common::aws::aws_config(TEST_REGION, None).await;

    Clients {
        glue: aws_sdk_glue::Client::new(&sdk_config),
        s3: S3Client::new(&sdk_config),
    }
}

/// Monotonic counter making every generated name unique within a process.
static NAME_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Build a name unique to this process and call site.
///
/// The output is valid as both an S3 bucket name and a Glue job name:
/// lowercase ASCII letters, digits, and hyphens only, and comfortably inside
/// the 3-63 character bucket-name bounds. Uses `std::process::id()` plus an
/// atomic counter, so no random-identifier crate is needed.
pub fn unique_name(label: &str) -> String {
    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("yard-it-{label}-{}-{counter}", std::process::id())
}

/// Creates an S3 bucket on construction and best-effort removes it on drop.
///
/// Teardown runs even when an assertion panics, satisfying the
/// `test-fixture-raii` rule.
pub struct BucketFixture {
    name: String,
    keys: Mutex<Vec<String>>,
}

impl BucketFixture {
    /// Create the bucket and return the fixture that owns its lifetime.
    ///
    /// The request carries no explicit bucket location: `us-east-1` rejects
    /// one, and it is the only region this suite uses.
    pub async fn create(s3: &S3Client, name: &str) -> BucketFixture {
        s3.create_bucket()
            .bucket(name)
            .send()
            .await
            .expect("test bucket should be created");

        BucketFixture {
            name: name.to_string(),
            keys: Mutex::new(Vec::new()),
        }
    }

    /// Register an object key so teardown removes it before the bucket.
    pub fn track_key(&self, key: &str) {
        let mut keys = self.keys.lock().unwrap_or_else(|e| e.into_inner());
        keys.push(key.to_string());
    }

    /// The bucket's name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for BucketFixture {
    fn drop(&mut self) {
        let bucket = self.name.clone();
        let keys = {
            let guard = self.keys.lock().unwrap_or_else(|e| e.into_inner());
            guard.clone()
        };

        // A fresh thread, a fresh runtime, and a fresh client. `Drop` is
        // synchronous and cannot await; driving a runtime handle's `block_on`
        // from inside a `#[tokio::test]` panics with "Cannot start a runtime
        // from within a runtime"; and a connection pool should not be driven
        // by a runtime that did not create it.
        let handle = std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };

            runtime.block_on(async move {
                let sdk_config =
                    yard_plugin_common::aws::aws_config(TEST_REGION, None).await;
                let s3 = S3Client::new(&sdk_config);

                // Tracked keys only — never a listing. Ministack light's
                // object-listing support is unverified, and a sweep could
                // delete something the test did not create.
                for key in &keys {
                    let _ = s3
                        .delete_object()
                        .bucket(&bucket)
                        .key(key)
                        .send()
                        .await;
                }

                let _ = s3.delete_bucket().bucket(&bucket).send().await;
            });
        });

        // Teardown is best-effort: a failure here must never mask the test's
        // real failure, so both the AWS errors above and a panicking cleanup
        // thread are discarded.
        let _ = handle.join();
    }
}

/// One completed run of the plugin binary.
pub struct PluginRun {
    /// The process exit code.
    pub exit_code: i32,
    /// The parsed response line, or `Value::Null` when the run failed.
    pub response: serde_json::Value,
    /// Everything the run wrote to stderr.
    pub stderr: String,
}

/// Spawn the plugin with a single request and assert its stdout contract.
///
/// Every successful run must emit exactly two stdout lines — the handshake
/// naming this plugin, then the response — so this helper is the single
/// chokepoint where protocol hygiene is enforced.
pub fn run_plugin(endpoint: &str, request: &serde_json::Value) -> PluginRun {
    let output = assert_cmd::Command::cargo_bin("yard-plugin-glue")
        .expect("yard-plugin-glue binary should build")
        .env("AWS_ENDPOINT_URL", endpoint)
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        // destroy and verify resolve their region from the environment, not
        // from config, so every spawn must carry it.
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .timeout(std::time::Duration::from_secs(30))
        .write_stdin(format!("{request}\n"))
        .output()
        .expect("plugin process should run to completion");

    // A hung binary is killed by the timeout above and reports no code; this
    // expect turns that into a legible failure instead of a stalled suite.
    let exit_code = output
        .status
        .code()
        .expect("process should exit with a code, not a signal");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf-8");
    let lines: Vec<&str> = stdout.trim().lines().collect();

    let response = if exit_code == 0 {
        assert_eq!(
            lines.len(),
            2,
            "expected exactly 2 stdout lines (handshake + response), got {lines:?}; stderr: {stderr}"
        );

        let handshake: serde_json::Value = serde_json::from_str(lines[0])
            .expect("handshake line should be valid JSON");
        assert_eq!(handshake["name"], "yard-plugin-glue");

        serde_json::from_str(lines[1]).expect("response line should be valid JSON")
    } else {
        // A failing run writes its error to stderr and emits no response line,
        // so there is nothing to parse — the handshake is not a response.
        serde_json::Value::Null
    };

    PluginRun {
        exit_code,
        response,
        stderr,
    }
}
