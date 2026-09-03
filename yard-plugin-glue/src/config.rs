//! Provider-specific configuration types for AWS Glue jobs.
//!
//! `GlueConfig` deserializes from the `glue` sub-block of a job config,
//! applying serde defaults for all optional fields. Unknown fields are
//! silently tolerated per D-02.
//!
//! The struct and helpers are used by deploy/destroy/verify in Plan 02.

#![allow(dead_code)]

use serde::Deserialize;
use std::collections::HashMap;

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_script_prefix() -> String {
    "yard-scripts/".to_string()
}

fn default_glue_version() -> String {
    "4.0".to_string()
}

fn default_worker_type() -> String {
    "G.1X".to_string()
}

fn default_number_of_workers() -> i32 {
    2
}

/// Typed configuration for the Glue provider.
///
/// Deserialized from the `glue` sub-block of the job config JSON.
/// All fields have serde defaults; `deny_unknown_fields` is intentionally
/// NOT set so that future config keys are silently tolerated (D-02).
#[derive(Debug, Deserialize)]
pub(crate) struct GlueConfig {
    /// AWS region for Glue API calls.
    #[serde(default = "default_region")]
    pub region: String,

    /// S3 key prefix for generated scripts.
    #[serde(default = "default_script_prefix")]
    pub script_prefix: String,

    /// S3 bucket for generated scripts (required at validate time).
    #[serde(default)]
    pub script_bucket: Option<String>,

    /// Glue ETL version (3.0, 4.0, or 5.0).
    #[serde(default = "default_glue_version")]
    pub glue_version: String,

    /// Glue worker type (G.025X, G.1X, G.2X, G.4X, G.8X, Z.2X).
    #[serde(default = "default_worker_type")]
    pub worker_type: String,

    /// Number of Glue workers (must be >= 1).
    #[serde(default = "default_number_of_workers")]
    pub number_of_workers: i32,

    /// Job timeout in minutes (must be >= 1 if set).
    #[serde(default)]
    pub timeout: Option<i32>,

    /// Maximum retry attempts (must be >= 0 if set).
    #[serde(default)]
    pub max_retries: Option<i32>,

    /// Maximum concurrent job runs.
    #[serde(default)]
    pub max_concurrent_runs: Option<i32>,

    /// Job bookmark setting ("enabled" or "disabled").
    #[serde(default)]
    pub bookmark: Option<String>,

    /// Glue connection names.
    #[serde(default)]
    pub connections: Vec<String>,

    /// Default job arguments passed to the Glue job.
    #[serde(default)]
    pub default_arguments: HashMap<String, String>,

    /// Opaque AWS credential/config block, passed through to `aws_config()`.
    #[serde(default, rename = "_aws")]
    pub aws: Option<serde_json::Value>,
}

/// Build the default arguments map for a Glue job.
///
/// Clones the user-provided `default_arguments`, inserts the
/// `--datalake-formats` default ("iceberg") if not already set,
/// and wires the bookmark setting to `--job-bookmark-option`.
pub(crate) fn build_default_arguments(config: &GlueConfig) -> HashMap<String, String> {
    let mut args = config.default_arguments.clone();

    // Iceberg-first: enable iceberg datalake format by default
    args.entry("--datalake-formats".to_string())
        .or_insert_with(|| "iceberg".to_string());

    // Wire bookmark setting to the Glue job argument
    if let Some(ref bookmark) = config.bookmark {
        let enabled = matches!(bookmark.as_str(), "enabled" | "true");
        args.insert(
            "--job-bookmark-option".to_string(),
            if enabled {
                "job-bookmark-enable"
            } else {
                "job-bookmark-disable"
            }
            .to_string(),
        );
    }

    args
}
