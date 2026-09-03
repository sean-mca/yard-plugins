//! Glue plugin handler implementing the `PluginHandler` trait.
//!
//! Implements validate (10 rules), codegen (via `generate_pyspark`),
//! and schema (12 fields + source/sink types). Deploy/destroy/verify
//! remain stubbed for Plan 02.

use anyhow::Result;
use yard_plugin_common::codegen::generate_pyspark;
use yard_plugin_sdk::{
    CodegenResponse, DeployResponse, DestroyResponse, PluginHandler, PluginValidationError,
    Resource, SchemaField, SchemaResponse, ValidateResponse, VerifyResponse,
};

/// Valid Glue worker type identifiers.
const VALID_WORKER_TYPES: &[&str] = &["G.025X", "G.1X", "G.2X", "G.4X", "G.8X", "Z.2X"];

/// Valid bookmark settings.
const VALID_BOOKMARK_VALUES: &[&str] = &["enabled", "disabled"];

/// Valid Glue ETL version strings.
const VALID_GLUE_VERSIONS: &[&str] = &["3.0", "4.0", "5.0"];

/// Construct a `PluginValidationError` with severity "error".
fn validation_error(field: &str, message: &str) -> PluginValidationError {
    PluginValidationError {
        field: field.to_string(),
        message: message.to_string(),
        severity: "error".to_string(),
    }
}

/// AWS Glue provider plugin handler.
///
/// Holds an embedded tokio runtime for bridging sync trait methods to
/// async AWS SDK calls. The `_rt` field keeps the runtime alive so
/// the `rt` handle remains valid for `block_on` in later phases.
pub(crate) struct GlueHandler {
    /// Tokio runtime -- kept alive so the handle remains valid.
    _rt: tokio::runtime::Runtime,
    /// Handle for `block_on` bridging in handler methods.
    #[allow(dead_code)]
    rt: tokio::runtime::Handle,
}

impl GlueHandler {
    /// Create a new `GlueHandler` with an embedded single-threaded tokio runtime.
    pub(crate) fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("BUG: failed to create tokio runtime");
        let handle = runtime.handle().clone();
        Self {
            _rt: runtime,
            rt: handle,
        }
    }
}

impl PluginHandler for GlueHandler {
    fn name(&self) -> &str {
        "yard-plugin-glue"
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn validate(
        &self,
        _job_name: &str,
        job_config: &serde_json::Value,
    ) -> Result<ValidateResponse> {
        let mut errors = Vec::new();

        // Rule 1: role required at top level (non-empty string)
        let has_role = job_config
            .get("role")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty());
        if !has_role {
            errors.push(validation_error(
                "role",
                "Glue jobs require a \"role\" (execution role ARN)",
            ));
        }

        // Provider-specific rules operate on the "glue" sub-block
        if let Some(inner) = job_config.get("glue") {
            // Rule 2: script_bucket required and non-empty (D-03)
            let has_bucket = inner
                .get("script_bucket")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.is_empty());
            if !has_bucket {
                errors.push(validation_error(
                    "glue.script_bucket",
                    "script_bucket is required",
                ));
            }

            // Rule 3: worker_type must be in valid set (only if present)
            if let Some(wt) = inner.get("worker_type").and_then(|v| v.as_str()) {
                if !VALID_WORKER_TYPES.contains(&wt) {
                    errors.push(validation_error(
                        "glue.worker_type",
                        &format!(
                            "invalid worker_type \"{wt}\"; valid values: {}",
                            VALID_WORKER_TYPES.join(", ")
                        ),
                    ));
                }
            }

            // Rule 4: number_of_workers >= 1 (only if present)
            if let Some(n) = inner.get("number_of_workers").and_then(|v| v.as_i64()) {
                if n < 1 {
                    errors.push(validation_error(
                        "glue.number_of_workers",
                        "must be at least 1",
                    ));
                }
            }

            // Rule 5: glue_version in valid set (only if present)
            if let Some(v) = inner.get("glue_version").and_then(|v| v.as_str()) {
                if !VALID_GLUE_VERSIONS.contains(&v) {
                    errors.push(validation_error(
                        "glue.glue_version",
                        &format!(
                            "invalid glue_version \"{v}\"; valid values: {}",
                            VALID_GLUE_VERSIONS.join(", ")
                        ),
                    ));
                }
            }

            // Rule 6: timeout >= 1 (only if present)
            if let Some(t) = inner.get("timeout").and_then(|v| v.as_i64()) {
                if t < 1 {
                    errors.push(validation_error(
                        "glue.timeout",
                        "must be at least 1 (minutes)",
                    ));
                }
            }

            // Rule 7: max_retries >= 0 (only if present)
            if let Some(r) = inner.get("max_retries").and_then(|v| v.as_i64()) {
                if r < 0 {
                    errors.push(validation_error(
                        "glue.max_retries",
                        "cannot be negative",
                    ));
                }
            }

            // Rule 8: bookmark must be in valid set (only if present)
            if let Some(b) = inner.get("bookmark").and_then(|v| v.as_str()) {
                if !VALID_BOOKMARK_VALUES.contains(&b) {
                    errors.push(validation_error(
                        "glue.bookmark",
                        &format!(
                            "invalid bookmark \"{b}\"; valid values: {}",
                            VALID_BOOKMARK_VALUES.join(", ")
                        ),
                    ));
                }
            }

            // Rule 9: connections must be an array (only if present)
            if let Some(conns) = inner.get("connections") {
                if !conns.is_array() {
                    errors.push(validation_error(
                        "glue.connections",
                        "must be an array of strings",
                    ));
                }
            }

            // Rule 10: default_arguments must be an object (only if present)
            if let Some(args) = inner.get("default_arguments") {
                if !args.is_object() {
                    errors.push(validation_error(
                        "glue.default_arguments",
                        "must be a map of string keys to string values",
                    ));
                }
            }
        } else {
            // No glue block at all -- script_bucket is still required
            errors.push(validation_error(
                "glue.script_bucket",
                "script_bucket is required",
            ));
        }

        Ok(ValidateResponse { errors })
    }

    fn codegen(
        &self,
        job_name: &str,
        job_config: &serde_json::Value,
    ) -> Result<CodegenResponse> {
        let script = generate_pyspark(job_name, job_config)?;
        Ok(CodegenResponse {
            script: Some(script),
        })
    }

    fn deploy(
        &self,
        _job_name: &str,
        _job_config: &serde_json::Value,
        _artifact: &str,
    ) -> Result<DeployResponse> {
        Ok(DeployResponse { resources: vec![] })
    }

    fn destroy(&self, _job_name: &str, _resources: &[Resource]) -> Result<DestroyResponse> {
        Ok(DestroyResponse {})
    }

    fn verify(&self, _job_name: &str, _resources: &[Resource]) -> Result<VerifyResponse> {
        Ok(VerifyResponse { statuses: vec![] })
    }

    fn schema(&self) -> Result<SchemaResponse> {
        Ok(SchemaResponse {
            fields: vec![
                SchemaField {
                    name: "region".into(),
                    field_type: "string".into(),
                    required: false,
                    description: "AWS region".into(),
                },
                SchemaField {
                    name: "script_bucket".into(),
                    field_type: "string".into(),
                    required: true,
                    description: "S3 bucket for generated scripts".into(),
                },
                SchemaField {
                    name: "script_prefix".into(),
                    field_type: "string".into(),
                    required: false,
                    description: "S3 key prefix for scripts".into(),
                },
                SchemaField {
                    name: "worker_type".into(),
                    field_type: "string".into(),
                    required: false,
                    description: "Glue worker type".into(),
                },
                SchemaField {
                    name: "glue_version".into(),
                    field_type: "string".into(),
                    required: false,
                    description: "Glue ETL version".into(),
                },
                SchemaField {
                    name: "number_of_workers".into(),
                    field_type: "integer".into(),
                    required: false,
                    description: "Number of Glue workers".into(),
                },
                SchemaField {
                    name: "timeout".into(),
                    field_type: "integer".into(),
                    required: false,
                    description: "Job timeout in minutes".into(),
                },
                SchemaField {
                    name: "max_retries".into(),
                    field_type: "integer".into(),
                    required: false,
                    description: "Maximum retry attempts".into(),
                },
                SchemaField {
                    name: "max_concurrent_runs".into(),
                    field_type: "integer".into(),
                    required: false,
                    description: "Max concurrent job runs".into(),
                },
                SchemaField {
                    name: "bookmark".into(),
                    field_type: "string".into(),
                    required: false,
                    description: "Job bookmark setting".into(),
                },
                SchemaField {
                    name: "connections".into(),
                    field_type: "array".into(),
                    required: false,
                    description: "Glue connection names".into(),
                },
                SchemaField {
                    name: "default_arguments".into(),
                    field_type: "object".into(),
                    required: false,
                    description: "Default job arguments".into(),
                },
            ],
            supported_source_types: Some(vec![
                "s3".into(),
                "jdbc".into(),
                "catalog".into(),
                "kafka".into(),
                "api".into(),
            ]),
            supported_sink_types: Some(vec![
                "s3".into(),
                "jdbc".into(),
                "catalog".into(),
                "iceberg".into(),
            ]),
        })
    }
}
