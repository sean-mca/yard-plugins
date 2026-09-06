//! Glue plugin handler implementing the `PluginHandler` trait.
//!
//! Implements validate (12 rules), codegen (via `generate_pyspark`),
//! deploy (S3 upload + Glue create-or-update upsert per D-08), destroy
//! (Glue delete + non-fatal S3 cleanup per D-09), verify (resource
//! existence checks per AWS-06), and schema (12 fields + source/sink
//! types).

use anyhow::{Context, Result};
use yard_plugin_common::aws::{aws_config, S3Client, S3ScriptOps};
use yard_plugin_common::codegen::generate_pyspark;
use yard_plugin_sdk::{
    tracing, CodegenResponse, DeployResponse, DestroyResponse, PluginHandler,
    PluginValidationError, Resource, ResourceStatus, SchemaField, SchemaResponse,
    ValidateResponse, VerifyResponse,
};

use crate::config::{build_default_arguments, GlueConfig};

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

/// Extract and deserialize the `glue` sub-block from a job config.
fn extract_glue_config(job_config: &serde_json::Value) -> Result<GlueConfig> {
    let glue_block = job_config
        .get("glue")
        .ok_or_else(|| anyhow::anyhow!("missing glue config block"))?;
    serde_json::from_value(glue_block.clone()).context("failed to parse glue config block")
}

/// Parse an S3 URI (`s3://bucket/key`) into `(bucket, key)`.
///
/// Returns `None` if the URI is not in the expected format or has
/// empty bucket/key components.
fn parse_s3_uri(uri: &str) -> Option<(&str, &str)> {
    let stripped = uri.strip_prefix("s3://")?;
    let (bucket, key) = stripped.split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((bucket, key))
}

/// Create or update a Glue job via the AWS API (update-first upsert per D-08).
///
/// Tries `UpdateJob` first. If the job does not exist
/// (`EntityNotFoundException`), falls back to `CreateJob`.
async fn create_or_update_glue_job(
    client: &aws_sdk_glue::Client,
    job_name: &str,
    script_location: &str,
    job_config: &serde_json::Value,
    glue_cfg: &GlueConfig,
) -> Result<()> {
    let execution_role = job_config
        .get("role")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            anyhow::anyhow!("Job \"{job_name}\" requires a \"role\" (Glue execution role)")
        })?;

    let command = aws_sdk_glue::types::JobCommand::builder()
        .name("glueetl")
        .script_location(script_location)
        .python_version("3")
        .build();

    let default_args = build_default_arguments(glue_cfg);

    // Build the update payload
    let mut update_builder = aws_sdk_glue::types::JobUpdate::builder()
        .role(execution_role)
        .command(command.clone())
        .glue_version(&glue_cfg.glue_version)
        .worker_type(aws_sdk_glue::types::WorkerType::from(
            glue_cfg.worker_type.as_str(),
        ))
        .number_of_workers(glue_cfg.number_of_workers);

    if let Some(timeout) = glue_cfg.timeout {
        update_builder = update_builder.timeout(timeout);
    }
    if let Some(max_retries) = glue_cfg.max_retries {
        update_builder = update_builder.max_retries(max_retries);
    }
    if let Some(max_concurrent) = glue_cfg.max_concurrent_runs {
        update_builder = update_builder.execution_property(
            aws_sdk_glue::types::ExecutionProperty::builder()
                .max_concurrent_runs(max_concurrent)
                .build(),
        );
    }
    if !glue_cfg.connections.is_empty() {
        update_builder = update_builder.connections(
            aws_sdk_glue::types::ConnectionsList::builder()
                .set_connections(Some(glue_cfg.connections.clone()))
                .build(),
        );
    }
    for (k, v) in &default_args {
        update_builder = update_builder.default_arguments(k.clone(), v.clone());
    }

    let update_result = client
        .update_job()
        .job_name(job_name)
        .job_update(update_builder.build())
        .send()
        .await;

    match update_result {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.as_service_error()
                .is_some_and(|se| se.is_entity_not_found_exception())
            {
                // Job doesn't exist yet -- create it
                let mut create_builder = client
                    .create_job()
                    .name(job_name)
                    .role(execution_role)
                    .command(command)
                    .glue_version(&glue_cfg.glue_version)
                    .worker_type(aws_sdk_glue::types::WorkerType::from(
                        glue_cfg.worker_type.as_str(),
                    ))
                    .number_of_workers(glue_cfg.number_of_workers);

                if let Some(timeout) = glue_cfg.timeout {
                    create_builder = create_builder.timeout(timeout);
                }
                if let Some(max_retries) = glue_cfg.max_retries {
                    create_builder = create_builder.max_retries(max_retries);
                }
                if let Some(max_concurrent) = glue_cfg.max_concurrent_runs {
                    create_builder = create_builder.execution_property(
                        aws_sdk_glue::types::ExecutionProperty::builder()
                            .max_concurrent_runs(max_concurrent)
                            .build(),
                    );
                }
                if !glue_cfg.connections.is_empty() {
                    create_builder = create_builder.connections(
                        aws_sdk_glue::types::ConnectionsList::builder()
                            .set_connections(Some(glue_cfg.connections.clone()))
                            .build(),
                    );
                }
                for (k, v) in &default_args {
                    create_builder =
                        create_builder.default_arguments(k.clone(), v.clone());
                }

                create_builder
                    .send()
                    .await
                    .with_context(|| format!("Failed to create Glue job \"{job_name}\""))?;
                Ok(())
            } else {
                Err(e).with_context(|| format!("Failed to update Glue job \"{job_name}\""))
            }
        }
    }
}

/// Delete a Glue job by name.
async fn delete_glue_job(client: &aws_sdk_glue::Client, job_name: &str) -> Result<()> {
    client
        .delete_job()
        .job_name(job_name)
        .send()
        .await
        .with_context(|| format!("Failed to delete Glue job \"{job_name}\""))?;
    Ok(())
}

/// Check whether a Glue job exists by name.
///
/// Returns `true` if `GetJob` succeeds, `false` if the job is not
/// found (`EntityNotFoundException`), or an error for other failures.
async fn glue_job_exists(client: &aws_sdk_glue::Client, job_name: &str) -> Result<bool> {
    let result = client.get_job().job_name(job_name).send().await;

    match result {
        Ok(_) => Ok(true),
        Err(e) => {
            if e.as_service_error()
                .is_some_and(|se| se.is_entity_not_found_exception())
            {
                Ok(false)
            } else {
                Err(e).with_context(|| format!("Failed to check Glue job: {job_name}"))
            }
        }
    }
}

/// AWS Glue provider plugin handler.
///
/// Holds an embedded tokio runtime for bridging sync trait methods to
/// async AWS SDK calls.
pub(crate) struct GlueHandler {
    /// Tokio runtime -- kept alive so the handle remains valid.
    _rt: tokio::runtime::Runtime,
    /// Handle for `block_on` bridging in handler methods.
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
        job_name: &str,
        job_config: &serde_json::Value,
    ) -> Result<ValidateResponse> {
        let mut errors = Vec::new();

        // Job name length check (AWS CreateJob limit: 255 chars)
        if job_name.len() > 255 {
            errors.push(validation_error(
                "job_name",
                "job name exceeds AWS limit of 255 characters",
            ));
        }

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

            // Rule 6: timeout must be 1-10080 minutes (only if present)
            if let Some(t) = inner.get("timeout").and_then(|v| v.as_i64()) {
                if !(1..=10080).contains(&t) {
                    errors.push(validation_error(
                        "glue.timeout",
                        "must be between 1 and 10080 minutes (7 days)",
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
        job_name: &str,
        job_config: &serde_json::Value,
        artifact: &str,
    ) -> Result<DeployResponse> {
        self.rt.block_on(async {
            let glue_cfg = extract_glue_config(job_config)?;

            // Early validation: role is required for Glue job creation
            if !job_config
                .get("role")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.is_empty())
            {
                anyhow::bail!("deploy requires a non-empty \"role\" in job_config");
            }

            let sdk_config =
                aws_config(&glue_cfg.region, glue_cfg.aws.as_ref()).await;
            let glue_client = aws_sdk_glue::Client::new(&sdk_config);
            let s3_client = S3Client::new(&sdk_config);

            let s3_ops = S3ScriptOps {
                s3_client,
                script_bucket: glue_cfg
                    .script_bucket
                    .clone()
                    .unwrap_or_default(),
                script_prefix: glue_cfg.script_prefix.clone(),
            };

            let script_location =
                s3_ops.upload_script(job_name, artifact).await?;

            create_or_update_glue_job(
                &glue_client,
                job_name,
                &script_location,
                job_config,
                &glue_cfg,
            )
            .await?;

            Ok(DeployResponse {
                resources: vec![
                    Resource {
                        r#type: "s3_object".to_string(),
                        id: script_location,
                        provider: "glue".to_string(),
                    },
                    Resource {
                        r#type: "glue_job".to_string(),
                        id: job_name.to_string(),
                        provider: "glue".to_string(),
                    },
                ],
            })
        })
    }

    fn destroy(
        &self,
        _job_name: &str,
        resources: &[Resource],
    ) -> Result<DestroyResponse> {
        self.rt.block_on(async {
            let region = std::env::var("AWS_DEFAULT_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string());
            let sdk_config = aws_config(&region, None).await;
            let glue_client = aws_sdk_glue::Client::new(&sdk_config);
            let s3_client = S3Client::new(&sdk_config);

            // Delete Glue jobs (fatal)
            for resource in resources {
                if resource.r#type == "glue_job" {
                    delete_glue_job(&glue_client, &resource.id).await?;
                }
            }

            // S3 cleanup (non-fatal per D-09)
            for resource in resources {
                if resource.r#type == "s3_object" {
                    if let Some((bucket, key)) = parse_s3_uri(&resource.id) {
                        if let Err(e) = s3_client
                            .delete_object()
                            .bucket(bucket)
                            .key(key)
                            .send()
                            .await
                        {
                            tracing::warn!(
                                "Non-fatal: failed to delete S3 object {}: {e}",
                                resource.id
                            );
                        }
                    }
                }
            }

            Ok(DestroyResponse {})
        })
    }

    fn verify(
        &self,
        _job_name: &str,
        resources: &[Resource],
    ) -> Result<VerifyResponse> {
        self.rt.block_on(async {
            let region = std::env::var("AWS_DEFAULT_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string());
            let sdk_config = aws_config(&region, None).await;
            let glue_client = aws_sdk_glue::Client::new(&sdk_config);
            let s3_client = S3Client::new(&sdk_config);

            let mut statuses = Vec::with_capacity(resources.len());

            for resource in resources {
                let exists = match resource.r#type.as_str() {
                    "s3_object" => {
                        if let Some((bucket, key)) = parse_s3_uri(&resource.id)
                        {
                            let s3_ops = S3ScriptOps {
                                s3_client: s3_client.clone(),
                                script_bucket: bucket.to_string(),
                                script_prefix: String::new(),
                            };
                            s3_ops.s3_object_exists(key).await?
                        } else {
                            tracing::warn!(
                                "Could not parse S3 URI: {}",
                                resource.id
                            );
                            false
                        }
                    }
                    "glue_job" => {
                        glue_job_exists(&glue_client, &resource.id).await?
                    }
                    _ => true, // Unknown resource types assumed to exist
                };

                statuses.push(ResourceStatus {
                    resource: resource.clone(),
                    exists,
                });
            }

            Ok(VerifyResponse { statuses })
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Build a minimal valid job config that passes all validation rules.
    fn valid_config() -> serde_json::Value {
        json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {
                "script_bucket": "my-bucket"
            }
        })
    }

    // ── Validation rule tests ──────────────────────────────────────

    #[test]
    fn validate_valid_config_no_errors() {
        let handler = GlueHandler::new();
        let config = valid_config();

        let response = handler.validate("test-job", &config).unwrap();

        assert!(response.errors.is_empty(), "expected no errors, got: {:?}", response.errors);
    }

    #[test]
    fn validate_rejects_long_job_name() {
        let handler = GlueHandler::new();
        let config = valid_config();
        let long_name = "a".repeat(256);

        let response = handler.validate(&long_name, &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "job_name");
    }

    #[test]
    fn validate_rejects_missing_role() {
        let handler = GlueHandler::new();
        let config = json!({
            "glue": { "script_bucket": "my-bucket" }
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "role");
    }

    #[test]
    fn validate_rejects_missing_script_bucket() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {}
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.script_bucket");
    }

    #[test]
    fn validate_rejects_missing_glue_block() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole"
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.script_bucket");
    }

    #[test]
    fn validate_rejects_invalid_worker_type() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {
                "script_bucket": "my-bucket",
                "worker_type": "INVALID"
            }
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.worker_type");
    }

    #[test]
    fn validate_rejects_workers_below_one() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {
                "script_bucket": "my-bucket",
                "number_of_workers": 0
            }
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.number_of_workers");
    }

    #[test]
    fn validate_rejects_invalid_glue_version() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {
                "script_bucket": "my-bucket",
                "glue_version": "2.0"
            }
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.glue_version");
    }

    #[test]
    fn validate_rejects_timeout_below_one() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {
                "script_bucket": "my-bucket",
                "timeout": 0
            }
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.timeout");
    }

    #[test]
    fn validate_rejects_timeout_above_limit() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {
                "script_bucket": "my-bucket",
                "timeout": 10081
            }
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.timeout");
    }

    #[test]
    fn validate_rejects_negative_max_retries() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {
                "script_bucket": "my-bucket",
                "max_retries": -1
            }
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.max_retries");
    }

    #[test]
    fn validate_rejects_invalid_bookmark() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {
                "script_bucket": "my-bucket",
                "bookmark": "maybe"
            }
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.bookmark");
    }

    #[test]
    fn validate_rejects_connections_not_array() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {
                "script_bucket": "my-bucket",
                "connections": "not-an-array"
            }
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.connections");
    }

    #[test]
    fn validate_rejects_default_arguments_not_object() {
        let handler = GlueHandler::new();
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole",
            "glue": {
                "script_bucket": "my-bucket",
                "default_arguments": "not-an-object"
            }
        });

        let response = handler.validate("test-job", &config).unwrap();

        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].field, "glue.default_arguments");
    }

    // ── parse_s3_uri tests ─────────────────────────────────────────

    #[test]
    fn parse_s3_uri_valid_simple() {
        assert_eq!(parse_s3_uri("s3://bucket/key"), Some(("bucket", "key")));
    }

    #[test]
    fn parse_s3_uri_valid_nested_key() {
        assert_eq!(
            parse_s3_uri("s3://bucket/path/to/key"),
            Some(("bucket", "path/to/key"))
        );
    }

    #[test]
    fn parse_s3_uri_missing_prefix() {
        assert_eq!(parse_s3_uri("bucket/key"), None);
    }

    #[test]
    fn parse_s3_uri_empty_bucket() {
        assert_eq!(parse_s3_uri("s3:///key"), None);
    }

    #[test]
    fn parse_s3_uri_empty_key() {
        assert_eq!(parse_s3_uri("s3://bucket/"), None);
    }

    #[test]
    fn parse_s3_uri_no_key() {
        assert_eq!(parse_s3_uri("s3://bucket"), None);
    }

    // ── extract_glue_config tests ──────────────────────────────────

    #[test]
    fn extract_glue_config_valid() {
        let config = json!({
            "glue": {
                "script_bucket": "my-bucket",
                "worker_type": "G.1X"
            }
        });

        let result = extract_glue_config(&config);

        assert!(result.is_ok());
    }

    #[test]
    fn extract_glue_config_missing_block() {
        let config = json!({
            "role": "arn:aws:iam::123456789012:role/GlueRole"
        });

        let result = extract_glue_config(&config);

        assert!(result.is_err());
    }

    #[test]
    fn extract_glue_config_malformed() {
        let config = json!({
            "glue": "not_an_object"
        });

        let result = extract_glue_config(&config);

        assert!(result.is_err());
    }
}
