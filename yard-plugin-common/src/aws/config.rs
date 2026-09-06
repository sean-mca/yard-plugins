//! AWS SDK configuration builder with optional STS AssumeRole.
//!
//! Builds an [`aws_config::SdkConfig`] with region, retry policy, and
//! optional AssumeRole credentials wrapping the default provider chain.
//!
//! ## Credential Precedence (D-06)
//!
//! AssumeRole parameters are resolved with environment variables taking
//! highest priority, then request JSON — so CI can override static config:
//!
//! | Parameter      | 1st (env var)                   | 2nd (request JSON)          |
//! |----------------|---------------------------------|-----------------------------|
//! | `assume_role`  | `YARD_AWS_ASSUME_ROLE`          | `aws_cfg["assume_role"]`    |
//! | `session_name` | `YARD_AWS_SESSION_NAME`         | `aws_cfg["session_name"]`   |
//! | `external_id`  | `YARD_AWS_EXTERNAL_ID`          | `aws_cfg["external_id"]`    |
//!
//! When no role ARN is found, falls through to the default provider chain
//! (env vars, shared config, IMDS/ECS task role, SSO).

use aws_config::BehaviorVersion;
use serde_json::Value;

/// Resolve AssumeRole credential parameters with env > config precedence.
///
/// Returns `(assume_role, session_name, external_id)`. Environment variables
/// beat request JSON so CI pipelines can override static repo config
/// (matches the reference implementation).
pub fn resolve_credential_params(
    aws_cfg: Option<&Value>,
) -> (Option<String>, String, Option<String>) {
    let cfg_str = |key: &str| {
        aws_cfg
            .and_then(|v| v.get(key))
            .and_then(|v| v.as_str())
            .map(String::from)
    };

    // Env vars beat config so CI can override (matches reference implementation)
    let assume_role = std::env::var("YARD_AWS_ASSUME_ROLE")
        .ok()
        .or_else(|| cfg_str("assume_role"));
    let session_name = std::env::var("YARD_AWS_SESSION_NAME")
        .ok()
        .or_else(|| cfg_str("session_name"))
        .unwrap_or_else(|| "yard".to_string());
    let external_id = std::env::var("YARD_AWS_EXTERNAL_ID")
        .ok()
        .or_else(|| cfg_str("external_id"));

    (assume_role, session_name, external_id)
}

/// Build a standard AWS SDK config with region, retry policy, and optional
/// STS `AssumeRole` wrapped around the default credential provider chain.
///
/// # Credential precedence (D-06)
///
/// Resolution of AssumeRole params:
///   1. `YARD_AWS_ASSUME_ROLE` env var — CI override
///   2. Request JSON (`aws_cfg["assume_role"]`) — per-job config
///   3. Default provider chain (no AssumeRole)
///
/// Session name and external ID follow the same env-first pattern.
pub async fn aws_config(region: &str, aws_cfg: Option<&Value>) -> aws_config::SdkConfig {
    let region_obj = aws_config::Region::new(region.to_string());
    tracing::info!(region = %region, "AWS region resolved");

    let base = aws_config::defaults(BehaviorVersion::latest())
        .region(region_obj.clone())
        .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3));

    // D-06 precedence: env var > config > default chain
    let (assume_role, session_name, external_id) = resolve_credential_params(aws_cfg);

    if let Some(role_arn) = assume_role {
        tracing::info!(
            role_arn = %role_arn,
            session_name = %session_name,
            has_external_id = external_id.is_some(),
            "Using STS AssumeRole"
        );

        let mut builder = aws_config::sts::AssumeRoleProvider::builder(role_arn)
            .session_name(session_name)
            .region(region_obj);
        if let Some(eid) = external_id {
            builder = builder.external_id(eid);
        }
        let provider = builder.build().await;
        return base.credentials_provider(provider).load().await;
    }

    tracing::info!("Using default credential provider chain");
    base.load().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn aws_config_returns_sdk_config_with_region() {
        // Ensure no env vars interfere with this test
        std::env::remove_var("YARD_AWS_ASSUME_ROLE");
        std::env::remove_var("YARD_AWS_SESSION_NAME");
        std::env::remove_var("YARD_AWS_EXTERNAL_ID");

        let cfg = aws_config(
            "us-east-1",
            None,
        )
        .await;

        // Verify region is set correctly
        let region = cfg.region().expect("region should be set");
        assert_eq!(region.as_ref(), "us-east-1");
    }

    #[tokio::test]
    async fn aws_config_with_custom_region() {
        std::env::remove_var("YARD_AWS_ASSUME_ROLE");

        let cfg = aws_config("eu-west-1", None).await;

        let region = cfg.region().expect("region should be set");
        assert_eq!(region.as_ref(), "eu-west-1");
    }
}
