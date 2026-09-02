//! AWS SDK configuration builder with optional STS AssumeRole.
//!
//! Builds an [`aws_config::SdkConfig`] with region, retry policy, and
//! optional AssumeRole credentials wrapping the default provider chain.
//!
//! ## Credential Precedence (D-06)
//!
//! AssumeRole parameters are resolved with request JSON taking highest
//! priority, then environment variables, then YAML config values:
//!
//! | Parameter      | 1st (request JSON)              | 2nd (env var)               | 3rd (YAML config)         |
//! |----------------|---------------------------------|-----------------------------|---------------------------|
//! | `assume_role`  | `aws_cfg["assume_role"]`        | `YARD_AWS_ASSUME_ROLE`      | yaml `assume_role`        |
//! | `session_name` | —                               | `YARD_AWS_SESSION_NAME`     | yaml `session_name`       |
//! | `external_id`  | —                               | `YARD_AWS_EXTERNAL_ID`      | yaml `external_id`        |
//!
//! When no role ARN is found, falls through to the default provider chain
//! (env vars, shared config, IMDS/ECS task role, SSO).

use aws_config::BehaviorVersion;
use serde_json::Value;

/// Build a standard AWS SDK config with region, retry policy, and optional
/// STS `AssumeRole` wrapped around the default credential provider chain.
///
/// # Credential precedence (D-06)
///
/// Resolution of AssumeRole params:
///   1. Request JSON (`aws_cfg["assume_role"]`) — per-job isolation
///   2. `YARD_AWS_ASSUME_ROLE` env var — CI override
///   3. YAML config `assume_role` field — project-level default
///   4. Default provider chain (no AssumeRole)
///
/// Session name and external ID follow the same env-then-yaml pattern
/// (request JSON does not carry these; they come from the deployment
/// context rather than per-job config).
pub async fn aws_config(region: &str, aws_cfg: Option<&Value>) -> aws_config::SdkConfig {
    let region_obj = aws_config::Region::new(region.to_string());
    let base = aws_config::defaults(BehaviorVersion::latest())
        .region(region_obj.clone())
        .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3));

    let yaml_str = |key: &str| {
        aws_cfg
            .and_then(|v| v.get(key))
            .and_then(|v| v.as_str())
            .map(String::from)
    };

    // D-06 precedence: request JSON > env var > YAML config > default chain
    let assume_role = aws_cfg
        .and_then(|v| v.get("assume_role"))
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| std::env::var("YARD_AWS_ASSUME_ROLE").ok())
        .or_else(|| yaml_str("assume_role"));

    if let Some(role_arn) = assume_role {
        let session_name = std::env::var("YARD_AWS_SESSION_NAME")
            .ok()
            .or_else(|| yaml_str("session_name"))
            .unwrap_or_else(|| "yard".to_string());
        let external_id = std::env::var("YARD_AWS_EXTERNAL_ID")
            .ok()
            .or_else(|| yaml_str("external_id"));

        let mut builder = aws_config::sts::AssumeRoleProvider::builder(role_arn)
            .session_name(session_name)
            .region(region_obj);
        if let Some(eid) = external_id {
            builder = builder.external_id(eid);
        }
        let provider = builder.build().await;
        return base.credentials_provider(provider).load().await;
    }

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
