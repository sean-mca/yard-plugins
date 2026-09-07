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

    // Env vars beat config so CI can override (matches reference implementation).
    // Filter empty strings — `env::var().ok()` returns `Some("")` when a var
    // is set but blank, which would bypass config fallback and send an empty
    // ARN / external-id to the AWS API.
    let assume_role = std::env::var("YARD_AWS_ASSUME_ROLE")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| cfg_str("assume_role"));
    let session_name = std::env::var("YARD_AWS_SESSION_NAME")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| cfg_str("session_name"))
        .unwrap_or_else(|| "yard".to_string());
    let external_id = std::env::var("YARD_AWS_EXTERNAL_ID")
        .ok()
        .filter(|v| !v.is_empty())
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
    use std::sync::Mutex;

    /// Mutex to serialize tests that read/write YARD_AWS_* env vars.
    /// Env vars are process-global; concurrent tests race without this.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Clear all YARD_AWS_* env vars used by credential resolution.
    fn clear_yard_env() {
        std::env::remove_var("YARD_AWS_ASSUME_ROLE");
        std::env::remove_var("YARD_AWS_SESSION_NAME");
        std::env::remove_var("YARD_AWS_EXTERNAL_ID");
    }

    /// Drive an async body to completion on a fresh current-thread runtime.
    ///
    /// The two `aws_config` tests below are deliberately plain `#[test]` fns
    /// rather than `#[tokio::test]` (a departure from the `test-tokio-async`
    /// rule). `ENV_LOCK` must stay held for the whole SDK load — the provider
    /// chain reads process-global env vars while it resolves — but a
    /// `std::sync::MutexGuard` may not be held across an `.await`
    /// (`anti-lock-across-await`). Driving the future synchronously under the
    /// guard keeps the lock's coverage intact with no await point in the test
    /// body.
    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime should build")
            .block_on(future)
    }

    #[test]
    fn aws_config_returns_sdk_config_with_region() {
        let _lock = ENV_LOCK.lock().expect("env lock poisoned");
        clear_yard_env();

        let cfg = block_on(aws_config("us-east-1", None));

        let region = cfg.region().expect("region should be set");
        assert_eq!(region.as_ref(), "us-east-1");
    }

    #[test]
    fn aws_config_with_custom_region() {
        let _lock = ENV_LOCK.lock().expect("env lock poisoned");
        clear_yard_env();

        let cfg = block_on(aws_config("eu-west-1", None));

        let region = cfg.region().expect("region should be set");
        assert_eq!(region.as_ref(), "eu-west-1");
    }

    #[test]
    fn resolve_params_env_wins_over_config() {
        let _lock = ENV_LOCK.lock().expect("env lock poisoned");
        clear_yard_env();

        std::env::set_var("YARD_AWS_ASSUME_ROLE", "arn:aws:iam::111:role/EnvRole");
        std::env::set_var("YARD_AWS_SESSION_NAME", "env-session");
        std::env::set_var("YARD_AWS_EXTERNAL_ID", "env-ext-id");

        let cfg = serde_json::json!({
            "assume_role": "arn:aws:iam::222:role/ConfigRole",
            "session_name": "config-session",
            "external_id": "config-ext-id"
        });

        let (role, session, ext_id) = resolve_credential_params(Some(&cfg));

        assert_eq!(role.as_deref(), Some("arn:aws:iam::111:role/EnvRole"));
        assert_eq!(session, "env-session");
        assert_eq!(ext_id.as_deref(), Some("env-ext-id"));

        clear_yard_env();
    }

    #[test]
    fn resolve_params_config_fallback() {
        let _lock = ENV_LOCK.lock().expect("env lock poisoned");
        clear_yard_env();

        let cfg = serde_json::json!({
            "assume_role": "arn:aws:iam::222:role/ConfigRole",
            "session_name": "config-session",
            "external_id": "config-ext-id"
        });

        let (role, session, ext_id) = resolve_credential_params(Some(&cfg));

        assert_eq!(role.as_deref(), Some("arn:aws:iam::222:role/ConfigRole"));
        assert_eq!(session, "config-session");
        assert_eq!(ext_id.as_deref(), Some("config-ext-id"));
    }

    #[test]
    fn resolve_params_env_only() {
        let _lock = ENV_LOCK.lock().expect("env lock poisoned");
        clear_yard_env();

        std::env::set_var("YARD_AWS_ASSUME_ROLE", "arn:aws:iam::111:role/EnvRole");

        let (role, session, ext_id) = resolve_credential_params(None);

        assert_eq!(role.as_deref(), Some("arn:aws:iam::111:role/EnvRole"));
        assert_eq!(session, "yard");
        assert!(ext_id.is_none());

        clear_yard_env();
    }

    #[test]
    fn resolve_params_defaults_when_neither_set() {
        let _lock = ENV_LOCK.lock().expect("env lock poisoned");
        clear_yard_env();

        let (role, session, ext_id) = resolve_credential_params(None);

        assert!(role.is_none());
        assert_eq!(session, "yard");
        assert!(ext_id.is_none());
    }
}
