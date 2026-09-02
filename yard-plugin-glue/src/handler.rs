//! Glue plugin handler implementing the `PluginHandler` trait.
//!
//! All eight trait methods are stubbed with valid empty responses.
//! The embedded tokio runtime provides the sync/async bridge for
//! future phases that call the AWS SDK.

use anyhow::Result;
use yard_plugin_sdk::{
    CodegenResponse, DeployResponse, DestroyResponse, PluginHandler, Resource, SchemaResponse,
    ValidateResponse, VerifyResponse,
};

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
        _job_config: &serde_json::Value,
    ) -> Result<ValidateResponse> {
        Ok(ValidateResponse { errors: vec![] })
    }

    fn codegen(
        &self,
        _job_name: &str,
        _job_config: &serde_json::Value,
    ) -> Result<CodegenResponse> {
        Ok(CodegenResponse { script: None })
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
        Ok(SchemaResponse::default())
    }
}
