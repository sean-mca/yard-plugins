//! AWS utility module for credential resolution and S3 operations.
//!
//! Provides shared AWS config loading ([`aws_config`]) and S3 script
//! management ([`S3ScriptOps`]) used by both plugin handlers during
//! deploy/destroy/verify operations.

pub mod config;
pub mod s3;

pub use config::aws_config;
pub use s3::S3ScriptOps;
