//! Shared codegen and AWS utility library for yard plugins.
//!
//! Provides the PySpark codegen pipeline (`codegen::generate_pyspark`)
//! and AWS helper types (`aws`) used by both `yard-plugin-glue` and
//! `yard-plugin-emr`.

pub mod aws;
pub mod codegen;
