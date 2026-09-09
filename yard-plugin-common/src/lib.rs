//! Shared codegen and AWS utility library for yard plugins.
//!
//! Provides the PySpark codegen pipeline (`codegen::generate_pyspark`)
//! and AWS helper types (`aws`) used by `yard-plugin-glue`.

pub mod aws;
pub mod codegen;
