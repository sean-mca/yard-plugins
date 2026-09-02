//! PySpark codegen from yard job definitions.
//!
//! This module generates complete Python scripts for AWS Glue and EMR
//! jobs by combining Tera templates with dynamic source, transform,
//! and sink rendering. Sub-modules handle distinct rendering concerns.

pub(crate) mod helpers;
mod sink;
mod source;
mod transform;
pub(crate) mod types;
