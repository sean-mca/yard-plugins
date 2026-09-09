//! Mirror types for codegen input, deserialized from `serde_json::Value`.
//!
//! These types faithfully represent the relevant parts of `yard-structs`
//! (`Source`, `Sink`, `Transform`, `Import`, `JobDefinition`, etc.) but
//! are independently owned by this crate (decision D-03). No dependency
//! on `yard-structs` is introduced.

use std::collections::HashMap;

use serde::Deserialize;

/// Top-level job configuration, deserialized from the host's
/// `serde_json::Value` payload.
#[derive(Debug, Deserialize)]
pub struct JobConfig {
    /// Job type string: "glue", etc.
    #[serde(default)]
    pub job_type: Option<String>,

    /// User-defined Python imports.
    #[serde(default)]
    pub imports: Vec<Import>,

    /// Optional body override: if set, replaces all source/transform/sink
    /// rendering with this raw Python code.
    #[serde(default)]
    pub body: Option<String>,

    /// Optional external script file path: if set, the file contents are
    /// returned verbatim.
    #[serde(default)]
    pub job_file: Option<String>,

    /// Source definitions.
    #[serde(default)]
    pub sources: Vec<Source>,

    /// Sink definition.
    #[serde(default)]
    pub sink: Option<Sink>,

    /// Transform definitions.
    #[serde(default)]
    pub transforms: Vec<Transform>,

    /// PII masking entity types.
    #[serde(default)]
    pub mask_pii: Vec<String>,

    /// Partition columns (year, month, day).
    #[serde(default)]
    pub partition_by: Vec<String>,

    /// Column to derive partition values from.
    #[serde(default)]
    pub partition_timestamp_column: Option<String>,

    /// Whether to create an ingestion_timestamp column.
    #[serde(default)]
    pub create_timestamp: bool,

    /// Provider-specific configuration (e.g. `{"glue": {"warehouse": "..."}}`).
    #[serde(default)]
    pub config: serde_json::Value,
}

/// A single Python import statement.
#[derive(Debug, Deserialize)]
pub struct Import {
    /// Module or symbol name to import.
    pub name: String,

    /// Optional `from` module (`from X import name`).
    #[serde(default)]
    pub from: Option<String>,
}

/// JDBC authentication configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct JdbcAuth {
    /// RDS IAM authentication configuration.
    #[serde(default)]
    pub rds_iam: Option<RdsIamAuth>,
}

/// RDS IAM authentication parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct RdsIamAuth {
    /// DB username. Optional when `secret_id` is also set.
    #[serde(default)]
    pub username: Option<String>,

    /// RDS endpoint hostname.
    pub host: String,

    /// RDS endpoint port.
    pub port: u16,

    /// AWS region for the RDS instance.
    pub region: String,
}

/// A data source definition.
#[derive(Debug, Deserialize)]
pub struct Source {
    /// Source name (used as dataframe variable: `df_{name}`).
    pub name: String,

    /// Source type: "s3", "jdbc", "catalog", "kafka", "api".
    pub source_type: String,

    /// Data format (e.g. "parquet", "csv", "json").
    #[serde(default)]
    pub format: Option<String>,

    /// File path or S3 URI.
    #[serde(default)]
    pub path: Option<String>,

    /// JDBC connection URL.
    #[serde(default)]
    pub connection_url: Option<String>,

    /// Database table name.
    #[serde(default)]
    pub table: Option<String>,

    /// Database name.
    #[serde(default)]
    pub database: Option<String>,

    /// AWS Secrets Manager secret ID.
    #[serde(default)]
    pub secret_id: Option<String>,

    /// Spark engine: "spark" or "glue".
    #[serde(default)]
    pub engine: Option<String>,

    /// JDBC connection type for Glue (mysql, postgresql, etc.).
    #[serde(default)]
    pub connection_type: Option<String>,

    /// Kafka topic name.
    #[serde(default)]
    pub topic: Option<String>,

    /// API endpoint URL.
    #[serde(default)]
    pub url: Option<String>,

    /// HTTP headers for API sources.
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Additional Spark reader options.
    #[serde(default)]
    pub options: HashMap<String, serde_json::Value>,

    /// JDBC authentication configuration.
    #[serde(default)]
    pub auth: Option<JdbcAuth>,

    /// Column names (used by some source types).
    #[serde(default)]
    #[allow(dead_code)]
    pub columns: Vec<String>,
}

/// A data sink definition.
#[derive(Debug, Clone, Deserialize)]
pub struct Sink {
    /// Source dataframe name to write from.
    #[serde(default)]
    pub source: Option<String>,

    /// Sink type: "s3", "jdbc", "catalog", "iceberg".
    pub sink_type: String,

    /// Data format (e.g. "parquet", "csv", "json").
    #[serde(default)]
    pub format: Option<String>,

    /// Output path or S3 URI.
    #[serde(default)]
    pub path: Option<String>,

    /// JDBC connection URL.
    #[serde(default)]
    pub connection_url: Option<String>,

    /// Database table name.
    #[serde(default)]
    pub table: Option<String>,

    /// Database name.
    #[serde(default)]
    pub database: Option<String>,

    /// AWS Secrets Manager secret ID.
    #[serde(default)]
    pub secret_id: Option<String>,

    /// Write mode (overwrite, append, etc.).
    #[serde(default)]
    pub mode: Option<String>,

    /// Partition columns.
    #[serde(default)]
    pub partition_by: Vec<String>,

    /// JDBC connection type for Glue.
    #[serde(default)]
    pub connection_type: Option<String>,

    /// Whether to emit fill_nulls schema-conform pass for Iceberg.
    #[serde(default)]
    pub fill_nulls: Option<bool>,

    /// JDBC authentication configuration.
    #[serde(default)]
    pub auth: Option<JdbcAuth>,

    /// Additional Spark writer options.
    #[serde(default)]
    #[allow(dead_code)]
    pub options: HashMap<String, serde_json::Value>,

    /// Glue catalog ID for Iceberg sinks.
    #[serde(default)]
    #[allow(dead_code)]
    pub catalog_id: Option<String>,
}

/// A transform operation definition.
#[derive(Debug, Deserialize)]
pub struct Transform {
    /// Transform type: "filter", "sql", "join", "drop_columns", "select",
    /// "rename", "add_column", "aggregate", "window".
    pub transform_type: String,

    /// Input source name (defaults to first source).
    #[serde(default)]
    pub source: Option<String>,

    /// Output dataframe name (defaults to input name).
    #[serde(default)]
    pub output: Option<String>,

    /// Filter condition expression.
    #[serde(default)]
    pub condition: Option<String>,

    /// SQL query string.
    #[serde(default)]
    pub query: Option<String>,

    /// Column names (for drop_columns, select).
    #[serde(default)]
    pub columns: Vec<String>,

    /// Rename mapping (old name -> new name).
    #[serde(default)]
    pub mapping: HashMap<String, String>,

    /// Column name (for add_column, window).
    #[serde(default)]
    pub name: Option<String>,

    /// Expression (for add_column, window).
    #[serde(default)]
    pub expression: Option<String>,

    /// Left dataframe name (for join).
    #[serde(default)]
    pub left: Option<String>,

    /// Right dataframe name (for join).
    #[serde(default)]
    pub right: Option<String>,

    /// Join key column (for join).
    #[serde(default)]
    pub on: Option<String>,

    /// Join type (for join: "inner", "left", etc.).
    #[serde(default)]
    pub how: Option<String>,

    /// Group-by columns (for aggregate).
    #[serde(default)]
    pub group_by: Vec<String>,

    /// Aggregation expressions (alias -> expression).
    #[serde(default)]
    pub aggs: HashMap<String, String>,

    /// Partition-by columns (for window).
    #[serde(default)]
    pub partition_by: Vec<String>,

    /// Order-by specifications (for window).
    #[serde(default)]
    pub order_by: Vec<OrderBySpec>,
}

/// Order-by column specification for window transforms.
#[derive(Debug, Deserialize)]
pub struct OrderBySpec {
    /// Column name.
    pub column: String,

    /// Descending order.
    #[serde(default)]
    pub desc: bool,
}
