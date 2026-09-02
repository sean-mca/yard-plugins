//! Codegen helper functions for rendering Python/PySpark fragments.
//!
//! These helpers are called by [`source`], [`sink`], [`transform`], and
//! [`super::generate_pyspark`] to emit import blocks, Spark option
//! chains, JDBC/Secrets Manager boilerplate, and partition derivation
//! logic. All string-building loops use [`std::fmt::Write`] to avoid
//! per-iteration allocations.

use std::fmt::Write;

use anyhow::{Result, anyhow};

use crate::codegen::types::{Import, JdbcAuth, JobConfig, RdsIamAuth, Source};

// --- Import rendering ---

/// Render a single [`Import`] as a Python import statement.
///
/// Returns `"from <module> import <name>"` when `import.from` is set,
/// otherwise `"import <name>"`.
#[inline]
#[must_use]
pub(super) fn render_import(import: &Import) -> String {
    match &import.from {
        Some(module) => format!("from {} import {}", module, import.name),
        None => format!("import {}", import.name),
    }
}

/// Render a slice of [`Import`]s as newline-separated Python import
/// statements.
#[must_use]
pub(super) fn render_imports(imports: &[Import]) -> String {
    let mut rendered = Vec::with_capacity(imports.len());
    for imp in imports {
        rendered.push(render_import(imp));
    }
    rendered.join("\n")
}

// --- Python string escaping ---

/// Escape a string and wrap it in double quotes for embedding inside
/// generated Python source. Handles backslashes, quotes, and control
/// characters (`\n`, `\r`, `\t`) so the result is always a valid
/// single-line Python string literal.
#[must_use]
pub(super) fn python_str_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

// --- Source rendering helpers ---

/// Render a [`serde_json::Value`] as a Python literal.
///
/// Strings, numbers, bools, and null map directly; arrays and objects
/// recurse. Used for opaque `options:` passthrough in Spark reader/writer
/// chains.
#[must_use]
pub(super) fn python_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "None".to_string(),
        serde_json::Value::Bool(b) => if *b { "True" } else { "False" }.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => python_str_literal(s),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(python_literal).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(obj) => {
            let mut keys: Vec<&String> = obj.keys().collect();
            keys.sort();
            let items: Vec<String> = keys
                .iter()
                .map(|k| format!("{}: {}", python_str_literal(k), python_literal(&obj[*k])))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

/// Return the engine for a source, falling back to `default_engine` when
/// the source does not specify one explicitly.
#[inline]
#[must_use]
pub(super) fn effective_engine(source: &Source, default_engine: &str) -> String {
    source
        .engine
        .clone()
        .unwrap_or_else(|| default_engine.to_string())
}

/// Return `value` if present, or an error naming the missing `field` on
/// the given source.
///
/// # Errors
///
/// Returns an error when `value` is `None`.
#[inline]
pub(super) fn require_str<'a>(
    value: Option<&'a str>,
    source_name: &str,
    field: &str,
) -> Result<&'a str> {
    value.ok_or_else(|| anyhow!("source '{source_name}': '{field}' is required"))
}

/// Build a Python dict literal from a seed of ordered (key, value) pairs,
/// merging in arbitrary user-supplied options afterward.
#[must_use]
pub(super) fn build_options_dict(
    seed: &[(&str, serde_json::Value)],
    user_opts: &std::collections::HashMap<String, serde_json::Value>,
) -> String {
    let mut opts = serde_json::Map::new();
    for (k, v) in seed {
        opts.insert((*k).to_string(), v.clone());
    }
    for (k, v) in user_opts {
        opts.insert(k.clone(), v.clone());
    }
    python_literal(&serde_json::Value::Object(opts))
}

/// Append `.option(k, v)` calls onto a Spark reader chain.
///
/// `seed` pairs are emitted as literal strings; `extra` entries use
/// [`python_literal`]. Writes directly into `chain` to avoid
/// per-iteration allocations.
pub(super) fn append_spark_options(
    chain: &mut String,
    seed: &[(&str, &str)],
    extra: &std::collections::HashMap<String, serde_json::Value>,
) {
    for (k, v) in seed {
        // write to String is infallible
        let _ = write!(chain, ".option({}, {})", python_str_literal(k), python_str_literal(v));
    }
    for (k, v) in extra {
        // write to String is infallible
        let _ = write!(chain, ".option({}, {})", python_str_literal(k), python_literal(v));
    }
}

// --- Sink helpers ---

/// Format a slice of column names as a comma-separated list of
/// double-quoted Python strings (e.g. `"col_a", "col_b"`).
#[inline]
#[must_use]
pub(super) fn quoted_list(cols: &[String]) -> String {
    cols.iter()
        .map(|c| python_str_literal(c))
        .collect::<Vec<_>>()
        .join(", ")
}

// --- Secrets Manager helper ---

/// Emit Python lines that fetch a secret from AWS Secrets Manager and
/// parse its JSON body into a local variable named `{prefix}_secret`.
#[must_use]
pub(super) fn render_secret_fetch(secret_id: &str, prefix: &str) -> String {
    let var = format!("{prefix}_secret");
    let escaped_id = python_str_literal(secret_id);
    [
        format!("    {var}_client = boto3.client(\"secretsmanager\")"),
        format!("    {var}_resp = {var}_client.get_secret_value(SecretId={escaped_id})"),
        format!("    {var} = json.loads({var}_resp[\"SecretString\"])"),
    ]
    .join("\n")
}

/// True when any source or the sink references a `secret_id`, meaning
/// the generated script needs `import boto3` and `import json`.
#[inline]
#[must_use]
pub(super) fn needs_secrets_imports(config: &JobConfig) -> bool {
    let source_has = config.sources.iter().any(|s| s.secret_id.is_some());
    let sink_has = config
        .sink
        .as_ref()
        .is_some_and(|s| s.secret_id.is_some());
    source_has || sink_has
}

// --- JDBC auth (RDS IAM) ---

/// True when any source or the sink uses JDBC auth (RDS IAM), meaning
/// the generated script needs `import boto3` for token generation.
#[inline]
#[must_use]
pub(super) fn needs_jdbc_auth_imports(config: &JobConfig) -> bool {
    let source_has = config.sources.iter().any(|s| s.auth.is_some());
    let sink_has = config.sink.as_ref().is_some_and(|s| s.auth.is_some());
    source_has || sink_has
}

/// Build a JDBC URL from RDS IAM auth credentials.
///
/// Produces `jdbc:<connection_type>://<host>:<port>/<database>` when
/// RDS IAM auth is configured. Returns an empty string otherwise.
#[must_use]
pub(super) fn derive_jdbc_url(auth: &JdbcAuth, connection_type: &str, database: &str) -> String {
    match &auth.rds_iam {
        Some(rds) => {
            format!(
                "jdbc:{connection_type}://{}:{}/{database}",
                rds.host, rds.port
            )
        }
        None => String::new(),
    }
}

/// Resolve the JDBC user / password expressions for a jdbc source/sink,
/// given its `secret_id` and `auth`. Returns `(user_expr, password_expr,
/// pre_lines)` -- `pre_lines` are Python statements emitted before the
/// reader/writer call, `user_expr` and `password_expr` are inlined into
/// `.option("user", ...)` / `.option("password", ...)`. Returns `None`
/// if no auth options should be emitted (neither secret_id nor auth set).
#[must_use]
pub(super) fn render_jdbc_auth(
    prefix: &str,
    secret_id: Option<&str>,
    auth: Option<&JdbcAuth>,
) -> Option<(String, String, Vec<String>)> {
    let secret_var = format!("{prefix}_secret");
    match (secret_id, auth) {
        (None, None) => None,
        (Some(_), None) => Some((
            format!("{secret_var}[\"username\"]"),
            format!("{secret_var}[\"password\"]"),
            Vec::new(),
        )),
        (secret, Some(jdbc_auth)) => {
            let rds = jdbc_auth.rds_iam.as_ref()?;
            let user_expr = if secret.is_some() {
                format!("{secret_var}[\"username\"]")
            } else {
                let u = rds.username.as_deref().unwrap_or("");
                python_str_literal(u)
            };
            let token_var = format!("{prefix}_token");
            let pre = render_rds_iam_token_fetch(prefix, rds, &user_expr);
            Some((user_expr, token_var, pre))
        }
    }
}

/// Emit Python lines that call `generate_db_auth_token` to get a
/// short-lived RDS IAM authentication token.
fn render_rds_iam_token_fetch(prefix: &str, rds: &RdsIamAuth, user_expr: &str) -> Vec<String> {
    let RdsIamAuth {
        host, port, region, ..
    } = rds;
    let client_var = format!("_{prefix}_rds");
    let token_var = format!("{prefix}_token");
    let escaped_region = python_str_literal(region);
    let escaped_host = python_str_literal(host);
    vec![
        format!("    {client_var} = boto3.client(\"rds\", region_name={escaped_region})"),
        format!("    {token_var} = {client_var}.generate_db_auth_token("),
        format!("        DBHostname={escaped_host},"),
        format!("        Port={port},"),
        format!("        DBUsername={user_expr},"),
        format!("        Region={escaped_region},"),
        "    )".to_string(),
    ]
}

/// True when the job definition includes an Iceberg sink.
#[inline]
#[must_use]
pub(super) fn has_iceberg_sink(config: &JobConfig) -> bool {
    config
        .sink
        .as_ref()
        .is_some_and(|s| s.sink_type == "iceberg")
}

/// True when the iceberg sink should emit the schema-conform pass.
/// Opt-in by default for iceberg sinks; `fill_nulls: false` opts out.
#[must_use]
pub(super) fn should_fill_nulls(config: &JobConfig) -> bool {
    config
        .sink
        .as_ref()
        .is_some_and(|s| s.sink_type == "iceberg" && s.fill_nulls != Some(false))
}

/// Emit Python lines that derive partition columns (year/month/day) from
/// a timestamp column, adding them to the dataframe only when absent.
///
/// Returns `Ok(None)` when `config.partition_by` is empty.
///
/// # Errors
///
/// Returns an error when `config.partition_by` contains an unrecognized
/// unit (anything other than "year", "month", or "day").
pub(super) fn render_partition_derivation(
    config: &JobConfig,
    sink_source: &str,
) -> Result<Option<String>> {
    if config.partition_by.is_empty() {
        return Ok(None);
    }
    let var = format!("df_{sink_source}");
    let mut lines = Vec::with_capacity(2 + config.partition_by.len());
    lines.push("    # --- Partition columns ---".to_string());
    if config.create_timestamp {
        lines.push(format!(
            "    {var} = {var}.withColumn(\"ingestion_timestamp\", F.current_timestamp())"
        ));
        lines.push("    _ts = \"ingestion_timestamp\"".to_string());
    } else {
        let col = config
            .partition_timestamp_column
            .as_deref()
            .unwrap_or("event_time");
        lines.push(format!("    _ts = {}", python_str_literal(col)));
    }
    for unit in &config.partition_by {
        let func = match unit.as_str() {
            "year" => "year",
            "month" => "month",
            "day" => "dayofmonth",
            other => {
                return Err(anyhow!(
                    "unsupported partition_by unit: '{other}' (expected year, month, or day)"
                ));
            }
        };
        let escaped_unit = python_str_literal(unit);
        lines.push(format!(
            "    if {escaped_unit} not in {var}.columns:\n        \
             {var} = {var}.withColumn({escaped_unit}, F.{func}(F.col(_ts)))"
        ));
    }
    Ok(Some(lines.join("\n")))
}

/// True when any transform requires `from pyspark.sql import functions as F`.
#[inline]
#[must_use]
pub(super) fn needs_functions_import(config: &JobConfig) -> bool {
    config
        .transforms
        .iter()
        .any(|t| matches!(t.transform_type.as_str(), "aggregate" | "window"))
}

/// True when any transform is a window function, requiring the
/// `from pyspark.sql.window import Window` import.
#[inline]
#[must_use]
pub(super) fn needs_window_import(config: &JobConfig) -> bool {
    config
        .transforms
        .iter()
        .any(|t| t.transform_type == "window")
}

/// True when the generated script needs
/// `from awsglue.dynamicframe import DynamicFrame`.
///
/// Covers catalog sources/sinks, glue-engine S3/JDBC sources, and PII
/// masking (which requires `DynamicFrame.fromDF()` / `.toDF()` for the
/// `EntityDetector.detect()` sandwich).
#[inline]
#[must_use]
pub(super) fn needs_dynamic_frame_import(config: &JobConfig, default_engine: &str) -> bool {
    config
        .sink
        .as_ref()
        .is_some_and(|s| s.sink_type == "catalog")
        || config.sources.iter().any(|s| {
            s.source_type == "catalog"
                || (matches!(s.source_type.as_str(), "s3" | "jdbc")
                    && effective_engine(s, default_engine) == "glue")
        })
        || needs_pii_imports(config)
}

/// True when any source is an API source, requiring `import requests`.
#[inline]
#[must_use]
pub(super) fn needs_requests_import(config: &JobConfig) -> bool {
    config.sources.iter().any(|s| s.source_type == "api")
}

/// True when the generated script needs the `EntityDetector` import
/// for PII masking.
///
/// Returns `true` only when `mask_pii` is non-empty AND neither `body`
/// nor `job_file` is set (those paths skip codegen entirely).
#[inline]
#[must_use]
pub(super) fn needs_pii_imports(config: &JobConfig) -> bool {
    !config.mask_pii.is_empty() && config.body.is_none() && config.job_file.is_none()
}

/// Determine the default Spark engine (`"spark"` or `"glue"`) for a job
/// by reading `config.<job_type>.default_engine`, falling back to
/// `"spark"`.
#[must_use]
pub(super) fn default_engine_for(config: &JobConfig) -> String {
    let job_type = config.job_type.as_deref().unwrap_or("glue");
    config
        .config
        .get(job_type)
        .and_then(|g| g.get("default_engine"))
        .and_then(|v| v.as_str())
        .unwrap_or("spark")
        .to_string()
}

/// Indent every non-blank line of `body` by four spaces for inclusion
/// inside the generated `run()` function.
#[must_use]
pub(super) fn indent_body(body: &str) -> String {
    body.lines()
        .map(|line| {
            if line.trim().is_empty() {
                String::new()
            } else {
                format!("    {}", line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}
