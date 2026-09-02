//! PySpark/Glue script codegen from yard job definitions.
//!
//! This module generates complete Python scripts for AWS Glue and EMR
//! jobs by combining Tera templates with dynamic source, transform,
//! and sink rendering. The generated scripts include import management,
//! Spark session setup, null-handling helpers for Iceberg sinks, and
//! proper Glue `job.commit()` teardown.
//!
//! Sub-modules handle distinct rendering concerns:
//! - `helpers` -- shared utilities (import rendering, Spark options, JDBC auth, partitions)
//! - `source` -- reader calls per source type (S3, JDBC, Catalog, Kafka, API)
//! - `sink` -- writer calls per sink type (S3, JDBC, Catalog, Iceberg)
//! - `transform` -- transform operations (filter, SQL, join, aggregate, window, etc.)
//! - `pii` -- PII masking (EntityDetector column transforms)

mod helpers;
mod pii;
mod sink;
mod source;
mod transform;
pub(crate) mod types;

use anyhow::{Context as AnyhowContext, Result, anyhow};
use tera::{Context, Tera};

use crate::codegen::types::JobConfig;

use helpers::*;
use pii::render_pii;
use sink::render_sink;
use source::render_sources;
use transform::render_transforms;

/// Tera template for AWS Glue PySpark jobs.
const GLUE_TEMPLATE: &str = include_str!("../templates/glue.py.tera");

/// Tera template for EMR PySpark jobs.
const EMR_TEMPLATE: &str = include_str!("../templates/emr.py.tera");

/// Emitted inline at module scope when an iceberg sink is writing with
/// `fill_nulls` enabled. Provides a single schema-conform path (Spark 3.5 /
/// Glue 5): voids are dropped recursively (never invented), the batch is
/// reconciled to a target schema via `DataFrame.to(target)`, and existing
/// tables auto-evolve by merging the live schema with the void-free batch.
/// A fail-fast guard refuses writes where the source kind diverges from the
/// table kind (struct vs list/map). Opt-out via `fill_nulls: false`.
const ICEBERG_FILL_NULLS_HELPERS: &str = r#"def _yard_has_void(dt):
    if isinstance(dt, NullType):
        return True
    if isinstance(dt, StructType):
        if len(dt.fields) == 0:
            return True
        return any(_yard_has_void(f.dataType) for f in dt.fields)
    if isinstance(dt, ArrayType):
        return _yard_has_void(dt.elementType)
    if isinstance(dt, MapType):
        return _yard_has_void(dt.keyType) or _yard_has_void(dt.valueType)
    return False


def _yard_void_free_ddl(dt):
    if isinstance(dt, NullType):
        return None
    if isinstance(dt, StructType):
        if len(dt.fields) == 0:
            return "struct<>"
        parts = []
        for f in dt.fields:
            sub = _yard_void_free_ddl(f.dataType)
            if sub is not None:
                parts.append("`" + f.name.replace("`", "``") + "`:" + sub)
        if not parts:
            return "struct<>"
        return "struct<" + ",".join(parts) + ">"
    if isinstance(dt, ArrayType):
        inner = _yard_void_free_ddl(dt.elementType)
        if inner is None:
            return None
        return "array<" + inner + ">"
    if isinstance(dt, MapType):
        k = _yard_void_free_ddl(dt.keyType)
        v = _yard_void_free_ddl(dt.valueType)
        if k is None or v is None:
            return None
        return "map<" + k + "," + v + ">"
    return dt.simpleString()


def _yard_void_free_dt(dt):
    # Recursively drop void leaves, empty structs, array<void>, map<..void..> at
    # any depth. Returns a cleaned DataType, or None when the type collapses
    # entirely (caller drops the field). Voids are dropped, never invented: a
    # NullType column carries zero inferable type, so the only honest move is to
    # omit it until a real value re-introduces it via schema evolution.
    if isinstance(dt, NullType):
        return None
    if isinstance(dt, StructType):
        fields = []
        for f in dt.fields:
            sub = _yard_void_free_dt(f.dataType)
            if sub is not None:
                fields.append(StructField(f.name, sub, True))
        if not fields:
            return None
        return StructType(fields)
    if isinstance(dt, ArrayType):
        inner = _yard_void_free_dt(dt.elementType)
        if inner is None:
            return None
        return ArrayType(inner, True)
    if isinstance(dt, MapType):
        k = _yard_void_free_dt(dt.keyType)
        v = _yard_void_free_dt(dt.valueType)
        if k is None or v is None:
            return None
        return MapType(k, v, True)
    return dt


def _yard_void_free_schema(schema):
    fields = []
    for f in schema.fields:
        sub = _yard_void_free_dt(f.dataType)
        if sub is not None:
            fields.append(StructField(f.name, sub, True))
    return StructType(fields)


def _yard_kind(dt):
    if isinstance(dt, StructType):
        return "struct"
    if isinstance(dt, ArrayType):
        return "array"
    if isinstance(dt, MapType):
        return "map"
    return "scalar"


def _yard_kind_mismatch(src_dt, tgt_dt):
    # True when the source-inferred kind diverges from the target kind in a way
    # that cannot be reconciled by casting (struct vs list/map, or scalar vs
    # container). Scalar-vs-scalar mismatches are handled by explicit cast.
    ks, kt = _yard_kind(src_dt), _yard_kind(tgt_dt)
    if ks == kt:
        return False
    return ks != "scalar" or kt != "scalar"


def _yard_merge_dt(live_dt, batch_dt):
    if isinstance(live_dt, StructType) and isinstance(batch_dt, StructType):
        return _yard_merge_schema(live_dt, batch_dt)
    if isinstance(live_dt, ArrayType) and isinstance(batch_dt, ArrayType):
        return ArrayType(_yard_merge_dt(live_dt.elementType, batch_dt.elementType), True)
    if isinstance(live_dt, MapType) and isinstance(batch_dt, MapType):
        return MapType(live_dt.keyType, _yard_merge_dt(live_dt.valueType, batch_dt.valueType), True)
    return live_dt


def _yard_merge_schema(live, batch):
    # Union: live field types win; genuinely-new typed fields from the batch are
    # added (including nested); nested structs merge field-by-field. The result
    # is the auto-evolve target the dataframe is conformed to before writing.
    batch_map = {f.name: f.dataType for f in batch.fields}
    out = []
    seen = set()
    for f in live.fields:
        seen.add(f.name)
        if f.name in batch_map:
            out.append(StructField(f.name, _yard_merge_dt(f.dataType, batch_map[f.name]), True))
        else:
            out.append(StructField(f.name, f.dataType, True))
    for f in batch.fields:
        if f.name not in seen:
            out.append(StructField(f.name, f.dataType, True))
    return StructType(out)


def _yard_read_iceberg_schema(spark, tbl):
    return spark.read.format("iceberg").load(tbl).schema


def _yard_conform(df, target_schema):
    src_types = {f.name: f.dataType for f in df.schema.fields}
    cols = []
    for f in target_schema.fields:
        if f.name in src_types and src_types[f.name] != f.dataType:
            cols.append(df[f.name].cast(f.dataType).alias(f.name))
        elif f.name in src_types:
            cols.append(df[f.name])
        else:
            cols.append(F.lit(None).cast(f.dataType).alias(f.name))
    return df.select(cols)
"#;

/// Generate a complete PySpark script for the given job definition.
///
/// For Glue and EMR job types, renders sources, transforms, sink, and
/// import management into a Tera template. If `job_file` is set, returns
/// the external file contents verbatim.
///
/// # Errors
///
/// Returns an error when:
/// - Required source/sink fields are missing
/// - The Tera template fails to render
/// - An external `job_file` cannot be read
/// - The job type has no codegen template
pub fn generate_pyspark(job_name: &str, job_config: &serde_json::Value) -> Result<String> {
    let config: JobConfig = serde_json::from_value(job_config.clone())
        .context("failed to parse job config for codegen")?;

    // If job_file is specified, use the external file as the complete script
    if let Some(ref path) = config.job_file {
        return std::fs::read_to_string(path)
            .with_context(|| format!("failed to read job_file: {path}"));
    }

    let job_type = config.job_type.as_deref().unwrap_or("glue");
    let template = match job_type {
        "glue" => GLUE_TEMPLATE,
        "emr" => EMR_TEMPLATE,
        other => return Err(anyhow!("unsupported job type for codegen: {other}")),
    };

    let mut tera = Tera::default();
    tera.add_raw_template("script", template)?;

    // Determine the default source name (first source, or "source")
    let default_source = config
        .sources
        .first()
        .map(|s| s.name.as_str())
        .unwrap_or("source");

    let all_source_names: Vec<String> = config.sources.iter().map(|s| s.name.clone()).collect();

    let default_engine = default_engine_for(&config);

    // Build extra imports needed by template features (at most ~8 entries)
    let mut extra_imports = Vec::with_capacity(8);
    let iceberg = has_iceberg_sink(&config);
    let partitioning = !config.partition_by.is_empty();
    if needs_secrets_imports(&config) || needs_jdbc_auth_imports(&config) || iceberg {
        extra_imports.push("import boto3".to_string());
        if needs_secrets_imports(&config) {
            extra_imports.push("import json".to_string());
        }
    }
    if needs_requests_import(&config) {
        extra_imports.push("import requests".to_string());
    }
    if needs_dynamic_frame_import(&config, &default_engine) {
        extra_imports.push("from awsglue.dynamicframe import DynamicFrame".to_string());
    }
    let fill_nulls = should_fill_nulls(&config);
    if needs_functions_import(&config) || partitioning || fill_nulls {
        extra_imports.push("from pyspark.sql import functions as F".to_string());
    }
    if fill_nulls {
        extra_imports.push(
            "from pyspark.sql.types import (StructType, StructField, ArrayType, MapType, DoubleType, \
             FloatType, IntegerType, LongType, ShortType, ByteType, TimestampType, DateType, \
             DecimalType, BinaryType, BooleanType, NullType)"
                .to_string(),
        );
    }
    if needs_window_import(&config) {
        extra_imports.push("from pyspark.sql.window import Window".to_string());
    }
    if needs_pii_imports(&config) {
        extra_imports.push("from awsglueml.transforms import EntityDetector".to_string());
    }

    let user_imports = render_imports(&config.imports);
    let mut all_imports = if extra_imports.is_empty() {
        user_imports
    } else if user_imports.is_empty() {
        extra_imports.join("\n")
    } else {
        format!("{}\n{}", user_imports, extra_imports.join("\n"))
    };
    if fill_nulls {
        // Append the helpers at module scope (after the imports block, before
        // the Glue/Spark setup). The template inlines `imports_block` verbatim.
        all_imports.push_str("\n\n\n");
        all_imports.push_str(ICEBERG_FILL_NULLS_HELPERS);
    }

    // Build the run() body
    let run_body = if let Some(body) = &config.body {
        indent_body(body)
    } else {
        let mut parts = Vec::with_capacity(3);
        if !config.sources.is_empty() {
            parts.push(format!(
                "    # --- Sources ---\n{}",
                render_sources(&config.sources, &default_engine)?
            ));
        }
        if !config.transforms.is_empty() {
            parts.push(format!(
                "    # --- Transforms ---\n{}",
                render_transforms(&config.transforms, default_source, &all_source_names)?
            ));
        }
        if let Some(sink) = &config.sink {
            let sink_source = sink.source.as_deref().unwrap_or(default_source);
            if let Some(deriv) = render_partition_derivation(&config, sink_source) {
                parts.push(deriv);
            }
            if !config.mask_pii.is_empty() {
                let pii_source = format!("df_{sink_source}");
                parts.push(format!(
                    "    # --- PII Masking ---\n{}",
                    render_pii(&config.mask_pii, &pii_source)
                ));
            }
            // Mirror job-level partition_by onto the iceberg sink so writeTo
            // emits `.partitionedBy(...)` on first table creation.
            let effective_sink = if sink.sink_type == "iceberg" && !config.partition_by.is_empty() {
                let mut partition_by = config.partition_by.clone();
                // Merge: keep sink's existing partition_by and append job-level ones
                if !sink.partition_by.is_empty() {
                    partition_by = sink.partition_by.clone();
                }
                crate::codegen::types::Sink {
                    source: sink.source.clone(),
                    sink_type: sink.sink_type.clone(),
                    format: sink.format.clone(),
                    path: sink.path.clone(),
                    connection_url: sink.connection_url.clone(),
                    table: sink.table.clone(),
                    database: sink.database.clone(),
                    secret_id: sink.secret_id.clone(),
                    mode: sink.mode.clone(),
                    partition_by,
                    connection_type: sink.connection_type.clone(),
                    fill_nulls: sink.fill_nulls,
                    auth: None,
                    options: sink.options.clone(),
                    catalog_id: sink.catalog_id.clone(),
                }
            } else {
                // Create a reference-compatible owned copy
                crate::codegen::types::Sink {
                    source: sink.source.clone(),
                    sink_type: sink.sink_type.clone(),
                    format: sink.format.clone(),
                    path: sink.path.clone(),
                    connection_url: sink.connection_url.clone(),
                    table: sink.table.clone(),
                    database: sink.database.clone(),
                    secret_id: sink.secret_id.clone(),
                    mode: sink.mode.clone(),
                    partition_by: sink.partition_by.clone(),
                    connection_type: sink.connection_type.clone(),
                    fill_nulls: sink.fill_nulls,
                    auth: None,
                    options: sink.options.clone(),
                    catalog_id: sink.catalog_id.clone(),
                }
            };
            let catalog_id = config
                .config
                .get("glue")
                .and_then(|g| g.get("catalog_id"))
                .and_then(|v| v.as_str());
            parts.push(format!(
                "    # --- Sink ---\n{}",
                render_sink(&effective_sink, default_source, fill_nulls, catalog_id)?
            ));
        }
        if parts.is_empty() {
            "    pass".to_string()
        } else {
            parts.join("\n\n")
        }
    };

    let mut context = Context::new();
    context.insert("job_name", job_name);
    context.insert("job_type", job_type);
    context.insert("imports_block", &all_imports);
    context.insert("body", &run_body);

    // Iceberg warehouse for the glue_catalog Spark catalog
    let glue_cfg = config.config.get("glue");
    let warehouse = glue_cfg
        .and_then(|g| g.get("warehouse"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    context.insert("iceberg_warehouse", warehouse);

    let catalog_id = glue_cfg
        .and_then(|g| g.get("catalog_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    context.insert("catalog_id", catalog_id);

    let rendered = tera
        .render("script", &context)
        .context("failed to render Python template")?;

    Ok(rendered)
}
