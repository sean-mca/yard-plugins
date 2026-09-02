//! Sink-writing codegen: renders PySpark/Glue writer calls for each
//! sink type (S3, JDBC, Catalog, Iceberg).

use std::fmt::Write;

use anyhow::{Result, anyhow};

use crate::codegen::types::Sink;

use super::helpers::{
    derive_jdbc_url, python_str_literal, quoted_list, render_jdbc_auth, render_secret_fetch,
};

/// Return `value` if present, or an error naming the missing `field` on
/// the sink.
///
/// # Errors
///
/// Returns an error when `value` is `None`.
#[inline]
pub(super) fn require_sink_str<'a>(
    value: Option<&'a str>,
    sink_type: &str,
    field: &str,
) -> Result<&'a str> {
    value.ok_or_else(|| anyhow!("sink: '{field}' is required for {sink_type} sink"))
}

/// Default Iceberg table properties emitted on `.create()` calls.
pub(super) const ICEBERG_TABLE_PROPERTIES: &[(&str, &str)] = &[
    ("format-version", "2"),
    ("write.spark.accept-any-schema", "true"),
    ("write.target-file-size-bytes", "536870912"),
    ("write.parquet.compression-codec", "zstd"),
    ("write.distribution-mode", "hash"),
];

/// Render the sink as Python/PySpark write statements.
///
/// Dispatches by `sink.sink_type` (s3, jdbc, catalog, iceberg) and
/// generates the appropriate writer chain including auth, partition,
/// and Iceberg table-property wiring.
///
/// # Errors
///
/// Returns an error when required fields are missing for the given
/// sink type.
pub(super) fn render_sink(
    sink: &Sink,
    default_source: &str,
    fill_nulls: bool,
    catalog_id: Option<&str>,
) -> Result<String> {
    let source_name = sink.source.as_deref().unwrap_or(default_source);
    let var = format!("df_{source_name}");
    let mut lines = Vec::with_capacity(4);

    if let Some(secret_id) = &sink.secret_id {
        lines.push(render_secret_fetch(secret_id, "sink"));
    }

    let mode = sink.mode.as_deref().unwrap_or("overwrite");

    match sink.sink_type.as_str() {
        "s3" => {
            let format = sink.format.as_deref().unwrap_or("parquet");
            let path = require_sink_str(sink.path.as_deref(), "s3", "path")?;
            let mut write = format!(
                "    {var}.write.format({}).mode({})",
                python_str_literal(format),
                python_str_literal(mode),
            );
            if !sink.partition_by.is_empty() {
                // write to String is infallible
                let _ = write!(write, ".partitionBy({})", quoted_list(&sink.partition_by));
            }
            // write to String is infallible
            let _ = write!(write, ".save({})", python_str_literal(path));
            lines.push(write);
        }
        "jdbc" => {
            let table = require_sink_str(sink.table.as_deref(), "jdbc", "table")?;
            let auth = render_jdbc_auth("sink", sink.secret_id.as_deref(), sink.auth.as_ref());
            if let Some((_, _, ref pre)) = auth {
                lines.extend(pre.iter().cloned());
            }
            let derived_url;
            let url = match sink.connection_url.as_deref() {
                Some(u) => u,
                None => {
                    let a = sink.auth.as_ref().ok_or_else(|| {
                        anyhow!("sink: 'connection_url' is required when 'auth' is not set")
                    })?;
                    let conn_type = sink.connection_type.as_deref().ok_or_else(|| {
                        anyhow!(
                            "sink: 'connection_type' is required when 'connection_url' is not set"
                        )
                    })?;
                    let db = require_sink_str(sink.database.as_deref(), "jdbc", "database")?;
                    derived_url = derive_jdbc_url(a, conn_type, db);
                    &derived_url
                }
            };
            lines.push(format!(
                "    {var}.write.format(\"jdbc\").option(\"url\", {}).option(\"dbtable\", {})\\",
                python_str_literal(url),
                python_str_literal(table),
            ));
            if let Some((user_expr, password_expr, _)) = &auth {
                lines.push(format!(
                    "        .option(\"user\", {user_expr}).option(\"password\", {password_expr})\\"
                ));
            }
            lines.push(format!("        .mode({}).save()", python_str_literal(mode)));
        }
        "catalog" => {
            let db = require_sink_str(sink.database.as_deref(), "catalog", "database")?;
            let table = require_sink_str(sink.table.as_deref(), "catalog", "table")?;
            lines.push(format!(
                "    sink_frame = DynamicFrame.fromDF({var}, glueContext, \"sink_frame\")"
            ));
            lines.push(format!(
                "    glueContext.write_dynamic_frame.from_catalog(frame=sink_frame, database={}, table_name={})",
                python_str_literal(db),
                python_str_literal(table),
            ));
        }
        "iceberg" => {
            let db = require_sink_str(sink.database.as_deref(), "iceberg", "database")?;
            let table = require_sink_str(sink.table.as_deref(), "iceberg", "table")?;
            let write_op = match mode {
                "overwrite" => "overwritePartitions",
                _ => "append",
            };
            let partition_clause = if sink.partition_by.is_empty() {
                String::new()
            } else {
                format!(
                    "\n            .partitionedBy({})",
                    quoted_list(&sink.partition_by)
                )
            };
            let tbl_props = ICEBERG_TABLE_PROPERTIES
                .iter()
                .map(|(k, v)| format!("\n            .tableProperty(\"{k}\", \"{v}\")"))
                .chain(
                    sink.path
                        .as_deref()
                        .filter(|p| !p.is_empty())
                        .map(|p| {
                            format!(
                                "\n            .tableProperty(\"location\", {})",
                                python_str_literal(p)
                            )
                        }),
                )
                .collect::<String>();
            let escaped_db = python_str_literal(db);
            if let Some(cid) = catalog_id {
                let escaped_cid = python_str_literal(cid);
                lines.push(format!(
                    "    _glue = boto3.client(\"glue\")\n    \
                     try:\n        \
                         _glue.get_database(CatalogId={escaped_cid}, Name={escaped_db})\n    \
                     except _glue.exceptions.EntityNotFoundException:\n        \
                         _glue.create_database(CatalogId={escaped_cid}, DatabaseInput={{\"Name\": {escaped_db}}})"
                ));
            } else {
                lines.push(format!(
                    "    _glue = boto3.client(\"glue\")\n    \
                     try:\n        \
                         _glue.get_database(Name={escaped_db})\n    \
                     except _glue.exceptions.EntityNotFoundException:\n        \
                         _glue.create_database(DatabaseInput={{\"Name\": {escaped_db}}})"
                ));
            }
            // Build qualified table name: glue_catalog.<db>.<table>
            // db and table are validated identifiers so simple concat is safe
            // for the Python string literal
            let tbl_literal = format!("glue_catalog.{db}.{table}");
            lines.push(format!("    _tbl = {}", python_str_literal(&tbl_literal)));
            let new_table_coerce = if fill_nulls {
                format!(
                    "_target = _yard_void_free_schema({var}.schema)\n        \
                     {var} = _yard_conform({var}, _target)\n        "
                )
            } else {
                String::new()
            };
            let existing_table_coerce = if fill_nulls {
                format!(
                    "_live = _yard_read_iceberg_schema(spark, _tbl)\n        \
                     _batch = _yard_void_free_schema({var}.schema)\n        \
                     _live_types = {{_f.name: _f.dataType for _f in _live.fields}}\n        \
                     for _f in _batch.fields:\n            \
                         if _f.name in _live_types and _yard_kind_mismatch(_f.dataType, _live_types[_f.name]):\n                \
                             raise ValueError(\"yard: schema kind mismatch for column '\" + _f.name + \"': source kind differs from the Iceberg table kind (struct vs list/map); refusing to write\")\n        \
                     _target = _yard_merge_schema(_live, _batch)\n        \
                     {var} = _yard_conform({var}, _target)\n        "
                )
            } else {
                String::new()
            };
            let column_order = format!(
                "_existing_cols = spark.table(_tbl).columns\n        \
                 _ordered = [_c for _c in _existing_cols if _c in {var}.columns]\n        \
                 _new = [_c for _c in {var}.columns if _c not in _existing_cols]\n        \
                 {var} = {var}.select(_ordered + _new)\n        "
            );
            lines.push(format!(
                "    if not spark.catalog.tableExists(_tbl):\n        \
                     {new_table_coerce}({var}.writeTo(_tbl)\n            \
                         .using(\"iceberg\"){partition_clause}{tbl_props}\n            \
                         .create())\n    \
                 else:\n        \
                     {existing_table_coerce}{column_order}{var}.writeTo(_tbl).option(\"merge-schema\", \"true\").{write_op}()"
            ));
        }
        other => {
            lines.push(format!("    # Unsupported sink type: {other}"));
        }
    }

    Ok(lines.join("\n"))
}
