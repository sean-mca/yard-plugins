//! PII masking codegen: renders the `EntityDetector.detect()` block for
//! AWS Glue jobs that need sensitive-data redaction.
//!
//! The rendered Python block converts the sink DataFrame to a
//! `DynamicFrame`, calls `EntityDetector.detect()` with fine-grained
//! `detectionParameters` (REDACT action, `"****"` mask), converts back
//! to a DataFrame, and drops the `DetectedEntities` metadata column.
//! All intermediate variables use the `_yard_pii_` prefix to avoid
//! collisions with user code.

use std::fmt::Write;

/// Render the PII masking block for the given entity types.
///
/// Emits a DynamicFrame conversion sandwich around an
/// `EntityDetector.detect()` call, then drops the metadata column.
/// Uses `_yard_pii_` prefixed variables to avoid collisions.
///
/// Returns the Python code block as an indented string. Cannot fail
/// because validation has already rejected invalid input.
#[must_use]
pub(super) fn render_pii(mask_pii: &[String], source_var: &str) -> String {
    let mut out = String::with_capacity(256);

    // DynamicFrame conversion
    let _ = writeln!(
        out,
        "    _yard_pii_dyf = DynamicFrame.fromDF({source_var}, glueContext, \"_yard_pii\")"
    );

    // EntityDetector.detect() call with detectionParameters
    let _ = writeln!(out, "    _yard_pii_dyf = EntityDetector.detect(");
    let _ = writeln!(out, "        _yard_pii_dyf,");
    let _ = writeln!(out, "        {{");
    for (i, entity) in mask_pii.iter().enumerate() {
        let comma = if i + 1 < mask_pii.len() { "," } else { "" };
        let _ = writeln!(
            out,
            "            \"{entity}\": [{{\"action\": \"REDACT\", \"actionOptions\": {{\"redactText\": \"****\"}}}}]{comma}"
        );
    }
    let _ = writeln!(out, "        }},");
    let _ = writeln!(out, "        \"DetectedEntities\"");
    let _ = writeln!(out, "    )");

    // Convert back to DataFrame and drop metadata column
    let _ = writeln!(out, "    {source_var} = _yard_pii_dyf.toDF()");
    let _ = write!(
        out,
        "    {source_var} = {source_var}.drop(\"DetectedEntities\")"
    );

    out
}
