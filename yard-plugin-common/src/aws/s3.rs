//! S3 script operations for plugin deploy/destroy lifecycle.
//!
//! [`S3ScriptOps`] encapsulates the upload, delete, and existence-check
//! operations that both Glue and EMR plugins perform on generated PySpark
//! scripts stored in S3.

use anyhow::{Context, Result};
pub use aws_sdk_s3::Client as S3Client;

/// Shared S3 script operations used by all providers that upload
/// generated PySpark scripts to S3.
pub struct S3ScriptOps {
    /// The S3 client used for script upload/delete/check operations.
    pub s3_client: S3Client,
    /// The S3 bucket name where scripts are stored.
    pub script_bucket: String,
    /// The key prefix (folder path) within the bucket.
    pub script_prefix: String,
}

impl S3ScriptOps {
    /// Build the full S3 key for a job's generated PySpark script.
    ///
    /// Normalises trailing slashes: `scripts/` + `my_job` → `scripts/my_job.py`,
    /// `scripts` + `my_job` → `scripts/my_job.py`.
    #[inline]
    pub fn script_key(&self, job_name: &str) -> String {
        if self.script_prefix.is_empty() {
            format!("{job_name}.py")
        } else if self.script_prefix.ends_with('/') {
            format!("{}{job_name}.py", self.script_prefix)
        } else {
            format!("{}/{job_name}.py", self.script_prefix)
        }
    }

    /// Upload a generated PySpark script to S3 and return its `s3://` URI.
    ///
    /// # Errors
    ///
    /// Returns an error if the S3 `PutObject` call fails.
    pub async fn upload_script(&self, job_name: &str, artifact: &str) -> Result<String> {
        let key = self.script_key(job_name);

        self.s3_client
            .put_object()
            .bucket(&self.script_bucket)
            .key(&key)
            .body(artifact.as_bytes().to_vec().into())
            .content_type("text/x-python")
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to upload script to s3://{}/{}",
                    self.script_bucket, key
                )
            })?;

        Ok(format!("s3://{}/{}", self.script_bucket, key))
    }

    /// Delete a previously uploaded PySpark script from S3.
    ///
    /// # Errors
    ///
    /// Returns an error if the S3 `DeleteObject` call fails.
    pub async fn delete_script(&self, job_name: &str) -> Result<()> {
        let key = self.script_key(job_name);

        self.s3_client
            .delete_object()
            .bucket(&self.script_bucket)
            .key(&key)
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to delete script at s3://{}/{}",
                    self.script_bucket, key
                )
            })?;

        Ok(())
    }

    /// Check whether an S3 object exists in the script bucket.
    ///
    /// Returns `true` if the `HeadObject` call succeeds, `false` if
    /// the object is not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the `HeadObject` call fails for a reason
    /// other than "not found" (e.g. permission denied, network error).
    pub async fn s3_object_exists(&self, key: &str) -> Result<bool> {
        let result = self
            .s3_client
            .head_object()
            .bucket(&self.script_bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.as_service_error()
                    .is_some_and(|se| se.is_not_found())
                {
                    Ok(false)
                } else {
                    Err(e).with_context(|| format!("Failed to check S3 object: {key}"))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build an [`S3ScriptOps`] without a real AWS client.
    /// Only used for testing `script_key()` which is a pure function.
    fn ops_with_prefix(prefix: &str) -> S3ScriptOps {
        // Build a minimal S3 client from a dummy config — script_key()
        // never touches the network so this is safe.
        let config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .build();
        let client = S3Client::from_conf(config);

        S3ScriptOps {
            s3_client: client,
            script_bucket: "test-bucket".to_string(),
            script_prefix: prefix.to_string(),
        }
    }

    #[test]
    fn script_key_with_trailing_slash() {
        let ops = ops_with_prefix("scripts/");
        assert_eq!(ops.script_key("my_job"), "scripts/my_job.py");
    }

    #[test]
    fn script_key_without_trailing_slash() {
        let ops = ops_with_prefix("scripts");
        assert_eq!(ops.script_key("my_job"), "scripts/my_job.py");
    }

    #[test]
    fn script_key_with_nested_prefix_trailing_slash() {
        let ops = ops_with_prefix("org/team/scripts/");
        assert_eq!(
            ops.script_key("etl_job"),
            "org/team/scripts/etl_job.py"
        );
    }

    #[test]
    fn script_key_with_nested_prefix_no_trailing_slash() {
        let ops = ops_with_prefix("org/team/scripts");
        assert_eq!(
            ops.script_key("etl_job"),
            "org/team/scripts/etl_job.py"
        );
    }

    #[test]
    fn script_key_with_empty_prefix() {
        let ops = ops_with_prefix("");
        assert_eq!(ops.script_key("my_job"), "my_job.py");
    }
}
