//! Storage backend configuration.

use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use url::Url;

use crate::stage_error::StageError;

/// Storage backend configuration.
///
/// Used by callers to decide which sink to construct. The `run_ingestion()` function
/// itself is generic over any [`futures::Sink<RecordBatch>`]; this type is a
/// convenience for building the right sink.
#[derive(Debug)]
#[non_exhaustive]
pub enum StorageConfig {
    /// Local filesystem directory containing Parquet files.
    Local { data_dir: PathBuf },
    /// Remote object store (S3, GCS, in-memory, etc.) with a key prefix.
    ObjectStore {
        store: Arc<dyn ObjectStore>,
        prefix: String,
    },
}

impl StorageConfig {
    /// Creates a local filesystem configuration.
    pub fn local(data_dir: PathBuf) -> Self {
        Self::Local { data_dir }
    }

    /// Creates a configuration from a URL using [`object_store::parse_url_opts`].
    ///
    /// The prefix is extracted from the URL path (leading/trailing slashes stripped).
    /// Works for `s3://`, `gs://`, `file://`, `memory://`, etc.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Write`] if the URL scheme is not supported by
    /// `object_store`.
    pub fn from_url(url: Url) -> Result<Self, StageError> {
        let (store, _) =
            object_store::parse_url_opts(&url, std::collections::HashMap::<String, String>::new())
                .map_err(|e| StageError::Write(format!("Failed to parse storage URL: {e}")))?;
        let prefix = url.path().trim_matches('/').to_owned();
        Ok(Self::ObjectStore {
            store: Arc::from(store),
            prefix,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_constructor() {
        let config = StorageConfig::local(PathBuf::from("/tmp/data"));
        match config {
            StorageConfig::Local { data_dir } => assert_eq!(data_dir, PathBuf::from("/tmp/data")),
            _ => panic!("expected Local variant"),
        }
    }

    #[test]
    fn from_url_memory() {
        let url = Url::parse("memory:///data/prefix").expect("valid URL");
        let config = StorageConfig::from_url(url).expect("should succeed");
        match config {
            StorageConfig::ObjectStore { store, prefix } => {
                assert_eq!(prefix, "data/prefix");
                // Verify it's a usable ObjectStore by listing (InMemory is always empty).
                let _ = &store;
            }
            _ => panic!("expected ObjectStore variant"),
        }
    }

    #[test]
    fn from_url_memory_empty_prefix() {
        let url = Url::parse("memory:///").expect("valid URL");
        let config = StorageConfig::from_url(url).expect("should succeed");
        match config {
            StorageConfig::ObjectStore { prefix, .. } => {
                assert_eq!(prefix, "");
            }
            _ => panic!("expected ObjectStore variant"),
        }
    }

    #[test]
    fn from_url_unsupported_scheme() {
        let url = Url::parse("ftp://example.com/data").expect("valid URL");
        let result = StorageConfig::from_url(url);
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Failed to parse storage URL"),
            "unexpected error: {msg}"
        );
    }
}
