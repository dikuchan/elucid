use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use object_store::ObjectStore;
use url::Url;

/// Storage backend configuration for table data.
#[derive(Debug)]
pub enum StorageConfig {
    /// Local filesystem directory containing Parquet files.
    Local { data_dir: PathBuf },
    /// Remote object store (S3, GCS, etc.) with a URL and key prefix.
    ObjectStore {
        store: Arc<dyn ObjectStore>,
        url: Url,
        prefix: String,
    },
}

impl StorageConfig {
    /// Creates a local filesystem configuration.
    pub fn local(data_dir: PathBuf) -> Self {
        Self::Local { data_dir }
    }

    /// Creates a configuration from a URL using `object_store::parse_url_opts`.
    ///
    /// The prefix is extracted from the URL path (leading/trailing slashes stripped).
    /// Works for `s3://`, `gs://`, `file://`, `memory://`, etc.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL scheme is not supported by `object_store`.
    pub fn from_url(url: Url) -> Result<Self> {
        let (store, _) = object_store::parse_url_opts(&url, HashMap::<String, String>::new())
            .map_err(|e| DataFusionError::External(e.into()))?;
        let prefix = url.path().trim_matches('/').to_owned();
        Ok(Self::ObjectStore {
            store: Arc::from(store),
            url,
            prefix,
        })
    }
}
