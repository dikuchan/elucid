use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;

/// Resolved storage configuration shared by all CLI commands.
#[derive(Debug)]
pub(crate) enum DataDirConfig {
    /// Local filesystem directory.
    Local(PathBuf),
    /// Remote object store (S3, GCS, in-memory, etc.).
    ObjectStore {
        store: Arc<dyn object_store::ObjectStore>,
        url: url::Url,
        prefix: String,
    },
}

/// Resolves the `--data-dir` flag value into a [`DataDirConfig`].
///
/// - `None` → `$HOME/.elucid/data` (local default)
/// - Contains `://` → parsed as a URL, converted via `object_store::parse_url_opts`
/// - Otherwise → treated as a local filesystem path
pub(crate) fn resolve_data_dir(input: Option<String>) -> anyhow::Result<DataDirConfig> {
    let input = match input {
        Some(s) => s,
        None => {
            let home = env::home_dir().context("Cannot access home directory")?;
            return Ok(DataDirConfig::Local(home.join(".elucid").join("data")));
        }
    };

    if input.contains("://") {
        let url = url::Url::parse(&input).context("Invalid data-dir URL")?;
        let (store, _path) = object_store::parse_url_opts(&url, HashMap::<String, String>::new())
            .context("Unsupported data-dir URL scheme")?;
        let prefix = url.path().trim_matches('/').to_owned();
        Ok(DataDirConfig::ObjectStore {
            store: Arc::from(store),
            url,
            prefix,
        })
    } else {
        Ok(DataDirConfig::Local(PathBuf::from(input)))
    }
}

/// Builds an engine [`Context`] from a resolved [`DataDirConfig`].
///
/// For local paths, verifies the directory exists before constructing
/// the context.
pub(crate) fn build_engine_context(config: &DataDirConfig) -> anyhow::Result<elucid_engine::Context> {
    match config {
        DataDirConfig::Local(path) => {
            if !path.exists() {
                anyhow::bail!("Data directory doesn't exist");
            }
            Ok(elucid_engine::Context::new(path))
        }
        DataDirConfig::ObjectStore { store, url, prefix } => {
            let engine_config = elucid_engine::StorageConfig::ObjectStore {
                store: store.clone(),
                url: url.clone(),
                prefix: prefix.clone(),
            };
            Ok(elucid_engine::Context::with_storage_config(engine_config))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_resolves_to_home_data() {
        let config = resolve_data_dir(None).expect("should resolve");
        match config {
            DataDirConfig::Local(path) => {
                let home = env::home_dir().expect("home");
                assert_eq!(path, home.join(".elucid").join("data"));
            }
            _ => panic!("expected Local variant"),
        }
    }

    #[test]
    fn bare_path_is_local() {
        let config = resolve_data_dir(Some("/tmp/data".to_owned())).expect("should resolve");
        match config {
            DataDirConfig::Local(path) => assert_eq!(path, PathBuf::from("/tmp/data")),
            _ => panic!("expected Local variant"),
        }
    }

    #[test]
    fn relative_path_is_local() {
        let config = resolve_data_dir(Some("./data".to_owned())).expect("should resolve");
        match config {
            DataDirConfig::Local(path) => assert_eq!(path, PathBuf::from("./data")),
            _ => panic!("expected Local variant"),
        }
    }

    #[test]
    fn memory_url_is_object_store() {
        let config = resolve_data_dir(Some("memory:///data/prefix".to_owned()))
            .expect("should resolve");
        match config {
            DataDirConfig::ObjectStore { prefix, .. } => assert_eq!(prefix, "data/prefix"),
            _ => panic!("expected ObjectStore variant"),
        }
    }

    #[test]
    fn memory_url_empty_prefix() {
        let config =
            resolve_data_dir(Some("memory:///".to_owned())).expect("should resolve");
        match config {
            DataDirConfig::ObjectStore { prefix, .. } => assert_eq!(prefix, ""),
            _ => panic!("expected ObjectStore variant"),
        }
    }

    #[test]
    fn invalid_url_returns_error() {
        let result = resolve_data_dir(Some("s3://[invalid".to_owned()));
        assert!(result.is_err());
    }
}
