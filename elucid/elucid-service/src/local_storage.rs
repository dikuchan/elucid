use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt as _;

use crate::{LocalStorageConfiguration, ServiceError};

const ACCESS_PROBE_NAME: &str = ".elucid-access";
const ACCESS_PROBE_BYTES: &[u8] = b"elucid\n";

#[derive(Debug)]
pub(crate) struct LocalStorageBoundary {
    spool_path: PathBuf,
    scratch_path: PathBuf,
}

impl LocalStorageBoundary {
    pub(crate) async fn open(
        configuration: &LocalStorageConfiguration,
    ) -> Result<Self, ServiceError> {
        prepare_directory(configuration.spool_path()).await?;
        prepare_directory(configuration.scratch_path()).await?;
        Ok(Self {
            spool_path: configuration.spool_path().to_owned(),
            scratch_path: configuration.scratch_path().to_owned(),
        })
    }

    #[must_use]
    pub(crate) const fn spool_used_bytes(&self) -> u64 {
        0
    }

    pub(crate) async fn is_accessible(&self) -> bool {
        is_directory(&self.spool_path).await && is_directory(&self.scratch_path).await
    }
}

async fn prepare_directory(path: &Path) -> Result<(), ServiceError> {
    tokio::fs::create_dir_all(path)
        .await
        .map_err(|source| local_storage_error(path, source))?;
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|source| local_storage_error(path, source))?;
    if !metadata.is_dir() {
        return Err(local_storage_error(
            path,
            std::io::Error::other("configured local-storage path is not a directory"),
        ));
    }

    let probe_path = path.join(ACCESS_PROBE_NAME);
    let mut probe = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&probe_path)
        .await
        .map_err(|source| local_storage_error(path, source))?;
    probe
        .write_all(ACCESS_PROBE_BYTES)
        .await
        .map_err(|source| local_storage_error(path, source))?;
    probe
        .sync_all()
        .await
        .map_err(|source| local_storage_error(path, source))?;
    drop(probe);
    tokio::fs::remove_file(&probe_path)
        .await
        .map_err(|source| local_storage_error(path, source))
}

async fn is_directory(path: &Path) -> bool {
    tokio::fs::metadata(path)
        .await
        .is_ok_and(|metadata| metadata.is_dir())
}

fn local_storage_error(path: &Path, source: std::io::Error) -> ServiceError {
    ServiceError::LocalStorage {
        path: path.to_owned(),
        source,
    }
}
