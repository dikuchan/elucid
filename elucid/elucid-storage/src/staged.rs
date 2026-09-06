use std::io;
use std::path::Path;

use bytes::Bytes;
use tokio::fs::File;
use tokio::io::AsyncReadExt as _;

use crate::{ObjectDescriptor, ObjectDigest, TransferLimit};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StagedObjectReadError {
    #[error("staged object exceeds the transfer or allocation limit")]
    CapacityExceeded,
    #[error("staged object size differs from its descriptor")]
    SizeMismatch,
    #[error("staged object digest differs from its descriptor")]
    DigestMismatch,
    #[error("staged object I/O failed")]
    Io(#[from] io::Error),
}

pub async fn read_staged_object(
    path: &Path,
    descriptor: &ObjectDescriptor,
    limit: TransferLimit,
) -> Result<Bytes, StagedObjectReadError> {
    let expected = descriptor.expected_byte_size().get();
    if expected > limit.get() {
        return Err(StagedObjectReadError::CapacityExceeded);
    }
    let mut file = File::open(path).await?;
    if file.metadata().await?.len() != expected {
        return Err(StagedObjectReadError::SizeMismatch);
    }
    let length = usize::try_from(expected).map_err(|_| StagedObjectReadError::CapacityExceeded)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| StagedObjectReadError::CapacityExceeded)?;
    bytes.resize(length, 0);
    if let Err(error) = file.read_exact(&mut bytes).await {
        return Err(if error.kind() == io::ErrorKind::UnexpectedEof {
            StagedObjectReadError::SizeMismatch
        } else {
            StagedObjectReadError::Io(error)
        });
    }
    // Bound the read even when the open file grows after the metadata check.
    if file.read(&mut [0_u8; 1]).await? != 0 {
        return Err(StagedObjectReadError::SizeMismatch);
    }
    if ObjectDigest::calculate(&bytes) != descriptor.digest() {
        return Err(StagedObjectReadError::DigestMismatch);
    }
    Ok(Bytes::from(bytes))
}
