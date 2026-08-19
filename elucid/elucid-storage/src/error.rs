use std::error::Error;
use std::fmt::{Display, Formatter};

use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageModelError {
    #[error("batch identity must be an RFC 9562 UUIDv7, got {value}")]
    BatchIdentityMustBeUuidV7 { value: Uuid },

    #[error("managed root prefix exceeds {maximum_bytes} bytes")]
    RootPrefixTooLong { maximum_bytes: usize },

    #[error("managed root prefix must not have leading or trailing slashes")]
    RootPrefixNotCanonical,

    #[error("managed root prefix is invalid")]
    RootPrefixInvalid {
        #[source]
        source: object_store::path::Error,
    },

    #[error("object format version must be positive")]
    ObjectFormatVersionMustBePositive,

    #[error("object transfer limit must be positive")]
    TransferLimitMustBePositive,

    #[error("object byte size cannot be represented as u64")]
    ObjectSizeOverflow,

    #[error("object read range [{start}, {end}) is invalid for {object_size} bytes")]
    InvalidObjectReadRange {
        start: u64,
        end: u64,
        object_size: u64,
    },

    #[error("object media type does not match its managed key")]
    MediaTypeDoesNotMatchManagedKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StorageErrorCode {
    ParquetBuildFailed,
    ParquetInvalid,
    ObjectStoreUnavailable,
    ObjectUploadFailed,
    ObjectVerificationFailed,
    ObjectIntegrityError,
    ObjectDeleteFailed,
    LocalCapacityExhausted,
}

impl StorageErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ParquetBuildFailed => "PARQUET_BUILD_FAILED",
            Self::ParquetInvalid => "PARQUET_INVALID",
            Self::ObjectStoreUnavailable => "OBJECT_STORE_UNAVAILABLE",
            Self::ObjectUploadFailed => "OBJECT_UPLOAD_FAILED",
            Self::ObjectVerificationFailed => "OBJECT_VERIFICATION_FAILED",
            Self::ObjectIntegrityError => "OBJECT_INTEGRITY_ERROR",
            Self::ObjectDeleteFailed => "OBJECT_DELETE_FAILED",
            Self::LocalCapacityExhausted => "LOCAL_CAPACITY_EXHAUSTED",
        }
    }
}

impl Display for StorageErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub struct StorageError {
    code: StorageErrorCode,
    source: StorageErrorSource,
}

impl StorageError {
    #[must_use]
    pub const fn code(&self) -> StorageErrorCode {
        self.code
    }

    pub(crate) fn unavailable(source: object_store::Error) -> Self {
        Self::object_store(StorageErrorCode::ObjectStoreUnavailable, source)
    }

    pub(crate) fn upload(source: object_store::Error) -> Self {
        Self::object_store(StorageErrorCode::ObjectUploadFailed, source)
    }

    pub(crate) fn verification(source: object_store::Error) -> Self {
        Self::object_store(StorageErrorCode::ObjectVerificationFailed, source)
    }

    pub(crate) fn delete(source: object_store::Error) -> Self {
        Self::object_store(StorageErrorCode::ObjectDeleteFailed, source)
    }

    pub(crate) fn integrity(message: &'static str) -> Self {
        Self::invariant(StorageErrorCode::ObjectIntegrityError, message)
    }

    pub(crate) fn verification_invariant(message: &'static str) -> Self {
        Self::invariant(StorageErrorCode::ObjectVerificationFailed, message)
    }

    pub(crate) fn delete_invariant(message: &'static str) -> Self {
        Self::invariant(StorageErrorCode::ObjectDeleteFailed, message)
    }

    pub(crate) fn capacity(message: &'static str) -> Self {
        Self::invariant(StorageErrorCode::LocalCapacityExhausted, message)
    }

    fn object_store(code: StorageErrorCode, source: object_store::Error) -> Self {
        Self {
            code,
            source: StorageErrorSource::ObjectStore(source),
        }
    }

    fn invariant(code: StorageErrorCode, message: &'static str) -> Self {
        Self {
            code,
            source: StorageErrorSource::Invariant(message),
        }
    }
}

impl Display for StorageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.code, formatter)
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug, thiserror::Error)]
enum StorageErrorSource {
    #[error("object-store operation failed")]
    ObjectStore(#[source] object_store::Error),
    #[error("{0}")]
    Invariant(&'static str),
}
