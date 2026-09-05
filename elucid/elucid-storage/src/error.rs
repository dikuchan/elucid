use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;

use elucid_core::{CodedError, ErrorCode};
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

    #[error("stored Parquet object key does not match its segment and object identities")]
    ParquetManagedKeyIdentityMismatch,

    #[error("stored dead-letter object key does not match its batch and object identities")]
    DeadLetterManagedKeyIdentityMismatch,

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

    #[error("Parquet segments require a Parquet managed object key")]
    ParquetManagedKeyRequired,

    #[error("Parquet segment row count must be positive")]
    ParquetRowCountMustBePositive,

    #[error("Parquet segment row count exceeds the supported range")]
    ParquetRowCountOutOfRange,

    #[error("Parquet record-batch fields do not exactly match the stored schema")]
    ParquetSchemaMismatch,

    #[error("Parquet record batch contains null in non-null field {field_ordinal}")]
    ParquetNonNullFieldContainsNull { field_ordinal: usize },

    #[error("Parquet segment system columns do not have the required representation")]
    ParquetSystemColumnsInvalid,

    #[error("Parquet segment rows span more than one UTC event day")]
    ParquetRowsSpanEventDays,

    #[error("Parquet segment rows are not ordered by event time and event identity")]
    ParquetRowsNotOrdered,

    #[error("Parquet write limit must be positive")]
    ParquetWriteLimitMustBePositive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StorageErrorKind {
    ParquetBuildFailed,
    ParquetInvalid,
    ObjectStoreUnavailable,
    ObjectUploadFailed,
    ObjectVerificationFailed,
    ObjectIntegrityError,
    ObjectDeleteFailed,
    LocalCapacityExhausted,
}

impl From<StorageErrorKind> for ErrorCode {
    fn from(value: StorageErrorKind) -> Self {
        match value {
            StorageErrorKind::ParquetBuildFailed => Self::ParquetBuildFailed,
            StorageErrorKind::ParquetInvalid => Self::ParquetInvalid,
            StorageErrorKind::ObjectStoreUnavailable => Self::ObjectStoreUnavailable,
            StorageErrorKind::ObjectUploadFailed => Self::ObjectUploadFailed,
            StorageErrorKind::ObjectVerificationFailed => Self::ObjectVerificationFailed,
            StorageErrorKind::ObjectIntegrityError => Self::ObjectIntegrityError,
            StorageErrorKind::ObjectDeleteFailed => Self::ObjectDeleteFailed,
            StorageErrorKind::LocalCapacityExhausted => Self::LocalCapacityExhausted,
        }
    }
}

impl StorageErrorKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        ErrorCode::from(self).as_str()
    }
}

impl Display for StorageErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub struct StorageError {
    kind: StorageErrorKind,
    source: StorageErrorSource,
}

impl StorageError {
    #[must_use]
    pub const fn kind(&self) -> StorageErrorKind {
        self.kind
    }

    pub(crate) fn unavailable(source: object_store::Error) -> Self {
        Self::object_store(StorageErrorKind::ObjectStoreUnavailable, source)
    }

    pub(crate) fn upload(source: object_store::Error) -> Self {
        Self::object_store(StorageErrorKind::ObjectUploadFailed, source)
    }

    pub(crate) fn verification(source: object_store::Error) -> Self {
        Self::object_store(StorageErrorKind::ObjectVerificationFailed, source)
    }

    pub(crate) fn delete(source: object_store::Error) -> Self {
        Self::object_store(StorageErrorKind::ObjectDeleteFailed, source)
    }

    pub(crate) fn integrity(message: &'static str) -> Self {
        Self::invariant(StorageErrorKind::ObjectIntegrityError, message)
    }

    pub(crate) fn verification_invariant(message: &'static str) -> Self {
        Self::invariant(StorageErrorKind::ObjectVerificationFailed, message)
    }

    pub(crate) fn delete_invariant(message: &'static str) -> Self {
        Self::invariant(StorageErrorKind::ObjectDeleteFailed, message)
    }

    pub(crate) fn capacity(message: &'static str) -> Self {
        Self::invariant(StorageErrorKind::LocalCapacityExhausted, message)
    }

    pub(crate) fn parquet_build(source: parquet::errors::ParquetError) -> Self {
        Self {
            kind: StorageErrorKind::ParquetBuildFailed,
            source: StorageErrorSource::Parquet(source),
        }
    }

    pub(crate) fn parquet_build_io(source: io::Error) -> Self {
        Self {
            kind: StorageErrorKind::ParquetBuildFailed,
            source: StorageErrorSource::Io(source),
        }
    }

    pub(crate) fn parquet_build_task(source: tokio::task::JoinError) -> Self {
        Self {
            kind: StorageErrorKind::ParquetBuildFailed,
            source: StorageErrorSource::Task(source),
        }
    }

    pub(crate) fn parquet_build_invariant(message: &'static str) -> Self {
        Self::invariant(StorageErrorKind::ParquetBuildFailed, message)
    }

    pub(crate) fn parquet_invalid(source: parquet::errors::ParquetError) -> Self {
        Self {
            kind: StorageErrorKind::ParquetInvalid,
            source: StorageErrorSource::Parquet(source),
        }
    }

    pub(crate) fn parquet_invalid_io(source: io::Error) -> Self {
        Self {
            kind: StorageErrorKind::ParquetInvalid,
            source: StorageErrorSource::Io(source),
        }
    }

    pub(crate) fn parquet_invalid_task(source: tokio::task::JoinError) -> Self {
        Self {
            kind: StorageErrorKind::ParquetInvalid,
            source: StorageErrorSource::Task(source),
        }
    }

    pub(crate) fn parquet_invalid_invariant(message: &'static str) -> Self {
        Self::invariant(StorageErrorKind::ParquetInvalid, message)
    }

    pub(crate) fn with_cleanup_failure(self, cleanup: io::Error) -> Self {
        Self {
            kind: self.kind,
            source: StorageErrorSource::Cleanup {
                original: Box::new(self.source),
                cleanup,
            },
        }
    }

    fn object_store(kind: StorageErrorKind, source: object_store::Error) -> Self {
        Self {
            kind,
            source: StorageErrorSource::ObjectStore(source),
        }
    }

    fn invariant(kind: StorageErrorKind, message: &'static str) -> Self {
        Self {
            kind,
            source: StorageErrorSource::Invariant(message),
        }
    }
}

impl Display for StorageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl CodedError for StorageError {
    fn error_code(&self) -> ErrorCode {
        self.kind().into()
    }
}

#[derive(Debug, thiserror::Error)]
enum StorageErrorSource {
    #[error("object-store operation failed")]
    ObjectStore(#[source] object_store::Error),
    #[error("local file operation failed")]
    Io(#[source] io::Error),
    #[error("Parquet operation failed")]
    Parquet(#[source] parquet::errors::ParquetError),
    #[error("blocking storage task failed")]
    Task(#[source] tokio::task::JoinError),
    #[error("cleanup after {original} failed")]
    Cleanup {
        original: Box<StorageErrorSource>,
        #[source]
        cleanup: io::Error,
    },
    #[error("{0}")]
    Invariant(&'static str),
}
