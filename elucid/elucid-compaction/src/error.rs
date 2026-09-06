use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;

use arrow::error::ArrowError;
use elucid_core::{CodedError, ErrorCode};
use elucid_metastore::{
    CompactionFailureReason, CompactionMetadataError,
    CompactionModelError as MetadataCompactionModelError, PublicationError,
};
use elucid_storage::{StagedObjectReadError, StorageError, StorageErrorKind, StorageModelError};
use parquet::errors::ParquetError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompactionErrorKind {
    InputInvalid,
    BuildFailed,
    NotBeneficial,
}

impl From<CompactionErrorKind> for CompactionFailureReason {
    fn from(value: CompactionErrorKind) -> Self {
        match value {
            CompactionErrorKind::InputInvalid => Self::InputInvalid,
            CompactionErrorKind::BuildFailed => Self::BuildFailed,
            CompactionErrorKind::NotBeneficial => Self::NotBeneficial,
        }
    }
}

impl Display for CompactionErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InputInvalid => "compaction input invalid",
            Self::BuildFailed => "compaction build failed",
            Self::NotBeneficial => "compaction run is not beneficial",
        })
    }
}

#[derive(Debug)]
pub struct CompactionError {
    kind: CompactionErrorKind,
    source: CompactionErrorSource,
}

impl CompactionError {
    #[must_use]
    pub const fn kind(&self) -> CompactionErrorKind {
        self.kind
    }

    pub(crate) fn input(message: &'static str) -> Self {
        Self {
            kind: CompactionErrorKind::InputInvalid,
            source: CompactionErrorSource::Invariant(message),
        }
    }

    pub(crate) fn build(message: &'static str) -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::Invariant(message),
        }
    }

    pub(crate) fn not_beneficial(message: &'static str) -> Self {
        Self {
            kind: CompactionErrorKind::NotBeneficial,
            source: CompactionErrorSource::Invariant(message),
        }
    }

    pub(crate) fn storage(source: StorageError) -> Self {
        let kind = match source.kind() {
            StorageErrorKind::ObjectIntegrityError | StorageErrorKind::ParquetInvalid => {
                CompactionErrorKind::InputInvalid
            }
            StorageErrorKind::ParquetBuildFailed
            | StorageErrorKind::ObjectStoreUnavailable
            | StorageErrorKind::ObjectUploadFailed
            | StorageErrorKind::ObjectVerificationFailed
            | StorageErrorKind::ObjectDeleteFailed
            | StorageErrorKind::LocalCapacityExhausted => CompactionErrorKind::BuildFailed,
            _ => CompactionErrorKind::BuildFailed,
        };
        Self {
            kind,
            source: CompactionErrorSource::Storage(source),
        }
    }

    pub(crate) fn staged_read(source: StagedObjectReadError) -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::StagedRead(source),
        }
    }

    pub(crate) fn output_storage(source: StorageError) -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::Storage(source),
        }
    }

    pub(crate) fn input_storage_model(source: StorageModelError) -> Self {
        Self {
            kind: CompactionErrorKind::InputInvalid,
            source: CompactionErrorSource::StorageModel(source),
        }
    }

    pub(crate) fn output_storage_model(source: StorageModelError) -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::StorageModel(source),
        }
    }

    pub(crate) fn metadata_model(source: MetadataCompactionModelError) -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::MetadataModel(source),
        }
    }

    pub(crate) fn parquet_input(source: ParquetError) -> Self {
        let kind = parquet_failure_kind(&source);
        Self {
            kind,
            source: CompactionErrorSource::Parquet(source),
        }
    }

    pub(crate) fn arrow(source: ArrowError) -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::Arrow(source),
        }
    }

    pub(crate) fn metadata(source: CompactionMetadataError) -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::Metadata(source),
        }
    }

    pub(crate) fn publication(source: PublicationError) -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::Publication(source),
        }
    }

    pub(crate) fn io(operation: &'static str, source: io::Error) -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::Io { operation, source },
        }
    }

    pub(crate) fn timeout() -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::Invariant("compaction run exceeded its duration limit"),
        }
    }

    pub(crate) fn with_cleanup_failure(self, cleanup: io::Error) -> Self {
        Self {
            kind: CompactionErrorKind::BuildFailed,
            source: CompactionErrorSource::Cleanup {
                original: Box::new(self),
                cleanup,
            },
        }
    }
}

impl Display for CompactionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for CompactionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl CodedError for CompactionError {
    fn error_code(&self) -> ErrorCode {
        match self.kind {
            CompactionErrorKind::InputInvalid => ErrorCode::CompactionInputInvalid,
            CompactionErrorKind::BuildFailed => ErrorCode::CompactionBuildFailed,
            CompactionErrorKind::NotBeneficial => ErrorCode::CompactionNotBeneficial,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum CompactionErrorSource {
    #[error("staged compaction output read failed")]
    StagedRead(#[source] StagedObjectReadError),
    #[error("compaction storage operation failed")]
    Storage(#[source] StorageError),
    #[error("compaction storage model is invalid")]
    StorageModel(#[source] StorageModelError),
    #[error("compaction input Parquet read failed")]
    Parquet(#[source] ParquetError),
    #[error("compaction Arrow operation failed")]
    Arrow(#[source] ArrowError),
    #[error("compaction metadata operation failed")]
    Metadata(#[source] CompactionMetadataError),
    #[error("compaction metadata model is invalid")]
    MetadataModel(#[source] MetadataCompactionModelError),
    #[error("compaction object publication operation failed")]
    Publication(#[source] PublicationError),
    #[error("{operation}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("compaction cleanup failed with {cleanup} after {original}")]
    Cleanup {
        #[source]
        original: Box<CompactionError>,
        cleanup: io::Error,
    },
    #[error("{0}")]
    Invariant(&'static str),
}

fn parquet_failure_kind(source: &ParquetError) -> CompactionErrorKind {
    let mut cause = source.source();
    while let Some(error) = cause {
        if let Some(object_store_error) = error.downcast_ref::<object_store::Error>() {
            return match object_store_error {
                object_store::Error::NotFound { .. } => CompactionErrorKind::InputInvalid,
                _ => CompactionErrorKind::BuildFailed,
            };
        }
        cause = error.source();
    }
    CompactionErrorKind::InputInvalid
}
