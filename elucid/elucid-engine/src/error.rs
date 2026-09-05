use std::error::Error;
use std::fmt::{Display, Formatter};

use datafusion::error::DataFusionError;
use elucid_core::{CodedError, ErrorCode};
use elucid_storage::StorageError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EngineErrorKind {
    PublishedObjectMissing,
    PublishedObjectCorrupt,
    CatalogCorrupt,
    QueryCastFailed,
    QueryEvaluationFailed,
    QueryResourceLimitExceeded,
    QueryTimeout,
    QueryCancelled,
    QueryExecutionFailed,
}

impl From<EngineErrorKind> for ErrorCode {
    fn from(value: EngineErrorKind) -> Self {
        match value {
            EngineErrorKind::PublishedObjectMissing => Self::PublishedObjectMissing,
            EngineErrorKind::PublishedObjectCorrupt => Self::PublishedObjectCorrupt,
            EngineErrorKind::CatalogCorrupt => Self::CatalogCorrupt,
            EngineErrorKind::QueryCastFailed => Self::QueryCastFailed,
            EngineErrorKind::QueryEvaluationFailed => Self::QueryEvaluationFailed,
            EngineErrorKind::QueryResourceLimitExceeded => Self::QueryResourceLimitExceeded,
            EngineErrorKind::QueryTimeout => Self::QueryTimeout,
            EngineErrorKind::QueryCancelled => Self::QueryCancelled,
            EngineErrorKind::QueryExecutionFailed => Self::QueryExecutionFailed,
        }
    }
}

impl EngineErrorKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        ErrorCode::from(self).as_str()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum QueryResourceLimitExceeded {
    #[error("query exceeds the limit of {maximum} selected segments")]
    SelectedSegments { maximum: u64 },

    #[error("query exceeds the limit of {maximum} selected Parquet bytes")]
    ScanBytes { maximum: u64 },

    #[error("query execution exhausted its memory or scratch capacity")]
    ExecutionResources,

    #[error("encoded query row exceeds the limit of {maximum} bytes")]
    EncodedRowBytes { maximum: u64 },
}

impl Display for EngineErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub struct EngineError {
    kind: EngineErrorKind,
    source: EngineErrorSource,
}

impl EngineError {
    #[must_use]
    pub const fn kind(&self) -> EngineErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn resource_limit_exceeded(&self) -> Option<QueryResourceLimitExceeded> {
        match (&self.source, self.kind) {
            (EngineErrorSource::ResourceLimit(source), _) => Some(*source),
            (EngineErrorSource::DataFusion(_), EngineErrorKind::QueryResourceLimitExceeded) => {
                Some(QueryResourceLimitExceeded::ExecutionResources)
            }
            _ => None,
        }
    }

    pub(crate) fn missing_object() -> Self {
        Self::invariant(
            EngineErrorKind::PublishedObjectMissing,
            "a selected published object is absent",
        )
    }

    pub(crate) fn corrupt_object(source: impl Into<EngineErrorSource>) -> Self {
        Self {
            kind: EngineErrorKind::PublishedObjectCorrupt,
            source: source.into(),
        }
    }

    pub(crate) fn corrupt_object_invariant(message: &'static str) -> Self {
        Self::invariant(EngineErrorKind::PublishedObjectCorrupt, message)
    }

    pub(crate) fn catalog_corrupt(message: &'static str) -> Self {
        Self::invariant(EngineErrorKind::CatalogCorrupt, message)
    }

    pub(crate) fn execution(source: impl Into<EngineErrorSource>) -> Self {
        Self {
            kind: EngineErrorKind::QueryExecutionFailed,
            source: source.into(),
        }
    }

    pub(crate) fn cast_failed(source: DataFusionError) -> Self {
        Self {
            kind: EngineErrorKind::QueryCastFailed,
            source: EngineErrorSource::DataFusion(source),
        }
    }

    pub(crate) fn evaluation_failed(source: DataFusionError) -> Self {
        Self {
            kind: EngineErrorKind::QueryEvaluationFailed,
            source: EngineErrorSource::DataFusion(source),
        }
    }

    pub(crate) fn evaluation_invariant(message: &'static str) -> Self {
        Self::invariant(EngineErrorKind::QueryEvaluationFailed, message)
    }

    pub(crate) fn resource_limit(source: QueryResourceLimitExceeded) -> Self {
        Self {
            kind: EngineErrorKind::QueryResourceLimitExceeded,
            source: EngineErrorSource::ResourceLimit(source),
        }
    }

    pub(crate) fn resources_exhausted(source: DataFusionError) -> Self {
        Self {
            kind: EngineErrorKind::QueryResourceLimitExceeded,
            source: EngineErrorSource::DataFusion(source),
        }
    }

    pub(crate) fn timeout() -> Self {
        Self::invariant(
            EngineErrorKind::QueryTimeout,
            "query exceeded its execution timeout",
        )
    }

    pub(crate) fn cancelled() -> Self {
        Self::invariant(
            EngineErrorKind::QueryCancelled,
            "query execution was cancelled",
        )
    }

    pub(crate) fn execution_invariant(message: &'static str) -> Self {
        Self::invariant(EngineErrorKind::QueryExecutionFailed, message)
    }

    fn invariant(kind: EngineErrorKind, message: &'static str) -> Self {
        Self {
            kind,
            source: EngineErrorSource::Invariant(message),
        }
    }
}

impl Display for EngineError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for EngineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl CodedError for EngineError {
    fn error_code(&self) -> ErrorCode {
        self.kind().into()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EngineErrorSource {
    #[error("immutable object validation failed")]
    Storage(#[source] StorageError),
    #[error("Parquet metadata decoding failed")]
    Parquet(#[source] parquet::errors::ParquetError),
    #[error("JSON result encoding failed")]
    Json(#[source] serde_json::Error),
    #[error("DataFusion query failed")]
    DataFusion(#[source] DataFusionError),
    #[error("query resource limit exceeded")]
    ResourceLimit(#[source] QueryResourceLimitExceeded),
    #[error("{0}")]
    Invariant(&'static str),
}

impl From<StorageError> for EngineErrorSource {
    fn from(source: StorageError) -> Self {
        Self::Storage(source)
    }
}

impl From<parquet::errors::ParquetError> for EngineErrorSource {
    fn from(source: parquet::errors::ParquetError) -> Self {
        Self::Parquet(source)
    }
}

impl From<serde_json::Error> for EngineErrorSource {
    fn from(source: serde_json::Error) -> Self {
        Self::Json(source)
    }
}

impl From<DataFusionError> for EngineErrorSource {
    fn from(source: DataFusionError) -> Self {
        Self::DataFusion(source)
    }
}
