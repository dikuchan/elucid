use std::error::Error;
use std::fmt::{Display, Formatter};

use datafusion::error::DataFusionError;
use elucid_storage::StorageError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EngineErrorCode {
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

impl EngineErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PublishedObjectMissing => "PUBLISHED_OBJECT_MISSING",
            Self::PublishedObjectCorrupt => "PUBLISHED_OBJECT_CORRUPT",
            Self::CatalogCorrupt => "CATALOG_CORRUPT",
            Self::QueryCastFailed => "QUERY_CAST_FAILED",
            Self::QueryEvaluationFailed => "QUERY_EVALUATION_FAILED",
            Self::QueryResourceLimitExceeded => "QUERY_RESOURCE_LIMIT_EXCEEDED",
            Self::QueryTimeout => "QUERY_TIMEOUT",
            Self::QueryCancelled => "QUERY_CANCELLED",
            Self::QueryExecutionFailed => "QUERY_EXECUTION_FAILED",
        }
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

impl Display for EngineErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub struct EngineError {
    code: EngineErrorCode,
    source: EngineErrorSource,
}

impl EngineError {
    #[must_use]
    pub const fn code(&self) -> EngineErrorCode {
        self.code
    }

    #[must_use]
    pub const fn resource_limit_exceeded(&self) -> Option<QueryResourceLimitExceeded> {
        match (&self.source, self.code) {
            (EngineErrorSource::ResourceLimit(source), _) => Some(*source),
            (EngineErrorSource::DataFusion(_), EngineErrorCode::QueryResourceLimitExceeded) => {
                Some(QueryResourceLimitExceeded::ExecutionResources)
            }
            _ => None,
        }
    }

    pub(crate) fn missing_object() -> Self {
        Self::invariant(
            EngineErrorCode::PublishedObjectMissing,
            "a selected published object is absent",
        )
    }

    pub(crate) fn corrupt_object(source: impl Into<EngineErrorSource>) -> Self {
        Self {
            code: EngineErrorCode::PublishedObjectCorrupt,
            source: source.into(),
        }
    }

    pub(crate) fn corrupt_object_invariant(message: &'static str) -> Self {
        Self::invariant(EngineErrorCode::PublishedObjectCorrupt, message)
    }

    pub(crate) fn catalog_corrupt(message: &'static str) -> Self {
        Self::invariant(EngineErrorCode::CatalogCorrupt, message)
    }

    pub(crate) fn execution(source: impl Into<EngineErrorSource>) -> Self {
        Self {
            code: EngineErrorCode::QueryExecutionFailed,
            source: source.into(),
        }
    }

    pub(crate) fn cast_failed(source: DataFusionError) -> Self {
        Self {
            code: EngineErrorCode::QueryCastFailed,
            source: EngineErrorSource::DataFusion(source),
        }
    }

    pub(crate) fn evaluation_failed(source: DataFusionError) -> Self {
        Self {
            code: EngineErrorCode::QueryEvaluationFailed,
            source: EngineErrorSource::DataFusion(source),
        }
    }

    pub(crate) fn evaluation_invariant(message: &'static str) -> Self {
        Self::invariant(EngineErrorCode::QueryEvaluationFailed, message)
    }

    pub(crate) fn resource_limit(source: QueryResourceLimitExceeded) -> Self {
        Self {
            code: EngineErrorCode::QueryResourceLimitExceeded,
            source: EngineErrorSource::ResourceLimit(source),
        }
    }

    pub(crate) fn resources_exhausted(source: DataFusionError) -> Self {
        Self {
            code: EngineErrorCode::QueryResourceLimitExceeded,
            source: EngineErrorSource::DataFusion(source),
        }
    }

    pub(crate) fn timeout() -> Self {
        Self::invariant(
            EngineErrorCode::QueryTimeout,
            "query exceeded its execution timeout",
        )
    }

    pub(crate) fn cancelled() -> Self {
        Self::invariant(
            EngineErrorCode::QueryCancelled,
            "query execution was cancelled",
        )
    }

    pub(crate) fn execution_invariant(message: &'static str) -> Self {
        Self::invariant(EngineErrorCode::QueryExecutionFailed, message)
    }

    fn invariant(code: EngineErrorCode, message: &'static str) -> Self {
        Self {
            code,
            source: EngineErrorSource::Invariant(message),
        }
    }
}

impl Display for EngineError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.code, formatter)
    }
}

impl Error for EngineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
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
