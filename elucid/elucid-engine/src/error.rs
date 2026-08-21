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
            Self::QueryExecutionFailed => "QUERY_EXECUTION_FAILED",
        }
    }
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
    #[error("DataFusion query failed")]
    DataFusion(#[source] DataFusionError),
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

impl From<DataFusionError> for EngineErrorSource {
    fn from(source: DataFusionError) -> Self {
        Self::DataFusion(source)
    }
}
