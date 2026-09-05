use axum::Json;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use elucid_catalog::{CatalogApplicationError, CatalogErrorKind};
use elucid_core::{CodedError, ErrorCode};
use elucid_engine::{EngineError, EngineErrorKind, QueryOutputRowLimitError};
use elucid_metastore::{
    CatalogPersistenceError, CatalogPersistenceErrorKind, PublicationError, PublicationErrorKind,
    QueryExecutionModelError, QueryExecutionPersistenceError, QueryExecutionPersistenceErrorKind,
    QuerySnapshotError, QuerySnapshotErrorKind,
};
use elucid_storage::{StorageError, StorageErrorKind};
use serde::Serialize;
use utoipa::ToSchema;

use crate::query::QueryFailure;

use super::health::ReadinessDetails;
use super::query::QueryDiagnosticResponse;

const RETRY_AFTER_SECONDS: u64 = 1;

#[derive(thiserror::Error)]
#[error("{message}")]
pub(super) struct ApiError {
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
    status: StatusCode,
    code: ErrorCode,
    message: &'static str,
    details: ErrorDetails,
    retry_after_seconds: Option<u64>,
}

impl ApiError {
    pub(super) fn invalid_request() -> Self {
        Self::plain(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            "Request is invalid",
        )
    }

    pub(super) fn unsupported_catalog_media_type() -> Self {
        Self::plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ErrorCode::InvalidRequest,
            "Content-Type must be application/yaml",
        )
    }

    pub(super) fn catalog_request_too_large() -> Self {
        Self::plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::InvalidRequest,
            "Request body exceeds the catalog document limit",
        )
    }

    pub(super) fn unsupported_ingestion_media_type() -> Self {
        Self::plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ErrorCode::InvalidRequest,
            "Content-Type must be application/x-ndjson",
        )
    }

    pub(super) fn unsupported_ingestion_encoding() -> Self {
        Self::plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ErrorCode::InvalidRequest,
            "Content-Encoding must be identity",
        )
    }

    pub(super) fn unsupported_query_media_type() -> Self {
        Self::plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ErrorCode::InvalidRequest,
            "Content-Type must be application/json",
        )
    }

    pub(super) fn query_request_too_large() -> Self {
        Self::plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::InvalidRequest,
            "Request body exceeds the query request limit",
        )
    }

    pub(super) fn ingestion_batch_limit_exceeded() -> Self {
        Self::plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::IngestionBatchLimitExceeded,
            "Ingestion batch exceeds an admission limit",
        )
    }

    pub(super) fn capacity_exhausted() -> Self {
        Self {
            source: None,
            status: StatusCode::TOO_MANY_REQUESTS,
            code: ErrorCode::CapacityExhausted,
            message: "Admission capacity is exhausted",
            details: ErrorDetails::Empty(EmptyDetails {}),
            retry_after_seconds: Some(RETRY_AFTER_SECONDS),
        }
    }

    pub(super) fn request_timed_out() -> Self {
        Self::plain(
            StatusCode::REQUEST_TIMEOUT,
            ErrorCode::InvalidRequest,
            "Request timed out",
        )
    }

    pub(super) fn method_not_allowed() -> Self {
        Self::plain(
            StatusCode::METHOD_NOT_ALLOWED,
            ErrorCode::InvalidRequest,
            "Method is not allowed",
        )
    }

    pub(super) fn not_found() -> Self {
        Self::plain(
            StatusCode::NOT_FOUND,
            ErrorCode::NotFound,
            "Resource was not found",
        )
    }

    pub(super) fn internal() -> Self {
        Self::plain(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::InternalError,
            "Internal server error",
        )
    }

    pub(super) fn server_not_ready(details: ReadinessDetails) -> Self {
        Self {
            source: None,
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: ErrorCode::ServerNotReady,
            message: "Server is not ready",
            details: ErrorDetails::Readiness(details),
            retry_after_seconds: Some(RETRY_AFTER_SECONDS),
        }
    }

    pub(super) fn server_draining() -> Self {
        Self {
            source: None,
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: ErrorCode::ServerDraining,
            message: "Server is draining",
            details: ErrorDetails::Empty(EmptyDetails {}),
            retry_after_seconds: Some(RETRY_AFTER_SECONDS),
        }
    }

    pub(super) fn metastore_unavailable() -> Self {
        Self::plain(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::MetastoreUnavailable,
            "Metastore is unavailable",
        )
    }

    pub(super) fn publication(error: PublicationError) -> Self {
        let response = match error.kind() {
            PublicationErrorKind::Unavailable => Self::metastore_unavailable(),
            PublicationErrorKind::Conflict => Self::plain(
                StatusCode::CONFLICT,
                ErrorCode::MetastoreConflict,
                "Metastore state changed concurrently",
            ),
            PublicationErrorKind::Corrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::MetastoreCorrupt,
                "Metastore state is corrupt",
            ),
            _ => Self::internal(),
        };
        response.with_source(error)
    }

    pub(super) fn storage(error: StorageError) -> Self {
        let response = match error.kind() {
            StorageErrorKind::ObjectStoreUnavailable
            | StorageErrorKind::ObjectUploadFailed
            | StorageErrorKind::ObjectVerificationFailed
            | StorageErrorKind::ObjectDeleteFailed => Self::plain(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::ObjectStoreUnavailable,
                "Object store is unavailable",
            ),
            StorageErrorKind::ObjectIntegrityError | StorageErrorKind::ParquetInvalid => {
                Self::object_integrity()
            }
            StorageErrorKind::ParquetBuildFailed | StorageErrorKind::LocalCapacityExhausted => {
                Self::internal()
            }
            _ => Self::internal(),
        };
        response.with_source(error)
    }

    pub(super) fn object_integrity() -> Self {
        Self::plain(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::ObjectIntegrityError,
            "Stored object failed integrity validation",
        )
    }

    pub(super) fn query(error: QueryFailure) -> Self {
        match error {
            QueryFailure::ExecutionModel(error) => (match &error {
                QueryExecutionModelError::QueryTextTooLarge { .. } => {
                    Self::query_request_too_large()
                }
                QueryExecutionModelError::OutputRowsMustBePositive => Self::invalid_request(),
                QueryExecutionModelError::ListLimitOutOfRange { .. } => Self::internal(),
                _ => Self::internal(),
            })
            .with_source(error),
            QueryFailure::Persistence(error) => Self::query_execution_persistence(error),
            QueryFailure::Snapshot(error) => Self::query_snapshot(error),
            QueryFailure::Engine(error) => Self::query_engine(error),
            QueryFailure::OutputRowLimit(error) => (match &error {
                QueryOutputRowLimitError::MustBePositive
                | QueryOutputRowLimitError::ExceedsConfiguredMaximum { .. } => {
                    Self::invalid_request()
                }
                _ => Self::invalid_request(),
            })
            .with_source(error),
            QueryFailure::Cancelled => Self::plain(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::QueryCancelled,
                "Query execution was cancelled",
            ),
        }
    }

    pub(super) fn query_execution_persistence(error: QueryExecutionPersistenceError) -> Self {
        let response = match error.kind() {
            QueryExecutionPersistenceErrorKind::Conflict => Self::plain(
                StatusCode::CONFLICT,
                ErrorCode::MetastoreConflict,
                "Metastore state changed concurrently",
            ),
            QueryExecutionPersistenceErrorKind::Unavailable => Self::metastore_unavailable(),
            QueryExecutionPersistenceErrorKind::Corrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::MetastoreCorrupt,
                "Metastore state is corrupt",
            ),
            _ => Self::internal(),
        };
        response.with_source(error)
    }

    fn query_snapshot(error: QuerySnapshotError) -> Self {
        let response = match error.kind() {
            QuerySnapshotErrorKind::Analysis => {
                let Some(analysis) = error.analysis_error() else {
                    return Self::internal().with_source(error);
                };
                Self {
                    source: None,
                    status: StatusCode::BAD_REQUEST,
                    code: analysis.error_code(),
                    message: "Query is invalid",
                    details: ErrorDetails::Query(QueryErrorDetails {
                        diagnostics: analysis
                            .diagnostics()
                            .iter()
                            .map(QueryDiagnosticResponse::from_diagnostic)
                            .collect(),
                    }),
                    retry_after_seconds: None,
                }
            }
            QuerySnapshotErrorKind::ResourceLimit => Self::plain(
                StatusCode::UNPROCESSABLE_ENTITY,
                ErrorCode::QueryResourceLimitExceeded,
                "Query exceeds an execution limit",
            ),
            QuerySnapshotErrorKind::Unavailable => Self::metastore_unavailable(),
            QuerySnapshotErrorKind::Corrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::CatalogCorrupt,
                "Query metadata is corrupt",
            ),
            _ => Self::internal(),
        };
        response.with_source(error)
    }

    fn query_engine(error: EngineError) -> Self {
        let response = match error.kind() {
            EngineErrorKind::PublishedObjectMissing => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.error_code(),
                "A published query object is missing",
            ),
            EngineErrorKind::PublishedObjectCorrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.error_code(),
                "A published query object is corrupt",
            ),
            EngineErrorKind::CatalogCorrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.error_code(),
                "Query catalog state is corrupt",
            ),
            EngineErrorKind::QueryCastFailed => Self::plain(
                StatusCode::UNPROCESSABLE_ENTITY,
                error.error_code(),
                "Query cast failed",
            ),
            EngineErrorKind::QueryEvaluationFailed => Self::plain(
                StatusCode::UNPROCESSABLE_ENTITY,
                error.error_code(),
                "Query evaluation failed",
            ),
            EngineErrorKind::QueryResourceLimitExceeded => Self::plain(
                StatusCode::UNPROCESSABLE_ENTITY,
                error.error_code(),
                "Query exceeds an execution limit",
            ),
            EngineErrorKind::QueryTimeout => Self::plain(
                StatusCode::REQUEST_TIMEOUT,
                error.error_code(),
                "Query exceeded its execution timeout",
            ),
            EngineErrorKind::QueryCancelled => Self::plain(
                StatusCode::SERVICE_UNAVAILABLE,
                error.error_code(),
                "Query execution was cancelled",
            ),
            EngineErrorKind::QueryExecutionFailed => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.error_code(),
                "Query execution failed",
            ),
            _ => Self::internal(),
        };
        response.with_source(error)
    }

    pub(super) fn catalog(error: CatalogApplicationError) -> Self {
        let status = match error.kind() {
            CatalogErrorKind::ManifestInvalid | CatalogErrorKind::ProfileInvalid => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            CatalogErrorKind::DefinitionConflict | CatalogErrorKind::SchemaIncompatible => {
                StatusCode::CONFLICT
            }
            CatalogErrorKind::Corrupt => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let response = Self {
            source: None,
            status,
            code: error.error_code(),
            message: "Catalog application was rejected",
            details: ErrorDetails::Catalog(CatalogErrorDetails {
                path: error.path().as_str().to_owned(),
            }),
            retry_after_seconds: None,
        };
        response.with_source(error)
    }

    pub(super) fn catalog_persistence(error: CatalogPersistenceError) -> Self {
        if let Some(catalog_error) = error.catalog_error() {
            let status = match catalog_error.kind() {
                CatalogErrorKind::ManifestInvalid | CatalogErrorKind::ProfileInvalid => {
                    StatusCode::UNPROCESSABLE_ENTITY
                }
                CatalogErrorKind::DefinitionConflict | CatalogErrorKind::SchemaIncompatible => {
                    StatusCode::CONFLICT
                }
                CatalogErrorKind::Corrupt => StatusCode::INTERNAL_SERVER_ERROR,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            return Self {
                source: None,
                status,
                code: catalog_error.error_code(),
                message: "Catalog application was rejected",
                details: ErrorDetails::Catalog(CatalogErrorDetails {
                    path: catalog_error.path().as_str().to_owned(),
                }),
                retry_after_seconds: None,
            }
            .with_source(error);
        }
        let response = match error.kind() {
            CatalogPersistenceErrorKind::Unavailable => Self::metastore_unavailable(),
            CatalogPersistenceErrorKind::Conflict => Self::plain(
                StatusCode::CONFLICT,
                ErrorCode::MetastoreConflict,
                "Metastore state changed concurrently",
            ),
            CatalogPersistenceErrorKind::Corrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::MetastoreCorrupt,
                "Metastore state is corrupt",
            ),
            _ => Self::internal(),
        };
        response.with_source(error)
    }

    fn plain(status: StatusCode, code: ErrorCode, message: &'static str) -> Self {
        Self {
            source: None,
            status,
            code,
            message,
            details: ErrorDetails::Empty(EmptyDetails {}),
            retry_after_seconds: None,
        }
    }
    fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }
}

impl std::fmt::Debug for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApiError")
            .field("status", &self.status)
            .field("code", &self.code)
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

impl CodedError for ApiError {
    fn error_code(&self) -> ErrorCode {
        self.code
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(ErrorEnvelope {
                error: ErrorBody {
                    code: self.error_code().as_str(),
                    message: self.message,
                    details: self.details,
                },
            }),
        )
            .into_response();
        if let Some(seconds) = self.retry_after_seconds
            && let Ok(value) = HeaderValue::from_str(&seconds.to_string())
        {
            response.headers_mut().insert("retry-after", value);
        }
        response
    }
}

#[derive(Serialize, ToSchema)]
pub(super) struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize, ToSchema)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
    details: ErrorDetails,
}

#[derive(Serialize, ToSchema)]
#[serde(untagged)]
enum ErrorDetails {
    Empty(EmptyDetails),
    Readiness(ReadinessDetails),
    Catalog(CatalogErrorDetails),
    Query(QueryErrorDetails),
}

#[derive(Serialize, ToSchema)]
struct EmptyDetails {}

#[derive(Serialize, ToSchema)]
struct CatalogErrorDetails {
    path: String,
}

#[derive(Serialize, ToSchema)]
struct QueryErrorDetails {
    diagnostics: Vec<QueryDiagnosticResponse>,
}
