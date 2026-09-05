use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

/// Stable categories of operational failures.
///
/// Each variant represents a machine-distinguishable failure in the product contract. Wire spellings are compatibility contracts.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorCode {
    CapacityExhausted,
    CatalogCorrupt,
    CatalogDefinitionConflict,
    CatalogManifestInvalid,
    CatalogProfileInvalid,
    CatalogSchemaIncompatible,
    ClientTimeout,
    CommandInvalid,
    CompactionBuildFailed,
    CompactionInputInvalid,
    CompactionNotBeneficial,
    CompactionPublicationFailed,
    CompactionRecoveryFailed,
    ConfigurationConstraintViolation,
    ConfigurationDocumentInvalid,
    ConfigurationDocumentMalformed,
    ConfigurationDocumentTooLarge,
    ConfigurationEnvironmentOverrideInvalid,
    ConfigurationFileUnreadable,
    ConfigurationFileNotUtf8,
    ConfigurationSecretInvalid,
    ConfigurationSecretMissing,
    ConfigurationValueInvalid,
    EndpointUrlConstructionFailed,
    HttpClientInitializationFailed,
    IngestionBatchLimitExceeded,
    IngestionInitializationFailed,
    IngestionRuntimeFailed,
    InputFileUnreadable,
    InputReadFailed,
    InternalError,
    InvalidRequest,
    LocalCapacityExhausted,
    LocalStorageUnavailable,
    MaintenanceInitializationFailed,
    MaintenanceRuntimeFailed,
    MetastoreConflict,
    MetastoreCorrupt,
    MetastoreMigrationFailed,
    MetastoreUnavailable,
    NotFound,
    ObjectDeleteFailed,
    ObjectIntegrityError,
    ObjectStoreUnavailable,
    ObjectUploadFailed,
    ObjectVerificationFailed,
    ParquetBuildFailed,
    ParquetInvalid,
    PublishedObjectCorrupt,
    PublishedObjectMissing,
    QueryCancelled,
    QueryCastFailed,
    QueryEvaluationFailed,
    QueryExecutionFailed,
    QueryInitializationFailed,
    QueryResourceLimitExceeded,
    QuerySemanticError,
    QuerySyntaxError,
    QueryTimeout,
    RemoteResponseFailed,
    RemoteResponseInvalid,
    RemoteResponseTooLarge,
    RemoteServiceUnavailable,
    RetentionCleanupFailed,
    RetentionStateConflict,
    RetentionTimestampOverflow,
    ServerBindFailed,
    ServerDraining,
    ServerNotReady,
    ServerRuntimeFailed,
    ServerShutdownTimedOut,
    ServerSignalFailed,
    SpoolCorrupt,
    SpoolUnavailable,
    StandardOutputWriteFailed,
    VersionEncodingFailed,
}

impl ErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CapacityExhausted => "CAPACITY_EXHAUSTED",
            Self::CatalogCorrupt => "CATALOG_CORRUPT",
            Self::CatalogDefinitionConflict => "CATALOG_DEFINITION_CONFLICT",
            Self::CatalogManifestInvalid => "CATALOG_MANIFEST_INVALID",
            Self::CatalogProfileInvalid => "CATALOG_PROFILE_INVALID",
            Self::CatalogSchemaIncompatible => "CATALOG_SCHEMA_INCOMPATIBLE",
            Self::ClientTimeout => "CLIENT_TIMEOUT",
            Self::CommandInvalid => "COMMAND_INVALID",
            Self::CompactionBuildFailed => "COMPACTION_BUILD_FAILED",
            Self::CompactionInputInvalid => "COMPACTION_INPUT_INVALID",
            Self::CompactionNotBeneficial => "COMPACTION_NOT_BENEFICIAL",
            Self::CompactionPublicationFailed => "COMPACTION_PUBLICATION_FAILED",
            Self::CompactionRecoveryFailed => "COMPACTION_RECOVERY_FAILED",
            Self::ConfigurationConstraintViolation => "CONFIGURATION_CONSTRAINT_VIOLATION",
            Self::ConfigurationDocumentInvalid => "CONFIGURATION_DOCUMENT_INVALID",
            Self::ConfigurationDocumentMalformed => "CONFIGURATION_DOCUMENT_MALFORMED",
            Self::ConfigurationDocumentTooLarge => "CONFIGURATION_DOCUMENT_TOO_LARGE",
            Self::ConfigurationEnvironmentOverrideInvalid => {
                "CONFIGURATION_ENVIRONMENT_OVERRIDE_INVALID"
            }
            Self::ConfigurationFileUnreadable => "CONFIGURATION_FILE_UNREADABLE",
            Self::ConfigurationFileNotUtf8 => "CONFIGURATION_FILE_NOT_UTF8",
            Self::ConfigurationSecretInvalid => "CONFIGURATION_SECRET_INVALID",
            Self::ConfigurationSecretMissing => "CONFIGURATION_SECRET_MISSING",
            Self::ConfigurationValueInvalid => "CONFIGURATION_VALUE_INVALID",
            Self::EndpointUrlConstructionFailed => "ENDPOINT_URL_CONSTRUCTION_FAILED",
            Self::HttpClientInitializationFailed => "HTTP_CLIENT_INITIALIZATION_FAILED",
            Self::IngestionBatchLimitExceeded => "INGESTION_BATCH_LIMIT_EXCEEDED",
            Self::IngestionInitializationFailed => "INGESTION_INITIALIZATION_FAILED",
            Self::IngestionRuntimeFailed => "INGESTION_RUNTIME_FAILED",
            Self::InputFileUnreadable => "INPUT_FILE_UNREADABLE",
            Self::InputReadFailed => "INPUT_READ_FAILED",
            Self::InternalError => "INTERNAL_ERROR",
            Self::InvalidRequest => "INVALID_REQUEST",
            Self::LocalCapacityExhausted => "LOCAL_CAPACITY_EXHAUSTED",
            Self::LocalStorageUnavailable => "LOCAL_STORAGE_UNAVAILABLE",
            Self::MaintenanceInitializationFailed => "MAINTENANCE_INITIALIZATION_FAILED",
            Self::MaintenanceRuntimeFailed => "MAINTENANCE_RUNTIME_FAILED",
            Self::MetastoreConflict => "METASTORE_CONFLICT",
            Self::MetastoreCorrupt => "METASTORE_CORRUPT",
            Self::MetastoreMigrationFailed => "METASTORE_MIGRATION_FAILED",
            Self::MetastoreUnavailable => "METASTORE_UNAVAILABLE",
            Self::NotFound => "NOT_FOUND",
            Self::ObjectDeleteFailed => "OBJECT_DELETE_FAILED",
            Self::ObjectIntegrityError => "OBJECT_INTEGRITY_ERROR",
            Self::ObjectStoreUnavailable => "OBJECT_STORE_UNAVAILABLE",
            Self::ObjectUploadFailed => "OBJECT_UPLOAD_FAILED",
            Self::ObjectVerificationFailed => "OBJECT_VERIFICATION_FAILED",
            Self::ParquetBuildFailed => "PARQUET_BUILD_FAILED",
            Self::ParquetInvalid => "PARQUET_INVALID",
            Self::PublishedObjectCorrupt => "PUBLISHED_OBJECT_CORRUPT",
            Self::PublishedObjectMissing => "PUBLISHED_OBJECT_MISSING",
            Self::QueryCancelled => "QUERY_CANCELLED",
            Self::QueryCastFailed => "QUERY_CAST_FAILED",
            Self::QueryEvaluationFailed => "QUERY_EVALUATION_FAILED",
            Self::QueryExecutionFailed => "QUERY_EXECUTION_FAILED",
            Self::QueryInitializationFailed => "QUERY_INITIALIZATION_FAILED",
            Self::QueryResourceLimitExceeded => "QUERY_RESOURCE_LIMIT_EXCEEDED",
            Self::QuerySemanticError => "QUERY_SEMANTIC_ERROR",
            Self::QuerySyntaxError => "QUERY_SYNTAX_ERROR",
            Self::QueryTimeout => "QUERY_TIMEOUT",
            Self::RemoteResponseFailed => "REMOTE_RESPONSE_FAILED",
            Self::RemoteResponseInvalid => "REMOTE_RESPONSE_INVALID",
            Self::RemoteResponseTooLarge => "REMOTE_RESPONSE_TOO_LARGE",
            Self::RemoteServiceUnavailable => "REMOTE_SERVICE_UNAVAILABLE",
            Self::RetentionCleanupFailed => "RETENTION_CLEANUP_FAILED",
            Self::RetentionStateConflict => "RETENTION_STATE_CONFLICT",
            Self::RetentionTimestampOverflow => "RETENTION_TIMESTAMP_OVERFLOW",
            Self::ServerBindFailed => "SERVER_BIND_FAILED",
            Self::ServerDraining => "SERVER_DRAINING",
            Self::ServerNotReady => "SERVER_NOT_READY",
            Self::ServerRuntimeFailed => "SERVER_RUNTIME_FAILED",
            Self::ServerShutdownTimedOut => "SERVER_SHUTDOWN_TIMED_OUT",
            Self::ServerSignalFailed => "SERVER_SIGNAL_FAILED",
            Self::SpoolCorrupt => "SPOOL_CORRUPT",
            Self::SpoolUnavailable => "SPOOL_UNAVAILABLE",
            Self::StandardOutputWriteFailed => "STANDARD_OUTPUT_WRITE_FAILED",
            Self::VersionEncodingFailed => "VERSION_ENCODING_FAILED",
        }
    }
}

impl FromStr for ErrorCode {
    type Err = UnknownErrorCode;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "CAPACITY_EXHAUSTED" => Ok(Self::CapacityExhausted),
            "CATALOG_CORRUPT" => Ok(Self::CatalogCorrupt),
            "CATALOG_DEFINITION_CONFLICT" => Ok(Self::CatalogDefinitionConflict),
            "CATALOG_MANIFEST_INVALID" => Ok(Self::CatalogManifestInvalid),
            "CATALOG_PROFILE_INVALID" => Ok(Self::CatalogProfileInvalid),
            "CATALOG_SCHEMA_INCOMPATIBLE" => Ok(Self::CatalogSchemaIncompatible),
            "CLIENT_TIMEOUT" => Ok(Self::ClientTimeout),
            "COMMAND_INVALID" => Ok(Self::CommandInvalid),
            "COMPACTION_BUILD_FAILED" => Ok(Self::CompactionBuildFailed),
            "COMPACTION_INPUT_INVALID" => Ok(Self::CompactionInputInvalid),
            "COMPACTION_NOT_BENEFICIAL" => Ok(Self::CompactionNotBeneficial),
            "COMPACTION_PUBLICATION_FAILED" => Ok(Self::CompactionPublicationFailed),
            "COMPACTION_RECOVERY_FAILED" => Ok(Self::CompactionRecoveryFailed),
            "CONFIGURATION_CONSTRAINT_VIOLATION" => Ok(Self::ConfigurationConstraintViolation),
            "CONFIGURATION_DOCUMENT_INVALID" => Ok(Self::ConfigurationDocumentInvalid),
            "CONFIGURATION_DOCUMENT_MALFORMED" => Ok(Self::ConfigurationDocumentMalformed),
            "CONFIGURATION_DOCUMENT_TOO_LARGE" => Ok(Self::ConfigurationDocumentTooLarge),
            "CONFIGURATION_ENVIRONMENT_OVERRIDE_INVALID" => {
                Ok(Self::ConfigurationEnvironmentOverrideInvalid)
            }
            "CONFIGURATION_FILE_UNREADABLE" => Ok(Self::ConfigurationFileUnreadable),
            "CONFIGURATION_FILE_NOT_UTF8" => Ok(Self::ConfigurationFileNotUtf8),
            "CONFIGURATION_SECRET_INVALID" => Ok(Self::ConfigurationSecretInvalid),
            "CONFIGURATION_SECRET_MISSING" => Ok(Self::ConfigurationSecretMissing),
            "CONFIGURATION_VALUE_INVALID" => Ok(Self::ConfigurationValueInvalid),
            "ENDPOINT_URL_CONSTRUCTION_FAILED" => Ok(Self::EndpointUrlConstructionFailed),
            "HTTP_CLIENT_INITIALIZATION_FAILED" => Ok(Self::HttpClientInitializationFailed),
            "INGESTION_BATCH_LIMIT_EXCEEDED" => Ok(Self::IngestionBatchLimitExceeded),
            "INGESTION_INITIALIZATION_FAILED" => Ok(Self::IngestionInitializationFailed),
            "INGESTION_RUNTIME_FAILED" => Ok(Self::IngestionRuntimeFailed),
            "INPUT_FILE_UNREADABLE" => Ok(Self::InputFileUnreadable),
            "INPUT_READ_FAILED" => Ok(Self::InputReadFailed),
            "INTERNAL_ERROR" => Ok(Self::InternalError),
            "INVALID_REQUEST" => Ok(Self::InvalidRequest),
            "LOCAL_CAPACITY_EXHAUSTED" => Ok(Self::LocalCapacityExhausted),
            "LOCAL_STORAGE_UNAVAILABLE" => Ok(Self::LocalStorageUnavailable),
            "MAINTENANCE_INITIALIZATION_FAILED" => Ok(Self::MaintenanceInitializationFailed),
            "MAINTENANCE_RUNTIME_FAILED" => Ok(Self::MaintenanceRuntimeFailed),
            "METASTORE_CONFLICT" => Ok(Self::MetastoreConflict),
            "METASTORE_CORRUPT" => Ok(Self::MetastoreCorrupt),
            "METASTORE_MIGRATION_FAILED" => Ok(Self::MetastoreMigrationFailed),
            "METASTORE_UNAVAILABLE" => Ok(Self::MetastoreUnavailable),
            "NOT_FOUND" => Ok(Self::NotFound),
            "OBJECT_DELETE_FAILED" => Ok(Self::ObjectDeleteFailed),
            "OBJECT_INTEGRITY_ERROR" => Ok(Self::ObjectIntegrityError),
            "OBJECT_STORE_UNAVAILABLE" => Ok(Self::ObjectStoreUnavailable),
            "OBJECT_UPLOAD_FAILED" => Ok(Self::ObjectUploadFailed),
            "OBJECT_VERIFICATION_FAILED" => Ok(Self::ObjectVerificationFailed),
            "PARQUET_BUILD_FAILED" => Ok(Self::ParquetBuildFailed),
            "PARQUET_INVALID" => Ok(Self::ParquetInvalid),
            "PUBLISHED_OBJECT_CORRUPT" => Ok(Self::PublishedObjectCorrupt),
            "PUBLISHED_OBJECT_MISSING" => Ok(Self::PublishedObjectMissing),
            "QUERY_CANCELLED" => Ok(Self::QueryCancelled),
            "QUERY_CAST_FAILED" => Ok(Self::QueryCastFailed),
            "QUERY_EVALUATION_FAILED" => Ok(Self::QueryEvaluationFailed),
            "QUERY_EXECUTION_FAILED" => Ok(Self::QueryExecutionFailed),
            "QUERY_INITIALIZATION_FAILED" => Ok(Self::QueryInitializationFailed),
            "QUERY_RESOURCE_LIMIT_EXCEEDED" => Ok(Self::QueryResourceLimitExceeded),
            "QUERY_SEMANTIC_ERROR" => Ok(Self::QuerySemanticError),
            "QUERY_SYNTAX_ERROR" => Ok(Self::QuerySyntaxError),
            "QUERY_TIMEOUT" => Ok(Self::QueryTimeout),
            "REMOTE_RESPONSE_FAILED" => Ok(Self::RemoteResponseFailed),
            "REMOTE_RESPONSE_INVALID" => Ok(Self::RemoteResponseInvalid),
            "REMOTE_RESPONSE_TOO_LARGE" => Ok(Self::RemoteResponseTooLarge),
            "REMOTE_SERVICE_UNAVAILABLE" => Ok(Self::RemoteServiceUnavailable),
            "RETENTION_CLEANUP_FAILED" => Ok(Self::RetentionCleanupFailed),
            "RETENTION_STATE_CONFLICT" => Ok(Self::RetentionStateConflict),
            "RETENTION_TIMESTAMP_OVERFLOW" => Ok(Self::RetentionTimestampOverflow),
            "SERVER_BIND_FAILED" => Ok(Self::ServerBindFailed),
            "SERVER_DRAINING" => Ok(Self::ServerDraining),
            "SERVER_NOT_READY" => Ok(Self::ServerNotReady),
            "SERVER_RUNTIME_FAILED" => Ok(Self::ServerRuntimeFailed),
            "SERVER_SHUTDOWN_TIMED_OUT" => Ok(Self::ServerShutdownTimedOut),
            "SERVER_SIGNAL_FAILED" => Ok(Self::ServerSignalFailed),
            "SPOOL_CORRUPT" => Ok(Self::SpoolCorrupt),
            "SPOOL_UNAVAILABLE" => Ok(Self::SpoolUnavailable),
            "STANDARD_OUTPUT_WRITE_FAILED" => Ok(Self::StandardOutputWriteFailed),
            "VERSION_ENCODING_FAILED" => Ok(Self::VersionEncodingFailed),
            _ => Err(UnknownErrorCode),
        }
    }
}

impl Display for ErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// An unrecognized operational error code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnknownErrorCode;

impl Display for UnknownErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("unknown error code")
    }
}

impl Error for UnknownErrorCode {}

/// The stable operational code of this error at its own layer.
///
/// A boundary chooses the code and public presentation of its wrapping error.
pub trait CodedError: Error + Send + Sync + 'static {
    fn error_code(&self) -> ErrorCode;
}
