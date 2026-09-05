use std::collections::HashSet;

use elucid_core::{ErrorCode, UnknownErrorCode};

#[test]
fn existing_error_codes_keep_their_wire_spelling_and_parse_exactly() {
    let contract = [
        (
            ErrorCode::ConfigurationFileNotUtf8,
            "CONFIGURATION_FILE_NOT_UTF8",
        ),
        (ErrorCode::CapacityExhausted, "CAPACITY_EXHAUSTED"),
        (ErrorCode::CatalogCorrupt, "CATALOG_CORRUPT"),
        (
            ErrorCode::CatalogDefinitionConflict,
            "CATALOG_DEFINITION_CONFLICT",
        ),
        (
            ErrorCode::CatalogManifestInvalid,
            "CATALOG_MANIFEST_INVALID",
        ),
        (ErrorCode::CatalogProfileInvalid, "CATALOG_PROFILE_INVALID"),
        (
            ErrorCode::CatalogSchemaIncompatible,
            "CATALOG_SCHEMA_INCOMPATIBLE",
        ),
        (ErrorCode::ClientTimeout, "CLIENT_TIMEOUT"),
        (ErrorCode::CommandInvalid, "COMMAND_INVALID"),
        (ErrorCode::CompactionBuildFailed, "COMPACTION_BUILD_FAILED"),
        (
            ErrorCode::CompactionInputInvalid,
            "COMPACTION_INPUT_INVALID",
        ),
        (
            ErrorCode::CompactionNotBeneficial,
            "COMPACTION_NOT_BENEFICIAL",
        ),
        (
            ErrorCode::CompactionPublicationFailed,
            "COMPACTION_PUBLICATION_FAILED",
        ),
        (
            ErrorCode::CompactionRecoveryFailed,
            "COMPACTION_RECOVERY_FAILED",
        ),
        (
            ErrorCode::ConfigurationConstraintViolation,
            "CONFIGURATION_CONSTRAINT_VIOLATION",
        ),
        (
            ErrorCode::ConfigurationDocumentInvalid,
            "CONFIGURATION_DOCUMENT_INVALID",
        ),
        (
            ErrorCode::ConfigurationDocumentMalformed,
            "CONFIGURATION_DOCUMENT_MALFORMED",
        ),
        (
            ErrorCode::ConfigurationDocumentTooLarge,
            "CONFIGURATION_DOCUMENT_TOO_LARGE",
        ),
        (
            ErrorCode::ConfigurationEnvironmentOverrideInvalid,
            "CONFIGURATION_ENVIRONMENT_OVERRIDE_INVALID",
        ),
        (
            ErrorCode::ConfigurationFileUnreadable,
            "CONFIGURATION_FILE_UNREADABLE",
        ),
        (
            ErrorCode::ConfigurationSecretInvalid,
            "CONFIGURATION_SECRET_INVALID",
        ),
        (
            ErrorCode::ConfigurationSecretMissing,
            "CONFIGURATION_SECRET_MISSING",
        ),
        (
            ErrorCode::ConfigurationValueInvalid,
            "CONFIGURATION_VALUE_INVALID",
        ),
        (
            ErrorCode::EndpointUrlConstructionFailed,
            "ENDPOINT_URL_CONSTRUCTION_FAILED",
        ),
        (
            ErrorCode::HttpClientInitializationFailed,
            "HTTP_CLIENT_INITIALIZATION_FAILED",
        ),
        (
            ErrorCode::IngestionBatchLimitExceeded,
            "INGESTION_BATCH_LIMIT_EXCEEDED",
        ),
        (
            ErrorCode::IngestionInitializationFailed,
            "INGESTION_INITIALIZATION_FAILED",
        ),
        (
            ErrorCode::IngestionRuntimeFailed,
            "INGESTION_RUNTIME_FAILED",
        ),
        (ErrorCode::InputFileUnreadable, "INPUT_FILE_UNREADABLE"),
        (ErrorCode::InputReadFailed, "INPUT_READ_FAILED"),
        (ErrorCode::InternalError, "INTERNAL_ERROR"),
        (ErrorCode::InvalidRequest, "INVALID_REQUEST"),
        (
            ErrorCode::LocalCapacityExhausted,
            "LOCAL_CAPACITY_EXHAUSTED",
        ),
        (
            ErrorCode::LocalStorageUnavailable,
            "LOCAL_STORAGE_UNAVAILABLE",
        ),
        (
            ErrorCode::MaintenanceInitializationFailed,
            "MAINTENANCE_INITIALIZATION_FAILED",
        ),
        (
            ErrorCode::MaintenanceRuntimeFailed,
            "MAINTENANCE_RUNTIME_FAILED",
        ),
        (ErrorCode::MetastoreConflict, "METASTORE_CONFLICT"),
        (ErrorCode::MetastoreCorrupt, "METASTORE_CORRUPT"),
        (
            ErrorCode::MetastoreMigrationFailed,
            "METASTORE_MIGRATION_FAILED",
        ),
        (ErrorCode::MetastoreUnavailable, "METASTORE_UNAVAILABLE"),
        (ErrorCode::NotFound, "NOT_FOUND"),
        (ErrorCode::ObjectDeleteFailed, "OBJECT_DELETE_FAILED"),
        (ErrorCode::ObjectIntegrityError, "OBJECT_INTEGRITY_ERROR"),
        (
            ErrorCode::ObjectStoreUnavailable,
            "OBJECT_STORE_UNAVAILABLE",
        ),
        (ErrorCode::ObjectUploadFailed, "OBJECT_UPLOAD_FAILED"),
        (
            ErrorCode::ObjectVerificationFailed,
            "OBJECT_VERIFICATION_FAILED",
        ),
        (ErrorCode::ParquetBuildFailed, "PARQUET_BUILD_FAILED"),
        (ErrorCode::ParquetInvalid, "PARQUET_INVALID"),
        (
            ErrorCode::PublishedObjectCorrupt,
            "PUBLISHED_OBJECT_CORRUPT",
        ),
        (
            ErrorCode::PublishedObjectMissing,
            "PUBLISHED_OBJECT_MISSING",
        ),
        (ErrorCode::QueryCancelled, "QUERY_CANCELLED"),
        (ErrorCode::QueryCastFailed, "QUERY_CAST_FAILED"),
        (ErrorCode::QueryEvaluationFailed, "QUERY_EVALUATION_FAILED"),
        (ErrorCode::QueryExecutionFailed, "QUERY_EXECUTION_FAILED"),
        (
            ErrorCode::QueryInitializationFailed,
            "QUERY_INITIALIZATION_FAILED",
        ),
        (
            ErrorCode::QueryResourceLimitExceeded,
            "QUERY_RESOURCE_LIMIT_EXCEEDED",
        ),
        (ErrorCode::QuerySemanticError, "QUERY_SEMANTIC_ERROR"),
        (ErrorCode::QuerySyntaxError, "QUERY_SYNTAX_ERROR"),
        (ErrorCode::QueryTimeout, "QUERY_TIMEOUT"),
        (ErrorCode::RemoteResponseFailed, "REMOTE_RESPONSE_FAILED"),
        (ErrorCode::RemoteResponseInvalid, "REMOTE_RESPONSE_INVALID"),
        (
            ErrorCode::RemoteResponseTooLarge,
            "REMOTE_RESPONSE_TOO_LARGE",
        ),
        (
            ErrorCode::RemoteServiceUnavailable,
            "REMOTE_SERVICE_UNAVAILABLE",
        ),
        (
            ErrorCode::RetentionCleanupFailed,
            "RETENTION_CLEANUP_FAILED",
        ),
        (
            ErrorCode::RetentionStateConflict,
            "RETENTION_STATE_CONFLICT",
        ),
        (
            ErrorCode::RetentionTimestampOverflow,
            "RETENTION_TIMESTAMP_OVERFLOW",
        ),
        (ErrorCode::ServerBindFailed, "SERVER_BIND_FAILED"),
        (ErrorCode::ServerDraining, "SERVER_DRAINING"),
        (ErrorCode::ServerNotReady, "SERVER_NOT_READY"),
        (ErrorCode::ServerRuntimeFailed, "SERVER_RUNTIME_FAILED"),
        (
            ErrorCode::ServerShutdownTimedOut,
            "SERVER_SHUTDOWN_TIMED_OUT",
        ),
        (ErrorCode::ServerSignalFailed, "SERVER_SIGNAL_FAILED"),
        (ErrorCode::SpoolCorrupt, "SPOOL_CORRUPT"),
        (ErrorCode::SpoolUnavailable, "SPOOL_UNAVAILABLE"),
        (
            ErrorCode::StandardOutputWriteFailed,
            "STANDARD_OUTPUT_WRITE_FAILED",
        ),
        (ErrorCode::VersionEncodingFailed, "VERSION_ENCODING_FAILED"),
    ];
    let mut spellings = HashSet::new();
    for (code, spelling) in contract {
        assert_eq!(code.as_str(), spelling);
        assert_eq!(spelling.parse::<ErrorCode>(), Ok(code));
        assert!(
            spellings.insert(code.as_str()),
            "duplicate spelling: {spelling}"
        );
    }
    for unknown in [
        "",
        "query_timeout",
        " QUERY_TIMEOUT",
        "QUERY_TIMEOUT ",
        "FUTURE_SERVER_FAILURE",
        "QUERY_FIELD_NOT_FOUND",
        "RECORD_FIELD_MISSING",
    ] {
        assert_eq!(
            unknown.parse::<ErrorCode>(),
            Err(UnknownErrorCode),
            "{unknown}"
        );
    }
}
