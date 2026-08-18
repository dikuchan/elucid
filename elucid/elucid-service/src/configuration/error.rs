use std::io;
use std::path::PathBuf;

use thiserror::Error;

use super::model::RuntimeRole;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigurationErrorCode {
    FileUnreadable,
    FileNotUtf8,
    DocumentTooLarge,
    DocumentMalformed,
    DocumentInvalid,
    EnvironmentOverrideInvalid,
    ValueInvalid,
    ArithmeticOverflow,
    ConstraintViolation,
    SecretMissing,
    SecretInvalid,
}

impl ConfigurationErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FileUnreadable => "CONFIGURATION_FILE_UNREADABLE",
            Self::FileNotUtf8 => "CONFIGURATION_FILE_NOT_UTF8",
            Self::DocumentTooLarge => "CONFIGURATION_DOCUMENT_TOO_LARGE",
            Self::DocumentMalformed => "CONFIGURATION_DOCUMENT_MALFORMED",
            Self::DocumentInvalid => "CONFIGURATION_DOCUMENT_INVALID",
            Self::EnvironmentOverrideInvalid => "CONFIGURATION_ENVIRONMENT_OVERRIDE_INVALID",
            Self::ValueInvalid => "CONFIGURATION_VALUE_INVALID",
            Self::ArithmeticOverflow => "CONFIGURATION_ARITHMETIC_OVERFLOW",
            Self::ConstraintViolation => "CONFIGURATION_CONSTRAINT_VIOLATION",
            Self::SecretMissing => "CONFIGURATION_SECRET_MISSING",
            Self::SecretInvalid => "CONFIGURATION_SECRET_INVALID",
        }
    }
}

impl std::fmt::Display for ConfigurationErrorCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConfigurationField(&'static str);

impl ConfigurationField {
    pub(crate) const fn new(path: &'static str) -> Self {
        Self(path)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for ConfigurationField {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InvalidValueReason {
    RequiredPositive,
    Empty,
    InvalidSocketAddress,
    InvalidUrl,
    InvalidEnvironmentVariableName,
    InvalidMetricsPath,
    InvalidObjectStoreEndpoint,
}

impl std::fmt::Display for InvalidValueReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::RequiredPositive => "a required positive value cannot be zero",
            Self::Empty => "the value cannot be empty",
            Self::InvalidSocketAddress => "the value is not an IP socket address",
            Self::InvalidUrl => "the value is not an absolute URL",
            Self::InvalidEnvironmentVariableName => {
                "the value is not a portable environment-variable name"
            }
            Self::InvalidMetricsPath => "the value is not an absolute HTTP path",
            Self::InvalidObjectStoreEndpoint => {
                "the endpoint must use http or https and contain no credentials, query, or fragment"
            }
        };
        formatter.write_str(message)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EnvironmentOverrideInvalidReason {
    InvalidPath,
    SectionIsNotTable,
    ValueNotUnicode,
}

impl std::fmt::Display for EnvironmentOverrideInvalidReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidPath => "the name must have the form ELUCID_<SECTION>__<FIELD>",
            Self::SectionIsNotTable => "the target section is not a TOML table",
            Self::ValueNotUnicode => "the value is not valid Unicode",
        };
        formatter.write_str(message)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigurationExpression {
    IngestionStaleThreshold,
    CompactionStaleThreshold,
    OrphanGracePeriodMinimum,
    ConcurrentCompactionOutputCapacity,
    InputRowOutputCapacity,
    InputUncompressedOutputCapacity,
}

impl std::fmt::Display for ConfigurationExpression {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let expression = match self {
            Self::IngestionStaleThreshold => "ingestion.attempt_heartbeat_interval_seconds * 3",
            Self::CompactionStaleThreshold => "compaction.run_heartbeat_interval_seconds * 3",
            Self::OrphanGracePeriodMinimum => {
                "compaction.run_timeout_seconds + object_store.request_timeout_seconds * object_store.maximum_request_attempts"
            }
            Self::ConcurrentCompactionOutputCapacity => {
                "compaction.maximum_concurrent_runs * compaction.maximum_output_parquet_bytes"
            }
            Self::InputRowOutputCapacity => {
                "compaction.maximum_output_segments * compaction.maximum_output_segment_rows"
            }
            Self::InputUncompressedOutputCapacity => {
                "compaction.maximum_output_segments * compaction.target_output_segment_uncompressed_bytes"
            }
        };
        formatter.write_str(expression)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigurationViolation {
    EmptyRuntimeRoles,
    DuplicateRuntimeRole { role: RuntimeRole },
    DefaultPageItemsExceedMaximum,
    DefaultOutputRowsExceedMaximum,
    IngestionStaleThresholdTooSmall,
    CompactionStaleThresholdTooSmall,
    BrowserOriginSchemeUnsupported,
    BrowserOriginIsNotAnOrigin,
    MaximumRecordBytesExceedRequestBody,
    DeadLetterPageBytesDoNotExceedCompleteRaw,
    QueryTimeoutExceedsSnapshotLifetime,
    RetiredObjectGracePeriodDoesNotExceedSnapshotLifetime,
    OrphanGracePeriodTooShort,
    MinimumInputSegmentsBelowTwo,
    MinimumInputSegmentsExceedMaximum,
    MaximumOutputSegmentsNotBelowMaximumInputSegments,
    MaximumOutputObjectBytesExceedTotal,
    LocalCompactionConcurrencyExceedsCluster,
    RetentionTaskDurationNotBelowScanInterval,
    AttemptTimeoutNotBelowIdempotencyRetention,
    IdempotencyRetentionDoesNotExceedAttemptStale,
    IdempotencyRetentionExceedsIngestProvenance,
    EventDataRetentionExceedsIngestProvenance,
    DeadLetterRetentionExceedsIngestProvenance,
    IngestionStagingCapacityBelowMaximumRequest,
    QueryMemoryCapacityBelowMaximumResult,
    QuerySpillCapacityBelowMaximumResult,
    CompactionWorkingCapacityBelowConcurrentOutput,
    MaximumInputRowsExceedOutputCapacity,
    MaximumInputUncompressedBytesExceedOutputCapacity,
    LoopbackTrustRequiresLoopbackBind,
    LocalContainerRequiresHttpLoopbackOrigin,
    TrustedNetworkRequiresOperatorSecretReference,
    OperatorSecretReferenceRequiresTrustedNetwork,
    TrustedNetworkRequiresHttpsOrigin,
}

impl std::fmt::Display for ConfigurationViolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyRuntimeRoles => formatter.write_str("server.roles cannot be empty"),
            Self::DuplicateRuntimeRole { role } => {
                write!(formatter, "server.roles contains duplicate role {role}")
            }
            Self::DefaultPageItemsExceedMaximum => {
                formatter.write_str("server.default_page_items exceeds server.maximum_page_items")
            }
            Self::DefaultOutputRowsExceedMaximum => {
                formatter.write_str("query.default_output_rows exceeds query.maximum_output_rows")
            }
            Self::IngestionStaleThresholdTooSmall => formatter.write_str(
                "ingestion.attempt_stale_after_seconds is less than three heartbeat intervals",
            ),
            Self::CompactionStaleThresholdTooSmall => formatter.write_str(
                "compaction.run_stale_after_seconds is less than three heartbeat intervals",
            ),
            Self::BrowserOriginSchemeUnsupported => {
                formatter.write_str("server.browser_origin must use http or https")
            }
            Self::BrowserOriginIsNotAnOrigin => formatter.write_str(
                "server.browser_origin must not contain credentials, path, query, or fragment",
            ),
            Self::MaximumRecordBytesExceedRequestBody => formatter.write_str(
                "ingestion.maximum_record_bytes exceeds ingestion.maximum_request_body_bytes",
            ),
            Self::DeadLetterPageBytesDoNotExceedCompleteRaw => formatter.write_str(
                "ingestion.maximum_dead_letter_page_bytes must exceed ingestion.dead_letter_complete_raw_maximum_bytes",
            ),
            Self::QueryTimeoutExceedsSnapshotLifetime => formatter.write_str(
                "query.execution_timeout_seconds exceeds the maximum query snapshot lifetime",
            ),
            Self::RetiredObjectGracePeriodDoesNotExceedSnapshotLifetime => formatter.write_str(
                "garbage_collection.retired_object_grace_period_seconds must exceed the maximum query snapshot lifetime",
            ),
            Self::OrphanGracePeriodTooShort => formatter.write_str(
                "garbage_collection.orphan_grace_period_seconds does not exceed the maximum compaction upload attempt lifetime",
            ),
            Self::MinimumInputSegmentsBelowTwo => {
                formatter.write_str("compaction.minimum_input_segments is below two")
            }
            Self::MinimumInputSegmentsExceedMaximum => formatter.write_str(
                "compaction.minimum_input_segments exceeds compaction.maximum_input_segments",
            ),
            Self::MaximumOutputSegmentsNotBelowMaximumInputSegments => formatter.write_str(
                "compaction.maximum_output_segments must be below compaction.maximum_input_segments",
            ),
            Self::MaximumOutputObjectBytesExceedTotal => formatter.write_str(
                "compaction.maximum_output_parquet_object_bytes exceeds compaction.maximum_output_parquet_bytes",
            ),
            Self::LocalCompactionConcurrencyExceedsCluster => formatter.write_str(
                "compaction.maximum_concurrent_runs exceeds compaction.maximum_cluster_concurrent_runs",
            ),
            Self::RetentionTaskDurationNotBelowScanInterval => formatter.write_str(
                "retention.maximum_task_duration_seconds must be below retention.scan_interval_seconds",
            ),
            Self::AttemptTimeoutNotBelowIdempotencyRetention => formatter.write_str(
                "ingestion.attempt_timeout_seconds must be below retention.idempotency_retention_seconds",
            ),
            Self::IdempotencyRetentionDoesNotExceedAttemptStale => formatter.write_str(
                "retention.idempotency_retention_seconds must exceed ingestion.attempt_stale_after_seconds",
            ),
            Self::IdempotencyRetentionExceedsIngestProvenance => formatter.write_str(
                "retention.idempotency_retention_seconds exceeds retention.ingest_provenance_retention_seconds",
            ),
            Self::EventDataRetentionExceedsIngestProvenance => formatter.write_str(
                "retention.event_data_retention_seconds exceeds retention.ingest_provenance_retention_seconds",
            ),
            Self::DeadLetterRetentionExceedsIngestProvenance => formatter.write_str(
                "retention.dead_letter_retention_seconds exceeds retention.ingest_provenance_retention_seconds",
            ),
            Self::IngestionStagingCapacityBelowMaximumRequest => formatter.write_str(
                "ingestion.staging_capacity_bytes is below ingestion.maximum_request_body_bytes",
            ),
            Self::QueryMemoryCapacityBelowMaximumResult => formatter.write_str(
                "query.memory_pool_bytes is below query.maximum_result_bytes",
            ),
            Self::QuerySpillCapacityBelowMaximumResult => formatter.write_str(
                "query.spill_capacity_bytes is below query.maximum_result_bytes",
            ),
            Self::CompactionWorkingCapacityBelowConcurrentOutput => formatter.write_str(
                "compaction.working_capacity_bytes is below concurrent output capacity",
            ),
            Self::MaximumInputRowsExceedOutputCapacity => formatter.write_str(
                "compaction.maximum_input_rows exceeds bounded output row capacity",
            ),
            Self::MaximumInputUncompressedBytesExceedOutputCapacity => formatter.write_str(
                "compaction.maximum_input_uncompressed_bytes exceeds bounded output byte capacity",
            ),
            Self::LoopbackTrustRequiresLoopbackBind => formatter.write_str(
                "LOOPBACK_ONLY requires server.bind to contain a loopback address",
            ),
            Self::LocalContainerRequiresHttpLoopbackOrigin => formatter.write_str(
                "LOCAL_CONTAINER requires an http browser origin with a loopback host",
            ),
            Self::TrustedNetworkRequiresOperatorSecretReference => formatter.write_str(
                "TRUSTED_NETWORK requires server.operator_bearer_token_environment_variable",
            ),
            Self::OperatorSecretReferenceRequiresTrustedNetwork => formatter.write_str(
                "server.operator_bearer_token_environment_variable is valid only under TRUSTED_NETWORK",
            ),
            Self::TrustedNetworkRequiresHttpsOrigin => {
                formatter.write_str("TRUSTED_NETWORK requires an https browser origin")
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SecretKind {
    CursorHmacKey,
    PostgreSqlDsn,
    S3AccessKeyId,
    S3SecretAccessKey,
    S3SessionToken,
    OperatorBearerToken,
}

impl std::fmt::Display for SecretKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::CursorHmacKey => "cursor HMAC key",
            Self::PostgreSqlDsn => "PostgreSQL DSN",
            Self::S3AccessKeyId => "S3 access key ID",
            Self::S3SecretAccessKey => "S3 secret access key",
            Self::S3SessionToken => "S3 session token",
            Self::OperatorBearerToken => "operator bearer token",
        };
        formatter.write_str(name)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SecretInvalidReason {
    ValueNotUnicode,
    Empty,
    CursorKeyTooShort,
    OperatorTokenTooShort,
    OperatorTokenNotVisibleAscii,
}

impl std::fmt::Display for SecretInvalidReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::ValueNotUnicode => "the value is not valid Unicode",
            Self::Empty => "the value cannot be empty",
            Self::CursorKeyTooShort => "the decoded key must contain at least 32 bytes",
            Self::OperatorTokenTooShort => "the token must contain at least 32 bytes",
            Self::OperatorTokenNotVisibleAscii => "the token must contain only visible ASCII bytes",
        };
        formatter.write_str(message)
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigurationError {
    #[error("configuration file {path:?} could not be read")]
    FileUnreadable {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("configuration file {path:?} is not valid UTF-8")]
    FileNotUtf8 { path: PathBuf },

    #[error("configuration document exceeds {maximum_bytes} bytes")]
    DocumentTooLarge { maximum_bytes: usize },

    #[error("configuration document is malformed at byte {byte_offset:?}")]
    DocumentMalformed { byte_offset: Option<usize> },

    #[error("configuration document does not match the runtime configuration schema")]
    DocumentInvalid,

    #[error("environment override {name:?} is invalid: {reason}")]
    EnvironmentOverrideInvalid {
        name: String,
        reason: EnvironmentOverrideInvalidReason,
    },

    #[error("configuration value {field} is invalid: {reason}")]
    ValueInvalid {
        field: ConfigurationField,
        reason: InvalidValueReason,
    },

    #[error("configuration arithmetic overflowed while evaluating {expression}")]
    ArithmeticOverflow { expression: ConfigurationExpression },

    #[error("configuration constraint violated: {violation}")]
    ConstraintViolation { violation: ConfigurationViolation },

    #[error("required {kind} secret is missing from environment variable {environment_variable:?}")]
    SecretMissing {
        kind: SecretKind,
        environment_variable: String,
    },

    #[error(
        "{kind} secret from environment variable {environment_variable:?} is invalid: {reason}"
    )]
    SecretInvalid {
        kind: SecretKind,
        environment_variable: String,
        reason: SecretInvalidReason,
    },
}

impl ConfigurationError {
    #[must_use]
    pub const fn code(&self) -> ConfigurationErrorCode {
        match self {
            Self::FileUnreadable { .. } => ConfigurationErrorCode::FileUnreadable,
            Self::FileNotUtf8 { .. } => ConfigurationErrorCode::FileNotUtf8,
            Self::DocumentTooLarge { .. } => ConfigurationErrorCode::DocumentTooLarge,
            Self::DocumentMalformed { .. } => ConfigurationErrorCode::DocumentMalformed,
            Self::DocumentInvalid => ConfigurationErrorCode::DocumentInvalid,
            Self::EnvironmentOverrideInvalid { .. } => {
                ConfigurationErrorCode::EnvironmentOverrideInvalid
            }
            Self::ValueInvalid { .. } => ConfigurationErrorCode::ValueInvalid,
            Self::ArithmeticOverflow { .. } => ConfigurationErrorCode::ArithmeticOverflow,
            Self::ConstraintViolation { .. } => ConfigurationErrorCode::ConstraintViolation,
            Self::SecretMissing { .. } => ConfigurationErrorCode::SecretMissing,
            Self::SecretInvalid { .. } => ConfigurationErrorCode::SecretInvalid,
        }
    }

    #[must_use]
    pub const fn violation(&self) -> Option<&ConfigurationViolation> {
        match self {
            Self::ConstraintViolation { violation } => Some(violation),
            _ => None,
        }
    }

    #[must_use]
    pub const fn secret_kind(&self) -> Option<SecretKind> {
        match self {
            Self::SecretMissing { kind, .. } | Self::SecretInvalid { kind, .. } => Some(*kind),
            _ => None,
        }
    }
}
