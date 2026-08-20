use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigurationErrorCode {
    FileUnreadable,
    FileNotUtf8,
    DocumentTooLarge,
    DocumentMalformed,
    EnvironmentOverrideInvalid,
    DocumentInvalid,
    ValueInvalid,
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
            Self::EnvironmentOverrideInvalid => "CONFIGURATION_ENVIRONMENT_OVERRIDE_INVALID",
            Self::DocumentInvalid => "CONFIGURATION_DOCUMENT_INVALID",
            Self::ValueInvalid => "CONFIGURATION_VALUE_INVALID",
            Self::ConstraintViolation => "CONFIGURATION_CONSTRAINT_VIOLATION",
            Self::SecretMissing => "CONFIGURATION_SECRET_MISSING",
            Self::SecretInvalid => "CONFIGURATION_SECRET_INVALID",
        }
    }
}

impl Display for ConfigurationErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigurationField(&'static str);

impl ConfigurationField {
    pub(super) const fn new(name: &'static str) -> Self {
        Self(name)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl Display for ConfigurationField {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InvalidValueReason {
    RequiredPositive,
    InvalidSocketAddress,
    InvalidUrl,
    UrlSchemeUnsupported,
    UrlAuthorityMissing,
    UrlMustBeOrigin,
    InvalidObjectStorePrefix,
    RequiredNonEmpty,
    TooLong,
    PathMustBeAbsolute,
}

impl Display for InvalidValueReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::RequiredPositive => "must be positive",
            Self::InvalidSocketAddress => "must be a valid socket address",
            Self::InvalidUrl => "must be a valid absolute URL",
            Self::UrlSchemeUnsupported => "must use http or https",
            Self::UrlAuthorityMissing => "must contain a host",
            Self::UrlMustBeOrigin => {
                "must not contain credentials, a non-root path, a query, or a fragment"
            }
            Self::InvalidObjectStorePrefix => {
                "must be a canonical object-store prefix that leaves room for managed keys"
            }
            Self::RequiredNonEmpty => "must not be empty",
            Self::TooLong => "exceeds the implementation limit",
            Self::PathMustBeAbsolute => "must be an absolute path",
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

impl Display for EnvironmentOverrideInvalidReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidPath => "name does not identify one section and field",
            Self::SectionIsNotTable => "target section is not a TOML table",
            Self::ValueNotUnicode => "value is not valid Unicode",
        };
        formatter.write_str(message)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigurationViolation {
    AutomaticMaintenanceRequiresTwoConnections,
    MaximumHttpBatchExceedsSpoolCapacity,
    MaximumHttpBatchExceedsScratchCapacity,
    MaximumResultExceedsQueryMemory,
    MaximumResultExceedsScratchCapacity,
    SpoolAndScratchPathsMustDiffer,
}

impl Display for ConfigurationViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::AutomaticMaintenanceRequiresTwoConnections => {
                "metastore.maximum_connections must be at least 2 when maintenance.mode is AUTOMATIC"
            }
            Self::MaximumHttpBatchExceedsSpoolCapacity => {
                "ingestion.maximum_http_batch_bytes and its durable frame exceed local_storage.spool_capacity_bytes"
            }
            Self::MaximumHttpBatchExceedsScratchCapacity => {
                "ingestion.maximum_http_batch_bytes exceeds local_storage.scratch_capacity_bytes"
            }
            Self::MaximumResultExceedsQueryMemory => {
                "query.maximum_result_bytes exceeds query.memory_bytes"
            }
            Self::MaximumResultExceedsScratchCapacity => {
                "query.maximum_result_bytes exceeds local_storage.scratch_capacity_bytes"
            }
            Self::SpoolAndScratchPathsMustDiffer => {
                "local_storage.spool_path and local_storage.scratch_path must differ"
            }
        };
        formatter.write_str(message)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SecretKind {
    PostgreSqlUrl,
    ObjectStoreAccessKeyId,
    ObjectStoreSecretAccessKey,
    ObjectStoreSessionToken,
}

impl Display for SecretKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::PostgreSqlUrl => "PostgreSQL URL",
            Self::ObjectStoreAccessKeyId => "object-store access-key ID",
            Self::ObjectStoreSecretAccessKey => "object-store secret access key",
            Self::ObjectStoreSessionToken => "object-store session token",
        };
        formatter.write_str(name)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SecretInvalidReason {
    NotUnicode,
    Empty,
    TooLong,
    ContainsWhitespaceOrControl,
    InvalidPostgreSqlUrl,
}

impl Display for SecretInvalidReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::NotUnicode => "is not valid Unicode",
            Self::Empty => "is empty",
            Self::TooLong => "exceeds the implementation limit",
            Self::ContainsWhitespaceOrControl => "contains whitespace or control characters",
            Self::InvalidPostgreSqlUrl => "is not a valid PostgreSQL URL",
        };
        formatter.write_str(message)
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigurationError {
    #[error("configuration file {path:?} is unreadable")]
    FileUnreadable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("configuration file {path:?} is not UTF-8")]
    FileNotUtf8 { path: PathBuf },
    #[error("configuration document exceeds {maximum_bytes} bytes")]
    DocumentTooLarge { maximum_bytes: usize },
    #[error("configuration document is malformed")]
    DocumentMalformed { byte_offset: Option<usize> },
    #[error("configuration environment override {name:?} is invalid: {reason}")]
    EnvironmentOverrideInvalid {
        name: String,
        reason: EnvironmentOverrideInvalidReason,
    },
    #[error("configuration document contains missing, unknown, or invalid fields")]
    DocumentInvalid,
    #[error("configuration value {field} is invalid: {reason}")]
    ValueInvalid {
        field: ConfigurationField,
        reason: InvalidValueReason,
    },
    #[error("configuration constraint is violated: {violation}")]
    ConstraintViolation { violation: ConfigurationViolation },
    #[error("required {kind} secret is missing")]
    SecretMissing { kind: SecretKind },
    #[error("{kind} secret is invalid: {reason}")]
    SecretInvalid {
        kind: SecretKind,
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
            Self::EnvironmentOverrideInvalid { .. } => {
                ConfigurationErrorCode::EnvironmentOverrideInvalid
            }
            Self::DocumentInvalid => ConfigurationErrorCode::DocumentInvalid,
            Self::ValueInvalid { .. } => ConfigurationErrorCode::ValueInvalid,
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
            Self::SecretMissing { kind } | Self::SecretInvalid { kind, .. } => Some(*kind),
            _ => None,
        }
    }
}
