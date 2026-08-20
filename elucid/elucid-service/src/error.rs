use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::path::PathBuf;

use elucid_ingestion::{SpoolError, SpoolErrorCode};
use elucid_metastore::{CatalogPersistenceError, MetastoreMigrationError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServiceErrorCode {
    BindFailed,
    MetastoreUnavailable,
    MetastoreMigrationFailed,
    MetastoreCorrupt,
    ObjectStoreUnavailable,
    LocalStorageUnavailable,
    SpoolUnavailable,
    SpoolCorrupt,
    IngestionInitializationFailed,
    IngestionRuntimeFailed,
    RuntimeFailed,
    ShutdownTimedOut,
    SignalFailed,
}

impl ServiceErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BindFailed => "SERVER_BIND_FAILED",
            Self::MetastoreUnavailable => "METASTORE_UNAVAILABLE",
            Self::MetastoreMigrationFailed => "METASTORE_MIGRATION_FAILED",
            Self::MetastoreCorrupt => "METASTORE_CORRUPT",
            Self::ObjectStoreUnavailable => "OBJECT_STORE_UNAVAILABLE",
            Self::LocalStorageUnavailable => "LOCAL_STORAGE_UNAVAILABLE",
            Self::SpoolUnavailable => "SPOOL_UNAVAILABLE",
            Self::SpoolCorrupt => "SPOOL_CORRUPT",
            Self::IngestionInitializationFailed => "INGESTION_INITIALIZATION_FAILED",
            Self::IngestionRuntimeFailed => "INGESTION_RUNTIME_FAILED",
            Self::RuntimeFailed => "SERVER_RUNTIME_FAILED",
            Self::ShutdownTimedOut => "SERVER_SHUTDOWN_TIMED_OUT",
            Self::SignalFailed => "SERVER_SIGNAL_FAILED",
        }
    }
}

impl Display for ServiceErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServiceError {
    #[error("server could not bind {address}")]
    Bind {
        address: SocketAddr,
        #[source]
        source: std::io::Error,
    },

    #[error("metastore connection failed")]
    MetastoreConnection {
        #[source]
        source: sqlx::Error,
    },

    #[error("metastore migration failed")]
    MetastoreMigration {
        #[source]
        source: MetastoreMigrationError,
    },

    #[error("catalog initialization failed")]
    CatalogInitialization {
        #[source]
        source: CatalogPersistenceError,
    },

    #[error("object-store initialization failed")]
    ObjectStoreInitialization {
        #[source]
        source: object_store::Error,
    },

    #[error("local storage at {path:?} is unavailable")]
    LocalStorage {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("spool initialization failed")]
    SpoolInitialization {
        #[source]
        source: SpoolError,
    },

    #[error("ingestion initialization failed: {reason}")]
    IngestionInitialization { reason: &'static str },

    #[error("ingestion runtime failed: {reason}")]
    IngestionRuntime { reason: &'static str },

    #[error("HTTP runtime failed")]
    HttpRuntime {
        #[source]
        source: std::io::Error,
    },

    #[error("server supervisor failed")]
    Supervisor {
        #[source]
        source: tokio::task::JoinError,
    },

    #[error("server did not stop within its configured deadline")]
    ShutdownTimedOut,

    #[error("server could not listen for the shutdown signal")]
    Signal {
        #[source]
        source: std::io::Error,
    },
}

impl ServiceError {
    #[must_use]
    pub const fn code(&self) -> ServiceErrorCode {
        match self {
            Self::Bind { .. } => ServiceErrorCode::BindFailed,
            Self::MetastoreConnection { .. } => ServiceErrorCode::MetastoreUnavailable,
            Self::MetastoreMigration { .. } => ServiceErrorCode::MetastoreMigrationFailed,
            Self::CatalogInitialization { source } => match source.kind() {
                elucid_metastore::CatalogPersistenceErrorKind::Unavailable => {
                    ServiceErrorCode::MetastoreUnavailable
                }
                elucid_metastore::CatalogPersistenceErrorKind::Conflict
                | elucid_metastore::CatalogPersistenceErrorKind::Corrupt => {
                    ServiceErrorCode::MetastoreCorrupt
                }
                _ => ServiceErrorCode::MetastoreCorrupt,
            },
            Self::ObjectStoreInitialization { .. } => ServiceErrorCode::ObjectStoreUnavailable,
            Self::LocalStorage { .. } => ServiceErrorCode::LocalStorageUnavailable,
            Self::SpoolInitialization { source } => match source.code() {
                SpoolErrorCode::Corrupt => ServiceErrorCode::SpoolCorrupt,
                SpoolErrorCode::CapacityExhausted
                | SpoolErrorCode::BatchLimitExceeded
                | SpoolErrorCode::Unavailable => ServiceErrorCode::SpoolUnavailable,
                _ => ServiceErrorCode::SpoolUnavailable,
            },
            Self::IngestionInitialization { .. } => ServiceErrorCode::IngestionInitializationFailed,
            Self::IngestionRuntime { .. } => ServiceErrorCode::IngestionRuntimeFailed,
            Self::HttpRuntime { .. } | Self::Supervisor { .. } => ServiceErrorCode::RuntimeFailed,
            Self::ShutdownTimedOut => ServiceErrorCode::ShutdownTimedOut,
            Self::Signal { .. } => ServiceErrorCode::SignalFailed,
        }
    }
}
