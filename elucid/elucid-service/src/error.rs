use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::path::PathBuf;

use elucid_engine::{EngineError, QueryExecutionLimitsError};
use elucid_ingestion::{SpoolError, SpoolErrorCode};
use elucid_metastore::{
    CatalogPersistenceError, CompactionMetadataError, CompactionModelError,
    MetastoreMigrationError, ObjectReclamationError, ObjectReclamationModelError,
    PublicationModelError, QuerySnapshotModelError, RetentionError, RetentionModelError,
};

use elucid_compaction::CompactionBuildModelError;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MaintenanceError {
    #[error("compaction selection limits are invalid")]
    CompactionModel(#[from] CompactionModelError),

    #[error("compaction construction limits are invalid")]
    CompactionBuildModel(#[from] CompactionBuildModelError),

    #[error("compaction recovery grace is invalid")]
    PublicationModel(#[from] PublicationModelError),

    #[error("retention limits are invalid")]
    RetentionModel(#[from] RetentionModelError),

    #[error("object reclamation limits are invalid")]
    ObjectReclamationModel(#[from] ObjectReclamationModelError),

    #[error("compaction metadata operation failed")]
    CompactionMetadata(#[from] CompactionMetadataError),

    #[error("retention operation failed")]
    Retention(#[from] RetentionError),

    #[error("object reclamation operation failed")]
    ObjectReclamation(#[from] ObjectReclamationError),

    #[error("unfinished compaction recovery exceeded its startup bound")]
    RecoveryBoundExceeded,

    #[error("metastore returned an unsupported maintenance ownership state")]
    OwnershipStateUnsupported,

    #[error("maintenance cycle exceeded its execution bound")]
    CycleTimedOut,

    #[error("maintenance loop stopped unexpectedly")]
    StoppedUnexpectedly,
}

impl MaintenanceError {
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::CompactionMetadata(source) => {
                source.kind() == elucid_metastore::CompactionMetadataErrorKind::Unavailable
            }
            Self::Retention(source) => {
                source.kind() == elucid_metastore::RetentionErrorKind::Unavailable
            }
            Self::ObjectReclamation(source) => {
                source.kind() == elucid_metastore::ObjectReclamationErrorKind::Unavailable
            }
            Self::CompactionModel(_)
            | Self::CompactionBuildModel(_)
            | Self::PublicationModel(_)
            | Self::RetentionModel(_)
            | Self::ObjectReclamationModel(_)
            | Self::RecoveryBoundExceeded
            | Self::OwnershipStateUnsupported
            | Self::StoppedUnexpectedly => false,
            Self::CycleTimedOut => true,
        }
    }

    pub(crate) fn loses_maintenance_owner(&self) -> bool {
        matches!(self, Self::CycleTimedOut)
            || matches!(
                self,
                Self::CompactionMetadata(source)
                    if source.kind() == elucid_metastore::CompactionMetadataErrorKind::Unavailable
            )
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum QueryInitializationError {
    #[error("maximum concurrent queries {maximum} exceeds the runtime limit")]
    ConcurrencyUnsupported { maximum: u64 },

    #[error("query execution limits are invalid")]
    ExecutionLimits(#[from] QueryExecutionLimitsError),

    #[error("query snapshot limits are invalid")]
    SnapshotLimits(#[from] QuerySnapshotModelError),

    #[error("query engine initialization failed")]
    Engine(#[from] EngineError),
}

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
    QueryInitializationFailed,
    MaintenanceInitializationFailed,
    MaintenanceRuntimeFailed,
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
            Self::QueryInitializationFailed => "QUERY_INITIALIZATION_FAILED",
            Self::MaintenanceInitializationFailed => "MAINTENANCE_INITIALIZATION_FAILED",
            Self::MaintenanceRuntimeFailed => "MAINTENANCE_RUNTIME_FAILED",
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

    #[error("query initialization failed")]
    QueryInitialization {
        #[source]
        source: QueryInitializationError,
    },

    #[error("maintenance initialization failed")]
    MaintenanceInitialization {
        #[source]
        source: MaintenanceError,
    },

    #[error("maintenance runtime failed")]
    MaintenanceRuntime {
        #[source]
        source: MaintenanceError,
    },

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
            Self::QueryInitialization { .. } => ServiceErrorCode::QueryInitializationFailed,
            Self::MaintenanceInitialization { .. } => {
                ServiceErrorCode::MaintenanceInitializationFailed
            }
            Self::MaintenanceRuntime { .. } => ServiceErrorCode::MaintenanceRuntimeFailed,
            Self::HttpRuntime { .. } | Self::Supervisor { .. } => ServiceErrorCode::RuntimeFailed,
            Self::ShutdownTimedOut => ServiceErrorCode::ShutdownTimedOut,
            Self::Signal { .. } => ServiceErrorCode::SignalFailed,
        }
    }
}
