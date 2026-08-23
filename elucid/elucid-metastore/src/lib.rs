//! PostgreSQL boundary for Elucid control-plane state.

mod catalog;
mod error;
mod inspection;
mod migration;
mod publication;
mod query;
mod query_execution;
mod retention;

pub use catalog::{CatalogApplyOutcome, CatalogSnapshot, CatalogStore};
pub use error::{
    CatalogPersistenceError, CatalogPersistenceErrorKind, MetastoreErrorCode,
    MetastoreMigrationError, PublicationError, PublicationErrorKind, PublicationModelError,
};
pub use inspection::{
    BoundedOperationalList, DeadLetterObject, DeadLetterSummary, OperationalBacklog,
    OperationalLimit, OperationalModelError, OperationalSegmentOrigin, OperationalSegmentState,
    OperationalStore, SegmentInspection,
};
pub use migration::{
    MAXIMUM_SUPPORTED_MIGRATION_VERSION, MINIMUM_SUPPORTED_MIGRATION_VERSION, install,
};
pub use publication::{
    AbandonmentOutcome, DeadLetterRegistration, IngestionSegmentRegistration,
    IngestionSegmentTimes, ObjectPublicationState, ObjectUploadRecordOutcome, OrphanGracePeriod,
    PublicationOutcome, PublicationStore, ReconciliationLimit, RegistrationOutcome,
    RetentionPeriod, StoredObjectState, UnreferencedOutputReconciliation,
};
pub use query::{
    MAXIMUM_QUERY_SNAPSHOT_SEGMENTS, QueryRequestTimeRange, QuerySegment, QuerySnapshot,
    QuerySnapshotError, QuerySnapshotErrorKind, QuerySnapshotLimitExceeded, QuerySnapshotLimits,
    QuerySnapshotModelError, QuerySnapshotStore,
};
pub use query_execution::{
    BoundedQueryExecutions, MAXIMUM_RETAINED_QUERY_EXECUTIONS, NewQueryExecution, QueryExecutionId,
    QueryExecutionListLimit, QueryExecutionModelError, QueryExecutionPersistenceError,
    QueryExecutionPersistenceErrorKind, QueryExecutionRecord, QueryExecutionStore,
};
pub use retention::{
    MAXIMUM_RETENTION_SCAN_ITEMS, ReclamationGracePeriod, RetentionError, RetentionErrorCode,
    RetentionErrorKind, RetentionModelError, RetentionScanLimit, RetentionStore, SegmentExpiration,
};
