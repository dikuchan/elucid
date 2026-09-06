//! PostgreSQL boundary for Elucid control-plane state.

mod catalog;
mod compaction;
mod compaction_lifecycle;
mod error;
mod inspection;
mod migration;
mod publication;
mod query;
mod query_execution;
mod reclamation;
mod retention;

pub use catalog::{CatalogApplyOutcome, CatalogSnapshot, CatalogStore};
pub use compaction::{
    CompactionClaimLimitConfiguration, CompactionClaimLimits, CompactionInputSegment,
    CompactionMetadataError, CompactionMetadataErrorKind, CompactionModelError,
    CompactionOutputRegistration, CompactionOutputRegistrationOutcome, CompactionRunClaim,
    CompactionRunId, CompactionStore, MAXIMUM_COMPACTION_CANDIDATE_SEGMENTS,
    MAXIMUM_COMPACTION_INPUT_SEGMENTS, MAXIMUM_COMPACTION_OUTPUT_SEGMENTS, MaintenanceOwner,
    MaintenanceOwnership,
};
pub use compaction_lifecycle::{
    CompactionFailureOutcome, CompactionFailureReason, CompactionPublicationOutcome,
    CompactionRecovery, CompactionRecoveryLimit, MAXIMUM_COMPACTION_RECOVERY_RUNS,
};
pub use error::{
    CatalogPersistenceError, CatalogPersistenceErrorKind, MetastoreMigrationError,
    PublicationError, PublicationErrorKind, PublicationModelError,
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
    AbandonmentOutcome, ObjectPublicationState, ObjectUploadRecordOutcome, OrphanGracePeriod,
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
pub use reclamation::{
    MAXIMUM_OBJECT_RECLAMATION_ITEMS, ObjectDeletionAttempt, ObjectDeletionClaim,
    ObjectDeletionCompletion, ObjectDeletionFailure, ObjectDeletionFailureRecording,
    ObjectDeletionRetryDelay, ObjectReclamationError, ObjectReclamationErrorKind,
    ObjectReclamationLimit, ObjectReclamationModelError, ObjectReclamationStore,
};
pub use retention::{
    MAXIMUM_METADATA_CLEANUP_ROOTS, MAXIMUM_RETENTION_SCAN_ITEMS, MetadataCleanup,
    MetadataCleanupLimit, ReclamationGracePeriod, RetentionError, RetentionErrorKind,
    RetentionModelError, RetentionScanLimit, RetentionStore, SegmentExpiration,
};
