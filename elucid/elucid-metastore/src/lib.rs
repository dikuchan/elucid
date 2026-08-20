//! PostgreSQL boundary for Elucid control-plane state.

mod catalog;
mod error;
mod inspection;
mod migration;
mod publication;

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
