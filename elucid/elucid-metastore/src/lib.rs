//! PostgreSQL boundary for Elucid control-plane state.

mod catalog;
mod error;
mod migration;
mod publication;

pub use catalog::{CatalogApplyOutcome, CatalogSnapshot, CatalogStore};
pub use error::{
    CatalogPersistenceError, CatalogPersistenceErrorKind, MetastoreErrorCode,
    MetastoreMigrationError, PublicationError, PublicationErrorKind, PublicationModelError,
};
pub use migration::{
    MAXIMUM_SUPPORTED_MIGRATION_VERSION, MINIMUM_SUPPORTED_MIGRATION_VERSION, install,
};
pub use publication::{
    DeadLetterRegistration, IngestionSegmentRegistration, IngestionSegmentTimes,
    ObjectUploadRecordOutcome, PublicationOutcome, PublicationStore, RegistrationOutcome,
    RetentionPeriod, StoredObjectState,
};
