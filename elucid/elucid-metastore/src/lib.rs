//! PostgreSQL boundary for Elucid control-plane state.

mod catalog;
mod error;
mod migration;

pub use catalog::{CatalogApplyOutcome, CatalogSnapshot, CatalogStore};
pub use error::{
    CatalogPersistenceError, CatalogPersistenceErrorKind, MetastoreErrorCode,
    MetastoreMigrationError,
};
pub use migration::{
    MAXIMUM_SUPPORTED_MIGRATION_VERSION, MINIMUM_SUPPORTED_MIGRATION_VERSION, install,
};
