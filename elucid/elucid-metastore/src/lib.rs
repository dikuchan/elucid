//! PostgreSQL boundary for Elucid control-plane state.

use std::fmt::{Display, Formatter};

use sqlx::PgPool;
use sqlx::migrate::{MigrateError, Migrator};
use thiserror::Error;

static MIGRATOR: Migrator = sqlx::migrate!();

pub const MINIMUM_SUPPORTED_MIGRATION_VERSION: u64 = 1;
pub const MAXIMUM_SUPPORTED_MIGRATION_VERSION: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MetastoreErrorCode {
    MigrationFailed,
}

impl MetastoreErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MigrationFailed => "METASTORE_MIGRATION_FAILED",
        }
    }
}

impl Display for MetastoreErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Error)]
#[error("metastore migration failed")]
pub struct MetastoreMigrationError {
    #[source]
    source: MigrateError,
}

impl MetastoreMigrationError {
    #[must_use]
    pub const fn code(&self) -> MetastoreErrorCode {
        MetastoreErrorCode::MigrationFailed
    }
}

/// Applies the migrations embedded in this crate and validates previously applied migrations.
///
/// # Errors
///
/// Returns [`MetastoreMigrationError`] when PostgreSQL is unavailable, an embedded migration
/// cannot be applied, or SQLx detects an incompatible migration history.
pub async fn install(pool: &PgPool) -> Result<(), MetastoreMigrationError> {
    MIGRATOR
        .run(pool)
        .await
        .map_err(|source| MetastoreMigrationError { source })
}
