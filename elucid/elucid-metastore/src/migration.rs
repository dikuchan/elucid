use sqlx::PgPool;
use sqlx::migrate::Migrator;

use crate::MetastoreMigrationError;

static MIGRATOR: Migrator = sqlx::migrate!();

pub const MINIMUM_SUPPORTED_MIGRATION_VERSION: u64 = 1;
pub const MAXIMUM_SUPPORTED_MIGRATION_VERSION: u64 = 4;

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
