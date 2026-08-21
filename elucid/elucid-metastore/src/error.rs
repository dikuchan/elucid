use std::error::Error;
use std::fmt::{Display, Formatter};

use elucid_catalog::{CatalogApplicationError, CatalogErrorCode, CatalogModelError};
use sqlx::migrate::MigrateError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MetastoreErrorCode {
    Unavailable,
    MigrationFailed,
    Conflict,
    Corrupt,
}

impl MetastoreErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "METASTORE_UNAVAILABLE",
            Self::MigrationFailed => "METASTORE_MIGRATION_FAILED",
            Self::Conflict => "METASTORE_CONFLICT",
            Self::Corrupt => "METASTORE_CORRUPT",
        }
    }
}

impl Display for MetastoreErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("metastore migration failed")]
pub struct MetastoreMigrationError {
    #[source]
    pub(crate) source: MigrateError,
}

impl MetastoreMigrationError {
    #[must_use]
    pub const fn code(&self) -> MetastoreErrorCode {
        MetastoreErrorCode::MigrationFailed
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PublicationErrorKind {
    Conflict,
    Unavailable,
    Corrupt,
}

impl Display for PublicationErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Conflict => "publication state conflict",
            Self::Unavailable => "publication state unavailable",
            Self::Corrupt => "publication state corrupt",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum PublicationModelError {
    #[error("segment event-time bounds are not ordered")]
    EventTimeBoundsNotOrdered,

    #[error("segment ingestion-time bounds are not ordered")]
    IngestionTimeBoundsNotOrdered,

    #[error("segment event-time bounds do not belong to its UTC event day")]
    EventDayMismatch,

    #[error("segment timestamps exceed PostgreSQL microsecond precision")]
    TimestampPrecisionUnsupported,

    #[error("segment row count exceeds the PostgreSQL BIGINT range")]
    RowCountOutOfRange,

    #[error("segment uncompressed byte count exceeds the PostgreSQL BIGINT range")]
    UncompressedByteCountOutOfRange,

    #[error("object byte size exceeds the PostgreSQL BIGINT range")]
    ObjectByteSizeOutOfRange,

    #[error("object format version exceeds the PostgreSQL BIGINT range")]
    ObjectFormatVersionOutOfRange,

    #[error("Parquet object owner does not match the segment identity")]
    SegmentObjectOwnerMismatch,

    #[error("dead-letter object owner does not match the batch identity")]
    DeadLetterObjectOwnerMismatch,

    #[error("retention period must be positive")]
    RetentionPeriodMustBePositive,

    #[error("retention period exceeds the PostgreSQL BIGINT range")]
    RetentionPeriodOutOfRange,

    #[error("orphan grace period must be positive")]
    OrphanGracePeriodMustBePositive,

    #[error("orphan grace period exceeds the PostgreSQL BIGINT range")]
    OrphanGracePeriodOutOfRange,

    #[error("reconciliation limit must be between 1 and {maximum} items")]
    ReconciliationLimitOutOfRange { maximum: u64 },
}

#[derive(Debug)]
pub struct PublicationError {
    kind: PublicationErrorKind,
    source: PublicationErrorSource,
}

impl PublicationError {
    #[must_use]
    pub const fn kind(&self) -> PublicationErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn code(&self) -> MetastoreErrorCode {
        match self.kind {
            PublicationErrorKind::Conflict => MetastoreErrorCode::Conflict,
            PublicationErrorKind::Unavailable => MetastoreErrorCode::Unavailable,
            PublicationErrorKind::Corrupt => MetastoreErrorCode::Corrupt,
        }
    }

    #[must_use]
    pub const fn model_error(&self) -> Option<PublicationModelError> {
        match &self.source {
            PublicationErrorSource::Model(source) => Some(*source),
            PublicationErrorSource::Database(_) | PublicationErrorSource::Invariant(_) => None,
        }
    }

    pub(crate) fn from_model(source: PublicationModelError) -> Self {
        Self {
            kind: PublicationErrorKind::Conflict,
            source: PublicationErrorSource::Model(source),
        }
    }

    pub(crate) fn unavailable(source: sqlx::Error) -> Self {
        Self {
            kind: PublicationErrorKind::Unavailable,
            source: PublicationErrorSource::Database(source),
        }
    }

    pub(crate) fn read(source: sqlx::Error) -> Self {
        let kind = if is_row_decode_error(&source) {
            PublicationErrorKind::Corrupt
        } else {
            PublicationErrorKind::Unavailable
        };
        Self {
            kind,
            source: PublicationErrorSource::Database(source),
        }
    }

    pub(crate) fn write(source: sqlx::Error) -> Self {
        let kind = if is_database_conflict(&source) {
            PublicationErrorKind::Conflict
        } else {
            PublicationErrorKind::Unavailable
        };
        Self {
            kind,
            source: PublicationErrorSource::Database(source),
        }
    }

    pub(crate) fn conflict(message: &'static str) -> Self {
        Self {
            kind: PublicationErrorKind::Conflict,
            source: PublicationErrorSource::Invariant(message),
        }
    }

    pub(crate) fn corrupt(message: &'static str) -> Self {
        Self {
            kind: PublicationErrorKind::Corrupt,
            source: PublicationErrorSource::Invariant(message),
        }
    }
}

impl Display for PublicationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for PublicationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug, thiserror::Error)]
enum PublicationErrorSource {
    #[error("publication metadata is invalid")]
    Model(#[source] PublicationModelError),
    #[error("PostgreSQL operation failed")]
    Database(#[source] sqlx::Error),
    #[error("{0}")]
    Invariant(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CatalogPersistenceErrorKind {
    Conflict,
    Unavailable,
    Corrupt,
}

impl Display for CatalogPersistenceErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::Conflict => "catalog persistence conflict",
            Self::Unavailable => "catalog persistence unavailable",
            Self::Corrupt => "catalog persistence corrupt",
        };
        formatter.write_str(message)
    }
}

#[derive(Debug)]
pub struct CatalogPersistenceError {
    kind: CatalogPersistenceErrorKind,
    source: CatalogPersistenceErrorSource,
}

impl CatalogPersistenceError {
    #[must_use]
    pub const fn kind(&self) -> CatalogPersistenceErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn code(&self) -> MetastoreErrorCode {
        match self.kind {
            CatalogPersistenceErrorKind::Conflict => MetastoreErrorCode::Conflict,
            CatalogPersistenceErrorKind::Unavailable => MetastoreErrorCode::Unavailable,
            CatalogPersistenceErrorKind::Corrupt => MetastoreErrorCode::Corrupt,
        }
    }

    #[must_use]
    pub const fn catalog_error(&self) -> Option<&CatalogApplicationError> {
        match &self.source {
            CatalogPersistenceErrorSource::Catalog(source) => Some(source),
            CatalogPersistenceErrorSource::Database(_)
            | CatalogPersistenceErrorSource::Model(_)
            | CatalogPersistenceErrorSource::Invariant(_) => None,
        }
    }

    pub(crate) fn from_catalog(source: CatalogApplicationError) -> Self {
        let kind = if source.code() == CatalogErrorCode::Corrupt {
            CatalogPersistenceErrorKind::Corrupt
        } else {
            CatalogPersistenceErrorKind::Conflict
        };
        Self {
            kind,
            source: CatalogPersistenceErrorSource::Catalog(source),
        }
    }

    pub(crate) fn unavailable(source: sqlx::Error) -> Self {
        Self {
            kind: CatalogPersistenceErrorKind::Unavailable,
            source: CatalogPersistenceErrorSource::Database(source),
        }
    }

    pub(crate) fn read(source: sqlx::Error) -> Self {
        let kind = if is_row_decode_error(&source) {
            CatalogPersistenceErrorKind::Corrupt
        } else {
            CatalogPersistenceErrorKind::Unavailable
        };
        Self {
            kind,
            source: CatalogPersistenceErrorSource::Database(source),
        }
    }

    pub(crate) fn write(source: sqlx::Error) -> Self {
        let kind = if is_database_conflict(&source) {
            CatalogPersistenceErrorKind::Conflict
        } else {
            CatalogPersistenceErrorKind::Unavailable
        };
        Self {
            kind,
            source: CatalogPersistenceErrorSource::Database(source),
        }
    }

    pub(crate) fn corrupt_model(source: CatalogModelError) -> Self {
        Self {
            kind: CatalogPersistenceErrorKind::Corrupt,
            source: CatalogPersistenceErrorSource::Model(source),
        }
    }

    pub(crate) fn conflict(message: &'static str) -> Self {
        Self {
            kind: CatalogPersistenceErrorKind::Conflict,
            source: CatalogPersistenceErrorSource::Invariant(message),
        }
    }

    pub(crate) fn corrupt(message: &'static str) -> Self {
        Self {
            kind: CatalogPersistenceErrorKind::Corrupt,
            source: CatalogPersistenceErrorSource::Invariant(message),
        }
    }
}

impl Display for CatalogPersistenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for CatalogPersistenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug, thiserror::Error)]
enum CatalogPersistenceErrorSource {
    #[error("catalog definition was rejected")]
    Catalog(#[source] CatalogApplicationError),
    #[error("PostgreSQL operation failed")]
    Database(#[source] sqlx::Error),
    #[error("stored catalog value is invalid")]
    Model(#[source] CatalogModelError),
    #[error("{0}")]
    Invariant(&'static str),
}

pub(crate) fn is_row_decode_error(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::RowNotFound
            | sqlx::Error::TypeNotFound { .. }
            | sqlx::Error::ColumnIndexOutOfBounds { .. }
            | sqlx::Error::ColumnNotFound(_)
            | sqlx::Error::ColumnDecode { .. }
            | sqlx::Error::Decode(_)
    )
}

fn is_database_conflict(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| {
            code.starts_with("23")
                || code == "22003"
                || code == "22008"
                || code == "22015"
                || code == "40001"
                || code == "40P01"
        })
}
