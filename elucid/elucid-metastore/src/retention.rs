use std::error::Error;
use std::fmt::{Display, Formatter};

use sqlx::postgres::PgPool;
use uuid::Uuid;

use crate::error::{is_database_conflict, is_row_decode_error};

pub const MAXIMUM_RETENTION_SCAN_ITEMS: u64 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RetentionModelError {
    #[error("maximum query lifetime must be positive")]
    MaximumQueryLifetimeMustBePositive,

    #[error("reclamation safety margin must be positive")]
    ReclamationSafetyMarginMustBePositive,

    #[error("reclamation grace exceeds the PostgreSQL BIGINT range")]
    ReclamationGraceOutOfRange,

    #[error("retention scan limit must be between 1 and {maximum} items")]
    ScanLimitOutOfRange { maximum: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReclamationGracePeriod(i64);

impl ReclamationGracePeriod {
    pub fn new(
        maximum_query_lifetime_seconds: u64,
        safety_margin_seconds: u64,
    ) -> Result<Self, RetentionModelError> {
        if maximum_query_lifetime_seconds == 0 {
            return Err(RetentionModelError::MaximumQueryLifetimeMustBePositive);
        }
        if safety_margin_seconds == 0 {
            return Err(RetentionModelError::ReclamationSafetyMarginMustBePositive);
        }
        maximum_query_lifetime_seconds
            .checked_add(safety_margin_seconds)
            .and_then(|seconds| i64::try_from(seconds).ok())
            .map(Self)
            .ok_or(RetentionModelError::ReclamationGraceOutOfRange)
    }

    const fn seconds(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RetentionScanLimit(i64);

impl RetentionScanLimit {
    pub fn new(items: u64) -> Result<Self, RetentionModelError> {
        if items == 0 || items > MAXIMUM_RETENTION_SCAN_ITEMS {
            return Err(RetentionModelError::ScanLimitOutOfRange {
                maximum: MAXIMUM_RETENTION_SCAN_ITEMS,
            });
        }
        i64::try_from(items)
            .map(Self)
            .map_err(|_| RetentionModelError::ScanLimitOutOfRange {
                maximum: MAXIMUM_RETENTION_SCAN_ITEMS,
            })
    }

    const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RetentionErrorKind {
    TimestampOverflow,
    StateConflict,
    Unavailable,
    Corrupt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RetentionErrorCode {
    TimestampOverflow,
    StateConflict,
    CleanupFailed,
}

impl RetentionErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TimestampOverflow => "RETENTION_TIMESTAMP_OVERFLOW",
            Self::StateConflict => "RETENTION_STATE_CONFLICT",
            Self::CleanupFailed => "RETENTION_CLEANUP_FAILED",
        }
    }
}

impl Display for RetentionErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Display for RetentionErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::TimestampOverflow => "retention timestamp overflow",
            Self::StateConflict => "retention state conflict",
            Self::Unavailable => "retention metadata unavailable",
            Self::Corrupt => "retention metadata corrupt",
        })
    }
}

#[derive(Debug)]
pub struct RetentionError {
    kind: RetentionErrorKind,
    source: RetentionErrorSource,
}

impl RetentionError {
    #[must_use]
    pub const fn kind(&self) -> RetentionErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn code(&self) -> RetentionErrorCode {
        match self.kind {
            RetentionErrorKind::TimestampOverflow => RetentionErrorCode::TimestampOverflow,
            RetentionErrorKind::StateConflict => RetentionErrorCode::StateConflict,
            RetentionErrorKind::Unavailable | RetentionErrorKind::Corrupt => {
                RetentionErrorCode::CleanupFailed
            }
        }
    }

    fn unavailable(source: sqlx::Error) -> Self {
        Self {
            kind: RetentionErrorKind::Unavailable,
            source: RetentionErrorSource::Database(source),
        }
    }

    fn read(source: sqlx::Error) -> Self {
        let kind = if is_row_decode_error(&source) {
            RetentionErrorKind::Corrupt
        } else if is_database_conflict(&source) {
            RetentionErrorKind::StateConflict
        } else {
            RetentionErrorKind::Unavailable
        };
        Self {
            kind,
            source: RetentionErrorSource::Database(source),
        }
    }

    fn write(source: sqlx::Error) -> Self {
        let kind = if is_timestamp_overflow(&source) {
            RetentionErrorKind::TimestampOverflow
        } else if is_database_conflict(&source) {
            RetentionErrorKind::StateConflict
        } else {
            RetentionErrorKind::Unavailable
        };
        Self {
            kind,
            source: RetentionErrorSource::Database(source),
        }
    }

    fn state_conflict(message: &'static str) -> Self {
        Self {
            kind: RetentionErrorKind::StateConflict,
            source: RetentionErrorSource::Invariant(message),
        }
    }
}

impl Display for RetentionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for RetentionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug, thiserror::Error)]
enum RetentionErrorSource {
    #[error("PostgreSQL operation failed")]
    Database(#[source] sqlx::Error),
    #[error("{0}")]
    Invariant(&'static str),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct SegmentExpiration {
    expired_segments: u64,
    expired_rows: u128,
}

impl SegmentExpiration {
    #[must_use]
    pub const fn expired_segments(self) -> u64 {
        self.expired_segments
    }

    #[must_use]
    pub const fn expired_rows(self) -> u128 {
        self.expired_rows
    }
}

#[derive(Clone, Debug)]
pub struct RetentionStore {
    pool: PgPool,
}

impl RetentionStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn expire_segments(
        &self,
        grace: ReclamationGracePeriod,
        limit: RetentionScanLimit,
    ) -> Result<SegmentExpiration, RetentionError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(RetentionError::unavailable)?;
        let segment_ids = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT segment_id
            FROM segments
            WHERE state = 'ACTIVE'
              AND claimed_by_compaction_run_id IS NULL
              AND data_expires_at <= CURRENT_TIMESTAMP
            ORDER BY data_expires_at, segment_id
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(limit.get())
        .fetch_all(&mut *transaction)
        .await
        .map_err(RetentionError::read)?;
        if segment_ids.is_empty() {
            transaction.commit().await.map_err(RetentionError::write)?;
            return Ok(SegmentExpiration::default());
        }

        let row_counts = sqlx::query_scalar::<_, i64>(
            r#"
            UPDATE segments
            SET
                state = 'EXPIRED',
                retired_at = CURRENT_TIMESTAMP,
                reclaim_after = CURRENT_TIMESTAMP + make_interval(secs => $2::double precision),
                updated_at = CURRENT_TIMESTAMP
            WHERE segment_id = ANY($1::uuid[])
              AND state = 'ACTIVE'
              AND claimed_by_compaction_run_id IS NULL
            RETURNING row_count
            "#,
        )
        .bind(&segment_ids)
        .bind(grace.seconds())
        .fetch_all(&mut *transaction)
        .await
        .map_err(RetentionError::write)?;
        if row_counts.len() != segment_ids.len() {
            return Err(RetentionError::state_conflict(
                "locked retention candidates did not expire exactly once",
            ));
        }

        let expired_rows = row_counts.into_iter().try_fold(0_u128, |total, rows| {
            let rows = u64::try_from(rows)
                .ok()
                .filter(|rows| *rows > 0)
                .ok_or_else(|| {
                    RetentionError::state_conflict("expired segment has an invalid row count")
                })?;
            Ok::<_, RetentionError>(total + u128::from(rows))
        })?;
        let expired_segments = u64::try_from(segment_ids.len()).map_err(|_| {
            RetentionError::state_conflict("expired segment count exceeds the Rust u64 range")
        })?;
        transaction.commit().await.map_err(RetentionError::write)?;
        Ok(SegmentExpiration {
            expired_segments,
            expired_rows,
        })
    }
}

fn is_timestamp_overflow(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| code == "22008" || code == "22015")
}
