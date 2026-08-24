use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

use sqlx::postgres::PgPool;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::error::{is_database_conflict, is_row_decode_error};

pub const MAXIMUM_RETENTION_SCAN_ITEMS: u64 = 1_000;
pub const MAXIMUM_METADATA_CLEANUP_ROOTS: u64 = 1_000;

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

    #[error("metadata cleanup limit must be between 1 and {maximum} roots")]
    CleanupLimitOutOfRange { maximum: u64 },
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
pub struct MetadataCleanupLimit(i64);

impl MetadataCleanupLimit {
    pub fn new(roots: u64) -> Result<Self, RetentionModelError> {
        if roots == 0 || roots > MAXIMUM_METADATA_CLEANUP_ROOTS {
            return Err(RetentionModelError::CleanupLimitOutOfRange {
                maximum: MAXIMUM_METADATA_CLEANUP_ROOTS,
            });
        }
        i64::try_from(roots)
            .map(Self)
            .map_err(|_| RetentionModelError::CleanupLimitOutOfRange {
                maximum: MAXIMUM_METADATA_CLEANUP_ROOTS,
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

    fn corrupt(message: &'static str) -> Self {
        Self {
            kind: RetentionErrorKind::Corrupt,
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct MetadataCleanup {
    removed_objects: u64,
    removed_segments: u64,
    removed_compaction_runs: u64,
    removed_roots: u64,
    removed_rows: u64,
}

impl MetadataCleanup {
    fn new(
        removed_objects: usize,
        removed_segments: usize,
        removed_compaction_runs: usize,
    ) -> Result<Self, RetentionError> {
        let removed_objects = cleanup_count(removed_objects)?;
        let removed_segments = cleanup_count(removed_segments)?;
        let removed_compaction_runs = cleanup_count(removed_compaction_runs)?;
        let removed_roots = removed_objects
            .checked_add(removed_compaction_runs)
            .ok_or_else(|| RetentionError::corrupt("metadata cleanup root count overflowed"))?;
        let removed_rows = removed_roots
            .checked_add(removed_segments)
            .ok_or_else(|| RetentionError::corrupt("metadata cleanup row count overflowed"))?;
        Ok(Self {
            removed_objects,
            removed_segments,
            removed_compaction_runs,
            removed_roots,
            removed_rows,
        })
    }

    #[must_use]
    pub const fn removed_objects(self) -> u64 {
        self.removed_objects
    }

    #[must_use]
    pub const fn removed_segments(self) -> u64 {
        self.removed_segments
    }

    #[must_use]
    pub const fn removed_compaction_runs(self) -> u64 {
        self.removed_compaction_runs
    }

    #[must_use]
    pub const fn removed_roots(self) -> u64 {
        self.removed_roots
    }

    #[must_use]
    pub const fn removed_rows(self) -> u64 {
        self.removed_rows
    }
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

    /// Removes a bounded number of terminal metadata roots without crossing live relationships.
    ///
    /// A root is either one deleted dead-letter object, one deleted Parquet object with its
    /// terminal segment, or one unreferenced terminal compaction run.
    ///
    /// # Errors
    ///
    /// Returns a state conflict, unavailable, or corrupt error when PostgreSQL cannot preserve
    /// lifecycle references while removing terminal metadata.
    pub async fn clean_terminal_metadata(
        &self,
        limit: MetadataCleanupLimit,
    ) -> Result<MetadataCleanup, RetentionError> {
        let mut transaction = self.pool.begin().await.map_err(RetentionError::write)?;
        let candidates = load_cleanup_object_candidates(&mut transaction, limit).await?;
        let segment_ids = candidates
            .iter()
            .filter_map(CleanupObjectCandidate::segment_id)
            .collect::<Vec<_>>();
        let locked_segment_ids = lock_cleanup_segments(&mut transaction, &segment_ids).await?;
        let locked_segments = locked_segment_ids.iter().copied().collect::<BTreeSet<_>>();
        let selected = candidates
            .into_iter()
            .filter(|candidate| {
                candidate
                    .segment_id()
                    .is_none_or(|segment_id| locked_segments.contains(&segment_id))
            })
            .collect::<Vec<_>>();
        let object_ids = selected
            .iter()
            .map(|candidate| candidate.object_id)
            .collect::<Vec<_>>();
        remove_cleanup_objects(&mut transaction, &object_ids).await?;
        remove_cleanup_segments(&mut transaction, &locked_segment_ids).await?;

        let removed_object_roots = i64::try_from(selected.len())
            .map_err(|_| RetentionError::corrupt("metadata cleanup object count overflowed"))?;
        let remaining_roots = limit
            .get()
            .checked_sub(removed_object_roots)
            .ok_or_else(|| RetentionError::corrupt("metadata cleanup limit underflowed"))?;
        let removed_compaction_runs =
            remove_cleanup_compaction_runs(&mut transaction, remaining_roots).await?;
        let cleanup = MetadataCleanup::new(
            selected.len(),
            locked_segment_ids.len(),
            removed_compaction_runs,
        )?;
        transaction.commit().await.map_err(RetentionError::write)?;
        Ok(cleanup)
    }
}

async fn load_cleanup_object_candidates(
    transaction: &mut Transaction<'_, Postgres>,
    limit: MetadataCleanupLimit,
) -> Result<Vec<CleanupObjectCandidate>, RetentionError> {
    let rows = sqlx::query_as::<_, CleanupObjectCandidate>(
        r#"
        SELECT object.object_id, object.kind, object.segment_id
        FROM stored_objects AS object
        WHERE object.state = 'DELETED'
          AND (
              (object.kind = 'DEAD_LETTER' AND object.segment_id IS NULL)
              OR (
                  object.kind = 'PARQUET_DATA'
                  AND object.segment_id IS NOT NULL
                  AND EXISTS (
                      SELECT 1
                      FROM segments AS segment
                      WHERE segment.segment_id = object.segment_id
                        AND segment.state IN ('SUPERSEDED', 'EXPIRED', 'ABANDONED')
                        AND NOT EXISTS (
                            SELECT 1
                            FROM compaction_runs AS run
                            WHERE run.state IN ('BUILDING', 'UPLOADING')
                              AND (
                                  run.compaction_run_id = segment.produced_by_compaction_run_id
                                  OR run.compaction_run_id = segment.claimed_by_compaction_run_id
                              )
                        )
                  )
              )
          )
        ORDER BY object.updated_at, object.object_id
        LIMIT $1
        FOR UPDATE OF object SKIP LOCKED
        "#,
    )
    .bind(limit.get())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RetentionError::read)?;
    for row in &rows {
        row.validate()?;
    }
    Ok(rows)
}

async fn lock_cleanup_segments(
    transaction: &mut Transaction<'_, Postgres>,
    segment_ids: &[Uuid],
) -> Result<Vec<Uuid>, RetentionError> {
    if segment_ids.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT segment.segment_id
        FROM segments AS segment
        WHERE segment.segment_id = ANY($1::uuid[])
          AND segment.state IN ('SUPERSEDED', 'EXPIRED', 'ABANDONED')
          AND NOT EXISTS (
              SELECT 1
              FROM compaction_runs AS run
              WHERE run.state IN ('BUILDING', 'UPLOADING')
                AND (
                    run.compaction_run_id = segment.produced_by_compaction_run_id
                    OR run.compaction_run_id = segment.claimed_by_compaction_run_id
                )
          )
        ORDER BY segment.segment_id
        FOR UPDATE OF segment SKIP LOCKED
        "#,
    )
    .bind(segment_ids)
    .fetch_all(&mut **transaction)
    .await
    .map_err(RetentionError::read)
}

async fn remove_cleanup_objects(
    transaction: &mut Transaction<'_, Postgres>,
    object_ids: &[Uuid],
) -> Result<(), RetentionError> {
    if object_ids.is_empty() {
        return Ok(());
    }
    let result = sqlx::query(
        "DELETE FROM stored_objects WHERE object_id = ANY($1::uuid[]) AND state = 'DELETED'",
    )
    .bind(object_ids)
    .execute(&mut **transaction)
    .await
    .map_err(RetentionError::write)?;
    require_cleanup_rows(
        result.rows_affected(),
        object_ids.len(),
        "locked terminal object rows were not removed exactly once",
    )
}

async fn remove_cleanup_segments(
    transaction: &mut Transaction<'_, Postgres>,
    segment_ids: &[Uuid],
) -> Result<(), RetentionError> {
    if segment_ids.is_empty() {
        return Ok(());
    }
    let result = sqlx::query(
        r#"
        DELETE FROM segments AS segment
        WHERE segment.segment_id = ANY($1::uuid[])
          AND segment.state IN ('SUPERSEDED', 'EXPIRED', 'ABANDONED')
          AND NOT EXISTS (
              SELECT 1
              FROM stored_objects AS object
              WHERE object.segment_id = segment.segment_id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM compaction_runs AS run
              WHERE run.state IN ('BUILDING', 'UPLOADING')
                AND (
                    run.compaction_run_id = segment.produced_by_compaction_run_id
                    OR run.compaction_run_id = segment.claimed_by_compaction_run_id
                )
          )
        "#,
    )
    .bind(segment_ids)
    .execute(&mut **transaction)
    .await
    .map_err(RetentionError::write)?;
    require_cleanup_rows(
        result.rows_affected(),
        segment_ids.len(),
        "locked terminal segment rows were not removed exactly once",
    )
}

async fn remove_cleanup_compaction_runs(
    transaction: &mut Transaction<'_, Postgres>,
    limit: i64,
) -> Result<usize, RetentionError> {
    if limit == 0 {
        return Ok(0);
    }
    let removed = sqlx::query_scalar::<_, Uuid>(
        r#"
        WITH candidates AS (
            SELECT run.compaction_run_id
            FROM compaction_runs AS run
            WHERE run.state IN ('COMMITTED', 'FAILED')
              AND NOT EXISTS (
                  SELECT 1
                  FROM segments AS segment
                  WHERE segment.produced_by_compaction_run_id = run.compaction_run_id
                     OR segment.claimed_by_compaction_run_id = run.compaction_run_id
              )
            ORDER BY run.completed_at, run.compaction_run_id
            LIMIT $1
            FOR UPDATE OF run SKIP LOCKED
        )
        DELETE FROM compaction_runs AS run
        USING candidates
        WHERE run.compaction_run_id = candidates.compaction_run_id
          AND run.state IN ('COMMITTED', 'FAILED')
          AND NOT EXISTS (
              SELECT 1
              FROM segments AS segment
              WHERE segment.produced_by_compaction_run_id = run.compaction_run_id
                 OR segment.claimed_by_compaction_run_id = run.compaction_run_id
          )
        RETURNING run.compaction_run_id
        "#,
    )
    .bind(limit)
    .fetch_all(&mut **transaction)
    .await
    .map_err(RetentionError::write)?;
    Ok(removed.len())
}

fn require_cleanup_rows(
    affected: u64,
    expected: usize,
    message: &'static str,
) -> Result<(), RetentionError> {
    let expected = cleanup_count(expected)?;
    if affected == expected {
        Ok(())
    } else {
        Err(RetentionError::state_conflict(message))
    }
}

fn cleanup_count(count: usize) -> Result<u64, RetentionError> {
    u64::try_from(count).map_err(|_| RetentionError::corrupt("metadata cleanup count overflowed"))
}

#[derive(Debug, FromRow)]
struct CleanupObjectCandidate {
    object_id: Uuid,
    kind: String,
    segment_id: Option<Uuid>,
}

impl CleanupObjectCandidate {
    fn validate(&self) -> Result<(), RetentionError> {
        match (self.kind.as_str(), self.segment_id) {
            ("PARQUET_DATA", Some(_)) | ("DEAD_LETTER", None) => Ok(()),
            ("PARQUET_DATA" | "DEAD_LETTER", _) => Err(RetentionError::corrupt(
                "terminal object owner does not match its kind",
            )),
            _ => Err(RetentionError::corrupt(
                "terminal object has an unknown kind",
            )),
        }
    }

    const fn segment_id(&self) -> Option<Uuid> {
        self.segment_id
    }
}

fn is_timestamp_overflow(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| code == "22008" || code == "22015")
}
