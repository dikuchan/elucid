use std::error::Error;
use std::fmt::{Display, Formatter};

use elucid_core::{CodedError, ErrorCode};
use elucid_storage::{
    BatchId, ManagedObjectKey, ObjectByteSize, ObjectDescriptor, ObjectDigest, ObjectFormatVersion,
    ObjectMediaType, SegmentId, StorageModelError, StoredObjectId,
};
use sqlx::postgres::PgPool;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::error::{is_database_conflict, is_row_decode_error};

pub const MAXIMUM_OBJECT_RECLAMATION_ITEMS: u64 = 1_000;

const LOAD_OBJECT_FOR_UPDATE: &str = r#"
    SELECT
        object_id,
        kind,
        segment_id,
        input_id,
        batch_id,
        object_key,
        expected_byte_size,
        blake3_digest,
        media_type,
        format_version,
        state
    FROM stored_objects
    WHERE object_id = $1
    FOR UPDATE
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ObjectReclamationModelError {
    #[error("object deletion retry delay must be positive")]
    RetryDelayMustBePositive,

    #[error("object deletion retry delay exceeds the PostgreSQL BIGINT range")]
    RetryDelayOutOfRange,

    #[error("object reclamation limit must be between 1 and {maximum} items")]
    LimitOutOfRange { maximum: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ObjectDeletionRetryDelay(i64);

impl ObjectDeletionRetryDelay {
    pub fn new(seconds: u64) -> Result<Self, ObjectReclamationModelError> {
        if seconds == 0 {
            return Err(ObjectReclamationModelError::RetryDelayMustBePositive);
        }
        i64::try_from(seconds)
            .map(Self)
            .map_err(|_| ObjectReclamationModelError::RetryDelayOutOfRange)
    }

    const fn seconds(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ObjectReclamationLimit(i64);

impl ObjectReclamationLimit {
    pub fn new(items: u64) -> Result<Self, ObjectReclamationModelError> {
        if items == 0 || items > MAXIMUM_OBJECT_RECLAMATION_ITEMS {
            return Err(ObjectReclamationModelError::LimitOutOfRange {
                maximum: MAXIMUM_OBJECT_RECLAMATION_ITEMS,
            });
        }
        i64::try_from(items)
            .map(Self)
            .map_err(|_| ObjectReclamationModelError::LimitOutOfRange {
                maximum: MAXIMUM_OBJECT_RECLAMATION_ITEMS,
            })
    }

    const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectDeletionAttempt {
    Initial,
    Retry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ObjectDeletionClaim {
    descriptor: ObjectDescriptor,
    attempt: ObjectDeletionAttempt,
}

impl ObjectDeletionClaim {
    #[must_use]
    pub const fn descriptor(&self) -> &ObjectDescriptor {
        &self.descriptor
    }

    #[must_use]
    pub const fn attempt(&self) -> ObjectDeletionAttempt {
        self.attempt
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectDeletionFailure {
    Retryable,
    Integrity,
}

impl ObjectDeletionFailure {
    const fn error_code(self) -> ErrorCode {
        match self {
            Self::Retryable => ErrorCode::ObjectDeleteFailed,
            Self::Integrity => ErrorCode::ObjectIntegrityError,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectDeletionCompletion {
    Deleted,
    AlreadyDeleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectDeletionFailureRecording {
    Recorded,
    AlreadyDeleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectReclamationErrorKind {
    Conflict,
    Unavailable,
    Corrupt,
}

impl Display for ObjectReclamationErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Conflict => "object reclamation state conflict",
            Self::Unavailable => "object reclamation state unavailable",
            Self::Corrupt => "object reclamation state corrupt",
        })
    }
}

#[derive(Debug)]
pub struct ObjectReclamationError {
    kind: ObjectReclamationErrorKind,
    source: ObjectReclamationErrorSource,
}

impl ObjectReclamationError {
    #[must_use]
    pub const fn kind(&self) -> ObjectReclamationErrorKind {
        self.kind
    }

    fn unavailable(source: sqlx::Error) -> Self {
        Self {
            kind: ObjectReclamationErrorKind::Unavailable,
            source: ObjectReclamationErrorSource::Database(source),
        }
    }

    fn read(source: sqlx::Error) -> Self {
        let kind = if is_row_decode_error(&source) {
            ObjectReclamationErrorKind::Corrupt
        } else {
            ObjectReclamationErrorKind::Unavailable
        };
        Self {
            kind,
            source: ObjectReclamationErrorSource::Database(source),
        }
    }

    fn write(source: sqlx::Error) -> Self {
        let kind = if is_database_conflict(&source) {
            ObjectReclamationErrorKind::Conflict
        } else {
            ObjectReclamationErrorKind::Unavailable
        };
        Self {
            kind,
            source: ObjectReclamationErrorSource::Database(source),
        }
    }

    fn corrupt(message: &'static str) -> Self {
        Self {
            kind: ObjectReclamationErrorKind::Corrupt,
            source: ObjectReclamationErrorSource::Invariant(message),
        }
    }

    fn conflict(message: &'static str) -> Self {
        Self {
            kind: ObjectReclamationErrorKind::Conflict,
            source: ObjectReclamationErrorSource::Invariant(message),
        }
    }

    fn storage_model(source: StorageModelError) -> Self {
        Self {
            kind: ObjectReclamationErrorKind::Corrupt,
            source: ObjectReclamationErrorSource::StorageModel(source),
        }
    }
}

impl Display for ObjectReclamationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for ObjectReclamationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl CodedError for ObjectReclamationError {
    fn error_code(&self) -> ErrorCode {
        match self.kind {
            ObjectReclamationErrorKind::Conflict => ErrorCode::MetastoreConflict,
            ObjectReclamationErrorKind::Unavailable => ErrorCode::MetastoreUnavailable,
            ObjectReclamationErrorKind::Corrupt => ErrorCode::MetastoreCorrupt,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ObjectReclamationErrorSource {
    #[error("PostgreSQL operation failed")]
    Database(#[source] sqlx::Error),
    #[error("stored object descriptor is invalid")]
    StorageModel(#[source] StorageModelError),
    #[error("{0}")]
    Invariant(&'static str),
}

#[derive(Clone, Debug)]
pub struct ObjectReclamationStore {
    pool: PgPool,
}

impl ObjectReclamationStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn claim(
        &self,
        retry_delay: ObjectDeletionRetryDelay,
        limit: ObjectReclamationLimit,
    ) -> Result<Vec<ObjectDeletionClaim>, ObjectReclamationError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(ObjectReclamationError::unavailable)?;
        let retry_rows = load_due_retries(&mut transaction, retry_delay, limit).await?;
        let retry_count = i64::try_from(retry_rows.len()).map_err(|_| {
            ObjectReclamationError::corrupt("reclamation retry count exceeds the i64 range")
        })?;
        let remaining = limit
            .get()
            .checked_sub(retry_count)
            .ok_or_else(|| ObjectReclamationError::corrupt("reclamation limit underflowed"))?;
        let initial_rows = if remaining == 0 {
            Vec::new()
        } else {
            load_initial_candidates(&mut transaction, remaining).await?
        };

        mark_retry_attempts(&mut transaction, &retry_rows).await?;
        mark_initial_claims(&mut transaction, &initial_rows).await?;

        let mut claims = Vec::with_capacity(retry_rows.len() + initial_rows.len());
        materialize_claims(&mut claims, retry_rows, ObjectDeletionAttempt::Retry)?;
        materialize_claims(&mut claims, initial_rows, ObjectDeletionAttempt::Initial)?;
        transaction
            .commit()
            .await
            .map_err(ObjectReclamationError::write)?;
        Ok(claims)
    }

    pub async fn record_deleted(
        &self,
        claim: &ObjectDeletionClaim,
    ) -> Result<ObjectDeletionCompletion, ObjectReclamationError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(ObjectReclamationError::unavailable)?;
        let row = load_claimed_object(&mut transaction, claim).await?;
        let outcome = match row.state.as_str() {
            "DELETE_PENDING" => {
                let result = sqlx::query(
                    r#"
                    UPDATE stored_objects
                    SET
                        state = 'DELETED',
                        deleted_at = CURRENT_TIMESTAMP,
                        last_error_code = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE object_id = $1 AND state = 'DELETE_PENDING'
                    "#,
                )
                .bind(claim.descriptor.key().object_id().as_uuid())
                .execute(&mut *transaction)
                .await
                .map_err(ObjectReclamationError::write)?;
                require_one_row(
                    result.rows_affected(),
                    "claimed object was not marked deleted",
                )?;
                ObjectDeletionCompletion::Deleted
            }
            "DELETED" => ObjectDeletionCompletion::AlreadyDeleted,
            _ => {
                return Err(ObjectReclamationError::conflict(
                    "object is not pending deletion",
                ));
            }
        };
        transaction
            .commit()
            .await
            .map_err(ObjectReclamationError::write)?;
        Ok(outcome)
    }

    pub async fn record_failure(
        &self,
        claim: &ObjectDeletionClaim,
        failure: ObjectDeletionFailure,
    ) -> Result<ObjectDeletionFailureRecording, ObjectReclamationError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(ObjectReclamationError::unavailable)?;
        let row = load_claimed_object(&mut transaction, claim).await?;
        let outcome = match row.state.as_str() {
            "DELETE_PENDING" => {
                let result = sqlx::query(
                    r#"
                    UPDATE stored_objects
                    SET last_error_code = $2, updated_at = CURRENT_TIMESTAMP
                    WHERE object_id = $1 AND state = 'DELETE_PENDING'
                    "#,
                )
                .bind(claim.descriptor.key().object_id().as_uuid())
                .bind(failure.error_code().as_str())
                .execute(&mut *transaction)
                .await
                .map_err(ObjectReclamationError::write)?;
                require_one_row(
                    result.rows_affected(),
                    "claimed object deletion failure was not recorded",
                )?;
                ObjectDeletionFailureRecording::Recorded
            }
            "DELETED" => ObjectDeletionFailureRecording::AlreadyDeleted,
            _ => {
                return Err(ObjectReclamationError::conflict(
                    "object is not pending deletion",
                ));
            }
        };
        transaction
            .commit()
            .await
            .map_err(ObjectReclamationError::write)?;
        Ok(outcome)
    }
}

async fn load_due_retries(
    transaction: &mut Transaction<'_, Postgres>,
    retry_delay: ObjectDeletionRetryDelay,
    limit: ObjectReclamationLimit,
) -> Result<Vec<StoredObjectRow>, ObjectReclamationError> {
    sqlx::query_as::<_, StoredObjectRow>(
        r#"
        SELECT
            object_id,
            kind,
            segment_id,
            input_id,
            batch_id,
            object_key,
            expected_byte_size,
            blake3_digest,
            media_type,
            format_version,
            state
        FROM stored_objects
        WHERE state = 'DELETE_PENDING'
          AND (last_error_code IS NULL OR last_error_code = 'OBJECT_DELETE_FAILED')
          AND updated_at <= CURRENT_TIMESTAMP - make_interval(secs => $1::double precision)
        ORDER BY updated_at, object_id
        LIMIT $2
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(retry_delay.seconds())
    .bind(limit.get())
    .fetch_all(&mut **transaction)
    .await
    .map_err(ObjectReclamationError::read)
}

async fn load_initial_candidates(
    transaction: &mut Transaction<'_, Postgres>,
    limit: i64,
) -> Result<Vec<StoredObjectRow>, ObjectReclamationError> {
    sqlx::query_as::<_, StoredObjectRow>(
        r#"
        SELECT
            object.object_id,
            object.kind,
            object.segment_id,
            object.input_id,
            object.batch_id,
            object.object_key,
            object.expected_byte_size,
            object.blake3_digest,
            object.media_type,
            object.format_version,
            object.state
        FROM stored_objects AS object
        LEFT JOIN segments AS segment ON segment.segment_id = object.segment_id
        WHERE (
            object.kind = 'PARQUET_DATA'
            AND segment.state IN ('SUPERSEDED', 'EXPIRED', 'ABANDONED')
            AND segment.reclaim_after <= CURRENT_TIMESTAMP
            AND (
                (segment.state IN ('SUPERSEDED', 'EXPIRED') AND object.state = 'PUBLISHED')
                OR (segment.state = 'ABANDONED' AND object.state IN ('PLANNED', 'UPLOADED'))
            )
        ) OR (
            object.kind = 'DEAD_LETTER'
            AND object.state = 'PUBLISHED'
            AND object.retention_deadline <= CURRENT_TIMESTAMP
        )
        ORDER BY COALESCE(segment.reclaim_after, object.retention_deadline), object.object_id
        LIMIT $1
        FOR UPDATE OF object SKIP LOCKED
        "#,
    )
    .bind(limit)
    .fetch_all(&mut **transaction)
    .await
    .map_err(ObjectReclamationError::read)
}

async fn mark_retry_attempts(
    transaction: &mut Transaction<'_, Postgres>,
    rows: &[StoredObjectRow],
) -> Result<(), ObjectReclamationError> {
    if rows.is_empty() {
        return Ok(());
    }
    let object_ids = rows.iter().map(|row| row.object_id).collect::<Vec<_>>();
    let result = sqlx::query(
        r#"
        UPDATE stored_objects
        SET updated_at = CURRENT_TIMESTAMP
        WHERE object_id = ANY($1::uuid[]) AND state = 'DELETE_PENDING'
        "#,
    )
    .bind(&object_ids)
    .execute(&mut **transaction)
    .await
    .map_err(ObjectReclamationError::write)?;
    require_exact_rows(
        result.rows_affected(),
        rows.len(),
        "locked object retries were not reserved exactly once",
    )
}

async fn mark_initial_claims(
    transaction: &mut Transaction<'_, Postgres>,
    rows: &[StoredObjectRow],
) -> Result<(), ObjectReclamationError> {
    if rows.is_empty() {
        return Ok(());
    }
    let object_ids = rows.iter().map(|row| row.object_id).collect::<Vec<_>>();
    let result = sqlx::query(
        r#"
        UPDATE stored_objects AS object
        SET
            state = 'DELETE_PENDING',
            delete_requested_at = CURRENT_TIMESTAMP,
            last_error_code = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE object.object_id = ANY($1::uuid[])
          AND object.state IN ('PLANNED', 'UPLOADED', 'PUBLISHED')
          AND (
              (
                  object.kind = 'PARQUET_DATA'
                  AND EXISTS (
                      SELECT 1
                      FROM segments AS segment
                      WHERE segment.segment_id = object.segment_id
                        AND segment.state IN ('SUPERSEDED', 'EXPIRED', 'ABANDONED')
                        AND segment.reclaim_after <= CURRENT_TIMESTAMP
                        AND (
                            (
                                segment.state IN ('SUPERSEDED', 'EXPIRED')
                                AND object.state = 'PUBLISHED'
                            ) OR (
                                segment.state = 'ABANDONED'
                                AND object.state IN ('PLANNED', 'UPLOADED')
                            )
                        )
                  )
              ) OR (
                  object.kind = 'DEAD_LETTER'
                  AND object.state = 'PUBLISHED'
                  AND object.retention_deadline <= CURRENT_TIMESTAMP
              )
          )
        "#,
    )
    .bind(&object_ids)
    .execute(&mut **transaction)
    .await
    .map_err(ObjectReclamationError::write)?;
    require_exact_rows(
        result.rows_affected(),
        rows.len(),
        "locked reclaimable objects were not claimed exactly once",
    )
}

fn materialize_claims(
    claims: &mut Vec<ObjectDeletionClaim>,
    rows: Vec<StoredObjectRow>,
    attempt: ObjectDeletionAttempt,
) -> Result<(), ObjectReclamationError> {
    for row in rows {
        claims.push(ObjectDeletionClaim {
            descriptor: materialize_descriptor(&row)?,
            attempt,
        });
    }
    Ok(())
}

async fn load_claimed_object(
    transaction: &mut Transaction<'_, Postgres>,
    claim: &ObjectDeletionClaim,
) -> Result<StoredObjectRow, ObjectReclamationError> {
    let row = sqlx::query_as::<_, StoredObjectRow>(LOAD_OBJECT_FOR_UPDATE)
        .bind(claim.descriptor.key().object_id().as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(ObjectReclamationError::read)?
        .ok_or_else(|| ObjectReclamationError::corrupt("claimed object metadata is missing"))?;
    if materialize_descriptor(&row)? != claim.descriptor {
        return Err(ObjectReclamationError::conflict(
            "claimed object descriptor changed before deletion completion",
        ));
    }
    Ok(row)
}

fn materialize_descriptor(
    row: &StoredObjectRow,
) -> Result<ObjectDescriptor, ObjectReclamationError> {
    let object_id = StoredObjectId::from(row.object_id);
    let (key, media_type) = match row.kind.as_str() {
        "PARQUET_DATA"
            if row.input_id.is_none() && row.batch_id.is_none() && row.segment_id.is_some() =>
        {
            let segment_id = SegmentId::from(
                row.segment_id
                    .ok_or_else(|| ObjectReclamationError::corrupt("Parquet owner is missing"))?,
            );
            let key = ManagedObjectKey::parse_parquet(&row.object_key, segment_id, object_id)
                .map_err(ObjectReclamationError::storage_model)?;
            (key, ObjectMediaType::ParquetData)
        }
        "DEAD_LETTER"
            if row.segment_id.is_none() && row.input_id.is_some() && row.batch_id.is_some() =>
        {
            let batch_id = BatchId::try_from(row.batch_id.ok_or_else(|| {
                ObjectReclamationError::corrupt("dead-letter batch owner is missing")
            })?)
            .map_err(ObjectReclamationError::storage_model)?;
            let key = ManagedObjectKey::parse_dead_letter(&row.object_key, batch_id, object_id)
                .map_err(ObjectReclamationError::storage_model)?;
            (key, ObjectMediaType::DeadLetter)
        }
        "PARQUET_DATA" | "DEAD_LETTER" => {
            return Err(ObjectReclamationError::corrupt(
                "stored object owner does not match its kind",
            ));
        }
        _ => {
            return Err(ObjectReclamationError::corrupt(
                "stored object has an unknown kind",
            ));
        }
    };
    if row.media_type != media_type.as_str() {
        return Err(ObjectReclamationError::corrupt(
            "stored object media type does not match its kind",
        ));
    }
    let expected_byte_size = u64::try_from(row.expected_byte_size)
        .map(ObjectByteSize::new)
        .map_err(|_| ObjectReclamationError::corrupt("stored object byte size is negative"))?;
    let digest = <[u8; 32]>::try_from(row.blake3_digest.as_slice())
        .map(ObjectDigest::new)
        .map_err(|_| ObjectReclamationError::corrupt("stored object digest is not 32 bytes"))?;
    let format_version = u64::try_from(row.format_version)
        .map_err(|_| ObjectReclamationError::corrupt("stored object format version is negative"))?;
    let format_version =
        ObjectFormatVersion::new(format_version).map_err(ObjectReclamationError::storage_model)?;
    ObjectDescriptor::new(key, expected_byte_size, digest, media_type, format_version)
        .map_err(ObjectReclamationError::storage_model)
}

fn require_one_row(rows: u64, message: &'static str) -> Result<(), ObjectReclamationError> {
    if rows == 1 {
        Ok(())
    } else {
        Err(ObjectReclamationError::corrupt(message))
    }
}

fn require_exact_rows(
    affected: u64,
    expected: usize,
    message: &'static str,
) -> Result<(), ObjectReclamationError> {
    let expected = u64::try_from(expected)
        .map_err(|_| ObjectReclamationError::corrupt("reclamation batch size overflowed"))?;
    if affected == expected {
        Ok(())
    } else {
        Err(ObjectReclamationError::corrupt(message))
    }
}

#[derive(Debug, FromRow)]
struct StoredObjectRow {
    object_id: Uuid,
    kind: String,
    segment_id: Option<Uuid>,
    input_id: Option<Uuid>,
    batch_id: Option<Uuid>,
    object_key: String,
    expected_byte_size: i64,
    blake3_digest: Vec<u8>,
    media_type: String,
    format_version: i64,
    state: String,
}
