use std::num::NonZeroU64;

use chrono::{DateTime, NaiveDate, Utc};
use elucid_catalog::{InputId, SchemaId, SourceId};
use elucid_storage::{
    BatchId, ManagedObjectKind, ObjectDescriptor, ObjectOwner, SegmentId, StoredObjectId,
};
use sqlx::postgres::PgPool;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::{PublicationError, PublicationModelError};

const LOAD_SEGMENT_FOR_UPDATE: &str = r#"
    SELECT
        segment_id,
        source_id,
        schema_id,
        origin,
        event_day,
        minimum_event_time,
        maximum_event_time,
        minimum_ingestion_time,
        maximum_ingestion_time,
        row_count,
        uncompressed_bytes,
        state,
        published_at
    FROM segments
    WHERE segment_id = $1
    FOR UPDATE
"#;

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
        state,
        published_at
    FROM stored_objects
    WHERE object_id = $1
    FOR UPDATE
"#;

const LOAD_SEGMENT_OBJECT_FOR_UPDATE: &str = r#"
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
        state,
        published_at
    FROM stored_objects
    WHERE segment_id = $1
    FOR UPDATE
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct IngestionSegmentTimes {
    event_day: NaiveDate,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    minimum_ingestion_time: DateTime<Utc>,
    maximum_ingestion_time: DateTime<Utc>,
}

impl IngestionSegmentTimes {
    pub fn new(
        event_day: NaiveDate,
        minimum_event_time: DateTime<Utc>,
        maximum_event_time: DateTime<Utc>,
        minimum_ingestion_time: DateTime<Utc>,
        maximum_ingestion_time: DateTime<Utc>,
    ) -> Result<Self, PublicationModelError> {
        if minimum_event_time > maximum_event_time {
            return Err(PublicationModelError::EventTimeBoundsNotOrdered);
        }
        if minimum_ingestion_time > maximum_ingestion_time {
            return Err(PublicationModelError::IngestionTimeBoundsNotOrdered);
        }
        if minimum_event_time.date_naive() != event_day
            || maximum_event_time.date_naive() != event_day
        {
            return Err(PublicationModelError::EventDayMismatch);
        }
        if [
            minimum_event_time,
            maximum_event_time,
            minimum_ingestion_time,
            maximum_ingestion_time,
        ]
        .into_iter()
        .any(|timestamp| timestamp.timestamp_subsec_nanos() % 1_000 != 0)
        {
            return Err(PublicationModelError::TimestampPrecisionUnsupported);
        }
        Ok(Self {
            event_day,
            minimum_event_time,
            maximum_event_time,
            minimum_ingestion_time,
            maximum_ingestion_time,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct IngestionSegmentRegistration {
    segment_id: SegmentId,
    source_id: SourceId,
    schema_id: SchemaId,
    times: IngestionSegmentTimes,
    row_count: i64,
    uncompressed_bytes: i64,
    object: ObjectDescriptor,
}

impl IngestionSegmentRegistration {
    pub fn new(
        segment_id: SegmentId,
        source_id: SourceId,
        schema_id: SchemaId,
        times: IngestionSegmentTimes,
        row_count: NonZeroU64,
        uncompressed_bytes: NonZeroU64,
        object: ObjectDescriptor,
    ) -> Result<Self, PublicationModelError> {
        if object.key().owner() != ObjectOwner::Segment(segment_id)
            || object.key().kind() != ManagedObjectKind::ParquetData
        {
            return Err(PublicationModelError::SegmentObjectOwnerMismatch);
        }
        validate_object_database_values(&object)?;
        let row_count = i64::try_from(row_count.get())
            .map_err(|_| PublicationModelError::RowCountOutOfRange)?;
        let uncompressed_bytes = i64::try_from(uncompressed_bytes.get())
            .map_err(|_| PublicationModelError::UncompressedByteCountOutOfRange)?;
        Ok(Self {
            segment_id,
            source_id,
            schema_id,
            times,
            row_count,
            uncompressed_bytes,
            object,
        })
    }

    #[must_use]
    pub const fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    #[must_use]
    pub const fn object(&self) -> &ObjectDescriptor {
        &self.object
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeadLetterRegistration {
    input_id: InputId,
    batch_id: BatchId,
    object: ObjectDescriptor,
}

impl DeadLetterRegistration {
    pub fn new(
        input_id: InputId,
        batch_id: BatchId,
        object: ObjectDescriptor,
    ) -> Result<Self, PublicationModelError> {
        if object.key().owner() != ObjectOwner::DeadLetterBatch(batch_id)
            || object.key().kind() != ManagedObjectKind::DeadLetter
        {
            return Err(PublicationModelError::DeadLetterObjectOwnerMismatch);
        }
        validate_object_database_values(&object)?;
        Ok(Self {
            input_id,
            batch_id,
            object,
        })
    }

    #[must_use]
    pub const fn object(&self) -> &ObjectDescriptor {
        &self.object
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RetentionPeriod(i64);

impl RetentionPeriod {
    pub fn new(seconds: u64) -> Result<Self, PublicationModelError> {
        if seconds == 0 {
            return Err(PublicationModelError::RetentionPeriodMustBePositive);
        }
        i64::try_from(seconds)
            .map(Self)
            .map_err(|_| PublicationModelError::RetentionPeriodOutOfRange)
    }

    #[must_use]
    const fn seconds(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RegistrationOutcome {
    Registered,
    AlreadyRegistered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectUploadRecordOutcome {
    Recorded,
    AlreadyRecorded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PublicationOutcome {
    Published,
    AlreadyPublished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
enum SegmentState {
    Prepared,
    Active,
    Superseded,
    Expired,
    Abandoned,
}

impl SegmentState {
    fn from_database(value: &str) -> Result<Self, PublicationError> {
        match value {
            "PREPARED" => Ok(Self::Prepared),
            "ACTIVE" => Ok(Self::Active),
            "SUPERSEDED" => Ok(Self::Superseded),
            "EXPIRED" => Ok(Self::Expired),
            "ABANDONED" => Ok(Self::Abandoned),
            _ => Err(PublicationError::corrupt(
                "stored segment has an unknown lifecycle state",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StoredObjectState {
    Planned,
    Uploaded,
    Published,
    DeletePending,
    Deleted,
}

impl StoredObjectState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "PLANNED",
            Self::Uploaded => "UPLOADED",
            Self::Published => "PUBLISHED",
            Self::DeletePending => "DELETE_PENDING",
            Self::Deleted => "DELETED",
        }
    }

    fn from_database(value: &str) -> Result<Self, PublicationError> {
        match value {
            "PLANNED" => Ok(Self::Planned),
            "UPLOADED" => Ok(Self::Uploaded),
            "PUBLISHED" => Ok(Self::Published),
            "DELETE_PENDING" => Ok(Self::DeletePending),
            "DELETED" => Ok(Self::Deleted),
            _ => Err(PublicationError::corrupt(
                "stored object has an unknown lifecycle state",
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PublicationStore {
    pool: PgPool,
}

impl PublicationStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn register_ingestion_segment(
        &self,
        registration: &IngestionSegmentRegistration,
    ) -> Result<RegistrationOutcome, PublicationError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(PublicationError::unavailable)?;
        let segment_inserted = sqlx::query(
            r#"
            INSERT INTO segments (
                segment_id,
                source_id,
                schema_id,
                origin,
                event_day,
                minimum_event_time,
                maximum_event_time,
                minimum_ingestion_time,
                maximum_ingestion_time,
                row_count,
                uncompressed_bytes,
                state
            ) VALUES ($1, $2, $3, 'INGESTION', $4, $5, $6, $7, $8, $9, $10, 'PREPARED')
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(registration.segment_id.as_uuid())
        .bind(registration.source_id.as_uuid())
        .bind(registration.schema_id.as_uuid())
        .bind(registration.times.event_day)
        .bind(registration.times.minimum_event_time)
        .bind(registration.times.maximum_event_time)
        .bind(registration.times.minimum_ingestion_time)
        .bind(registration.times.maximum_ingestion_time)
        .bind(registration.row_count)
        .bind(registration.uncompressed_bytes)
        .execute(&mut *transaction)
        .await
        .map_err(PublicationError::write)?
        .rows_affected();
        let object_inserted = insert_planned_object(
            &mut transaction,
            &registration.object,
            ObjectDatabaseOwner::Segment(registration.segment_id),
        )
        .await?;

        match (segment_inserted, object_inserted) {
            (1, 1) => {
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(RegistrationOutcome::Registered)
            }
            (0, 0) => {
                let segment = load_segment_for_update(&mut transaction, registration.segment_id)
                    .await?
                    .ok_or_else(|| {
                        PublicationError::conflict(
                            "segment identity or immutable object metadata conflicts",
                        )
                    })?;
                let object =
                    load_object_for_update(&mut transaction, registration.object.key().object_id())
                        .await?
                        .ok_or_else(|| {
                            PublicationError::conflict(
                                "segment identity or immutable object metadata conflicts",
                            )
                        })?;
                if !segment_matches_registration(&segment, registration)?
                    || !object_matches_descriptor(
                        &object,
                        &registration.object,
                        ObjectDatabaseOwner::Segment(registration.segment_id),
                    )?
                {
                    return rollback_with(
                        transaction,
                        PublicationError::conflict(
                            "segment identity or immutable object metadata conflicts",
                        ),
                    )
                    .await;
                }
                validate_registered_ingestion_states(&segment, &object)?;
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(RegistrationOutcome::AlreadyRegistered)
            }
            (0 | 1, 0 | 1) => {
                rollback_with(
                    transaction,
                    PublicationError::conflict(
                        "segment and object registration did not resolve atomically",
                    ),
                )
                .await
            }
            _ => {
                rollback_with(
                    transaction,
                    PublicationError::corrupt(
                        "segment registration affected an impossible number of rows",
                    ),
                )
                .await
            }
        }
    }

    pub async fn register_dead_letter(
        &self,
        registration: &DeadLetterRegistration,
    ) -> Result<RegistrationOutcome, PublicationError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(PublicationError::unavailable)?;
        let inserted = insert_planned_object(
            &mut transaction,
            &registration.object,
            ObjectDatabaseOwner::DeadLetter {
                input_id: registration.input_id,
                batch_id: registration.batch_id,
            },
        )
        .await?;
        match inserted {
            1 => {
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(RegistrationOutcome::Registered)
            }
            0 => {
                let object = load_object_for_update(
                    &mut transaction,
                    registration.object.key().object_id(),
                )
                .await?
                .ok_or_else(|| {
                    PublicationError::conflict(
                        "dead-letter identity, owner, or immutable object metadata conflicts",
                    )
                })?;
                if !object_matches_descriptor(
                    &object,
                    &registration.object,
                    ObjectDatabaseOwner::DeadLetter {
                        input_id: registration.input_id,
                        batch_id: registration.batch_id,
                    },
                )? {
                    return rollback_with(
                        transaction,
                        PublicationError::conflict(
                            "dead-letter identity, owner, or immutable object metadata conflicts",
                        ),
                    )
                    .await;
                }
                object.state()?;
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(RegistrationOutcome::AlreadyRegistered)
            }
            _ => {
                rollback_with(
                    transaction,
                    PublicationError::corrupt(
                        "dead-letter registration affected an impossible number of rows",
                    ),
                )
                .await
            }
        }
    }

    pub async fn record_verified_upload(
        &self,
        descriptor: &ObjectDescriptor,
    ) -> Result<ObjectUploadRecordOutcome, PublicationError> {
        validate_object_database_values(descriptor).map_err(PublicationError::from_model)?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(PublicationError::unavailable)?;
        let object = load_object_for_update(&mut transaction, descriptor.key().object_id())
            .await?
            .ok_or_else(|| PublicationError::conflict("verified object is not registered"))?;
        if !object_matches_descriptor_owner_from_key(&object, descriptor)? {
            return rollback_with(
                transaction,
                PublicationError::conflict(
                    "verified object does not match its registered immutable metadata",
                ),
            )
            .await;
        }
        match object.state()? {
            StoredObjectState::Planned => {
                require_one_row(
                    sqlx::query(
                        "UPDATE stored_objects SET state = 'UPLOADED', uploaded_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE object_id = $1 AND state = 'PLANNED'",
                    )
                    .bind(object.object_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(PublicationError::write)?,
                    "locked planned object did not advance to uploaded",
                )?;
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(ObjectUploadRecordOutcome::Recorded)
            }
            StoredObjectState::Uploaded | StoredObjectState::Published => {
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(ObjectUploadRecordOutcome::AlreadyRecorded)
            }
            StoredObjectState::DeletePending | StoredObjectState::Deleted => {
                rollback_with(
                    transaction,
                    PublicationError::conflict(
                        "object deletion has already advanced beyond upload recording",
                    ),
                )
                .await
            }
        }
    }

    pub async fn publish_ingestion_segment(
        &self,
        segment_id: SegmentId,
        retention: RetentionPeriod,
    ) -> Result<PublicationOutcome, PublicationError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(PublicationError::unavailable)?;
        let segment = load_segment_for_update(&mut transaction, segment_id)
            .await?
            .ok_or_else(|| PublicationError::conflict("ingestion segment is not registered"))?;
        let object = load_segment_object_for_update(&mut transaction, segment_id)
            .await?
            .ok_or_else(|| {
                PublicationError::corrupt("registered ingestion segment has no Parquet object")
            })?;
        if segment.origin != "INGESTION" {
            return rollback_with(
                transaction,
                PublicationError::conflict("compaction segment cannot use ingestion publication"),
            )
            .await;
        }
        validate_ingestion_object_owner(&object, segment_id)?;
        let segment_state = segment.state()?;
        let object_state = object.state()?;

        match (segment_state, object_state) {
            (SegmentState::Prepared, StoredObjectState::Uploaded) => {
                require_one_row(
                    sqlx::query(
                        "UPDATE stored_objects SET state = 'PUBLISHED', published_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE object_id = $1 AND state = 'UPLOADED'",
                    )
                    .bind(object.object_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(PublicationError::write)?,
                    "locked uploaded Parquet object did not advance to published",
                )?;
                require_one_row(
                    sqlx::query(
                        "UPDATE segments SET state = 'ACTIVE', published_at = CURRENT_TIMESTAMP, data_expires_at = CURRENT_TIMESTAMP + make_interval(secs => $2::double precision), updated_at = CURRENT_TIMESTAMP WHERE segment_id = $1 AND origin = 'INGESTION' AND state = 'PREPARED'",
                    )
                    .bind(segment_id.as_uuid())
                    .bind(retention.seconds())
                    .execute(&mut *transaction)
                    .await
                    .map_err(PublicationError::write)?,
                    "locked prepared ingestion segment did not advance to active",
                )?;
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(PublicationOutcome::Published)
            }
            (SegmentState::Active, StoredObjectState::Published)
                if publication_times_match(&segment, &object) =>
            {
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(PublicationOutcome::AlreadyPublished)
            }
            (
                SegmentState::Superseded | SegmentState::Expired,
                StoredObjectState::Published
                | StoredObjectState::DeletePending
                | StoredObjectState::Deleted,
            ) if publication_times_match(&segment, &object) => {
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(PublicationOutcome::AlreadyPublished)
            }
            (SegmentState::Prepared, StoredObjectState::Planned) => {
                rollback_with(
                    transaction,
                    PublicationError::conflict(
                        "Parquet object must be verified before segment publication",
                    ),
                )
                .await
            }
            (SegmentState::Abandoned, _) => {
                rollback_with(
                    transaction,
                    PublicationError::conflict("abandoned segment cannot be published"),
                )
                .await
            }
            _ => {
                rollback_with(
                    transaction,
                    PublicationError::corrupt(
                        "segment and Parquet object lifecycle states are inconsistent",
                    ),
                )
                .await
            }
        }
    }

    pub async fn publish_dead_letter(
        &self,
        object_id: StoredObjectId,
        retention: RetentionPeriod,
    ) -> Result<PublicationOutcome, PublicationError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(PublicationError::unavailable)?;
        let object = load_object_for_update(&mut transaction, object_id)
            .await?
            .ok_or_else(|| PublicationError::conflict("dead-letter object is not registered"))?;
        validate_dead_letter_owner(&object)?;
        match object.state()? {
            StoredObjectState::Uploaded => {
                require_one_row(
                    sqlx::query(
                        "UPDATE stored_objects SET state = 'PUBLISHED', published_at = CURRENT_TIMESTAMP, retention_deadline = CURRENT_TIMESTAMP + make_interval(secs => $2::double precision), updated_at = CURRENT_TIMESTAMP WHERE object_id = $1 AND kind = 'DEAD_LETTER' AND state = 'UPLOADED'",
                    )
                    .bind(object_id.as_uuid())
                    .bind(retention.seconds())
                    .execute(&mut *transaction)
                    .await
                    .map_err(PublicationError::write)?,
                    "locked uploaded dead-letter object did not advance to published",
                )?;
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(PublicationOutcome::Published)
            }
            StoredObjectState::Published => {
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(PublicationOutcome::AlreadyPublished)
            }
            StoredObjectState::DeletePending | StoredObjectState::Deleted
                if object.published_at.is_some() =>
            {
                transaction
                    .commit()
                    .await
                    .map_err(PublicationError::write)?;
                Ok(PublicationOutcome::AlreadyPublished)
            }
            StoredObjectState::Planned => {
                rollback_with(
                    transaction,
                    PublicationError::conflict(
                        "dead-letter object must be verified before publication",
                    ),
                )
                .await
            }
            StoredObjectState::DeletePending | StoredObjectState::Deleted => {
                rollback_with(
                    transaction,
                    PublicationError::conflict(
                        "unpublished dead-letter object is already being deleted",
                    ),
                )
                .await
            }
        }
    }

    pub async fn stored_object_state(
        &self,
        object_id: StoredObjectId,
    ) -> Result<Option<StoredObjectState>, PublicationError> {
        let state = sqlx::query_scalar::<_, String>(
            "SELECT state FROM stored_objects WHERE object_id = $1",
        )
        .bind(object_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(PublicationError::read)?;
        state
            .as_deref()
            .map(StoredObjectState::from_database)
            .transpose()
    }
}

#[derive(Clone, Copy, Debug)]
enum ObjectDatabaseOwner {
    Segment(SegmentId),
    DeadLetter {
        input_id: InputId,
        batch_id: BatchId,
    },
}

#[derive(Debug, FromRow)]
struct SegmentRow {
    segment_id: Uuid,
    source_id: Uuid,
    schema_id: Uuid,
    origin: String,
    event_day: NaiveDate,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    minimum_ingestion_time: DateTime<Utc>,
    maximum_ingestion_time: DateTime<Utc>,
    row_count: i64,
    uncompressed_bytes: i64,
    state: String,
    published_at: Option<DateTime<Utc>>,
}

impl SegmentRow {
    fn state(&self) -> Result<SegmentState, PublicationError> {
        SegmentState::from_database(&self.state)
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
    published_at: Option<DateTime<Utc>>,
}

impl StoredObjectRow {
    fn state(&self) -> Result<StoredObjectState, PublicationError> {
        StoredObjectState::from_database(&self.state)
    }
}

async fn insert_planned_object(
    transaction: &mut Transaction<'_, Postgres>,
    descriptor: &ObjectDescriptor,
    owner: ObjectDatabaseOwner,
) -> Result<u64, PublicationError> {
    let (kind, segment_id, input_id, batch_id) = match owner {
        ObjectDatabaseOwner::Segment(segment_id) => {
            ("PARQUET_DATA", Some(segment_id.as_uuid()), None, None)
        }
        ObjectDatabaseOwner::DeadLetter { input_id, batch_id } => (
            "DEAD_LETTER",
            None,
            Some(input_id.as_uuid()),
            Some(batch_id.as_uuid()),
        ),
    };
    let expected_byte_size =
        database_object_byte_size(descriptor).map_err(PublicationError::from_model)?;
    let format_version =
        database_object_format_version(descriptor).map_err(PublicationError::from_model)?;
    let result = sqlx::query(
        r#"
        INSERT INTO stored_objects (
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
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'PLANNED')
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(descriptor.key().object_id().as_uuid())
    .bind(kind)
    .bind(segment_id)
    .bind(input_id)
    .bind(batch_id)
    .bind(descriptor.key().as_str())
    .bind(expected_byte_size)
    .bind(descriptor.digest().as_bytes().to_vec())
    .bind(descriptor.media_type().as_str())
    .bind(format_version)
    .execute(&mut **transaction)
    .await
    .map_err(PublicationError::write)?;
    Ok(result.rows_affected())
}

async fn load_segment_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    segment_id: SegmentId,
) -> Result<Option<SegmentRow>, PublicationError> {
    sqlx::query_as::<_, SegmentRow>(LOAD_SEGMENT_FOR_UPDATE)
        .bind(segment_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(PublicationError::read)
}

async fn load_object_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    object_id: StoredObjectId,
) -> Result<Option<StoredObjectRow>, PublicationError> {
    sqlx::query_as::<_, StoredObjectRow>(LOAD_OBJECT_FOR_UPDATE)
        .bind(object_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(PublicationError::read)
}

async fn load_segment_object_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    segment_id: SegmentId,
) -> Result<Option<StoredObjectRow>, PublicationError> {
    sqlx::query_as::<_, StoredObjectRow>(LOAD_SEGMENT_OBJECT_FOR_UPDATE)
        .bind(segment_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(PublicationError::read)
}

fn segment_matches_registration(
    row: &SegmentRow,
    registration: &IngestionSegmentRegistration,
) -> Result<bool, PublicationError> {
    row.state()?;
    Ok(row.segment_id == registration.segment_id.as_uuid()
        && row.source_id == registration.source_id.as_uuid()
        && row.schema_id == registration.schema_id.as_uuid()
        && row.origin == "INGESTION"
        && row.event_day == registration.times.event_day
        && row.minimum_event_time == registration.times.minimum_event_time
        && row.maximum_event_time == registration.times.maximum_event_time
        && row.minimum_ingestion_time == registration.times.minimum_ingestion_time
        && row.maximum_ingestion_time == registration.times.maximum_ingestion_time
        && row.row_count == registration.row_count
        && row.uncompressed_bytes == registration.uncompressed_bytes)
}

fn object_matches_descriptor(
    row: &StoredObjectRow,
    descriptor: &ObjectDescriptor,
    owner: ObjectDatabaseOwner,
) -> Result<bool, PublicationError> {
    row.state()?;
    let expected_byte_size =
        database_object_byte_size(descriptor).map_err(PublicationError::from_model)?;
    let format_version =
        database_object_format_version(descriptor).map_err(PublicationError::from_model)?;
    let (kind, segment_id, input_id, batch_id) = match owner {
        ObjectDatabaseOwner::Segment(segment_id) => {
            ("PARQUET_DATA", Some(segment_id.as_uuid()), None, None)
        }
        ObjectDatabaseOwner::DeadLetter { input_id, batch_id } => (
            "DEAD_LETTER",
            None,
            Some(input_id.as_uuid()),
            Some(batch_id.as_uuid()),
        ),
    };
    Ok(row.object_id == descriptor.key().object_id().as_uuid()
        && row.kind == kind
        && row.segment_id == segment_id
        && row.input_id == input_id
        && row.batch_id == batch_id
        && row.object_key == descriptor.key().as_str()
        && row.expected_byte_size == expected_byte_size
        && row.blake3_digest.as_slice() == descriptor.digest().as_bytes()
        && row.media_type == descriptor.media_type().as_str()
        && row.format_version == format_version)
}

fn object_matches_descriptor_owner_from_key(
    row: &StoredObjectRow,
    descriptor: &ObjectDescriptor,
) -> Result<bool, PublicationError> {
    match descriptor.key().owner() {
        ObjectOwner::Segment(segment_id) => {
            object_matches_descriptor(row, descriptor, ObjectDatabaseOwner::Segment(segment_id))
        }
        ObjectOwner::DeadLetterBatch(batch_id) => {
            let Some(input_id) = row.input_id else {
                return Ok(false);
            };
            let input_id = InputId::try_from(input_id).map_err(|_| {
                PublicationError::corrupt("stored dead-letter input identity is invalid")
            })?;
            object_matches_descriptor(
                row,
                descriptor,
                ObjectDatabaseOwner::DeadLetter { input_id, batch_id },
            )
        }
        _ => Err(PublicationError::conflict(
            "verified object has an unsupported owner",
        )),
    }
}

fn validate_registered_ingestion_states(
    segment: &SegmentRow,
    object: &StoredObjectRow,
) -> Result<(), PublicationError> {
    match (segment.state()?, object.state()?) {
        (SegmentState::Prepared, StoredObjectState::Planned | StoredObjectState::Uploaded) => {
            Ok(())
        }
        (SegmentState::Active, StoredObjectState::Published)
            if publication_times_match(segment, object) =>
        {
            Ok(())
        }
        (
            SegmentState::Superseded | SegmentState::Expired,
            StoredObjectState::Published
            | StoredObjectState::DeletePending
            | StoredObjectState::Deleted,
        ) if publication_times_match(segment, object) => Ok(()),
        (SegmentState::Abandoned, _) => Err(PublicationError::conflict(
            "abandoned segment registration cannot be resumed",
        )),
        _ => Err(PublicationError::corrupt(
            "registered segment and object lifecycle states are inconsistent",
        )),
    }
}

fn validate_ingestion_object_owner(
    object: &StoredObjectRow,
    segment_id: SegmentId,
) -> Result<(), PublicationError> {
    if object.kind == "PARQUET_DATA"
        && object.segment_id == Some(segment_id.as_uuid())
        && object.input_id.is_none()
        && object.batch_id.is_none()
    {
        Ok(())
    } else {
        Err(PublicationError::corrupt(
            "ingestion segment is linked to an invalid stored object",
        ))
    }
}

fn validate_dead_letter_owner(object: &StoredObjectRow) -> Result<(), PublicationError> {
    if object.kind == "DEAD_LETTER"
        && object.segment_id.is_none()
        && object.input_id.is_some()
        && object.batch_id.is_some()
    {
        Ok(())
    } else {
        Err(PublicationError::conflict(
            "stored object is not a dead-letter object",
        ))
    }
}

fn publication_times_match(segment: &SegmentRow, object: &StoredObjectRow) -> bool {
    segment.published_at.is_some() && segment.published_at == object.published_at
}

fn validate_object_database_values(
    descriptor: &ObjectDescriptor,
) -> Result<(), PublicationModelError> {
    database_object_byte_size(descriptor)?;
    database_object_format_version(descriptor)?;
    Ok(())
}

fn database_object_byte_size(descriptor: &ObjectDescriptor) -> Result<i64, PublicationModelError> {
    i64::try_from(descriptor.expected_byte_size().get())
        .map_err(|_| PublicationModelError::ObjectByteSizeOutOfRange)
}

fn database_object_format_version(
    descriptor: &ObjectDescriptor,
) -> Result<i64, PublicationModelError> {
    i64::try_from(descriptor.format_version().get())
        .map_err(|_| PublicationModelError::ObjectFormatVersionOutOfRange)
}

fn require_one_row(
    result: sqlx::postgres::PgQueryResult,
    message: &'static str,
) -> Result<(), PublicationError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PublicationError::corrupt(message))
    }
}

async fn rollback_with<T>(
    transaction: Transaction<'_, Postgres>,
    error: PublicationError,
) -> Result<T, PublicationError> {
    transaction
        .rollback()
        .await
        .map_err(PublicationError::unavailable)?;
    Err(error)
}
