use std::fmt::{Display, Formatter};

use chrono::{DateTime, NaiveDate, Utc};
use elucid_catalog::{InputId, SchemaId, SchemaVersion, SourceId};
use elucid_storage::{
    BatchId, ManagedObjectKey, ManagedRoot, ObjectByteSize, ObjectDescriptor, ObjectDigest,
    ObjectFormatVersion, ObjectMediaType, SegmentId, StoredObjectId,
};
use sqlx::FromRow;
use sqlx::postgres::PgPool;
use uuid::Uuid;

use crate::PublicationError;

const MAXIMUM_OPERATIONAL_ITEMS: u64 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum OperationalModelError {
    #[error("operational read limit must be between 1 and {maximum}")]
    LimitOutOfRange { maximum: u64 },
    #[error("segment state is not part of the lifecycle vocabulary")]
    SegmentStateInvalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct OperationalLimit(i64);

impl OperationalLimit {
    pub fn new(items: u64) -> Result<Self, OperationalModelError> {
        if items == 0 || items > MAXIMUM_OPERATIONAL_ITEMS {
            return Err(OperationalModelError::LimitOutOfRange {
                maximum: MAXIMUM_OPERATIONAL_ITEMS,
            });
        }
        i64::try_from(items)
            .map(Self)
            .map_err(|_| OperationalModelError::LimitOutOfRange {
                maximum: MAXIMUM_OPERATIONAL_ITEMS,
            })
    }

    const fn query_limit(self) -> i64 {
        self.0 + 1
    }

    const fn item_limit(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OperationalSegmentState {
    Prepared,
    Active,
    Superseded,
    Expired,
    Abandoned,
}

impl OperationalSegmentState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "PREPARED",
            Self::Active => "ACTIVE",
            Self::Superseded => "SUPERSEDED",
            Self::Expired => "EXPIRED",
            Self::Abandoned => "ABANDONED",
        }
    }

    fn from_database(value: &str) -> Result<Self, PublicationError> {
        Self::try_from(value)
            .map_err(|_| PublicationError::corrupt("stored segment has an unknown lifecycle state"))
    }
}

impl TryFrom<&str> for OperationalSegmentState {
    type Error = OperationalModelError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "PREPARED" => Ok(Self::Prepared),
            "ACTIVE" => Ok(Self::Active),
            "SUPERSEDED" => Ok(Self::Superseded),
            "EXPIRED" => Ok(Self::Expired),
            "ABANDONED" => Ok(Self::Abandoned),
            _ => Err(OperationalModelError::SegmentStateInvalid),
        }
    }
}

impl Display for OperationalSegmentState {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OperationalSegmentOrigin {
    Ingestion,
    Compaction,
}

impl OperationalSegmentOrigin {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ingestion => "INGESTION",
            Self::Compaction => "COMPACTION",
        }
    }

    fn from_database(value: &str) -> Result<Self, PublicationError> {
        match value {
            "INGESTION" => Ok(Self::Ingestion),
            "COMPACTION" => Ok(Self::Compaction),
            _ => Err(PublicationError::corrupt(
                "stored segment has an unknown origin",
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SegmentInspection {
    segment_id: SegmentId,
    source_id: SourceId,
    schema_id: SchemaId,
    schema_version: SchemaVersion,
    state: OperationalSegmentState,
    origin: OperationalSegmentOrigin,
    event_day: NaiveDate,
    row_count: u64,
    uncompressed_bytes: u64,
    parquet_bytes: u64,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    minimum_ingestion_time: DateTime<Utc>,
    maximum_ingestion_time: DateTime<Utc>,
    published_at: Option<DateTime<Utc>>,
    retired_at: Option<DateTime<Utc>>,
}

impl SegmentInspection {
    #[must_use]
    pub const fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    #[must_use]
    pub const fn state(&self) -> OperationalSegmentState {
        self.state
    }

    #[must_use]
    pub const fn origin(&self) -> OperationalSegmentOrigin {
        self.origin
    }

    #[must_use]
    pub const fn event_day(&self) -> NaiveDate {
        self.event_day
    }

    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    #[must_use]
    pub const fn uncompressed_bytes(&self) -> u64 {
        self.uncompressed_bytes
    }

    #[must_use]
    pub const fn parquet_bytes(&self) -> u64 {
        self.parquet_bytes
    }

    #[must_use]
    pub const fn minimum_event_time(&self) -> DateTime<Utc> {
        self.minimum_event_time
    }

    #[must_use]
    pub const fn maximum_event_time(&self) -> DateTime<Utc> {
        self.maximum_event_time
    }

    #[must_use]
    pub const fn minimum_ingestion_time(&self) -> DateTime<Utc> {
        self.minimum_ingestion_time
    }

    #[must_use]
    pub const fn maximum_ingestion_time(&self) -> DateTime<Utc> {
        self.maximum_ingestion_time
    }

    #[must_use]
    pub const fn published_at(&self) -> Option<DateTime<Utc>> {
        self.published_at
    }

    #[must_use]
    pub const fn retired_at(&self) -> Option<DateTime<Utc>> {
        self.retired_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeadLetterSummary {
    object_id: StoredObjectId,
    source_id: SourceId,
    input_id: InputId,
    batch_id: BatchId,
    byte_size: u64,
    published_at: DateTime<Utc>,
    retention_deadline: DateTime<Utc>,
}

impl DeadLetterSummary {
    #[must_use]
    pub const fn object_id(&self) -> StoredObjectId {
        self.object_id
    }

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn input_id(&self) -> InputId {
        self.input_id
    }

    #[must_use]
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    #[must_use]
    pub const fn byte_size(&self) -> u64 {
        self.byte_size
    }

    #[must_use]
    pub const fn published_at(&self) -> DateTime<Utc> {
        self.published_at
    }

    #[must_use]
    pub const fn retention_deadline(&self) -> DateTime<Utc> {
        self.retention_deadline
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DeadLetterObject {
    summary: DeadLetterSummary,
    descriptor: ObjectDescriptor,
}

impl DeadLetterObject {
    #[must_use]
    pub const fn summary(&self) -> &DeadLetterSummary {
        &self.summary
    }

    #[must_use]
    pub const fn descriptor(&self) -> &ObjectDescriptor {
        &self.descriptor
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BoundedOperationalList<T> {
    items: Vec<T>,
    truncated: bool,
    limit: usize,
}

impl<T> BoundedOperationalList<T> {
    #[must_use]
    pub fn items(&self) -> &[T] {
        &self.items
    }

    #[must_use]
    pub const fn is_truncated(&self) -> bool {
        self.truncated
    }

    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct OperationalBacklog {
    prepared_segments: u64,
    planned_objects: u64,
    uploaded_objects: u64,
}

impl OperationalBacklog {
    #[must_use]
    pub const fn prepared_segments(self) -> u64 {
        self.prepared_segments
    }

    #[must_use]
    pub const fn planned_objects(self) -> u64 {
        self.planned_objects
    }

    #[must_use]
    pub const fn uploaded_objects(self) -> u64 {
        self.uploaded_objects
    }
}

#[derive(Clone, Debug)]
pub struct OperationalStore {
    pool: PgPool,
    root: ManagedRoot,
}

impl OperationalStore {
    #[must_use]
    pub fn new(pool: PgPool, root: ManagedRoot) -> Self {
        Self { pool, root }
    }

    pub async fn publication_backlog(&self) -> Result<OperationalBacklog, PublicationError> {
        let row = sqlx::query_as::<_, BacklogRow>(
            r#"
            SELECT
                (SELECT count(*) FROM segments WHERE state = 'PREPARED') AS prepared_segments,
                (
                    SELECT count(*)
                    FROM stored_objects AS object
                    LEFT JOIN segments AS segment USING (segment_id)
                    WHERE object.state = 'PLANNED'
                      AND (object.kind = 'DEAD_LETTER' OR segment.state = 'PREPARED')
                ) AS planned_objects,
                (
                    SELECT count(*)
                    FROM stored_objects AS object
                    LEFT JOIN segments AS segment USING (segment_id)
                    WHERE object.state = 'UPLOADED'
                      AND (object.kind = 'DEAD_LETTER' OR segment.state = 'PREPARED')
                ) AS uploaded_objects
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(PublicationError::read)?;
        Ok(OperationalBacklog {
            prepared_segments: non_negative(row.prepared_segments, "prepared segment count")?,
            planned_objects: non_negative(row.planned_objects, "planned object count")?,
            uploaded_objects: non_negative(row.uploaded_objects, "uploaded object count")?,
        })
    }

    pub async fn segments(
        &self,
        source_id: SourceId,
        state: Option<OperationalSegmentState>,
        limit: OperationalLimit,
    ) -> Result<BoundedOperationalList<SegmentInspection>, PublicationError> {
        let rows = sqlx::query_as::<_, SegmentInspectionRow>(
            r#"
            SELECT
                segment.segment_id,
                segment.source_id,
                segment.schema_id,
                schema.version AS schema_version,
                segment.state,
                segment.origin,
                segment.event_day,
                segment.row_count,
                segment.uncompressed_bytes,
                object.expected_byte_size AS parquet_bytes,
                segment.minimum_event_time,
                segment.maximum_event_time,
                segment.minimum_ingestion_time,
                segment.maximum_ingestion_time,
                segment.published_at,
                segment.retired_at
            FROM segments AS segment
            JOIN stored_objects AS object USING (segment_id)
            JOIN schema_versions AS schema
              ON schema.source_id = segment.source_id
             AND schema.schema_id = segment.schema_id
            WHERE segment.source_id = $1
              AND ($2::TEXT IS NULL OR segment.state = $2)
            ORDER BY segment.event_day DESC, segment.minimum_event_time DESC, segment.segment_id
            LIMIT $3
            "#,
        )
        .bind(source_id.as_uuid())
        .bind(state.map(OperationalSegmentState::as_str))
        .bind(limit.query_limit())
        .fetch_all(&self.pool)
        .await
        .map_err(PublicationError::read)?;
        bounded(rows, limit, SegmentInspection::try_from)
    }

    pub async fn dead_letters(
        &self,
        source_id: SourceId,
        limit: OperationalLimit,
    ) -> Result<BoundedOperationalList<DeadLetterSummary>, PublicationError> {
        let rows = sqlx::query_as::<_, DeadLetterRow>(
            r#"
            SELECT
                object.object_id,
                input.source_id,
                object.input_id,
                object.batch_id,
                object.expected_byte_size,
                object.object_key,
                object.blake3_digest,
                object.media_type,
                object.format_version,
                object.published_at,
                object.retention_deadline
            FROM stored_objects AS object
            JOIN inputs AS input USING (input_id)
            WHERE input.source_id = $1
              AND object.kind = 'DEAD_LETTER'
              AND object.state = 'PUBLISHED'
            ORDER BY object.published_at DESC, object.object_id
            LIMIT $2
            "#,
        )
        .bind(source_id.as_uuid())
        .bind(limit.query_limit())
        .fetch_all(&self.pool)
        .await
        .map_err(PublicationError::read)?;
        bounded(rows, limit, |row| row.summary())
    }

    pub async fn dead_letter(
        &self,
        object_id: StoredObjectId,
    ) -> Result<Option<DeadLetterObject>, PublicationError> {
        let row = sqlx::query_as::<_, DeadLetterRow>(
            r#"
            SELECT
                object.object_id,
                input.source_id,
                object.input_id,
                object.batch_id,
                object.expected_byte_size,
                object.object_key,
                object.blake3_digest,
                object.media_type,
                object.format_version,
                object.published_at,
                object.retention_deadline
            FROM stored_objects AS object
            JOIN inputs AS input USING (input_id)
            WHERE object.object_id = $1
              AND object.kind = 'DEAD_LETTER'
              AND object.state = 'PUBLISHED'
            "#,
        )
        .bind(object_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(PublicationError::read)?;
        row.map(|row| row.object(&self.root)).transpose()
    }
}

#[derive(Debug, FromRow)]
struct BacklogRow {
    prepared_segments: i64,
    planned_objects: i64,
    uploaded_objects: i64,
}

#[derive(Debug, FromRow)]
struct SegmentInspectionRow {
    segment_id: Uuid,
    source_id: Uuid,
    schema_id: Uuid,
    schema_version: i64,
    state: String,
    origin: String,
    event_day: NaiveDate,
    row_count: i64,
    uncompressed_bytes: i64,
    parquet_bytes: i64,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    minimum_ingestion_time: DateTime<Utc>,
    maximum_ingestion_time: DateTime<Utc>,
    published_at: Option<DateTime<Utc>>,
    retired_at: Option<DateTime<Utc>>,
}

impl TryFrom<SegmentInspectionRow> for SegmentInspection {
    type Error = PublicationError;

    fn try_from(row: SegmentInspectionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            segment_id: SegmentId::from(row.segment_id),
            source_id: SourceId::try_from(row.source_id).map_err(|_| {
                PublicationError::corrupt("stored segment source identity is invalid")
            })?,
            schema_id: SchemaId::try_from(row.schema_id).map_err(|_| {
                PublicationError::corrupt("stored segment schema identity is invalid")
            })?,
            schema_version: u64::try_from(row.schema_version)
                .ok()
                .and_then(|version| SchemaVersion::new(version).ok())
                .ok_or_else(|| {
                    PublicationError::corrupt("stored segment schema version is invalid")
                })?,
            state: OperationalSegmentState::from_database(&row.state)?,
            origin: OperationalSegmentOrigin::from_database(&row.origin)?,
            event_day: row.event_day,
            row_count: non_negative(row.row_count, "segment row count")?,
            uncompressed_bytes: non_negative(
                row.uncompressed_bytes,
                "segment uncompressed byte count",
            )?,
            parquet_bytes: non_negative(row.parquet_bytes, "Parquet object byte count")?,
            minimum_event_time: row.minimum_event_time,
            maximum_event_time: row.maximum_event_time,
            minimum_ingestion_time: row.minimum_ingestion_time,
            maximum_ingestion_time: row.maximum_ingestion_time,
            published_at: row.published_at,
            retired_at: row.retired_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct DeadLetterRow {
    object_id: Uuid,
    source_id: Uuid,
    input_id: Option<Uuid>,
    batch_id: Option<Uuid>,
    expected_byte_size: i64,
    object_key: String,
    blake3_digest: Vec<u8>,
    media_type: String,
    format_version: i64,
    published_at: Option<DateTime<Utc>>,
    retention_deadline: Option<DateTime<Utc>>,
}

impl DeadLetterRow {
    fn summary(&self) -> Result<DeadLetterSummary, PublicationError> {
        let input_id = self
            .input_id
            .ok_or_else(|| PublicationError::corrupt("published dead letter has no input owner"))?;
        let batch_id = self
            .batch_id
            .ok_or_else(|| PublicationError::corrupt("published dead letter has no batch owner"))?;
        Ok(DeadLetterSummary {
            object_id: StoredObjectId::from(self.object_id),
            source_id: SourceId::try_from(self.source_id).map_err(|_| {
                PublicationError::corrupt("stored dead-letter source identity is invalid")
            })?,
            input_id: InputId::try_from(input_id).map_err(|_| {
                PublicationError::corrupt("stored dead-letter input identity is invalid")
            })?,
            batch_id: BatchId::try_from(batch_id).map_err(|_| {
                PublicationError::corrupt("stored dead-letter batch identity is invalid")
            })?,
            byte_size: non_negative(self.expected_byte_size, "dead-letter object byte count")?,
            published_at: self.published_at.ok_or_else(|| {
                PublicationError::corrupt("published dead letter has no publication time")
            })?,
            retention_deadline: self.retention_deadline.ok_or_else(|| {
                PublicationError::corrupt("published dead letter has no retention deadline")
            })?,
        })
    }

    fn object(self, root: &ManagedRoot) -> Result<DeadLetterObject, PublicationError> {
        let summary = self.summary()?;
        let key = ManagedObjectKey::dead_letter(root, summary.batch_id, summary.object_id);
        if self.object_key != key.as_str()
            || self.media_type != ObjectMediaType::DeadLetter.as_str()
        {
            return Err(PublicationError::corrupt(
                "published dead-letter object metadata is inconsistent",
            ));
        }
        let digest: [u8; 32] = self.blake3_digest.try_into().map_err(|_| {
            PublicationError::corrupt("published dead-letter digest has an invalid length")
        })?;
        let format_version = u64::try_from(self.format_version)
            .ok()
            .and_then(|value| ObjectFormatVersion::new(value).ok())
            .ok_or_else(|| {
                PublicationError::corrupt("published dead-letter format version is invalid")
            })?;
        let descriptor = ObjectDescriptor::new(
            key,
            ObjectByteSize::new(summary.byte_size),
            ObjectDigest::new(digest),
            ObjectMediaType::DeadLetter,
            format_version,
        )
        .map_err(|_| PublicationError::corrupt("published dead-letter descriptor is invalid"))?;
        Ok(DeadLetterObject {
            summary,
            descriptor,
        })
    }
}

fn bounded<Row, Item>(
    mut rows: Vec<Row>,
    limit: OperationalLimit,
    convert: impl Fn(Row) -> Result<Item, PublicationError>,
) -> Result<BoundedOperationalList<Item>, PublicationError> {
    let truncated = rows.len() > limit.item_limit();
    rows.truncate(limit.item_limit());
    let items = rows
        .into_iter()
        .map(convert)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BoundedOperationalList {
        items,
        truncated,
        limit: limit.item_limit(),
    })
}

fn non_negative(value: i64, field: &'static str) -> Result<u64, PublicationError> {
    u64::try_from(value).map_err(|_| PublicationError::corrupt(field))
}
