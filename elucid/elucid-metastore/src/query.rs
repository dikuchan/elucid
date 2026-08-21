use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::num::NonZeroU64;

use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use elucid_catalog::{
    CatalogApplicationError, CatalogModelError, Schema, SchemaId, SchemaVersion, Source, SourceId,
    SourceName, assemble_stored_source, decode_stored_schema_definition,
};
use elucid_language::ir::{TimeRange, UtcInstant};
use elucid_language::{Analysis, AnalyzeError, CatalogSnapshot, QueryTimeContext};
use elucid_storage::{
    ManagedObjectKey, ObjectByteSize, ObjectDescriptor, ObjectDigest, ObjectFormatVersion,
    ObjectMediaType, SegmentId, StorageModelError, StoredObjectId,
};
use serde_json::Value;
use sqlx::postgres::{PgConnection, PgPool};
use sqlx::types::Json;
use sqlx::{FromRow, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::error::is_row_decode_error;
use crate::{IngestionSegmentTimes, PublicationModelError};

pub const MAXIMUM_QUERY_SNAPSHOT_SEGMENTS: u64 = 10_000;
const QUERY_SNAPSHOT_ROW_LIMIT: i64 = 10_001;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum QuerySnapshotModelError {
    #[error("query request time range is not ordered")]
    RequestTimeRangeNotOrdered,

    #[error("query request timestamps must have millisecond precision")]
    RequestTimestampPrecisionUnsupported,

    #[error("maximum selected Parquet bytes must be positive")]
    MaximumParquetBytesMustBePositive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct QueryRequestTimeRange {
    start_inclusive: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
}

impl QueryRequestTimeRange {
    pub fn new(
        start_inclusive: DateTime<Utc>,
        end_exclusive: DateTime<Utc>,
    ) -> Result<Self, QuerySnapshotModelError> {
        if [start_inclusive, end_exclusive]
            .into_iter()
            .any(|timestamp| !timestamp.timestamp_subsec_nanos().is_multiple_of(1_000_000))
        {
            return Err(QuerySnapshotModelError::RequestTimestampPrecisionUnsupported);
        }
        if start_inclusive >= end_exclusive {
            return Err(QuerySnapshotModelError::RequestTimeRangeNotOrdered);
        }
        Ok(Self {
            start_inclusive,
            end_exclusive,
        })
    }

    #[must_use]
    pub const fn start_inclusive(self) -> DateTime<Utc> {
        self.start_inclusive
    }

    #[must_use]
    pub const fn end_exclusive(self) -> DateTime<Utc> {
        self.end_exclusive
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct QuerySnapshotLimits {
    maximum_parquet_bytes: NonZeroU64,
}

impl QuerySnapshotLimits {
    pub fn new(maximum_parquet_bytes: u64) -> Result<Self, QuerySnapshotModelError> {
        let maximum_parquet_bytes = NonZeroU64::new(maximum_parquet_bytes)
            .ok_or(QuerySnapshotModelError::MaximumParquetBytesMustBePositive)?;
        Ok(Self {
            maximum_parquet_bytes,
        })
    }

    #[must_use]
    pub const fn maximum_parquet_bytes(self) -> u64 {
        self.maximum_parquet_bytes.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum QuerySnapshotLimitExceeded {
    #[error("query snapshot exceeds the limit of {maximum} selected segments")]
    SegmentCount { maximum: u64 },

    #[error("query snapshot exceeds the limit of {maximum} selected Parquet bytes")]
    ParquetBytes { maximum: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QuerySnapshotErrorKind {
    Analysis,
    ResourceLimit,
    Unavailable,
    Corrupt,
}

impl Display for QuerySnapshotErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Analysis => "query analysis failed",
            Self::ResourceLimit => "query snapshot resource limit exceeded",
            Self::Unavailable => "query snapshot metadata unavailable",
            Self::Corrupt => "query snapshot metadata corrupt",
        })
    }
}

#[derive(Debug)]
pub struct QuerySnapshotError {
    kind: QuerySnapshotErrorKind,
    source: QuerySnapshotErrorSource,
}

impl QuerySnapshotError {
    #[must_use]
    pub const fn kind(&self) -> QuerySnapshotErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn analysis_error(&self) -> Option<&AnalyzeError> {
        match &self.source {
            QuerySnapshotErrorSource::Analysis(source) => Some(source),
            QuerySnapshotErrorSource::Limit(_)
            | QuerySnapshotErrorSource::CatalogApplication(_)
            | QuerySnapshotErrorSource::CatalogModel(_)
            | QuerySnapshotErrorSource::PublicationModel(_)
            | QuerySnapshotErrorSource::StorageModel(_)
            | QuerySnapshotErrorSource::Database(_)
            | QuerySnapshotErrorSource::Invariant(_) => None,
        }
    }

    #[must_use]
    pub const fn limit_exceeded(&self) -> Option<QuerySnapshotLimitExceeded> {
        match &self.source {
            QuerySnapshotErrorSource::Limit(source) => Some(*source),
            QuerySnapshotErrorSource::Analysis(_)
            | QuerySnapshotErrorSource::CatalogApplication(_)
            | QuerySnapshotErrorSource::CatalogModel(_)
            | QuerySnapshotErrorSource::PublicationModel(_)
            | QuerySnapshotErrorSource::StorageModel(_)
            | QuerySnapshotErrorSource::Database(_)
            | QuerySnapshotErrorSource::Invariant(_) => None,
        }
    }

    fn analysis(source: AnalyzeError) -> Self {
        Self {
            kind: QuerySnapshotErrorKind::Analysis,
            source: QuerySnapshotErrorSource::Analysis(source),
        }
    }

    fn resource_limit(source: QuerySnapshotLimitExceeded) -> Self {
        Self {
            kind: QuerySnapshotErrorKind::ResourceLimit,
            source: QuerySnapshotErrorSource::Limit(source),
        }
    }

    fn catalog_model(source: CatalogModelError) -> Self {
        Self {
            kind: QuerySnapshotErrorKind::Corrupt,
            source: QuerySnapshotErrorSource::CatalogModel(source),
        }
    }

    fn catalog_application(source: CatalogApplicationError) -> Self {
        Self {
            kind: QuerySnapshotErrorKind::Corrupt,
            source: QuerySnapshotErrorSource::CatalogApplication(source),
        }
    }

    fn publication_model(source: PublicationModelError) -> Self {
        Self {
            kind: QuerySnapshotErrorKind::Corrupt,
            source: QuerySnapshotErrorSource::PublicationModel(source),
        }
    }

    fn storage_model(source: StorageModelError) -> Self {
        Self {
            kind: QuerySnapshotErrorKind::Corrupt,
            source: QuerySnapshotErrorSource::StorageModel(source),
        }
    }

    fn unavailable(source: sqlx::Error) -> Self {
        Self {
            kind: QuerySnapshotErrorKind::Unavailable,
            source: QuerySnapshotErrorSource::Database(source),
        }
    }

    fn read(source: sqlx::Error) -> Self {
        let kind = if is_row_decode_error(&source) {
            QuerySnapshotErrorKind::Corrupt
        } else {
            QuerySnapshotErrorKind::Unavailable
        };
        Self {
            kind,
            source: QuerySnapshotErrorSource::Database(source),
        }
    }

    fn corrupt(message: &'static str) -> Self {
        Self {
            kind: QuerySnapshotErrorKind::Corrupt,
            source: QuerySnapshotErrorSource::Invariant(message),
        }
    }
}

impl Display for QuerySnapshotError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for QuerySnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug, thiserror::Error)]
enum QuerySnapshotErrorSource {
    #[error("query analysis failed")]
    Analysis(#[source] AnalyzeError),
    #[error("query snapshot limit exceeded")]
    Limit(#[source] QuerySnapshotLimitExceeded),
    #[error("stored catalog definition is invalid")]
    CatalogApplication(#[source] CatalogApplicationError),
    #[error("stored catalog identity is invalid")]
    CatalogModel(#[source] CatalogModelError),
    #[error("stored segment metadata is invalid")]
    PublicationModel(#[source] PublicationModelError),
    #[error("stored object descriptor is invalid")]
    StorageModel(#[source] StorageModelError),
    #[error("PostgreSQL operation failed")]
    Database(#[source] sqlx::Error),
    #[error("{0}")]
    Invariant(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct QuerySegment {
    segment_id: SegmentId,
    schema_id: SchemaId,
    times: IngestionSegmentTimes,
    row_count: NonZeroU64,
    uncompressed_bytes: NonZeroU64,
    published_at: DateTime<Utc>,
    object: ObjectDescriptor,
}

impl QuerySegment {
    #[must_use]
    pub const fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    #[must_use]
    pub const fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    #[must_use]
    pub const fn times(&self) -> IngestionSegmentTimes {
        self.times
    }

    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count.get()
    }

    #[must_use]
    pub const fn uncompressed_bytes(&self) -> u64 {
        self.uncompressed_bytes.get()
    }

    #[must_use]
    pub const fn published_at(&self) -> DateTime<Utc> {
        self.published_at
    }

    #[must_use]
    pub const fn object(&self) -> &ObjectDescriptor {
        &self.object
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct QuerySnapshot {
    analysis: Analysis,
    reference_time: UtcInstant,
    active_schema: Schema,
    stored_schemas: Vec<Schema>,
    segments: Vec<QuerySegment>,
    selected_parquet_bytes: u64,
}

impl QuerySnapshot {
    #[must_use]
    pub const fn analysis(&self) -> &Analysis {
        &self.analysis
    }

    #[must_use]
    pub const fn reference_time(&self) -> UtcInstant {
        self.reference_time
    }

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.analysis.pipeline().source().source_id()
    }

    #[must_use]
    pub const fn time_range(&self) -> TimeRange {
        *self.analysis.pipeline().time_range()
    }

    #[must_use]
    pub const fn active_schema(&self) -> &Schema {
        &self.active_schema
    }

    #[must_use]
    pub fn stored_schemas(&self) -> &[Schema] {
        &self.stored_schemas
    }

    #[must_use]
    pub fn segments(&self) -> &[QuerySegment] {
        &self.segments
    }

    #[must_use]
    pub const fn selected_parquet_bytes(&self) -> u64 {
        self.selected_parquet_bytes
    }
}

#[derive(Clone, Debug)]
pub struct QuerySnapshotStore {
    pool: PgPool,
}

impl QuerySnapshotStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Resolves a query and its exact visible Parquet objects under one PostgreSQL snapshot.
    ///
    /// # Errors
    ///
    /// Returns an analysis, resource-limit, unavailable, or corrupt error. No PostgreSQL
    /// transaction remains open after this method returns.
    pub async fn select(
        &self,
        query: &str,
        request_range: QueryRequestTimeRange,
        limits: QuerySnapshotLimits,
    ) -> Result<QuerySnapshot, QuerySnapshotError> {
        let parsed = elucid_language::parse(query).map_err(QuerySnapshotError::analysis)?;
        let source_name = parsed.source().name().as_str();
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(QuerySnapshotError::unavailable)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(QuerySnapshotError::unavailable)?;
        let reference_time: DateTime<Utc> =
            sqlx::query_scalar("SELECT date_trunc('milliseconds', CURRENT_TIMESTAMP)")
                .fetch_one(&mut *transaction)
                .await
                .map_err(QuerySnapshotError::read)?;
        if !reference_time
            .timestamp_subsec_nanos()
            .is_multiple_of(1_000_000)
        {
            return Err(QuerySnapshotError::corrupt(
                "PostgreSQL returned a query reference time below millisecond precision",
            ));
        }
        let time_context = QueryTimeContext::new(
            instant(reference_time),
            Some(instant(request_range.start_inclusive())),
            Some(instant(request_range.end_exclusive())),
        );
        let source = load_analysis_source(&mut transaction, source_name).await?;
        let language_catalog = source
            .as_ref()
            .map_or_else(CatalogSnapshot::empty, CatalogSnapshot::new);
        let analysis =
            elucid_language::analyze_parsed(query, &parsed, &language_catalog, &time_context)
                .map_err(QuerySnapshotError::analysis)?;
        let source = source.ok_or_else(|| {
            QuerySnapshotError::corrupt("analysis accepted a source absent from its catalog")
        })?;
        validate_analysis_source(&analysis, &source)?;

        let rows = select_segment_rows(
            &mut transaction,
            source.id(),
            *analysis.pipeline().time_range(),
        )
        .await?;
        if rows.len()
            > usize::try_from(MAXIMUM_QUERY_SNAPSHOT_SEGMENTS)
                .expect("query snapshot segment limit fits usize")
        {
            return Err(QuerySnapshotError::resource_limit(
                QuerySnapshotLimitExceeded::SegmentCount {
                    maximum: MAXIMUM_QUERY_SNAPSHOT_SEGMENTS,
                },
            ));
        }
        let (segments, selected_parquet_bytes) = materialize_segments(rows, &source)?;
        if selected_parquet_bytes > limits.maximum_parquet_bytes() {
            return Err(QuerySnapshotError::resource_limit(
                QuerySnapshotLimitExceeded::ParquetBytes {
                    maximum: limits.maximum_parquet_bytes(),
                },
            ));
        }
        let stored_schemas = load_stored_schemas(&mut transaction, &source, &segments).await?;
        let snapshot = QuerySnapshot {
            analysis,
            reference_time: instant(reference_time),
            active_schema: source.active_schema().clone(),
            stored_schemas,
            segments,
            selected_parquet_bytes,
        };
        transaction
            .commit()
            .await
            .map_err(QuerySnapshotError::unavailable)?;
        Ok(snapshot)
    }
}

fn instant(value: DateTime<Utc>) -> UtcInstant {
    UtcInstant::from_unix_milliseconds(value.timestamp_millis())
}

async fn load_analysis_source(
    connection: &mut PgConnection,
    source_name: &str,
) -> Result<Option<Source>, QuerySnapshotError> {
    let source = sqlx::query_as::<_, QuerySourceRow>(
        "SELECT source_id, name, display_name, active_schema_id FROM sources WHERE name = $1",
    )
    .bind(source_name)
    .fetch_optional(&mut *connection)
    .await
    .map_err(QuerySnapshotError::read)?;
    let Some(source) = source else {
        return Ok(None);
    };
    let active_schema = sqlx::query_as::<_, QuerySchemaRow>(
        "SELECT schema_id, source_id, version, definition FROM schema_versions WHERE source_id = $1 AND schema_id = $2",
    )
    .bind(source.source_id)
    .bind(source.active_schema_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(QuerySnapshotError::read)?
    .ok_or_else(|| {
        QuerySnapshotError::corrupt("captured source references a missing active schema")
    })?;
    let source_id =
        SourceId::try_from(source.source_id).map_err(QuerySnapshotError::catalog_model)?;
    let active_schema_id =
        SchemaId::try_from(source.active_schema_id).map_err(QuerySnapshotError::catalog_model)?;
    let name = SourceName::try_from(source.name).map_err(QuerySnapshotError::catalog_model)?;
    let active_schema = materialize_schema(active_schema)?;
    assemble_stored_source(
        source_id,
        name,
        source.display_name,
        active_schema_id,
        vec![active_schema],
        Vec::new(),
    )
    .map(Some)
    .map_err(QuerySnapshotError::catalog_application)
}

async fn load_stored_schemas(
    connection: &mut PgConnection,
    source: &Source,
    segments: &[QuerySegment],
) -> Result<Vec<Schema>, QuerySnapshotError> {
    let required = segments
        .iter()
        .map(QuerySegment::schema_id)
        .collect::<HashSet<_>>();
    if required.is_empty() {
        return Ok(Vec::new());
    }
    let required_ids = required
        .iter()
        .map(|schema_id| schema_id.as_uuid())
        .collect::<Vec<_>>();
    let rows = sqlx::query_as::<_, QuerySchemaRow>(
        "SELECT schema_id, source_id, version, definition FROM schema_versions WHERE source_id = $1 AND schema_id = ANY($2) ORDER BY version",
    )
    .bind(source.id().as_uuid())
    .bind(required_ids)
    .fetch_all(&mut *connection)
    .await
    .map_err(QuerySnapshotError::read)?;
    let stored_schemas = rows
        .into_iter()
        .map(materialize_schema)
        .collect::<Result<Vec<_>, _>>()?;
    if stored_schemas.len() != required.len()
        || stored_schemas
            .iter()
            .any(|schema| !required.contains(&schema.id()))
    {
        return Err(QuerySnapshotError::corrupt(
            "selected segment schema is absent from the captured catalog",
        ));
    }
    validate_schema_adapters(source, &stored_schemas)?;
    Ok(stored_schemas)
}

fn materialize_schema(row: QuerySchemaRow) -> Result<Schema, QuerySnapshotError> {
    let schema_id = SchemaId::try_from(row.schema_id).map_err(QuerySnapshotError::catalog_model)?;
    let source_id = SourceId::try_from(row.source_id).map_err(QuerySnapshotError::catalog_model)?;
    let version = u64::try_from(row.version)
        .map_err(|_| QuerySnapshotError::corrupt("stored schema version is negative"))?;
    let version = SchemaVersion::new(version).map_err(QuerySnapshotError::catalog_model)?;
    decode_stored_schema_definition(schema_id, source_id, version, row.definition.0)
        .map_err(QuerySnapshotError::catalog_application)
}

fn validate_schema_adapters(
    source: &Source,
    stored_schemas: &[Schema],
) -> Result<(), QuerySnapshotError> {
    let mut schemas = stored_schemas.to_vec();
    if !schemas
        .iter()
        .any(|schema| schema.id() == source.active_schema().id())
    {
        schemas.push(source.active_schema().clone());
    }
    schemas.sort_unstable_by_key(|schema| schema.version());
    assemble_stored_source(
        source.id(),
        source.name().clone(),
        source.display_name().to_owned(),
        source.active_schema().id(),
        schemas,
        Vec::new(),
    )
    .map(|_| ())
    .map_err(QuerySnapshotError::catalog_application)
}

fn validate_analysis_source(
    analysis: &Analysis,
    source: &Source,
) -> Result<(), QuerySnapshotError> {
    let analyzed = analysis.pipeline().source();
    if analyzed.source_id() != source.id()
        || analyzed.active_schema_id() != source.active_schema().id()
    {
        return Err(QuerySnapshotError::corrupt(
            "typed query source does not match the captured catalog source",
        ));
    }
    Ok(())
}

async fn select_segment_rows(
    connection: &mut PgConnection,
    source_id: SourceId,
    time_range: TimeRange,
) -> Result<Vec<QuerySegmentRow>, QuerySnapshotError> {
    let selection = SegmentTimeSelection::from_range(time_range);
    let SegmentTimeSelection::Bounds(bounds) = selection else {
        return Ok(Vec::new());
    };
    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            s.segment_id,
            s.source_id,
            s.schema_id,
            s.event_day,
            s.minimum_event_time,
            s.maximum_event_time,
            s.minimum_ingestion_time,
            s.maximum_ingestion_time,
            s.row_count,
            s.uncompressed_bytes,
            s.published_at AS segment_published_at,
            o.object_id,
            o.kind AS object_kind,
            o.segment_id AS object_segment_id,
            o.object_key,
            o.expected_byte_size AS object_expected_byte_size,
            o.blake3_digest AS object_blake3_digest,
            o.media_type AS object_media_type,
            o.format_version AS object_format_version,
            o.state AS object_state,
            o.published_at AS object_published_at
        FROM segments AS s
        LEFT JOIN stored_objects AS o ON o.segment_id = s.segment_id
        WHERE s.source_id =
        "#,
    );
    query.push_bind(source_id.as_uuid());
    query.push(" AND s.state = 'ACTIVE'");
    if let Some(start) = bounds.start_inclusive {
        query.push(" AND s.event_day >= ");
        query.push_bind(start.date_naive());
        query.push(" AND s.maximum_event_time >= ");
        query.push_bind(start);
    }
    if let Some(end) = bounds.end_exclusive {
        if let Some(last_day) = end
            .checked_sub_signed(TimeDelta::milliseconds(1))
            .map(|timestamp| timestamp.date_naive())
        {
            query.push(" AND s.event_day <= ");
            query.push_bind(last_day);
        }
        query.push(" AND s.minimum_event_time < ");
        query.push_bind(end);
    }
    query.push(" ORDER BY s.event_day, s.segment_id, o.object_id LIMIT ");
    query.push_bind(QUERY_SNAPSHOT_ROW_LIMIT);
    query
        .build_query_as::<QuerySegmentRow>()
        .fetch_all(&mut *connection)
        .await
        .map_err(QuerySnapshotError::read)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SegmentTimeSelection {
    Empty,
    Bounds(SegmentTimeBounds),
}

impl SegmentTimeSelection {
    fn from_range(range: TimeRange) -> Self {
        let start = DatabaseTimestamp::from_instant(range.start_inclusive());
        let end = DatabaseTimestamp::from_instant(range.end_exclusive());
        match (start, end) {
            (DatabaseTimestamp::AfterSupportedRange, _)
            | (_, DatabaseTimestamp::BeforeSupportedRange) => Self::Empty,
            (DatabaseTimestamp::BeforeSupportedRange, DatabaseTimestamp::AfterSupportedRange) => {
                Self::Bounds(SegmentTimeBounds {
                    start_inclusive: None,
                    end_exclusive: None,
                })
            }
            (DatabaseTimestamp::BeforeSupportedRange, DatabaseTimestamp::Value(end_exclusive)) => {
                Self::Bounds(SegmentTimeBounds {
                    start_inclusive: None,
                    end_exclusive: Some(end_exclusive),
                })
            }
            (DatabaseTimestamp::Value(start_inclusive), DatabaseTimestamp::AfterSupportedRange) => {
                Self::Bounds(SegmentTimeBounds {
                    start_inclusive: Some(start_inclusive),
                    end_exclusive: None,
                })
            }
            (
                DatabaseTimestamp::Value(start_inclusive),
                DatabaseTimestamp::Value(end_exclusive),
            ) => Self::Bounds(SegmentTimeBounds {
                start_inclusive: Some(start_inclusive),
                end_exclusive: Some(end_exclusive),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SegmentTimeBounds {
    start_inclusive: Option<DateTime<Utc>>,
    end_exclusive: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DatabaseTimestamp {
    BeforeSupportedRange,
    Value(DateTime<Utc>),
    AfterSupportedRange,
}

impl DatabaseTimestamp {
    fn from_instant(value: UtcInstant) -> Self {
        let milliseconds = value.unix_milliseconds();
        match DateTime::from_timestamp_millis(milliseconds) {
            Some(value) => Self::Value(value),
            None if milliseconds.is_negative() => Self::BeforeSupportedRange,
            None => Self::AfterSupportedRange,
        }
    }
}

fn materialize_segments(
    rows: Vec<QuerySegmentRow>,
    source: &Source,
) -> Result<(Vec<QuerySegment>, u64), QuerySnapshotError> {
    let mut segments = Vec::with_capacity(rows.len());
    let mut selected_parquet_bytes = 0_u64;
    for row in rows {
        let segment = materialize_segment(&row, source)?;
        selected_parquet_bytes = selected_parquet_bytes
            .checked_add(segment.object().expected_byte_size().get())
            .ok_or_else(|| QuerySnapshotError::corrupt("selected Parquet byte count overflowed"))?;
        segments.push(segment);
    }
    Ok((segments, selected_parquet_bytes))
}

fn materialize_segment(
    row: &QuerySegmentRow,
    source: &Source,
) -> Result<QuerySegment, QuerySnapshotError> {
    let source_id = SourceId::try_from(row.source_id).map_err(QuerySnapshotError::catalog_model)?;
    if source_id != source.id() {
        return Err(QuerySnapshotError::corrupt(
            "selected segment belongs to the wrong source",
        ));
    }
    let schema_id = SchemaId::try_from(row.schema_id).map_err(QuerySnapshotError::catalog_model)?;
    let segment_id = SegmentId::from(row.segment_id);
    let times = IngestionSegmentTimes::new(
        row.event_day,
        row.minimum_event_time,
        row.maximum_event_time,
        row.minimum_ingestion_time,
        row.maximum_ingestion_time,
    )
    .map_err(QuerySnapshotError::publication_model)?;
    let row_count = positive_database_count(
        row.row_count,
        "selected segment has a non-positive row count",
    )?;
    let uncompressed_bytes = positive_database_count(
        row.uncompressed_bytes,
        "selected segment has a non-positive uncompressed byte count",
    )?;
    let object = materialize_object(row, segment_id)?;
    Ok(QuerySegment {
        segment_id,
        schema_id,
        times,
        row_count,
        uncompressed_bytes,
        published_at: row.segment_published_at,
        object,
    })
}

fn positive_database_count(
    value: i64,
    message: &'static str,
) -> Result<NonZeroU64, QuerySnapshotError> {
    let value = u64::try_from(value).map_err(|_| QuerySnapshotError::corrupt(message))?;
    NonZeroU64::new(value).ok_or_else(|| QuerySnapshotError::corrupt(message))
}

fn materialize_object(
    row: &QuerySegmentRow,
    segment_id: SegmentId,
) -> Result<ObjectDescriptor, QuerySnapshotError> {
    let object_id = StoredObjectId::from(required_object(row.object_id)?);
    if required_object(row.object_kind.as_deref())? != "PARQUET_DATA"
        || required_object(row.object_segment_id)? != segment_id.as_uuid()
        || required_object(row.object_state.as_deref())? != "PUBLISHED"
    {
        return Err(QuerySnapshotError::corrupt(
            "active segment does not own one published Parquet object",
        ));
    }
    let object_published_at = required_object(row.object_published_at)?;
    if object_published_at != row.segment_published_at {
        return Err(QuerySnapshotError::corrupt(
            "segment and object publication times differ",
        ));
    }
    let key = ManagedObjectKey::parse_parquet(
        required_object(row.object_key.as_deref())?,
        segment_id,
        object_id,
    )
    .map_err(QuerySnapshotError::storage_model)?;
    let expected_byte_size = u64::try_from(required_object(row.object_expected_byte_size)?)
        .map(ObjectByteSize::new)
        .map_err(|_| QuerySnapshotError::corrupt("stored object byte size is negative"))?;
    let digest = required_object(row.object_blake3_digest.as_deref())?;
    let digest = <[u8; 32]>::try_from(digest)
        .map(ObjectDigest::new)
        .map_err(|_| QuerySnapshotError::corrupt("stored object digest is not 32 bytes"))?;
    if required_object(row.object_media_type.as_deref())? != ObjectMediaType::ParquetData.as_str() {
        return Err(QuerySnapshotError::corrupt(
            "stored Parquet object has an unexpected media type",
        ));
    }
    let format_version = u64::try_from(required_object(row.object_format_version)?)
        .map_err(|_| QuerySnapshotError::corrupt("stored object format version is negative"))?;
    let format_version =
        ObjectFormatVersion::new(format_version).map_err(QuerySnapshotError::storage_model)?;
    ObjectDescriptor::new(
        key,
        expected_byte_size,
        digest,
        ObjectMediaType::ParquetData,
        format_version,
    )
    .map_err(QuerySnapshotError::storage_model)
}

fn required_object<T>(value: Option<T>) -> Result<T, QuerySnapshotError> {
    value.ok_or_else(|| {
        QuerySnapshotError::corrupt("active segment has no complete stored object descriptor")
    })
}

#[derive(Debug, FromRow)]
struct QuerySourceRow {
    source_id: Uuid,
    name: String,
    display_name: String,
    active_schema_id: Uuid,
}

#[derive(Debug, FromRow)]
struct QuerySchemaRow {
    schema_id: Uuid,
    source_id: Uuid,
    version: i64,
    definition: Json<Value>,
}

#[derive(Debug, FromRow)]
struct QuerySegmentRow {
    segment_id: Uuid,
    source_id: Uuid,
    schema_id: Uuid,
    event_day: NaiveDate,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    minimum_ingestion_time: DateTime<Utc>,
    maximum_ingestion_time: DateTime<Utc>,
    row_count: i64,
    uncompressed_bytes: i64,
    segment_published_at: DateTime<Utc>,
    object_id: Option<Uuid>,
    object_kind: Option<String>,
    object_segment_id: Option<Uuid>,
    object_key: Option<String>,
    object_expected_byte_size: Option<i64>,
    object_blake3_digest: Option<Vec<u8>>,
    object_media_type: Option<String>,
    object_format_version: Option<i64>,
    object_state: Option<String>,
    object_published_at: Option<DateTime<Utc>>,
}
