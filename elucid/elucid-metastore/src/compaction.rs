use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::num::NonZeroU64;
use std::sync::LazyLock;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use elucid_catalog::{
    CatalogApplicationError, CatalogModelError, Schema, SchemaId, SchemaVersion, SourceId,
    decode_stored_schema_definition,
};
use elucid_storage::{
    ManagedObjectKey, ManagedObjectKind, ObjectByteSize, ObjectDescriptor, ObjectDigest,
    ObjectFormatVersion, ObjectMediaType, ObjectOwner, PARQUET_FORMAT_VERSION, SegmentId,
    StorageModelError, StoredObjectId,
};
use serde_json::Value;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgAdvisoryLock, PgAdvisoryLockGuard, PgConnection, PgPool};
use sqlx::types::Json;
use sqlx::{Connection as _, Either, FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::error::{is_database_conflict, is_row_decode_error};
use crate::{IngestionSegmentTimes, MetastoreErrorCode};

pub const MAXIMUM_COMPACTION_CANDIDATE_SEGMENTS: u64 = 10_000;
pub const MAXIMUM_COMPACTION_INPUT_SEGMENTS: u64 = 1_000;
pub const MAXIMUM_COMPACTION_OUTPUT_SEGMENTS: usize = 1_000;

static MAINTENANCE_ADVISORY_LOCK: LazyLock<PgAdvisoryLock> =
    LazyLock::new(|| PgAdvisoryLock::new("elucid-maintenance-owner"));

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CompactionRunId(Uuid);

impl CompactionRunId {
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for CompactionRunId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl Display for CompactionRunId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CompactionClaimLimitConfiguration {
    pub maximum_candidate_segments: u64,
    pub maximum_input_segments: u64,
    pub maximum_input_rows: u64,
    pub maximum_input_parquet_bytes: u64,
    pub maximum_input_uncompressed_bytes: u64,
    pub target_output_rows: u64,
    pub target_output_uncompressed_bytes: u64,
    pub minimum_retention: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionClaimLimits {
    maximum_candidate_segments: i64,
    maximum_input_segments: usize,
    maximum_input_rows: u64,
    maximum_input_parquet_bytes: u64,
    maximum_input_uncompressed_bytes: u64,
    target_output_rows: u64,
    target_output_uncompressed_bytes: u64,
    minimum_retention_seconds: i64,
}

impl CompactionClaimLimits {
    pub fn new(
        configuration: CompactionClaimLimitConfiguration,
    ) -> Result<Self, CompactionModelError> {
        if configuration.maximum_candidate_segments == 0
            || configuration.maximum_candidate_segments > MAXIMUM_COMPACTION_CANDIDATE_SEGMENTS
        {
            return Err(CompactionModelError::CandidateSegmentLimitOutOfRange {
                maximum: MAXIMUM_COMPACTION_CANDIDATE_SEGMENTS,
            });
        }
        if configuration.maximum_input_segments < 2
            || configuration.maximum_input_segments > MAXIMUM_COMPACTION_INPUT_SEGMENTS
        {
            return Err(CompactionModelError::InputSegmentLimitOutOfRange {
                maximum: MAXIMUM_COMPACTION_INPUT_SEGMENTS,
            });
        }
        if configuration.maximum_candidate_segments < configuration.maximum_input_segments {
            return Err(CompactionModelError::CandidateLimitBelowInputLimit);
        }
        if configuration.maximum_input_rows == 0
            || configuration.maximum_input_parquet_bytes == 0
            || configuration.maximum_input_uncompressed_bytes == 0
            || configuration.target_output_rows == 0
            || configuration.target_output_uncompressed_bytes == 0
        {
            return Err(CompactionModelError::ByteAndRowLimitsMustBePositive);
        }
        if configuration.minimum_retention.is_zero()
            || configuration.minimum_retention.subsec_nanos() != 0
        {
            return Err(CompactionModelError::MinimumRetentionMustBeWholePositiveSeconds);
        }
        let minimum_retention_seconds = i64::try_from(configuration.minimum_retention.as_secs())
            .map_err(|_| CompactionModelError::MinimumRetentionOutOfRange)?;
        let maximum_candidate_segments = i64::try_from(configuration.maximum_candidate_segments)
            .map_err(|_| CompactionModelError::CandidateSegmentLimitOutOfRange {
                maximum: MAXIMUM_COMPACTION_CANDIDATE_SEGMENTS,
            })?;
        let maximum_input_segments = usize::try_from(configuration.maximum_input_segments)
            .map_err(|_| CompactionModelError::InputSegmentLimitOutOfRange {
                maximum: MAXIMUM_COMPACTION_INPUT_SEGMENTS,
            })?;
        for value in [
            configuration.maximum_input_rows,
            configuration.maximum_input_parquet_bytes,
            configuration.maximum_input_uncompressed_bytes,
            configuration.target_output_rows,
            configuration.target_output_uncompressed_bytes,
        ] {
            i64::try_from(value).map_err(|_| CompactionModelError::LimitOutOfDatabaseRange)?;
        }
        Ok(Self {
            maximum_candidate_segments,
            maximum_input_segments,
            maximum_input_rows: configuration.maximum_input_rows,
            maximum_input_parquet_bytes: configuration.maximum_input_parquet_bytes,
            maximum_input_uncompressed_bytes: configuration.maximum_input_uncompressed_bytes,
            target_output_rows: configuration.target_output_rows,
            target_output_uncompressed_bytes: configuration.target_output_uncompressed_bytes,
            minimum_retention_seconds,
        })
    }

    #[must_use]
    pub const fn maximum_input_segments(self) -> usize {
        self.maximum_input_segments
    }

    #[must_use]
    pub const fn maximum_input_rows(self) -> u64 {
        self.maximum_input_rows
    }

    #[must_use]
    pub const fn maximum_input_parquet_bytes(self) -> u64 {
        self.maximum_input_parquet_bytes
    }

    #[must_use]
    pub const fn maximum_input_uncompressed_bytes(self) -> u64 {
        self.maximum_input_uncompressed_bytes
    }

    #[must_use]
    pub const fn target_output_rows(self) -> u64 {
        self.target_output_rows
    }

    #[must_use]
    pub const fn target_output_uncompressed_bytes(self) -> u64 {
        self.target_output_uncompressed_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum CompactionModelError {
    #[error("compaction candidate limit must be between 1 and {maximum} segments")]
    CandidateSegmentLimitOutOfRange { maximum: u64 },

    #[error("compaction input limit must be between 2 and {maximum} segments")]
    InputSegmentLimitOutOfRange { maximum: u64 },

    #[error("compaction candidate limit must not be below the input-segment limit")]
    CandidateLimitBelowInputLimit,

    #[error("compaction row and byte limits must be positive")]
    ByteAndRowLimitsMustBePositive,

    #[error("compaction limit exceeds the PostgreSQL BIGINT range")]
    LimitOutOfDatabaseRange,

    #[error("minimum compaction retention must be a whole positive number of seconds")]
    MinimumRetentionMustBeWholePositiveSeconds,

    #[error("minimum compaction retention exceeds the PostgreSQL BIGINT range")]
    MinimumRetentionOutOfRange,

    #[error("compaction output row count exceeds the PostgreSQL BIGINT range")]
    OutputRowCountOutOfRange,

    #[error("compaction output uncompressed byte count exceeds the PostgreSQL BIGINT range")]
    OutputUncompressedBytesOutOfRange,

    #[error("compaction output object byte size exceeds the PostgreSQL BIGINT range")]
    OutputObjectBytesOutOfRange,

    #[error("compaction output object format version exceeds the PostgreSQL BIGINT range")]
    OutputObjectFormatVersionOutOfRange,

    #[error("compaction output object uses an unsupported Parquet format version")]
    OutputObjectFormatVersionUnsupported,

    #[error("compaction output object owner does not match its segment")]
    OutputObjectOwnerMismatch,

    #[error("compaction output retention timestamp exceeds PostgreSQL precision")]
    OutputRetentionPrecisionUnsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompactionMetadataErrorKind {
    Conflict,
    Unavailable,
    Corrupt,
}

impl Display for CompactionMetadataErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Conflict => "compaction metadata conflict",
            Self::Unavailable => "compaction metadata unavailable",
            Self::Corrupt => "compaction metadata corrupt",
        })
    }
}

#[derive(Debug)]
pub struct CompactionMetadataError {
    kind: CompactionMetadataErrorKind,
    source: CompactionMetadataErrorSource,
}

impl CompactionMetadataError {
    #[must_use]
    pub const fn kind(&self) -> CompactionMetadataErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn code(&self) -> MetastoreErrorCode {
        match self.kind {
            CompactionMetadataErrorKind::Conflict => MetastoreErrorCode::Conflict,
            CompactionMetadataErrorKind::Unavailable => MetastoreErrorCode::Unavailable,
            CompactionMetadataErrorKind::Corrupt => MetastoreErrorCode::Corrupt,
        }
    }

    #[must_use]
    pub const fn model_error(&self) -> Option<CompactionModelError> {
        match &self.source {
            CompactionMetadataErrorSource::Model(source) => Some(*source),
            CompactionMetadataErrorSource::CatalogApplication(_)
            | CompactionMetadataErrorSource::CatalogModel(_)
            | CompactionMetadataErrorSource::StorageModel(_)
            | CompactionMetadataErrorSource::Database(_)
            | CompactionMetadataErrorSource::Invariant(_) => None,
        }
    }

    fn model(source: CompactionModelError) -> Self {
        Self {
            kind: CompactionMetadataErrorKind::Conflict,
            source: CompactionMetadataErrorSource::Model(source),
        }
    }

    fn unavailable(source: sqlx::Error) -> Self {
        Self {
            kind: CompactionMetadataErrorKind::Unavailable,
            source: CompactionMetadataErrorSource::Database(source),
        }
    }

    fn read(source: sqlx::Error) -> Self {
        let kind = if is_row_decode_error(&source) {
            CompactionMetadataErrorKind::Corrupt
        } else {
            CompactionMetadataErrorKind::Unavailable
        };
        Self {
            kind,
            source: CompactionMetadataErrorSource::Database(source),
        }
    }

    fn write(source: sqlx::Error) -> Self {
        let kind = if is_database_conflict(&source) {
            CompactionMetadataErrorKind::Conflict
        } else {
            CompactionMetadataErrorKind::Unavailable
        };
        Self {
            kind,
            source: CompactionMetadataErrorSource::Database(source),
        }
    }

    fn conflict(message: &'static str) -> Self {
        Self {
            kind: CompactionMetadataErrorKind::Conflict,
            source: CompactionMetadataErrorSource::Invariant(message),
        }
    }

    fn corrupt(message: &'static str) -> Self {
        Self {
            kind: CompactionMetadataErrorKind::Corrupt,
            source: CompactionMetadataErrorSource::Invariant(message),
        }
    }

    fn catalog_application(source: CatalogApplicationError) -> Self {
        Self {
            kind: CompactionMetadataErrorKind::Corrupt,
            source: CompactionMetadataErrorSource::CatalogApplication(source),
        }
    }

    fn catalog_model(source: CatalogModelError) -> Self {
        Self {
            kind: CompactionMetadataErrorKind::Corrupt,
            source: CompactionMetadataErrorSource::CatalogModel(source),
        }
    }

    fn storage_model(source: StorageModelError) -> Self {
        Self {
            kind: CompactionMetadataErrorKind::Corrupt,
            source: CompactionMetadataErrorSource::StorageModel(source),
        }
    }
}

impl Display for CompactionMetadataError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for CompactionMetadataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug, thiserror::Error)]
enum CompactionMetadataErrorSource {
    #[error("compaction configuration or output is invalid")]
    Model(#[source] CompactionModelError),
    #[error("stored schema definition is invalid")]
    CatalogApplication(#[source] CatalogApplicationError),
    #[error("stored catalog identity is invalid")]
    CatalogModel(#[source] CatalogModelError),
    #[error("stored object descriptor is invalid")]
    StorageModel(#[source] StorageModelError),
    #[error("PostgreSQL operation failed")]
    Database(#[source] sqlx::Error),
    #[error("{0}")]
    Invariant(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionInputSegment {
    segment_id: SegmentId,
    times: IngestionSegmentTimes,
    row_count: NonZeroU64,
    uncompressed_bytes: NonZeroU64,
    data_expires_at: DateTime<Utc>,
    published_at: DateTime<Utc>,
    object: ObjectDescriptor,
}

impl CompactionInputSegment {
    #[must_use]
    pub const fn segment_id(&self) -> SegmentId {
        self.segment_id
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
    pub const fn data_expires_at(&self) -> DateTime<Utc> {
        self.data_expires_at
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
pub struct CompactionRunClaim {
    run_id: CompactionRunId,
    source_id: SourceId,
    schema: Schema,
    event_day: NaiveDate,
    inputs: Vec<CompactionInputSegment>,
    input_rows: u64,
    input_parquet_bytes: u64,
    input_uncompressed_bytes: u64,
    data_expires_at: DateTime<Utc>,
}

impl CompactionRunClaim {
    #[must_use]
    pub const fn run_id(&self) -> CompactionRunId {
        self.run_id
    }

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn schema(&self) -> &Schema {
        &self.schema
    }

    #[must_use]
    pub const fn event_day(&self) -> NaiveDate {
        self.event_day
    }

    #[must_use]
    pub fn inputs(&self) -> &[CompactionInputSegment] {
        &self.inputs
    }

    #[must_use]
    pub const fn input_rows(&self) -> u64 {
        self.input_rows
    }

    #[must_use]
    pub const fn input_parquet_bytes(&self) -> u64 {
        self.input_parquet_bytes
    }

    #[must_use]
    pub const fn input_uncompressed_bytes(&self) -> u64 {
        self.input_uncompressed_bytes
    }

    #[must_use]
    pub const fn data_expires_at(&self) -> DateTime<Utc> {
        self.data_expires_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionOutputRegistration {
    run_id: CompactionRunId,
    segment_id: SegmentId,
    source_id: SourceId,
    schema_id: SchemaId,
    times: IngestionSegmentTimes,
    row_count: i64,
    uncompressed_bytes: i64,
    data_expires_at: DateTime<Utc>,
    object: ObjectDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionOutputRegistrationConfiguration {
    pub run_id: CompactionRunId,
    pub segment_id: SegmentId,
    pub source_id: SourceId,
    pub schema_id: SchemaId,
    pub times: IngestionSegmentTimes,
    pub row_count: NonZeroU64,
    pub uncompressed_bytes: NonZeroU64,
    pub data_expires_at: DateTime<Utc>,
    pub object: ObjectDescriptor,
}

impl CompactionOutputRegistration {
    pub fn new(
        configuration: CompactionOutputRegistrationConfiguration,
    ) -> Result<Self, CompactionModelError> {
        if configuration.object.key().owner() != ObjectOwner::Segment(configuration.segment_id)
            || configuration.object.key().kind() != ManagedObjectKind::ParquetData
        {
            return Err(CompactionModelError::OutputObjectOwnerMismatch);
        }
        if configuration.object.format_version().get() != PARQUET_FORMAT_VERSION {
            return Err(CompactionModelError::OutputObjectFormatVersionUnsupported);
        }
        if !configuration
            .data_expires_at
            .timestamp_subsec_nanos()
            .is_multiple_of(1_000)
        {
            return Err(CompactionModelError::OutputRetentionPrecisionUnsupported);
        }
        let row_count = i64::try_from(configuration.row_count.get())
            .map_err(|_| CompactionModelError::OutputRowCountOutOfRange)?;
        let uncompressed_bytes = i64::try_from(configuration.uncompressed_bytes.get())
            .map_err(|_| CompactionModelError::OutputUncompressedBytesOutOfRange)?;
        database_object_byte_size(&configuration.object)?;
        database_object_format_version(&configuration.object)?;
        Ok(Self {
            run_id: configuration.run_id,
            segment_id: configuration.segment_id,
            source_id: configuration.source_id,
            schema_id: configuration.schema_id,
            times: configuration.times,
            row_count,
            uncompressed_bytes,
            data_expires_at: configuration.data_expires_at,
            object: configuration.object,
        })
    }

    #[must_use]
    pub const fn run_id(&self) -> CompactionRunId {
        self.run_id
    }

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
    pub const fn times(&self) -> IngestionSegmentTimes {
        self.times
    }

    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count.unsigned_abs()
    }

    #[must_use]
    pub const fn uncompressed_bytes(&self) -> u64 {
        self.uncompressed_bytes.unsigned_abs()
    }

    #[must_use]
    pub const fn data_expires_at(&self) -> DateTime<Utc> {
        self.data_expires_at
    }

    #[must_use]
    pub const fn object(&self) -> &ObjectDescriptor {
        &self.object
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompactionOutputRegistrationOutcome {
    Registered,
    AlreadyRegistered,
}

#[non_exhaustive]
pub enum MaintenanceOwnership {
    Acquired(MaintenanceOwner),
    HeldElsewhere,
}

pub struct MaintenanceOwner {
    guard: PgAdvisoryLockGuard<'static, PoolConnection<Postgres>>,
}

impl MaintenanceOwner {
    pub async fn claim(
        &mut self,
        limits: &CompactionClaimLimits,
    ) -> Result<Option<CompactionRunClaim>, CompactionMetadataError> {
        claim_compaction(&mut self.guard, *limits).await
    }

    pub async fn release(self) -> Result<(), CompactionMetadataError> {
        self.guard
            .release_now()
            .await
            .map(|_| ())
            .map_err(CompactionMetadataError::unavailable)
    }
}

#[derive(Clone, Debug)]
pub struct CompactionStore {
    pool: PgPool,
}

impl CompactionStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn try_acquire_maintenance(
        &self,
    ) -> Result<MaintenanceOwnership, CompactionMetadataError> {
        let connection = self
            .pool
            .acquire()
            .await
            .map_err(CompactionMetadataError::unavailable)?;
        match MAINTENANCE_ADVISORY_LOCK
            .try_acquire(connection)
            .await
            .map_err(CompactionMetadataError::unavailable)?
        {
            Either::Left(guard) => Ok(MaintenanceOwnership::Acquired(MaintenanceOwner { guard })),
            Either::Right(connection) => {
                drop(connection);
                Ok(MaintenanceOwnership::HeldElsewhere)
            }
        }
    }

    pub async fn register_outputs(
        &self,
        run_id: CompactionRunId,
        outputs: &[CompactionOutputRegistration],
    ) -> Result<CompactionOutputRegistrationOutcome, CompactionMetadataError> {
        if outputs.is_empty() || outputs.len() > MAXIMUM_COMPACTION_OUTPUT_SEGMENTS {
            return Err(CompactionMetadataError::conflict(
                "compaction output count is outside its bounded range",
            ));
        }
        validate_output_identities(run_id, outputs)?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(CompactionMetadataError::unavailable)?;
        let run = load_run_for_update(&mut transaction, run_id).await?;
        let inputs = load_claimed_inputs_for_update(&mut transaction, run_id).await?;
        validate_registered_output_set(&run, &inputs, outputs)?;
        match run.state.as_str() {
            "BUILDING" => {
                insert_compaction_outputs(&mut transaction, outputs).await?;
                let updated = sqlx::query(
                    "UPDATE compaction_runs SET state = 'UPLOADING', updated_at = CURRENT_TIMESTAMP WHERE compaction_run_id = $1 AND state = 'BUILDING'",
                )
                .bind(run_id.as_uuid())
                .execute(&mut *transaction)
                .await
                .map_err(CompactionMetadataError::write)?;
                require_rows(
                    updated.rows_affected(),
                    1,
                    "locked building compaction run did not advance to uploading",
                )?;
                transaction
                    .commit()
                    .await
                    .map_err(CompactionMetadataError::write)?;
                Ok(CompactionOutputRegistrationOutcome::Registered)
            }
            "UPLOADING" => {
                validate_existing_outputs(&mut transaction, run_id, outputs).await?;
                transaction
                    .commit()
                    .await
                    .map_err(CompactionMetadataError::write)?;
                Ok(CompactionOutputRegistrationOutcome::AlreadyRegistered)
            }
            "COMMITTED" | "FAILED" => Err(CompactionMetadataError::conflict(
                "terminal compaction run cannot register outputs",
            )),
            _ => Err(CompactionMetadataError::corrupt(
                "compaction run has an unknown lifecycle state",
            )),
        }
    }
}

async fn claim_compaction(
    connection: &mut PgConnection,
    limits: CompactionClaimLimits,
) -> Result<Option<CompactionRunClaim>, CompactionMetadataError> {
    let mut transaction = connection
        .begin()
        .await
        .map_err(CompactionMetadataError::unavailable)?;
    let candidates = load_candidates(&mut transaction, limits).await?;
    let Some(selected) = choose_candidate_group(candidates, limits)? else {
        transaction
            .commit()
            .await
            .map_err(CompactionMetadataError::write)?;
        return Ok(None);
    };
    let locked = lock_selected_candidates(&mut transaction, &selected, limits).await?;
    if locked != selected {
        return Err(CompactionMetadataError::conflict(
            "compaction candidates changed before their claim was locked",
        ));
    }

    let first = selected.first().ok_or_else(|| {
        CompactionMetadataError::corrupt("beneficial compaction selection has no inputs")
    })?;
    let run_id = CompactionRunId::from(Uuid::now_v7());
    sqlx::query(
        r#"
        INSERT INTO compaction_runs (
            compaction_run_id, source_id, schema_id, event_day, state
        ) VALUES ($1, $2, $3, $4, 'BUILDING')
        "#,
    )
    .bind(run_id.as_uuid())
    .bind(first.source_id.as_uuid())
    .bind(first.schema_id.as_uuid())
    .bind(first.times.event_day())
    .execute(&mut *transaction)
    .await
    .map_err(CompactionMetadataError::write)?;
    let selected_ids = selected
        .iter()
        .map(|candidate| candidate.input.segment_id.as_uuid())
        .collect::<Vec<_>>();
    let claimed = sqlx::query(
        r#"
        UPDATE segments
        SET claimed_by_compaction_run_id = $2, updated_at = CURRENT_TIMESTAMP
        WHERE segment_id = ANY($1::uuid[])
          AND state = 'ACTIVE'
          AND claimed_by_compaction_run_id IS NULL
        "#,
    )
    .bind(&selected_ids)
    .bind(run_id.as_uuid())
    .execute(&mut *transaction)
    .await
    .map_err(CompactionMetadataError::write)?;
    require_rows(
        claimed.rows_affected(),
        selected.len() as u64,
        "locked compaction inputs were not claimed exactly once",
    )?;
    let schema = load_stored_schema(&mut transaction, first.source_id, first.schema_id).await?;
    let claim = materialize_claim(run_id, schema, selected)?;
    transaction
        .commit()
        .await
        .map_err(CompactionMetadataError::write)?;
    Ok(Some(claim))
}

async fn load_candidates(
    transaction: &mut Transaction<'_, Postgres>,
    limits: CompactionClaimLimits,
) -> Result<Vec<CandidateSegment>, CompactionMetadataError> {
    let rows = sqlx::query_as::<_, CandidateRow>(
        r#"
        SELECT
            segment.segment_id,
            segment.source_id,
            segment.schema_id,
            segment.event_day,
            segment.minimum_event_time,
            segment.maximum_event_time,
            segment.minimum_ingestion_time,
            segment.maximum_ingestion_time,
            segment.row_count,
            segment.uncompressed_bytes,
            segment.data_expires_at,
            segment.published_at,
            object.published_at AS object_published_at,
            object.object_id,
            object.object_key,
            object.expected_byte_size,
            object.blake3_digest,
            object.media_type,
            object.format_version
        FROM segments AS segment
        JOIN stored_objects AS object ON object.segment_id = segment.segment_id
        WHERE segment.state = 'ACTIVE'
          AND segment.claimed_by_compaction_run_id IS NULL
          AND segment.row_count < $1
          AND segment.uncompressed_bytes < $2
          AND segment.data_expires_at > CURRENT_TIMESTAMP + make_interval(secs => $3::double precision)
          AND object.kind = 'PARQUET_DATA'
          AND object.state = 'PUBLISHED'
          AND object.expected_byte_size <= $4
        ORDER BY segment.published_at, segment.uncompressed_bytes, segment.segment_id
        LIMIT $5
        "#,
    )
    .bind(database_i64(limits.target_output_rows)?)
    .bind(database_i64(limits.target_output_uncompressed_bytes)?)
    .bind(limits.minimum_retention_seconds)
    .bind(database_i64(limits.maximum_input_parquet_bytes)?)
    .bind(limits.maximum_candidate_segments)
    .fetch_all(&mut **transaction)
    .await
    .map_err(CompactionMetadataError::read)?;
    rows.into_iter().map(materialize_candidate).collect()
}

fn choose_candidate_group(
    candidates: Vec<CandidateSegment>,
    limits: CompactionClaimLimits,
) -> Result<Option<Vec<CandidateSegment>>, CompactionMetadataError> {
    let mut groups = Vec::<CandidateGroup>::new();
    let mut indexes = HashMap::<CandidateGroupKey, usize>::new();
    for candidate in candidates {
        let key = candidate.group_key();
        let index = match indexes.get(&key).copied() {
            Some(index) => index,
            None => {
                let index = groups.len();
                groups.push(CandidateGroup::new(key));
                indexes.insert(key, index);
                index
            }
        };
        groups[index].consider(candidate, limits)?;
    }
    for mut group in groups {
        if let Some(length) = group.beneficial_prefix_length {
            group.candidates.truncate(length);
            return Ok(Some(group.candidates));
        }
    }
    Ok(None)
}

async fn lock_selected_candidates(
    transaction: &mut Transaction<'_, Postgres>,
    selected: &[CandidateSegment],
    limits: CompactionClaimLimits,
) -> Result<Vec<CandidateSegment>, CompactionMetadataError> {
    let identities = selected
        .iter()
        .map(|candidate| candidate.input.segment_id.as_uuid())
        .collect::<Vec<_>>();
    let rows = sqlx::query_as::<_, CandidateRow>(
        r#"
        SELECT
            segment.segment_id,
            segment.source_id,
            segment.schema_id,
            segment.event_day,
            segment.minimum_event_time,
            segment.maximum_event_time,
            segment.minimum_ingestion_time,
            segment.maximum_ingestion_time,
            segment.row_count,
            segment.uncompressed_bytes,
            segment.data_expires_at,
            segment.published_at,
            object.published_at AS object_published_at,
            object.object_id,
            object.object_key,
            object.expected_byte_size,
            object.blake3_digest,
            object.media_type,
            object.format_version
        FROM segments AS segment
        JOIN stored_objects AS object ON object.segment_id = segment.segment_id
        WHERE segment.segment_id = ANY($1::uuid[])
          AND segment.state = 'ACTIVE'
          AND segment.claimed_by_compaction_run_id IS NULL
          AND segment.row_count < $2
          AND segment.uncompressed_bytes < $3
          AND segment.data_expires_at > CURRENT_TIMESTAMP + make_interval(secs => $4::double precision)
          AND object.kind = 'PARQUET_DATA'
          AND object.state = 'PUBLISHED'
          AND object.expected_byte_size <= $5
        ORDER BY segment.published_at, segment.uncompressed_bytes, segment.segment_id
        FOR UPDATE OF segment
        "#,
    )
    .bind(&identities)
    .bind(database_i64(limits.target_output_rows)?)
    .bind(database_i64(limits.target_output_uncompressed_bytes)?)
    .bind(limits.minimum_retention_seconds)
    .bind(database_i64(limits.maximum_input_parquet_bytes)?)
    .fetch_all(&mut **transaction)
    .await
    .map_err(CompactionMetadataError::read)?;
    rows.into_iter().map(materialize_candidate).collect()
}

async fn load_stored_schema(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: SourceId,
    schema_id: SchemaId,
) -> Result<Schema, CompactionMetadataError> {
    let row = sqlx::query_as::<_, StoredSchemaRow>(
        "SELECT schema_id, source_id, version, definition FROM schema_versions WHERE source_id = $1 AND schema_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind(schema_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(CompactionMetadataError::read)?
    .ok_or_else(|| CompactionMetadataError::corrupt("compaction schema is missing"))?;
    let stored_schema_id =
        SchemaId::try_from(row.schema_id).map_err(CompactionMetadataError::catalog_model)?;
    let stored_source_id =
        SourceId::try_from(row.source_id).map_err(CompactionMetadataError::catalog_model)?;
    let version = u64::try_from(row.version)
        .map_err(|_| CompactionMetadataError::corrupt("stored schema version is negative"))?;
    let version = SchemaVersion::new(version).map_err(CompactionMetadataError::catalog_model)?;
    decode_stored_schema_definition(
        stored_schema_id,
        stored_source_id,
        version,
        row.definition.0,
    )
    .map_err(CompactionMetadataError::catalog_application)
}

fn materialize_claim(
    run_id: CompactionRunId,
    schema: Schema,
    selected: Vec<CandidateSegment>,
) -> Result<CompactionRunClaim, CompactionMetadataError> {
    let first = selected.first().ok_or_else(|| {
        CompactionMetadataError::corrupt("claimed compaction run has no input segments")
    })?;
    let source_id = first.source_id;
    let schema_id = first.schema_id;
    let event_day = first.times.event_day();
    if selected.len() < 2
        || schema.source_id() != source_id
        || schema.id() != schema_id
        || selected.iter().any(|candidate| {
            candidate.source_id != source_id
                || candidate.schema_id != schema_id
                || candidate.times.event_day() != event_day
        })
    {
        return Err(CompactionMetadataError::corrupt(
            "claimed compaction inputs are not one homogeneous group",
        ));
    }
    let mut input_rows = 0_u64;
    let mut input_parquet_bytes = 0_u64;
    let mut input_uncompressed_bytes = 0_u64;
    let mut data_expires_at = first.input.data_expires_at;
    let mut inputs = Vec::with_capacity(selected.len());
    for candidate in selected {
        input_rows = checked_sum(input_rows, candidate.input.row_count.get())?;
        input_parquet_bytes = checked_sum(
            input_parquet_bytes,
            candidate.input.object.expected_byte_size().get(),
        )?;
        input_uncompressed_bytes = checked_sum(
            input_uncompressed_bytes,
            candidate.input.uncompressed_bytes.get(),
        )?;
        data_expires_at = data_expires_at.max(candidate.input.data_expires_at);
        inputs.push(candidate.input);
    }
    Ok(CompactionRunClaim {
        run_id,
        source_id: schema.source_id(),
        event_day,
        schema,
        inputs,
        input_rows,
        input_parquet_bytes,
        input_uncompressed_bytes,
        data_expires_at,
    })
}

fn materialize_candidate(row: CandidateRow) -> Result<CandidateSegment, CompactionMetadataError> {
    let source_id =
        SourceId::try_from(row.source_id).map_err(CompactionMetadataError::catalog_model)?;
    let schema_id =
        SchemaId::try_from(row.schema_id).map_err(CompactionMetadataError::catalog_model)?;
    let segment_id = SegmentId::from(row.segment_id);
    let object_id = StoredObjectId::from(row.object_id);
    let key = ManagedObjectKey::parse_parquet(&row.object_key, segment_id, object_id)
        .map_err(CompactionMetadataError::storage_model)?;
    let expected_byte_size = positive_u64(row.expected_byte_size, "object byte size is invalid")?;
    let digest = row
        .blake3_digest
        .as_slice()
        .try_into()
        .map(ObjectDigest::new)
        .map_err(|_| CompactionMetadataError::corrupt("object digest length is invalid"))?;
    if row.media_type != ObjectMediaType::ParquetData.as_str() {
        return Err(CompactionMetadataError::corrupt(
            "compaction input object has an invalid media type",
        ));
    }
    let format_version = positive_u64(row.format_version, "object format version is invalid")?;
    let object = ObjectDescriptor::new(
        key,
        ObjectByteSize::new(expected_byte_size),
        digest,
        ObjectMediaType::ParquetData,
        ObjectFormatVersion::new(format_version).map_err(CompactionMetadataError::storage_model)?,
    )
    .map_err(CompactionMetadataError::storage_model)?;
    let times = IngestionSegmentTimes::new(
        row.event_day,
        row.minimum_event_time,
        row.maximum_event_time,
        row.minimum_ingestion_time,
        row.maximum_ingestion_time,
    )
    .map_err(|_| CompactionMetadataError::corrupt("compaction input time bounds are invalid"))?;
    if row.object_published_at != row.published_at {
        return Err(CompactionMetadataError::corrupt(
            "compaction input segment and object publication times differ",
        ));
    }
    let row_count = NonZeroU64::new(positive_u64(row.row_count, "input row count is invalid")?)
        .ok_or_else(|| CompactionMetadataError::corrupt("input row count is zero"))?;
    let uncompressed_bytes = NonZeroU64::new(positive_u64(
        row.uncompressed_bytes,
        "input uncompressed byte count is invalid",
    )?)
    .ok_or_else(|| CompactionMetadataError::corrupt("input uncompressed byte count is zero"))?;
    Ok(CandidateSegment {
        source_id,
        schema_id,
        times,
        input: CompactionInputSegment {
            segment_id,
            times,
            row_count,
            uncompressed_bytes,
            data_expires_at: row.data_expires_at,
            published_at: row.published_at,
            object,
        },
    })
}

fn validate_output_identities(
    run_id: CompactionRunId,
    outputs: &[CompactionOutputRegistration],
) -> Result<(), CompactionMetadataError> {
    let mut segment_ids = HashSet::with_capacity(outputs.len());
    let mut object_ids = HashSet::with_capacity(outputs.len());
    for output in outputs {
        if output.run_id != run_id {
            return Err(CompactionMetadataError::conflict(
                "compaction output belongs to a different run",
            ));
        }
        if !segment_ids.insert(output.segment_id)
            || !object_ids.insert(output.object.key().object_id())
        {
            return Err(CompactionMetadataError::conflict(
                "compaction output identities are duplicated",
            ));
        }
    }
    Ok(())
}

async fn load_run_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: CompactionRunId,
) -> Result<CompactionRunRow, CompactionMetadataError> {
    sqlx::query_as::<_, CompactionRunRow>(
        r#"
        SELECT compaction_run_id, source_id, schema_id, event_day, state
        FROM compaction_runs
        WHERE compaction_run_id = $1
        FOR UPDATE
        "#,
    )
    .bind(run_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(CompactionMetadataError::read)?
    .ok_or_else(|| CompactionMetadataError::conflict("compaction run is not registered"))
}

async fn load_claimed_inputs_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: CompactionRunId,
) -> Result<Vec<ClaimedInputRow>, CompactionMetadataError> {
    sqlx::query_as::<_, ClaimedInputRow>(
        r#"
        SELECT
            segment_id,
            source_id,
            schema_id,
            event_day,
            minimum_event_time,
            maximum_event_time,
            minimum_ingestion_time,
            maximum_ingestion_time,
            row_count,
            data_expires_at,
            state
        FROM segments
        WHERE claimed_by_compaction_run_id = $1
        ORDER BY segment_id
        FOR UPDATE
        "#,
    )
    .bind(run_id.as_uuid())
    .fetch_all(&mut **transaction)
    .await
    .map_err(CompactionMetadataError::read)
}

fn validate_registered_output_set(
    run: &CompactionRunRow,
    inputs: &[ClaimedInputRow],
    outputs: &[CompactionOutputRegistration],
) -> Result<(), CompactionMetadataError> {
    if run.compaction_run_id != outputs[0].run_id.as_uuid() {
        return Err(CompactionMetadataError::corrupt(
            "locked compaction run identity changed",
        ));
    }
    if inputs.len() < 2 || outputs.len() >= inputs.len() {
        return Err(CompactionMetadataError::conflict(
            "compaction output count does not reduce its inputs",
        ));
    }
    let mut input_rows = 0_u64;
    let mut input_minimum_event_time = inputs[0].minimum_event_time;
    let mut input_maximum_event_time = inputs[0].maximum_event_time;
    let mut input_minimum_ingestion_time = inputs[0].minimum_ingestion_time;
    let mut input_maximum_ingestion_time = inputs[0].maximum_ingestion_time;
    let mut input_deadline = inputs[0].data_expires_at;
    let mut input_ids = HashSet::with_capacity(inputs.len());
    for input in inputs {
        if !input_ids.insert(input.segment_id)
            || input.state != "ACTIVE"
            || input.source_id != run.source_id
            || input.schema_id != run.schema_id
            || input.event_day != run.event_day
        {
            return Err(CompactionMetadataError::conflict(
                "compaction inputs are no longer active and homogeneous",
            ));
        }
        input_rows = checked_sum(
            input_rows,
            positive_u64(input.row_count, "claimed input row count is invalid")?,
        )?;
        input_minimum_event_time = input_minimum_event_time.min(input.minimum_event_time);
        input_maximum_event_time = input_maximum_event_time.max(input.maximum_event_time);
        input_minimum_ingestion_time =
            input_minimum_ingestion_time.min(input.minimum_ingestion_time);
        input_maximum_ingestion_time =
            input_maximum_ingestion_time.max(input.maximum_ingestion_time);
        input_deadline = input_deadline.max(input.data_expires_at);
    }

    let mut output_rows = 0_u64;
    let mut output_minimum_event_time = outputs[0].times.minimum_event_time();
    let mut output_maximum_event_time = outputs[0].times.maximum_event_time();
    let mut output_minimum_ingestion_time = outputs[0].times.minimum_ingestion_time();
    let mut output_maximum_ingestion_time = outputs[0].times.maximum_ingestion_time();
    for output in outputs {
        if output.source_id.as_uuid() != run.source_id
            || output.schema_id.as_uuid() != run.schema_id
            || output.times.event_day() != run.event_day
            || output.data_expires_at != input_deadline
        {
            return Err(CompactionMetadataError::conflict(
                "compaction output owner, day, or retention differs from its inputs",
            ));
        }
        output_rows = checked_sum(output_rows, output.row_count())?;
        output_minimum_event_time =
            output_minimum_event_time.min(output.times.minimum_event_time());
        output_maximum_event_time =
            output_maximum_event_time.max(output.times.maximum_event_time());
        output_minimum_ingestion_time =
            output_minimum_ingestion_time.min(output.times.minimum_ingestion_time());
        output_maximum_ingestion_time =
            output_maximum_ingestion_time.max(output.times.maximum_ingestion_time());
    }
    if output_rows != input_rows
        || output_minimum_event_time != input_minimum_event_time
        || output_maximum_event_time != input_maximum_event_time
        || output_minimum_ingestion_time != input_minimum_ingestion_time
        || output_maximum_ingestion_time != input_maximum_ingestion_time
    {
        return Err(CompactionMetadataError::conflict(
            "compaction output totals or time bounds differ from its inputs",
        ));
    }
    Ok(())
}

async fn insert_compaction_outputs(
    transaction: &mut Transaction<'_, Postgres>,
    outputs: &[CompactionOutputRegistration],
) -> Result<(), CompactionMetadataError> {
    for output in outputs {
        sqlx::query(
            r#"
            INSERT INTO segments (
                segment_id,
                source_id,
                schema_id,
                origin,
                produced_by_compaction_run_id,
                event_day,
                minimum_event_time,
                maximum_event_time,
                minimum_ingestion_time,
                maximum_ingestion_time,
                row_count,
                uncompressed_bytes,
                data_expires_at,
                state
            ) VALUES ($1, $2, $3, 'COMPACTION', $4, $5, $6, $7, $8, $9, $10, $11, $12, 'PREPARED')
            "#,
        )
        .bind(output.segment_id.as_uuid())
        .bind(output.source_id.as_uuid())
        .bind(output.schema_id.as_uuid())
        .bind(output.run_id.as_uuid())
        .bind(output.times.event_day())
        .bind(output.times.minimum_event_time())
        .bind(output.times.maximum_event_time())
        .bind(output.times.minimum_ingestion_time())
        .bind(output.times.maximum_ingestion_time())
        .bind(output.row_count)
        .bind(output.uncompressed_bytes)
        .bind(output.data_expires_at)
        .execute(&mut **transaction)
        .await
        .map_err(CompactionMetadataError::write)?;
        let object = &output.object;
        sqlx::query(
            r#"
            INSERT INTO stored_objects (
                object_id,
                kind,
                segment_id,
                object_key,
                expected_byte_size,
                blake3_digest,
                media_type,
                format_version,
                state
            ) VALUES ($1, 'PARQUET_DATA', $2, $3, $4, $5, $6, $7, 'PLANNED')
            "#,
        )
        .bind(object.key().object_id().as_uuid())
        .bind(output.segment_id.as_uuid())
        .bind(object.key().as_str())
        .bind(database_object_byte_size(object).map_err(CompactionMetadataError::model)?)
        .bind(object.digest().as_bytes().to_vec())
        .bind(object.media_type().as_str())
        .bind(database_object_format_version(object).map_err(CompactionMetadataError::model)?)
        .execute(&mut **transaction)
        .await
        .map_err(CompactionMetadataError::write)?;
    }
    Ok(())
}

async fn validate_existing_outputs(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: CompactionRunId,
    outputs: &[CompactionOutputRegistration],
) -> Result<(), CompactionMetadataError> {
    let mut rows = sqlx::query_as::<_, StoredCompactionOutputRow>(
        r#"
        SELECT
            segment.segment_id,
            segment.source_id,
            segment.schema_id,
            segment.produced_by_compaction_run_id,
            segment.event_day,
            segment.minimum_event_time,
            segment.maximum_event_time,
            segment.minimum_ingestion_time,
            segment.maximum_ingestion_time,
            segment.row_count,
            segment.uncompressed_bytes,
            segment.data_expires_at,
            segment.state AS segment_state,
            object.object_id,
            object.object_key,
            object.expected_byte_size,
            object.blake3_digest,
            object.media_type,
            object.format_version,
            object.state AS object_state
        FROM segments AS segment
        JOIN stored_objects AS object ON object.segment_id = segment.segment_id
        WHERE segment.produced_by_compaction_run_id = $1
        ORDER BY segment.segment_id
        FOR UPDATE OF segment, object
        "#,
    )
    .bind(run_id.as_uuid())
    .fetch_all(&mut **transaction)
    .await
    .map_err(CompactionMetadataError::read)?;
    let mut expected = outputs.iter().collect::<Vec<_>>();
    expected.sort_unstable_by_key(|output| output.segment_id);
    if rows.len() != expected.len() {
        return Err(CompactionMetadataError::conflict(
            "registered compaction output count differs from the retry",
        ));
    }
    rows.sort_unstable_by_key(|row| row.segment_id);
    for (row, output) in rows.iter().zip(expected) {
        if !stored_output_matches(row, output)? {
            return Err(CompactionMetadataError::conflict(
                "registered compaction output metadata differs from the retry",
            ));
        }
    }
    Ok(())
}

fn stored_output_matches(
    row: &StoredCompactionOutputRow,
    output: &CompactionOutputRegistration,
) -> Result<bool, CompactionMetadataError> {
    let object = &output.object;
    Ok(row.segment_state == "PREPARED"
        && matches!(row.object_state.as_str(), "PLANNED" | "UPLOADED")
        && row.segment_id == output.segment_id.as_uuid()
        && row.source_id == output.source_id.as_uuid()
        && row.schema_id == output.schema_id.as_uuid()
        && row.produced_by_compaction_run_id == output.run_id.as_uuid()
        && row.event_day == output.times.event_day()
        && row.minimum_event_time == output.times.minimum_event_time()
        && row.maximum_event_time == output.times.maximum_event_time()
        && row.minimum_ingestion_time == output.times.minimum_ingestion_time()
        && row.maximum_ingestion_time == output.times.maximum_ingestion_time()
        && row.row_count == output.row_count
        && row.uncompressed_bytes == output.uncompressed_bytes
        && row.data_expires_at == output.data_expires_at
        && row.object_id == object.key().object_id().as_uuid()
        && row.object_key == object.key().as_str()
        && row.expected_byte_size
            == database_object_byte_size(object).map_err(CompactionMetadataError::model)?
        && row.blake3_digest.as_slice() == object.digest().as_bytes()
        && row.media_type == object.media_type().as_str()
        && row.format_version
            == database_object_format_version(object).map_err(CompactionMetadataError::model)?)
}

fn database_object_byte_size(object: &ObjectDescriptor) -> Result<i64, CompactionModelError> {
    i64::try_from(object.expected_byte_size().get())
        .map_err(|_| CompactionModelError::OutputObjectBytesOutOfRange)
}

fn database_object_format_version(object: &ObjectDescriptor) -> Result<i64, CompactionModelError> {
    i64::try_from(object.format_version().get())
        .map_err(|_| CompactionModelError::OutputObjectFormatVersionOutOfRange)
}

fn database_i64(value: u64) -> Result<i64, CompactionMetadataError> {
    i64::try_from(value).map_err(|_| CompactionMetadataError::corrupt("validated limit overflowed"))
}

fn positive_u64(value: i64, message: &'static str) -> Result<u64, CompactionMetadataError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| CompactionMetadataError::corrupt(message))
}

fn checked_sum(left: u64, right: u64) -> Result<u64, CompactionMetadataError> {
    left.checked_add(right)
        .ok_or_else(|| CompactionMetadataError::corrupt("compaction resource total overflowed"))
}

const fn ceiling_dividend(value: u64, divisor: u64) -> u64 {
    value / divisor + if value.is_multiple_of(divisor) { 0 } else { 1 }
}

fn require_rows(
    actual: u64,
    expected: u64,
    message: &'static str,
) -> Result<(), CompactionMetadataError> {
    if actual == expected {
        Ok(())
    } else {
        Err(CompactionMetadataError::corrupt(message))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CandidateSegment {
    source_id: SourceId,
    schema_id: SchemaId,
    times: IngestionSegmentTimes,
    input: CompactionInputSegment,
}

impl CandidateSegment {
    const fn group_key(&self) -> CandidateGroupKey {
        CandidateGroupKey {
            source_id: self.source_id,
            schema_id: self.schema_id,
            event_day: self.times.event_day(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CandidateGroupKey {
    source_id: SourceId,
    schema_id: SchemaId,
    event_day: NaiveDate,
}

#[derive(Debug)]
struct CandidateGroup {
    key: CandidateGroupKey,
    candidates: Vec<CandidateSegment>,
    input_rows: u64,
    input_parquet_bytes: u64,
    input_uncompressed_bytes: u64,
    beneficial_prefix_length: Option<usize>,
    stopped: bool,
}

impl CandidateGroup {
    fn new(key: CandidateGroupKey) -> Self {
        Self {
            key,
            candidates: Vec::new(),
            input_rows: 0,
            input_parquet_bytes: 0,
            input_uncompressed_bytes: 0,
            beneficial_prefix_length: None,
            stopped: false,
        }
    }

    fn consider(
        &mut self,
        candidate: CandidateSegment,
        limits: CompactionClaimLimits,
    ) -> Result<(), CompactionMetadataError> {
        if self.stopped {
            return Ok(());
        }
        if candidate.group_key() != self.key {
            return Err(CompactionMetadataError::corrupt(
                "candidate was routed to a different compaction group",
            ));
        }
        if self.candidates.len() == limits.maximum_input_segments {
            self.stopped = true;
            return Ok(());
        }
        let input_rows = checked_sum(self.input_rows, candidate.input.row_count.get())?;
        let input_parquet_bytes = checked_sum(
            self.input_parquet_bytes,
            candidate.input.object.expected_byte_size().get(),
        )?;
        let input_uncompressed_bytes = checked_sum(
            self.input_uncompressed_bytes,
            candidate.input.uncompressed_bytes.get(),
        )?;
        if input_rows > limits.maximum_input_rows
            || input_parquet_bytes > limits.maximum_input_parquet_bytes
            || input_uncompressed_bytes > limits.maximum_input_uncompressed_bytes
        {
            self.stopped = true;
            return Ok(());
        }
        self.input_rows = input_rows;
        self.input_parquet_bytes = input_parquet_bytes;
        self.input_uncompressed_bytes = input_uncompressed_bytes;
        self.candidates.push(candidate);
        let expected_outputs =
            ceiling_dividend(input_rows, limits.target_output_rows).max(ceiling_dividend(
                input_uncompressed_bytes,
                limits.target_output_uncompressed_bytes,
            ));
        if expected_outputs
            < u64::try_from(self.candidates.len()).map_err(|_| {
                CompactionMetadataError::corrupt("compaction candidate count overflowed")
            })?
        {
            self.beneficial_prefix_length = Some(self.candidates.len());
        }
        Ok(())
    }
}

#[derive(Debug, FromRow)]
struct CandidateRow {
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
    data_expires_at: DateTime<Utc>,
    published_at: DateTime<Utc>,
    object_published_at: DateTime<Utc>,
    object_id: Uuid,
    object_key: String,
    expected_byte_size: i64,
    blake3_digest: Vec<u8>,
    media_type: String,
    format_version: i64,
}

#[derive(Debug, FromRow)]
struct StoredSchemaRow {
    schema_id: Uuid,
    source_id: Uuid,
    version: i64,
    definition: Json<Value>,
}

#[derive(Debug, FromRow)]
struct CompactionRunRow {
    compaction_run_id: Uuid,
    source_id: Uuid,
    schema_id: Uuid,
    event_day: NaiveDate,
    state: String,
}

#[derive(Debug, FromRow)]
struct ClaimedInputRow {
    segment_id: Uuid,
    source_id: Uuid,
    schema_id: Uuid,
    event_day: NaiveDate,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    minimum_ingestion_time: DateTime<Utc>,
    maximum_ingestion_time: DateTime<Utc>,
    row_count: i64,
    data_expires_at: DateTime<Utc>,
    state: String,
}

#[derive(Debug, FromRow)]
struct StoredCompactionOutputRow {
    segment_id: Uuid,
    source_id: Uuid,
    schema_id: Uuid,
    produced_by_compaction_run_id: Uuid,
    event_day: NaiveDate,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    minimum_ingestion_time: DateTime<Utc>,
    maximum_ingestion_time: DateTime<Utc>,
    row_count: i64,
    uncompressed_bytes: i64,
    data_expires_at: DateTime<Utc>,
    segment_state: String,
    object_id: Uuid,
    object_key: String,
    expected_byte_size: i64,
    blake3_digest: Vec<u8>,
    media_type: String,
    format_version: i64,
    object_state: String,
}
