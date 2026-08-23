use std::collections::{BTreeMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::mem::size_of;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use chrono::{DateTime, NaiveDate, Utc};
use elucid_catalog::{SchemaId, SourceId};

use crate::{
    AcceptedRow, BatchId, BatchMetadata, DeadLetterEntry, EventTime, IngestionTime, JsonObject,
    NormalizedBatch, NormalizedRecord, NormalizedValue,
};

const TARGET_SEGMENT_ROWS: usize = 50_000;
const TARGET_SEGMENT_ESTIMATED_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_SEGMENT_OPEN_DURATION: Duration = Duration::from_secs(10);
const MAXIMUM_OPEN_SEGMENT_BUILDERS: usize = 32;
const MAXIMUM_SEGMENT_BUILDER_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
// Resident accounting is intentionally conservative but is not allocator telemetry.
const SEGMENT_ROW_VECTOR_CAPACITY_HEADROOM: usize = 2;
const HEAP_ALLOCATION_HEADROOM_BYTES: u64 = 32;
const JSON_OBJECT_ENTRY_HEADROOM_BYTES: u64 = 64;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct EventDay(NaiveDate);

impl EventDay {
    fn from_event_time(event_time: EventTime) -> Self {
        DateTime::<Utc>::from_timestamp_millis(event_time.unix_milliseconds())
            .map(|timestamp| Self(timestamp.date_naive()))
            .expect("normalized event time is representable in UTC")
    }

    #[must_use]
    pub const fn as_date(self) -> NaiveDate {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct SegmentKey {
    source_id: SourceId,
    schema_id: SchemaId,
    event_day: EventDay,
}

impl SegmentKey {
    const fn new(source_id: SourceId, schema_id: SchemaId, event_day: EventDay) -> Self {
        Self {
            source_id,
            schema_id,
            event_day,
        }
    }

    #[must_use]
    pub const fn source_id(self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn schema_id(self) -> SchemaId {
        self.schema_id
    }

    #[must_use]
    pub const fn event_day(self) -> EventDay {
        self.event_day
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct SegmentRow {
    batch_id: BatchId,
    row: AcceptedRow,
    estimated_uncompressed_bytes: u64,
    estimated_resident_bytes: u64,
}

impl SegmentRow {
    #[must_use]
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    #[must_use]
    pub const fn row(&self) -> &AcceptedRow {
        &self.row
    }

    #[must_use]
    pub const fn estimated_uncompressed_bytes(&self) -> u64 {
        self.estimated_uncompressed_bytes
    }

    #[must_use]
    pub const fn estimated_resident_bytes(&self) -> u64 {
        self.estimated_resident_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SegmentTimeBounds {
    minimum_event_time: EventTime,
    maximum_event_time: EventTime,
    minimum_ingestion_time: IngestionTime,
    maximum_ingestion_time: IngestionTime,
}

impl SegmentTimeBounds {
    #[must_use]
    pub const fn minimum_event_time(self) -> EventTime {
        self.minimum_event_time
    }

    #[must_use]
    pub const fn maximum_event_time(self) -> EventTime {
        self.maximum_event_time
    }

    #[must_use]
    pub const fn minimum_ingestion_time(self) -> IngestionTime {
        self.minimum_ingestion_time
    }

    #[must_use]
    pub const fn maximum_ingestion_time(self) -> IngestionTime {
        self.maximum_ingestion_time
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SealingReason {
    RowTarget,
    ByteTarget,
    MaximumAge,
    BuilderLimit,
    CapacityPressure(SegmentCapacity),
    Flush,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SegmentCapacity {
    ResidentMemory,
    Staging,
}

impl Display for SegmentCapacity {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ResidentMemory => "resident memory",
            Self::Staging => "local staging",
        })
    }
}

#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub struct SealedSegment {
    key: SegmentKey,
    rows: Vec<SegmentRow>,
    estimated_uncompressed_bytes: u64,
    estimated_resident_bytes: u64,
    bounds: SegmentTimeBounds,
    sealing_reason: SealingReason,
}

impl SealedSegment {
    #[must_use]
    pub const fn key(&self) -> SegmentKey {
        self.key
    }

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.key.source_id()
    }

    #[must_use]
    pub const fn schema_id(&self) -> SchemaId {
        self.key.schema_id()
    }

    #[must_use]
    pub const fn event_day(&self) -> EventDay {
        self.key.event_day()
    }

    #[must_use]
    pub fn rows(&self) -> &[SegmentRow] {
        &self.rows
    }

    #[must_use]
    pub fn row_count(&self) -> u64 {
        u64::try_from(self.rows.len()).expect("segment row target fits u64")
    }

    #[must_use]
    pub const fn estimated_uncompressed_bytes(&self) -> u64 {
        self.estimated_uncompressed_bytes
    }

    #[must_use]
    pub const fn estimated_resident_bytes(&self) -> u64 {
        self.estimated_resident_bytes
    }

    #[must_use]
    pub const fn bounds(&self) -> SegmentTimeBounds {
        self.bounds
    }

    #[must_use]
    pub const fn sealing_reason(&self) -> SealingReason {
        self.sealing_reason
    }
}

#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub struct SegmentBuildSummary {
    metadata: BatchMetadata,
    dead_letters: Vec<DeadLetterEntry>,
    ignored_records: u64,
}

impl SegmentBuildSummary {
    #[must_use]
    pub const fn metadata(&self) -> BatchMetadata {
        self.metadata
    }

    #[must_use]
    pub fn dead_letters(&self) -> &[DeadLetterEntry] {
        &self.dead_letters
    }

    #[must_use]
    pub const fn ignored_records(&self) -> u64 {
        self.ignored_records
    }
}

#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum SegmentBuildOutcome {
    Accepted(SegmentBuildSummary),
    Deferred {
        batch: NormalizedBatch,
        capacity: SegmentCapacity,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
/// Capacity reserved for the estimated uncompressed bytes of rows awaiting the staging writer.
pub struct SegmentStagingCapacity(NonZeroU64);

impl SegmentStagingCapacity {
    pub fn new(value: u64) -> Result<Self, SegmentBuilderModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(SegmentBuilderModelError::StagingCapacityMustBePositive)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SegmentBuilderUsage {
    open_builders: usize,
    sealed_segments: usize,
    estimated_resident_bytes: u64,
    estimated_staging_bytes: u64,
    maximum_estimated_resident_bytes: u64,
    maximum_staging_bytes: u64,
}

impl SegmentBuilderUsage {
    #[must_use]
    pub const fn open_builders(self) -> usize {
        self.open_builders
    }

    #[must_use]
    pub const fn sealed_segments(self) -> usize {
        self.sealed_segments
    }

    #[must_use]
    /// Estimated resident memory owned by open and sealed builders.
    pub const fn estimated_resident_bytes(self) -> u64 {
        self.estimated_resident_bytes
    }

    #[must_use]
    /// Estimated uncompressed bytes reserved for all rows not yet handed to the staging writer.
    pub const fn estimated_staging_bytes(self) -> u64 {
        self.estimated_staging_bytes
    }

    #[must_use]
    pub const fn maximum_estimated_resident_bytes(self) -> u64 {
        self.maximum_estimated_resident_bytes
    }

    #[must_use]
    pub const fn maximum_staging_bytes(self) -> u64 {
        self.maximum_staging_bytes
    }
}

#[derive(Clone, Copy, Debug)]
struct SegmentBuildLimits {
    target_rows: usize,
    target_estimated_bytes: u64,
    maximum_open_duration: Duration,
    maximum_open_builders: usize,
    maximum_estimated_resident_bytes: u64,
    maximum_staging_bytes: u64,
}

impl SegmentBuildLimits {
    fn standard(staging_capacity: SegmentStagingCapacity) -> Self {
        Self {
            target_rows: TARGET_SEGMENT_ROWS,
            target_estimated_bytes: TARGET_SEGMENT_ESTIMATED_BYTES,
            maximum_open_duration: MAXIMUM_SEGMENT_OPEN_DURATION,
            maximum_open_builders: MAXIMUM_OPEN_SEGMENT_BUILDERS,
            maximum_estimated_resident_bytes: MAXIMUM_SEGMENT_BUILDER_MEMORY_BYTES,
            maximum_staging_bytes: staging_capacity.get(),
        }
    }

    #[cfg(test)]
    const fn for_test(
        target_rows: usize,
        target_estimated_bytes: u64,
        maximum_open_duration: Duration,
        maximum_open_builders: usize,
        maximum_estimated_resident_bytes: u64,
        maximum_staging_bytes: u64,
    ) -> Self {
        Self {
            target_rows,
            target_estimated_bytes,
            maximum_open_duration,
            maximum_open_builders,
            maximum_estimated_resident_bytes,
            maximum_staging_bytes,
        }
    }

    const fn is_valid(self) -> bool {
        self.target_rows > 0
            && self.target_estimated_bytes > 0
            && !self.maximum_open_duration.is_zero()
            && self.maximum_open_builders > 0
            && self.maximum_estimated_resident_bytes > 0
            && self.maximum_staging_bytes > 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum SegmentBuilderModelError {
    #[error("segment staging capacity must be positive")]
    StagingCapacityMustBePositive,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SegmentBuildError {
    #[error("estimated segment bytes overflow u64")]
    EstimatedBytesOverflow,

    #[error("segment builder clock moved backwards")]
    ClockMovedBackwards,

    #[error("segment builder access sequence is exhausted")]
    AccessSequenceExhausted,

    #[error(
        "normalized batch requires {estimated_bytes} bytes of {capacity} capacity but the maximum is {maximum_bytes}"
    )]
    BatchExceedsCapacity {
        capacity: SegmentCapacity,
        estimated_bytes: u64,
        maximum_bytes: u64,
    },

    #[error("segment builder state is inconsistent")]
    StateInconsistent,
}

#[derive(Debug)]
#[non_exhaustive]
pub struct SegmentBuilders {
    limits: SegmentBuildLimits,
    open: BTreeMap<SegmentKey, OpenSegment>,
    sealed: VecDeque<SealedSegment>,
    estimated_resident_bytes: u64,
    estimated_staging_bytes: u64,
    access_sequence: u64,
    last_observed_at: Option<Instant>,
}

impl SegmentBuilders {
    #[must_use]
    pub fn new(staging_capacity: SegmentStagingCapacity) -> Self {
        Self::with_limits(SegmentBuildLimits::standard(staging_capacity))
    }

    fn with_limits(limits: SegmentBuildLimits) -> Self {
        assert!(
            limits.is_valid(),
            "segment builder implementation limits are consistent"
        );
        Self {
            limits,
            open: BTreeMap::new(),
            sealed: VecDeque::new(),
            estimated_resident_bytes: 0,
            estimated_staging_bytes: 0,
            access_sequence: 0,
            last_observed_at: None,
        }
    }

    #[must_use]
    pub fn usage(&self) -> SegmentBuilderUsage {
        SegmentBuilderUsage {
            open_builders: self.open.len(),
            sealed_segments: self.sealed.len(),
            estimated_resident_bytes: self.estimated_resident_bytes,
            estimated_staging_bytes: self.estimated_staging_bytes,
            maximum_estimated_resident_bytes: self.limits.maximum_estimated_resident_bytes,
            maximum_staging_bytes: self.limits.maximum_staging_bytes,
        }
    }

    /// Returns the remaining time before the oldest open builder must seal.
    ///
    /// # Errors
    ///
    /// Returns an error if `observed_at` precedes an earlier observation.
    pub fn next_expiration_in(
        &self,
        observed_at: Instant,
    ) -> Result<Option<Duration>, SegmentBuildError> {
        if self
            .last_observed_at
            .is_some_and(|previous| observed_at < previous)
        {
            return Err(SegmentBuildError::ClockMovedBackwards);
        }
        self.open.values().try_fold(None, |earliest, builder| {
            let age = observed_at
                .checked_duration_since(builder.opened_at)
                .ok_or(SegmentBuildError::ClockMovedBackwards)?;
            let remaining = self.limits.maximum_open_duration.saturating_sub(age);
            Ok(Some(earliest.map_or(remaining, |current: Duration| {
                current.min(remaining)
            })))
        })
    }

    /// Adds one normalized batch or returns it unchanged when queued segment data must drain first.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid time ordering, arithmetic overflow, a single batch larger than
    /// either complete capacity budget, or an internal state violation.
    pub fn push_batch(
        &mut self,
        batch: NormalizedBatch,
        observed_at: Instant,
    ) -> Result<SegmentBuildOutcome, SegmentBuildError> {
        self.observe_time(observed_at)?;
        self.seal_expired_at(observed_at)?;
        let row_plan = plan_batch_rows(&batch)?;
        if row_plan.total_estimated_resident_bytes > self.limits.maximum_estimated_resident_bytes {
            return Err(SegmentBuildError::BatchExceedsCapacity {
                capacity: SegmentCapacity::ResidentMemory,
                estimated_bytes: row_plan.total_estimated_resident_bytes,
                maximum_bytes: self.limits.maximum_estimated_resident_bytes,
            });
        }
        if row_plan.total_estimated_uncompressed_bytes > self.limits.maximum_staging_bytes {
            return Err(SegmentBuildError::BatchExceedsCapacity {
                capacity: SegmentCapacity::Staging,
                estimated_bytes: row_plan.total_estimated_uncompressed_bytes,
                maximum_bytes: self.limits.maximum_staging_bytes,
            });
        }
        let projected_resident_bytes = self
            .estimated_resident_bytes
            .checked_add(row_plan.total_estimated_resident_bytes)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
        let projected_staging_bytes = self
            .estimated_staging_bytes
            .checked_add(row_plan.total_estimated_uncompressed_bytes)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
        let capacity_pressure =
            if projected_resident_bytes > self.limits.maximum_estimated_resident_bytes {
                Some(SegmentCapacity::ResidentMemory)
            } else if projected_staging_bytes > self.limits.maximum_staging_bytes {
                Some(SegmentCapacity::Staging)
            } else {
                None
            };
        if let Some(capacity) = capacity_pressure {
            self.seal_all_with_reason(SealingReason::CapacityPressure(capacity))?;
            return Ok(SegmentBuildOutcome::Deferred { batch, capacity });
        }

        let accepted_rows = u64::try_from(row_plan.rows.len())
            .map_err(|_| SegmentBuildError::AccessSequenceExhausted)?;
        self.access_sequence
            .checked_add(accepted_rows)
            .ok_or(SegmentBuildError::AccessSequenceExhausted)?;

        let metadata = batch.metadata();
        let ignored_records = batch.ignored_records();
        let records = batch.into_records();
        let mut dead_letters = Vec::new();
        let mut planned_rows = row_plan.rows.into_iter();
        for record in records {
            match record {
                NormalizedRecord::Accepted(row) => {
                    let planned = planned_rows
                        .next()
                        .ok_or(SegmentBuildError::StateInconsistent)?;
                    self.push_row(metadata, row, planned, observed_at)?;
                }
                NormalizedRecord::DeadLetter(entry) => dead_letters.push(entry),
            }
        }
        if planned_rows.next().is_some() {
            return Err(SegmentBuildError::StateInconsistent);
        }
        Ok(SegmentBuildOutcome::Accepted(SegmentBuildSummary {
            metadata,
            dead_letters,
            ignored_records,
        }))
    }

    /// Seals builders whose maximum open duration has elapsed.
    ///
    /// # Errors
    ///
    /// Returns an error if `observed_at` precedes an earlier observation or state accounting fails.
    pub fn seal_expired(&mut self, observed_at: Instant) -> Result<u64, SegmentBuildError> {
        self.observe_time(observed_at)?;
        self.seal_expired_at(observed_at)
    }

    /// Seals every currently open builder in deterministic key order.
    ///
    /// # Errors
    ///
    /// Returns an error only if internal builder state or byte accounting is inconsistent.
    pub fn flush_all(&mut self) -> Result<u64, SegmentBuildError> {
        self.seal_all_with_reason(SealingReason::Flush)
    }

    /// Transfers ownership of the oldest sealed segment to the staging boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if resident or staging accounting is inconsistent.
    pub fn take_next_sealed(&mut self) -> Result<Option<SealedSegment>, SegmentBuildError> {
        let Some(segment) = self.sealed.front() else {
            return Ok(None);
        };
        let estimated_resident_bytes = self
            .estimated_resident_bytes
            .checked_sub(segment.estimated_resident_bytes)
            .ok_or(SegmentBuildError::StateInconsistent)?;
        let estimated_staging_bytes = self
            .estimated_staging_bytes
            .checked_sub(segment.estimated_uncompressed_bytes)
            .ok_or(SegmentBuildError::StateInconsistent)?;
        let segment = self
            .sealed
            .pop_front()
            .ok_or(SegmentBuildError::StateInconsistent)?;
        self.estimated_resident_bytes = estimated_resident_bytes;
        self.estimated_staging_bytes = estimated_staging_bytes;
        Ok(Some(segment))
    }

    fn observe_time(&mut self, observed_at: Instant) -> Result<(), SegmentBuildError> {
        if self
            .last_observed_at
            .is_some_and(|previous| observed_at < previous)
        {
            return Err(SegmentBuildError::ClockMovedBackwards);
        }
        self.last_observed_at = Some(observed_at);
        Ok(())
    }

    fn seal_expired_at(&mut self, observed_at: Instant) -> Result<u64, SegmentBuildError> {
        let keys = self
            .open
            .iter()
            .filter_map(|(key, builder)| {
                observed_at
                    .checked_duration_since(builder.opened_at)
                    .filter(|age| *age >= self.limits.maximum_open_duration)
                    .map(|_| *key)
            })
            .collect::<Vec<_>>();
        let count = u64::try_from(keys.len()).map_err(|_| SegmentBuildError::StateInconsistent)?;
        for key in keys {
            self.seal(key, SealingReason::MaximumAge)?;
        }
        Ok(count)
    }

    fn push_row(
        &mut self,
        metadata: BatchMetadata,
        row: AcceptedRow,
        planned: PlannedSegmentRow,
        observed_at: Instant,
    ) -> Result<(), SegmentBuildError> {
        let key = SegmentKey::new(
            metadata.catalog().source_id(),
            metadata.catalog().target_schema_id(),
            planned.event_day,
        );
        if let Some(builder) = self.open.get(&key) {
            let projected_bytes = builder
                .estimated_uncompressed_bytes
                .checked_add(planned.estimated_uncompressed_bytes)
                .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
            if projected_bytes > self.limits.target_estimated_bytes {
                self.seal(key, SealingReason::ByteTarget)?;
            }
        }
        if !self.open.contains_key(&key) && self.open.len() == self.limits.maximum_open_builders {
            let lru_key = self
                .open
                .iter()
                .min_by_key(|(candidate_key, builder)| {
                    (builder.last_touched_sequence, **candidate_key)
                })
                .map(|(candidate_key, _)| *candidate_key)
                .ok_or(SegmentBuildError::StateInconsistent)?;
            self.seal(lru_key, SealingReason::BuilderLimit)?;
        }

        self.access_sequence = self
            .access_sequence
            .checked_add(1)
            .ok_or(SegmentBuildError::AccessSequenceExhausted)?;
        let segment_row = SegmentRow {
            batch_id: metadata.batch_id(),
            row,
            estimated_uncompressed_bytes: planned.estimated_uncompressed_bytes,
            estimated_resident_bytes: planned.estimated_resident_bytes,
        };
        if let Some(builder) = self.open.get_mut(&key) {
            builder.push(segment_row, self.access_sequence)?;
        } else {
            self.open.insert(
                key,
                OpenSegment::new(segment_row, observed_at, self.access_sequence),
            );
        }
        self.estimated_resident_bytes = self
            .estimated_resident_bytes
            .checked_add(planned.estimated_resident_bytes)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
        self.estimated_staging_bytes = self
            .estimated_staging_bytes
            .checked_add(planned.estimated_uncompressed_bytes)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;

        let builder = self
            .open
            .get(&key)
            .ok_or(SegmentBuildError::StateInconsistent)?;
        if builder.rows.len() >= self.limits.target_rows {
            self.seal(key, SealingReason::RowTarget)?;
        } else if builder.estimated_uncompressed_bytes >= self.limits.target_estimated_bytes {
            self.seal(key, SealingReason::ByteTarget)?;
        }
        Ok(())
    }

    fn seal_all_with_reason(&mut self, reason: SealingReason) -> Result<u64, SegmentBuildError> {
        let keys = self.open.keys().copied().collect::<Vec<_>>();
        let count = u64::try_from(keys.len()).map_err(|_| SegmentBuildError::StateInconsistent)?;
        for key in keys {
            self.seal(key, reason)?;
        }
        Ok(count)
    }

    fn seal(&mut self, key: SegmentKey, reason: SealingReason) -> Result<(), SegmentBuildError> {
        let builder = self
            .open
            .remove(&key)
            .ok_or(SegmentBuildError::StateInconsistent)?;
        let segment = builder.seal(key, reason)?;
        self.sealed.push_back(segment);
        Ok(())
    }
}

#[derive(Debug)]
struct OpenSegment {
    rows: Vec<SegmentRow>,
    estimated_uncompressed_bytes: u64,
    estimated_resident_bytes: u64,
    opened_at: Instant,
    last_touched_sequence: u64,
}

impl OpenSegment {
    fn new(row: SegmentRow, opened_at: Instant, access_sequence: u64) -> Self {
        let estimated_uncompressed_bytes = row.estimated_uncompressed_bytes;
        let estimated_resident_bytes = row.estimated_resident_bytes;
        Self {
            rows: vec![row],
            estimated_uncompressed_bytes,
            estimated_resident_bytes,
            opened_at,
            last_touched_sequence: access_sequence,
        }
    }

    fn push(&mut self, row: SegmentRow, access_sequence: u64) -> Result<(), SegmentBuildError> {
        let estimated_uncompressed_bytes = self
            .estimated_uncompressed_bytes
            .checked_add(row.estimated_uncompressed_bytes)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
        let estimated_resident_bytes = self
            .estimated_resident_bytes
            .checked_add(row.estimated_resident_bytes)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
        self.rows.push(row);
        self.estimated_uncompressed_bytes = estimated_uncompressed_bytes;
        self.estimated_resident_bytes = estimated_resident_bytes;
        self.last_touched_sequence = access_sequence;
        Ok(())
    }

    fn seal(
        self,
        key: SegmentKey,
        sealing_reason: SealingReason,
    ) -> Result<SealedSegment, SegmentBuildError> {
        if self.rows.is_empty() {
            return Err(SegmentBuildError::StateInconsistent);
        }
        let mut rows = self.rows;
        rows.sort_by(|left, right| {
            left.row
                .event_time()
                .cmp(&right.row.event_time())
                .then_with(|| left.row.event_id().cmp(&right.row.event_id()))
        });
        let first = rows.first().ok_or(SegmentBuildError::StateInconsistent)?;
        let last = rows.last().ok_or(SegmentBuildError::StateInconsistent)?;
        let mut minimum_ingestion_time = first.row.ingestion_time();
        let mut maximum_ingestion_time = minimum_ingestion_time;
        for row in rows.iter().skip(1) {
            minimum_ingestion_time = minimum_ingestion_time.min(row.row.ingestion_time());
            maximum_ingestion_time = maximum_ingestion_time.max(row.row.ingestion_time());
        }
        Ok(SealedSegment {
            key,
            bounds: SegmentTimeBounds {
                minimum_event_time: first.row.event_time(),
                maximum_event_time: last.row.event_time(),
                minimum_ingestion_time,
                maximum_ingestion_time,
            },
            rows,
            estimated_uncompressed_bytes: self.estimated_uncompressed_bytes,
            estimated_resident_bytes: self.estimated_resident_bytes,
            sealing_reason,
        })
    }
}

struct BatchRowPlan {
    total_estimated_uncompressed_bytes: u64,
    total_estimated_resident_bytes: u64,
    rows: Vec<PlannedSegmentRow>,
}

#[derive(Clone, Copy, Debug)]
struct PlannedSegmentRow {
    event_day: EventDay,
    estimated_uncompressed_bytes: u64,
    estimated_resident_bytes: u64,
}

fn plan_batch_rows(batch: &NormalizedBatch) -> Result<BatchRowPlan, SegmentBuildError> {
    let mut total_estimated_uncompressed_bytes = 0_u64;
    let mut total_estimated_resident_bytes = 0_u64;
    let mut rows = Vec::new();
    for record in batch.records() {
        let NormalizedRecord::Accepted(row) = record else {
            continue;
        };
        let estimates = estimate_row(row)?;
        total_estimated_uncompressed_bytes = total_estimated_uncompressed_bytes
            .checked_add(estimates.uncompressed_bytes)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
        total_estimated_resident_bytes = total_estimated_resident_bytes
            .checked_add(estimates.resident_bytes)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
        rows.push(PlannedSegmentRow {
            event_day: EventDay::from_event_time(row.event_time()),
            estimated_uncompressed_bytes: estimates.uncompressed_bytes,
            estimated_resident_bytes: estimates.resident_bytes,
        });
    }
    Ok(BatchRowPlan {
        total_estimated_uncompressed_bytes,
        total_estimated_resident_bytes,
        rows,
    })
}

struct RowEstimates {
    uncompressed_bytes: u64,
    resident_bytes: u64,
}

fn estimate_row(row: &AcceptedRow) -> Result<RowEstimates, SegmentBuildError> {
    let mut uncompressed_bytes = 32_u64;
    for field in row.fields() {
        uncompressed_bytes = uncompressed_bytes
            .checked_add(estimate_uncompressed_value(field.value())?)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
    }
    let remainder_bytes = match row.remainder() {
        Some(remainder) => estimate_json(remainder)?
            .checked_add(5)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?,
        None => 1,
    };
    uncompressed_bytes = uncompressed_bytes
        .checked_add(remainder_bytes)
        .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
    Ok(RowEstimates {
        uncompressed_bytes,
        resident_bytes: estimate_resident_row(row)?,
    })
}

fn estimate_uncompressed_value(value: &NormalizedValue) -> Result<u64, SegmentBuildError> {
    match value {
        NormalizedValue::Null => Ok(1),
        NormalizedValue::Bool(_) => Ok(2),
        NormalizedValue::Int32(_) | NormalizedValue::UInt32(_) | NormalizedValue::Float32(_) => {
            Ok(5)
        }
        NormalizedValue::Int64(_)
        | NormalizedValue::UInt64(_)
        | NormalizedValue::Float64(_)
        | NormalizedValue::Datetime(_) => Ok(9),
        NormalizedValue::Utf8(value) => u64::try_from(value.len())
            .map_err(|_| SegmentBuildError::EstimatedBytesOverflow)?
            .checked_add(5)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow),
    }
}

fn estimate_resident_row(row: &AcceptedRow) -> Result<u64, SegmentBuildError> {
    let segment_row_bytes = bytes_for_items(
        SEGMENT_ROW_VECTOR_CAPACITY_HEADROOM,
        size_of::<SegmentRow>(),
    )?;
    let field_bytes = bytes_for_items(row.fields().len(), size_of::<crate::NormalizedField>())?;
    let mut bytes = segment_row_bytes
        .checked_add(estimated_allocation(field_bytes)?)
        .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
    for field in row.fields() {
        if let NormalizedValue::Utf8(value) = field.value() {
            bytes = bytes
                .checked_add(estimated_allocation(usize_bytes(value.capacity())?)?)
                .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
        }
    }
    if let Some(remainder) = row.remainder() {
        bytes = bytes
            .checked_add(estimate_json_object_resident(remainder)?)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
    }
    Ok(bytes)
}

fn estimate_json_object_resident(value: &JsonObject) -> Result<u64, SegmentBuildError> {
    estimate_json_map_resident(value.as_map())
}

fn estimate_json_map_resident(
    values: &serde_json::Map<String, serde_json::Value>,
) -> Result<u64, SegmentBuildError> {
    let entry_bytes = bytes_for_items(values.len(), size_of::<(String, serde_json::Value)>())?;
    let entry_overhead = usize_bytes(values.len())?
        .checked_mul(JSON_OBJECT_ENTRY_HEADROOM_BYTES)
        .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
    let mut bytes = entry_bytes
        .checked_add(entry_overhead)
        .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
    for (key, value) in values {
        bytes = bytes
            .checked_add(estimated_allocation(usize_bytes(key.capacity())?)?)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
        bytes = bytes
            .checked_add(estimate_json_value_resident(value)?)
            .ok_or(SegmentBuildError::EstimatedBytesOverflow)?;
    }
    Ok(bytes)
}

fn estimate_json_value_resident(value: &serde_json::Value) -> Result<u64, SegmentBuildError> {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            Ok(0)
        }
        serde_json::Value::String(value) => estimated_allocation(usize_bytes(value.capacity())?),
        serde_json::Value::Array(values) => {
            let inline_bytes = bytes_for_items(values.capacity(), size_of::<serde_json::Value>())?;
            values.iter().try_fold(
                estimated_allocation(inline_bytes)?,
                |resident_bytes, value| {
                    resident_bytes
                        .checked_add(estimate_json_value_resident(value)?)
                        .ok_or(SegmentBuildError::EstimatedBytesOverflow)
                },
            )
        }
        serde_json::Value::Object(values) => estimate_json_map_resident(values),
    }
}

fn bytes_for_items(count: usize, item_bytes: usize) -> Result<u64, SegmentBuildError> {
    usize_bytes(count)?
        .checked_mul(usize_bytes(item_bytes)?)
        .ok_or(SegmentBuildError::EstimatedBytesOverflow)
}

fn usize_bytes(bytes: usize) -> Result<u64, SegmentBuildError> {
    u64::try_from(bytes).map_err(|_| SegmentBuildError::EstimatedBytesOverflow)
}

const fn estimated_allocation(bytes: u64) -> Result<u64, SegmentBuildError> {
    if bytes == 0 {
        Ok(0)
    } else {
        match bytes.checked_add(HEAP_ALLOCATION_HEADROOM_BYTES) {
            Some(bytes) => Ok(bytes),
            None => Err(SegmentBuildError::EstimatedBytesOverflow),
        }
    }
}

fn estimate_json(value: &JsonObject) -> Result<u64, SegmentBuildError> {
    let mut counter = ByteCounter::default();
    serde_json::to_writer(&mut counter, value.as_map())
        .map_err(|_| SegmentBuildError::EstimatedBytesOverflow)?;
    Ok(counter.bytes)
}

#[derive(Default)]
struct ByteCounter {
    bytes: u64,
}

impl std::io::Write for ByteCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let bytes = u64::try_from(buffer.len())
            .map_err(|_| std::io::Error::other("serialized JSON length exceeds u64"))?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| std::io::Error::other("serialized JSON length exceeds u64"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use elucid_catalog::{
        DeclarationDigest, DefinitionDigests, EventTimeFormat, EventTimeMapping, FieldId,
        FieldMapping, IngestionProfile, IngestionProfileRevision, IngestionProfileRevisionId,
        Input, InputId, InputName, JsonPointer, MaterializedDigest, MaximumRecordBytes,
        Nullability, ProfileRevision, Schema, SchemaId, SchemaVersion, Source, SourceId,
        SourceName, UserField, UserFieldName, UserLogicalType,
    };
    use uuid::Uuid;

    use super::{
        SealingReason, SegmentBuildLimits, SegmentBuildOutcome, SegmentBuilders, SegmentCapacity,
    };
    use crate::{
        BatchId, BatchMetadata, DeadLetterCode, IngestionTime, PinnedCatalogIdentities,
        normalize_records,
    };

    #[test]
    fn builders_share_keys_across_batches_and_seal_deterministically_sorted_rows() {
        let fixture = Fixture::new();
        let limits = SegmentBuildLimits::for_test(
            100,
            1_000_000,
            Duration::from_secs(60),
            8,
            1_000_000,
            1_000_000,
        );
        let mut builders = SegmentBuilders::with_limits(limits);
        let observed_at = Instant::now();
        let first = fixture.normalize(
            10,
            br#"{"timestamp":"2026-08-20T12:00:02Z","message":"second"}
not-json
{"timestamp":"2026-08-21T00:00:00Z","message":"next-day"}"#,
        );
        let second = fixture.normalize(
            11,
            br#"{"timestamp":"2026-08-20T12:00:01Z","message":"first"}"#,
        );

        let first_summary = accepted_summary(
            builders
                .push_batch(first, observed_at)
                .expect("first batch is buildable"),
        );
        assert_eq!(first_summary.dead_letters().len(), 1);
        assert_eq!(
            first_summary.dead_letters()[0].code(),
            DeadLetterCode::ParseFailed
        );
        let second_summary = accepted_summary(
            builders
                .push_batch(second, observed_at + Duration::from_secs(1))
                .expect("second batch is buildable"),
        );
        assert!(second_summary.dead_letters().is_empty());
        assert_eq!(builders.usage().open_builders(), 2);
        assert_eq!(builders.flush_all().expect("flush builders"), 2);

        let first_segment = builders
            .take_next_sealed()
            .expect("queue accounting")
            .expect("first event day");
        assert_eq!(
            first_segment.event_day().as_date().to_string(),
            "2026-08-20"
        );
        assert_eq!(first_segment.sealing_reason(), SealingReason::Flush);
        assert_eq!(first_segment.row_count(), 2);
        assert_eq!(
            first_segment
                .rows()
                .iter()
                .map(|row| row.row().event_time().unix_milliseconds())
                .collect::<Vec<_>>(),
            [1_787_227_201_000, 1_787_227_202_000]
        );
        assert_eq!(first_segment.rows()[0].batch_id(), fixture.batch_id(11));
        assert_eq!(first_segment.rows()[1].batch_id(), fixture.batch_id(10));
        assert_eq!(
            first_segment.bounds().minimum_event_time(),
            first_segment.rows()[0].row().event_time()
        );
        assert_eq!(
            first_segment.bounds().maximum_event_time(),
            first_segment.rows()[1].row().event_time()
        );
        assert!(first_segment.estimated_uncompressed_bytes() > 0);

        let second_segment = builders
            .take_next_sealed()
            .expect("queue accounting")
            .expect("second event day");
        assert_eq!(
            second_segment.event_day().as_date().to_string(),
            "2026-08-21"
        );
        assert_eq!(second_segment.row_count(), 1);
        assert!(
            builders
                .take_next_sealed()
                .expect("queue accounting")
                .is_none()
        );
        assert_eq!(builders.usage().estimated_resident_bytes(), 0);
        assert_eq!(builders.usage().estimated_staging_bytes(), 0);
    }

    #[test]
    fn row_age_and_lru_limits_seal_the_expected_builder() {
        let fixture = Fixture::new();
        let limits = SegmentBuildLimits::for_test(
            3,
            1_000_000,
            Duration::from_secs(10),
            2,
            1_000_000,
            1_000_000,
        );
        let mut builders = SegmentBuilders::with_limits(limits);
        let started_at = Instant::now();

        push(
            &mut builders,
            fixture.normalize(20, &event("2026-08-20", "a")),
            started_at,
        );
        push(
            &mut builders,
            fixture.normalize(21, &event("2026-08-21", "b")),
            started_at + Duration::from_secs(1),
        );
        push(
            &mut builders,
            fixture.normalize(22, &event("2026-08-20", "c")),
            started_at + Duration::from_secs(2),
        );
        push(
            &mut builders,
            fixture.normalize(23, &event("2026-08-22", "d")),
            started_at + Duration::from_secs(3),
        );

        let lru = builders
            .take_next_sealed()
            .expect("queue accounting")
            .expect("least recently used builder");
        assert_eq!(lru.event_day().as_date().to_string(), "2026-08-21");
        assert_eq!(lru.sealing_reason(), SealingReason::BuilderLimit);

        push(
            &mut builders,
            fixture.normalize(24, &event("2026-08-20", "e")),
            started_at + Duration::from_secs(4),
        );
        let row_target = builders
            .take_next_sealed()
            .expect("queue accounting")
            .expect("row target segment");
        assert_eq!(row_target.event_day().as_date().to_string(), "2026-08-20");
        assert_eq!(row_target.row_count(), 3);
        assert_eq!(row_target.sealing_reason(), SealingReason::RowTarget);

        assert_eq!(
            builders
                .seal_expired(started_at + Duration::from_secs(13))
                .expect("monotonic clock"),
            1
        );
        let aged = builders
            .take_next_sealed()
            .expect("queue accounting")
            .expect("aged segment");
        assert_eq!(aged.event_day().as_date().to_string(), "2026-08-22");
        assert_eq!(aged.sealing_reason(), SealingReason::MaximumAge);
    }

    #[test]
    fn expiration_wait_tracks_the_first_row_without_resetting_on_append() {
        let fixture = Fixture::new();
        let limits = SegmentBuildLimits::for_test(
            100,
            1_000_000,
            Duration::from_secs(10),
            8,
            1_000_000,
            1_000_000,
        );
        let mut builders = SegmentBuilders::with_limits(limits);
        let started_at = Instant::now();

        assert_eq!(
            builders
                .next_expiration_in(started_at)
                .expect("monotonic clock"),
            None
        );
        push(
            &mut builders,
            fixture.normalize(25, &event("2026-08-20", "first")),
            started_at,
        );
        assert_eq!(
            builders
                .next_expiration_in(started_at + Duration::from_secs(4))
                .expect("monotonic clock"),
            Some(Duration::from_secs(6))
        );

        push(
            &mut builders,
            fixture.normalize(26, &event("2026-08-20", "second")),
            started_at + Duration::from_secs(6),
        );
        assert_eq!(
            builders
                .next_expiration_in(started_at + Duration::from_secs(7))
                .expect("monotonic clock"),
            Some(Duration::from_secs(3))
        );
        assert_eq!(
            builders
                .next_expiration_in(started_at + Duration::from_secs(10))
                .expect("monotonic clock"),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn byte_target_seals_a_single_large_row_without_splitting_it() {
        let fixture = Fixture::new();
        let limits =
            SegmentBuildLimits::for_test(100, 128, Duration::from_secs(60), 8, 10_000, 10_000);
        let mut builders = SegmentBuilders::with_limits(limits);
        let body = event("2026-08-20", &"x".repeat(256));

        push(&mut builders, fixture.normalize(30, &body), Instant::now());

        let segment = builders
            .take_next_sealed()
            .expect("queue accounting")
            .expect("byte target segment");
        assert_eq!(segment.row_count(), 1);
        assert!(segment.estimated_uncompressed_bytes() > 128);
        assert_eq!(segment.sealing_reason(), SealingReason::ByteTarget);
    }

    #[test]
    fn equal_event_times_are_ordered_by_event_identity_across_batches() {
        let fixture = Fixture::new();
        let limits = SegmentBuildLimits::for_test(
            100,
            1_000_000,
            Duration::from_secs(60),
            8,
            1_000_000,
            1_000_000,
        );
        let mut builders = SegmentBuilders::with_limits(limits);
        let observed_at = Instant::now();
        let left = fixture.normalize(31, &event("2026-08-20", "left"));
        let right = fixture.normalize(32, &event("2026-08-20", "right"));
        let left_id = accepted_event_id(&left);
        let right_id = accepted_event_id(&right);
        assert_ne!(left_id, right_id);
        let (larger, smaller, larger_id, smaller_id) = if left_id > right_id {
            (left, right, left_id, right_id)
        } else {
            (right, left, right_id, left_id)
        };

        push(&mut builders, larger, observed_at);
        push(&mut builders, smaller, observed_at + Duration::from_secs(1));
        assert_eq!(builders.flush_all().expect("flush builder"), 1);

        let segment = builders
            .take_next_sealed()
            .expect("queue accounting")
            .expect("same-day segment");
        let event_ids = segment
            .rows()
            .iter()
            .map(|row| row.row().event_id())
            .collect::<Vec<_>>();
        assert_eq!(event_ids, [smaller_id, larger_id]);
    }

    #[test]
    fn resident_capacity_defers_the_next_batch_without_losing_it() {
        let fixture = Fixture::new();
        let limits =
            SegmentBuildLimits::for_test(100, 10_000, Duration::from_secs(60), 8, 1_024, 10_000);
        let mut builders = SegmentBuilders::with_limits(limits);
        let observed_at = Instant::now();
        let body = event("2026-08-20", &"x".repeat(300));
        push(&mut builders, fixture.normalize(40, &body), observed_at);

        let deferred = match builders
            .push_batch(
                fixture.normalize(41, &body),
                observed_at + Duration::from_secs(1),
            )
            .expect("capacity is a typed outcome")
        {
            SegmentBuildOutcome::Deferred { batch, capacity } => {
                assert_eq!(capacity, SegmentCapacity::ResidentMemory);
                batch
            }
            SegmentBuildOutcome::Accepted(_) => panic!("the second batch must be deferred"),
        };
        let usage = builders.usage();
        assert_eq!(usage.open_builders(), 0);
        assert_eq!(usage.sealed_segments(), 1);
        assert!(usage.estimated_resident_bytes() <= usage.maximum_estimated_resident_bytes());
        assert!(usage.estimated_staging_bytes() <= usage.maximum_staging_bytes());
        assert_eq!(
            builders
                .take_next_sealed()
                .expect("queue accounting")
                .expect("memory-pressure segment")
                .sealing_reason(),
            SealingReason::CapacityPressure(SegmentCapacity::ResidentMemory)
        );

        push(
            &mut builders,
            deferred,
            observed_at + Duration::from_secs(2),
        );
        assert_eq!(builders.usage().open_builders(), 1);
    }

    #[test]
    fn staging_capacity_defers_the_next_batch_without_losing_it() {
        let fixture = Fixture::new();
        let limits =
            SegmentBuildLimits::for_test(100, 10_000, Duration::from_secs(60), 8, 10_000, 600);
        let mut builders = SegmentBuilders::with_limits(limits);
        let observed_at = Instant::now();
        let body = event("2026-08-20", &"x".repeat(300));
        push(&mut builders, fixture.normalize(42, &body), observed_at);

        let deferred = match builders
            .push_batch(
                fixture.normalize(43, &body),
                observed_at + Duration::from_secs(1),
            )
            .expect("capacity is a typed outcome")
        {
            SegmentBuildOutcome::Deferred { batch, capacity } => {
                assert_eq!(capacity, SegmentCapacity::Staging);
                batch
            }
            SegmentBuildOutcome::Accepted(_) => panic!("the second batch must be deferred"),
        };
        assert_eq!(
            builders
                .take_next_sealed()
                .expect("queue accounting")
                .expect("staging-pressure segment")
                .sealing_reason(),
            SealingReason::CapacityPressure(SegmentCapacity::Staging)
        );

        push(
            &mut builders,
            deferred,
            observed_at + Duration::from_secs(2),
        );
        assert_eq!(builders.usage().open_builders(), 1);
    }

    fn push(builders: &mut SegmentBuilders, batch: crate::NormalizedBatch, observed_at: Instant) {
        let summary = accepted_summary(
            builders
                .push_batch(batch, observed_at)
                .expect("batch build"),
        );
        assert!(summary.dead_letters().is_empty());
    }

    fn accepted_summary(outcome: SegmentBuildOutcome) -> crate::SegmentBuildSummary {
        match outcome {
            SegmentBuildOutcome::Accepted(summary) => summary,
            SegmentBuildOutcome::Deferred { .. } => panic!("batch unexpectedly deferred"),
        }
    }

    fn accepted_event_id(batch: &crate::NormalizedBatch) -> crate::EventId {
        match batch.records() {
            [crate::NormalizedRecord::Accepted(row)] => row.event_id(),
            _ => panic!("fixture must produce exactly one accepted row"),
        }
    }

    fn event(day: &str, message: &str) -> Vec<u8> {
        format!("{{\"timestamp\":\"{day}T12:00:00Z\",\"message\":\"{message}\"}}").into_bytes()
    }

    struct Fixture {
        source: Source,
        source_id: SourceId,
        schema_id: SchemaId,
        input_id: InputId,
        profile_revision_id: IngestionProfileRevisionId,
    }

    impl Fixture {
        fn new() -> Self {
            let source_id = source_id(1);
            let schema_id = schema_id(2);
            let input_id = input_id(3);
            let profile_revision_id = profile_revision_id(4);
            let message_id = field_id(5);
            let schema = Schema::new(
                schema_id,
                source_id,
                SchemaVersion::new(1).expect("schema version"),
                digests(1),
                vec![
                    UserField::new(
                        message_id,
                        UserFieldName::try_from("message").expect("field name"),
                        UserLogicalType::Utf8,
                        Nullability::NonNull,
                    )
                    .expect("field"),
                ],
            )
            .expect("schema");
            let profile = IngestionProfile::new(
                MaximumRecordBytes::new(2_048).expect("record bytes"),
                EventTimeMapping::new(
                    JsonPointer::parse("/timestamp").expect("event-time pointer"),
                    EventTimeFormat::Rfc3339,
                ),
                vec![
                    FieldMapping::new(
                        message_id,
                        JsonPointer::parse("/message").expect("field pointer"),
                    )
                    .expect("mapping"),
                ],
            )
            .expect("profile");
            let revision = IngestionProfileRevision::new(
                profile_revision_id,
                input_id,
                ProfileRevision::new(1).expect("profile revision"),
                schema_id,
                digests(2),
                profile,
            );
            let input = Input::new(
                input_id,
                source_id,
                InputName::try_from("vector").expect("input name"),
                digests(3),
                profile_revision_id,
                vec![revision],
            )
            .expect("input");
            let source = Source::new(
                source_id,
                SourceName::try_from("logs").expect("source name"),
                "Logs",
                DeclarationDigest::new([4; 32]),
                schema_id,
                vec![schema],
                vec![input],
            )
            .expect("source");
            Self {
                source,
                source_id,
                schema_id,
                input_id,
                profile_revision_id,
            }
        }

        fn normalize(&self, batch_sequence: u128, body: &[u8]) -> crate::NormalizedBatch {
            normalize_records(self.metadata(batch_sequence), body, &self.source)
                .expect("normalize batch")
        }

        fn metadata(&self, batch_sequence: u128) -> BatchMetadata {
            BatchMetadata::new(
                self.batch_id(batch_sequence),
                PinnedCatalogIdentities::new(
                    self.source_id,
                    self.input_id,
                    self.profile_revision_id,
                    self.schema_id,
                ),
                IngestionTime::from_unix_milliseconds(
                    1_787_227_200_000 + i64::try_from(batch_sequence).expect("test sequence"),
                )
                .expect("ingestion time"),
            )
        }

        fn batch_id(&self, batch_sequence: u128) -> BatchId {
            BatchId::try_from(identity(100 + batch_sequence)).expect("batch identity")
        }
    }

    fn digests(byte: u8) -> DefinitionDigests {
        DefinitionDigests::new(
            DeclarationDigest::new([byte; 32]),
            MaterializedDigest::new([byte.wrapping_add(64); 32]),
        )
    }

    fn source_id(value: u128) -> SourceId {
        SourceId::try_from(identity(value)).expect("source identity")
    }

    fn schema_id(value: u128) -> SchemaId {
        SchemaId::try_from(identity(value)).expect("schema identity")
    }

    fn field_id(value: u128) -> FieldId {
        FieldId::try_from(identity(value)).expect("field identity")
    }

    fn input_id(value: u128) -> InputId {
        InputId::try_from(identity(value)).expect("input identity")
    }

    fn profile_revision_id(value: u128) -> IngestionProfileRevisionId {
        IngestionProfileRevisionId::try_from(identity(value)).expect("profile revision identity")
    }

    fn identity(value: u128) -> Uuid {
        Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 | value)
    }
}
