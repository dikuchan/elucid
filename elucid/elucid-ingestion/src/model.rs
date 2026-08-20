use std::num::NonZeroU64;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use elucid_catalog::{IngestionProfileRevisionId, InputId, SchemaId, SourceId};

use crate::{BatchId, SpoolModelError};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct SpoolCapacity(NonZeroU64);

impl SpoolCapacity {
    pub fn new(value: u64) -> Result<Self, SpoolModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(SpoolModelError::CapacityMustBePositive)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns whether this capacity can reserve the complete durable frame for the body limit.
    #[must_use]
    pub fn can_reserve(self, body_limit: AppendBodyLimit) -> bool {
        body_limit
            .get()
            .checked_add(crate::frame::FRAME_OVERHEAD_BYTES)
            .is_some_and(|required_bytes| required_bytes <= self.get())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct AppendBodyLimit(NonZeroU64);

impl AppendBodyLimit {
    pub fn new(value: u64) -> Result<Self, SpoolModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(SpoolModelError::AppendBodyLimitMustBePositive)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct BatchByteSize(u64);

impl BatchByteSize {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct BodyDigest([u8; 32]);

impl BodyDigest {
    #[must_use]
    pub fn calculate(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct IngestionTime(i64);

impl IngestionTime {
    pub fn from_unix_milliseconds(value: i64) -> Result<Self, SpoolModelError> {
        DateTime::<Utc>::from_timestamp_millis(value)
            .map(|_| Self(value))
            .ok_or(SpoolModelError::IngestionTimeOutOfRange)
    }

    #[must_use]
    pub const fn unix_milliseconds(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct PinnedCatalogIdentities {
    source_id: SourceId,
    input_id: InputId,
    profile_revision_id: IngestionProfileRevisionId,
    target_schema_id: SchemaId,
}

impl PinnedCatalogIdentities {
    #[must_use]
    pub const fn new(
        source_id: SourceId,
        input_id: InputId,
        profile_revision_id: IngestionProfileRevisionId,
        target_schema_id: SchemaId,
    ) -> Self {
        Self {
            source_id,
            input_id,
            profile_revision_id,
            target_schema_id,
        }
    }

    #[must_use]
    pub const fn source_id(self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn input_id(self) -> InputId {
        self.input_id
    }

    #[must_use]
    pub const fn profile_revision_id(self) -> IngestionProfileRevisionId {
        self.profile_revision_id
    }

    #[must_use]
    pub const fn target_schema_id(self) -> SchemaId {
        self.target_schema_id
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct BatchMetadata {
    batch_id: BatchId,
    catalog: PinnedCatalogIdentities,
    ingestion_time: IngestionTime,
}

impl BatchMetadata {
    #[must_use]
    pub const fn new(
        batch_id: BatchId,
        catalog: PinnedCatalogIdentities,
        ingestion_time: IngestionTime,
    ) -> Self {
        Self {
            batch_id,
            catalog,
            ingestion_time,
        }
    }

    #[must_use]
    pub const fn batch_id(self) -> BatchId {
        self.batch_id
    }

    #[must_use]
    pub const fn catalog(self) -> PinnedCatalogIdentities {
        self.catalog
    }

    #[must_use]
    pub const fn ingestion_time(self) -> IngestionTime {
        self.ingestion_time
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DurableAppend {
    metadata: BatchMetadata,
    body_bytes: BatchByteSize,
    body_digest: BodyDigest,
    range: SpoolBatchRange,
}

impl DurableAppend {
    pub(crate) const fn new(
        metadata: BatchMetadata,
        body_bytes: BatchByteSize,
        body_digest: BodyDigest,
        range: SpoolBatchRange,
    ) -> Self {
        Self {
            metadata,
            body_bytes,
            body_digest,
            range,
        }
    }

    #[must_use]
    pub const fn metadata(self) -> BatchMetadata {
        self.metadata
    }

    #[must_use]
    pub const fn body_bytes(self) -> BatchByteSize {
        self.body_bytes
    }

    #[must_use]
    pub const fn body_digest(self) -> BodyDigest {
        self.body_digest
    }

    #[must_use]
    pub const fn range(self) -> SpoolBatchRange {
        self.range
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SpoolUsage {
    capacity_bytes: u64,
    committed_bytes: u64,
    reserved_bytes: u64,
    available_bytes: u64,
}

impl SpoolUsage {
    pub(crate) fn new(
        capacity_bytes: u64,
        committed_bytes: u64,
        reserved_bytes: u64,
    ) -> Result<Self, crate::SpoolError> {
        let occupied_bytes = committed_bytes
            .checked_add(reserved_bytes)
            .ok_or_else(|| crate::SpoolError::invariant("spool usage overflow"))?;
        let available_bytes = capacity_bytes.saturating_sub(occupied_bytes);
        Ok(Self {
            capacity_bytes,
            committed_bytes,
            reserved_bytes,
            available_bytes,
        })
    }

    #[must_use]
    pub const fn capacity_bytes(self) -> u64 {
        self.capacity_bytes
    }

    #[must_use]
    pub const fn committed_bytes(self) -> u64 {
        self.committed_bytes
    }

    #[must_use]
    pub const fn reserved_bytes(self) -> u64 {
        self.reserved_bytes
    }

    #[must_use]
    pub const fn available_bytes(self) -> u64 {
        self.available_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct SpoolCheckpoint(u64);

impl SpoolCheckpoint {
    pub(crate) const INITIAL: Self = Self(0);

    pub(crate) const fn from_position(position: u64) -> Self {
        Self(position)
    }

    #[must_use]
    pub const fn position(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct SpoolBatchRange {
    start: SpoolCheckpoint,
    end: SpoolCheckpoint,
}

impl SpoolBatchRange {
    pub(crate) fn new(start: u64, end: u64) -> Result<Self, crate::SpoolError> {
        if start >= end {
            return Err(crate::SpoolError::invariant(
                "spool batch range must be non-empty and ordered",
            ));
        }
        Ok(Self {
            start: SpoolCheckpoint::from_position(start),
            end: SpoolCheckpoint::from_position(end),
        })
    }

    #[must_use]
    pub const fn start(self) -> SpoolCheckpoint {
        self.start
    }

    #[must_use]
    pub const fn end(self) -> SpoolCheckpoint {
        self.end
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SpoolReclamation {
    reclaimed_bytes: Option<u64>,
}

impl SpoolReclamation {
    pub(crate) const DEFERRED: Self = Self {
        reclaimed_bytes: None,
    };

    pub(crate) const fn reclaimed(bytes: u64) -> Self {
        Self {
            reclaimed_bytes: Some(bytes),
        }
    }

    #[must_use]
    pub const fn reclaimed_bytes(self) -> Option<u64> {
        self.reclaimed_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MaximumBatchAdmission {
    Available,
    CapacityExhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RecoveryReport {
    checkpoint: SpoolCheckpoint,
    committed_batches: u64,
    pending_batches: u64,
    committed_bytes: u64,
    discarded_tail_bytes: u64,
    maximum_batch_admission: MaximumBatchAdmission,
}

impl RecoveryReport {
    pub(crate) const fn new(
        checkpoint: SpoolCheckpoint,
        committed_batches: u64,
        pending_batches: u64,
        committed_bytes: u64,
        discarded_tail_bytes: u64,
        maximum_batch_admission: MaximumBatchAdmission,
    ) -> Self {
        Self {
            checkpoint,
            committed_batches,
            pending_batches,
            committed_bytes,
            discarded_tail_bytes,
            maximum_batch_admission,
        }
    }

    #[must_use]
    pub const fn checkpoint(self) -> SpoolCheckpoint {
        self.checkpoint
    }

    #[must_use]
    pub const fn committed_batches(self) -> u64 {
        self.committed_batches
    }

    #[must_use]
    pub const fn pending_batches(self) -> u64 {
        self.pending_batches
    }

    #[must_use]
    pub const fn committed_bytes(self) -> u64 {
        self.committed_bytes
    }

    #[must_use]
    pub const fn discarded_tail_bytes(self) -> u64 {
        self.discarded_tail_bytes
    }

    #[must_use]
    pub const fn maximum_batch_admission(self) -> MaximumBatchAdmission {
        self.maximum_batch_admission
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RecoveredBatch {
    metadata: BatchMetadata,
    body: Bytes,
    body_digest: BodyDigest,
    range: SpoolBatchRange,
}

impl RecoveredBatch {
    pub(crate) const fn new(
        metadata: BatchMetadata,
        body: Bytes,
        body_digest: BodyDigest,
        range: SpoolBatchRange,
    ) -> Self {
        Self {
            metadata,
            body,
            body_digest,
            range,
        }
    }

    #[must_use]
    pub const fn metadata(&self) -> BatchMetadata {
        self.metadata
    }

    #[must_use]
    pub const fn body(&self) -> &Bytes {
        &self.body
    }

    #[must_use]
    pub const fn body_digest(&self) -> BodyDigest {
        self.body_digest
    }

    #[must_use]
    pub const fn range(&self) -> SpoolBatchRange {
        self.range
    }
}
