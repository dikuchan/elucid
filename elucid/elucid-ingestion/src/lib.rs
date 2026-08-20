//! Durable local admission spool.

mod checkpoint;
mod error;
mod frame;
mod model;
mod normalization;
mod recovery;
mod segment;
mod spool;

pub use elucid_storage::BatchId;
pub use error::{SpoolError, SpoolErrorCode, SpoolModelError};
pub use model::{
    AppendBodyLimit, BatchByteSize, BatchMetadata, BodyDigest, DurableAppend, IngestionTime,
    MaximumBatchAdmission, PinnedCatalogIdentities, RecoveredBatch, RecoveryReport, SpoolCapacity,
    SpoolCheckpoint, SpoolUsage,
};
pub use normalization::{
    AcceptedRow, DEAD_LETTER_PAYLOAD_PREFIX_BYTES, DeadLetterCode, DeadLetterEntry,
    DeadLetterPayload, EventId, EventTime, JsonObject, MAXIMUM_BATCH_EVENT_DAYS,
    NormalizationError, NormalizedBatch, NormalizedField, NormalizedRecord, NormalizedValue,
    PayloadEncoding, PayloadExtent, RecordLocation, RecordPayloadDigest, normalize_records,
};
pub use recovery::{RecoveredBatches, SpoolRecovery};
pub use segment::{
    EventDay, SealedSegment, SealingReason, SegmentBuildError, SegmentBuildOutcome,
    SegmentBuildSummary, SegmentBuilderModelError, SegmentBuilderUsage, SegmentBuilders,
    SegmentCapacity, SegmentKey, SegmentRow, SegmentStagingCapacity, SegmentTimeBounds,
};
pub use spool::{Spool, SpoolReservation};
