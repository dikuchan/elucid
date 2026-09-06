use chrono::{DateTime, NaiveDate, Utc};
use elucid_catalog::{SchemaId, SourceId};

use crate::{
    ManagedObjectKind, ObjectDescriptor, ObjectOwner, RowCount, SegmentId, StorageModelError,
    UncompressedByteSize,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SegmentTimes {
    event_day: NaiveDate,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    minimum_ingestion_time: DateTime<Utc>,
    maximum_ingestion_time: DateTime<Utc>,
}

impl SegmentTimes {
    pub fn new(
        event_day: NaiveDate,
        minimum_event_time: DateTime<Utc>,
        maximum_event_time: DateTime<Utc>,
        minimum_ingestion_time: DateTime<Utc>,
        maximum_ingestion_time: DateTime<Utc>,
    ) -> Result<Self, StorageModelError> {
        if minimum_event_time > maximum_event_time {
            return Err(StorageModelError::EventTimeBoundsNotOrdered);
        }
        if minimum_ingestion_time > maximum_ingestion_time {
            return Err(StorageModelError::IngestionTimeBoundsNotOrdered);
        }
        if minimum_event_time.date_naive() != event_day
            || maximum_event_time.date_naive() != event_day
        {
            return Err(StorageModelError::EventDayMismatch);
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
            return Err(StorageModelError::TimestampPrecisionUnsupported);
        }
        Ok(Self {
            event_day,
            minimum_event_time,
            maximum_event_time,
            minimum_ingestion_time,
            maximum_ingestion_time,
        })
    }

    #[must_use]
    pub const fn event_day(self) -> NaiveDate {
        self.event_day
    }

    #[must_use]
    pub const fn minimum_event_time(self) -> DateTime<Utc> {
        self.minimum_event_time
    }

    #[must_use]
    pub const fn maximum_event_time(self) -> DateTime<Utc> {
        self.maximum_event_time
    }

    #[must_use]
    pub const fn minimum_ingestion_time(self) -> DateTime<Utc> {
        self.minimum_ingestion_time
    }

    #[must_use]
    pub const fn maximum_ingestion_time(self) -> DateTime<Utc> {
        self.maximum_ingestion_time
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SegmentDescriptor {
    segment_id: SegmentId,
    source_id: SourceId,
    schema_id: SchemaId,
    times: SegmentTimes,
    row_count: RowCount,
    uncompressed_bytes: UncompressedByteSize,
    object: ObjectDescriptor,
}

impl SegmentDescriptor {
    pub fn new(
        segment_id: SegmentId,
        source_id: SourceId,
        schema_id: SchemaId,
        times: SegmentTimes,
        row_count: RowCount,
        uncompressed_bytes: UncompressedByteSize,
        object: ObjectDescriptor,
    ) -> Result<Self, StorageModelError> {
        if object.key().owner() != ObjectOwner::Segment(segment_id)
            || object.key().kind() != ManagedObjectKind::ParquetData
        {
            return Err(StorageModelError::SegmentObjectOwnerMismatch);
        }
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
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    #[must_use]
    pub const fn times(&self) -> SegmentTimes {
        self.times
    }

    #[must_use]
    pub const fn row_count(&self) -> RowCount {
        self.row_count
    }

    #[must_use]
    pub const fn uncompressed_bytes(&self) -> UncompressedByteSize {
        self.uncompressed_bytes
    }

    #[must_use]
    pub const fn object(&self) -> &ObjectDescriptor {
        &self.object
    }
}
