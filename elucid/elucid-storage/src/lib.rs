//! Exact immutable object access for Elucid-managed storage.

mod descriptor;
mod error;
mod identity;
mod key;
mod parquet_segment;
mod store;
mod value;

pub use descriptor::ObjectDescriptor;
pub use error::{StorageError, StorageErrorKind, StorageModelError};
pub use identity::{BatchId, SegmentId, StoredObjectId};
pub use key::{ManagedObjectKey, ManagedObjectKind, ManagedRoot, ObjectOwner};
pub use parquet_segment::{
    PARQUET_FORMAT_VERSION, PARQUET_MAX_ROW_GROUP_ROWS, ParquetSegmentExpectation,
    ParquetSegmentInput, ParquetWriteLimit, StagedParquetSegment, validate_parquet_segment,
    validate_parquet_segment_metadata, write_parquet_segment,
};
pub use store::{
    ImmutableObjectStore, ObjectDeleteOutcome, ObjectUploadOutcome, ObjectVerificationOutcome,
};
pub use value::{
    ObjectByteSize, ObjectDigest, ObjectFormatVersion, ObjectMediaType, ObjectReadRange,
    TransferLimit,
};
