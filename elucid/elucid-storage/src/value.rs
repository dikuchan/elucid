use std::num::NonZeroU64;

use bytes::Bytes;

use crate::StorageModelError;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ObjectByteSize(u64);

impl ObjectByteSize {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub(crate) fn from_usize(value: usize) -> Result<Self, StorageModelError> {
        u64::try_from(value)
            .map(Self)
            .map_err(|_| StorageModelError::ObjectSizeOverflow)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct ObjectDigest([u8; 32]);

impl ObjectDigest {
    #[must_use]
    pub const fn new(value: [u8; 32]) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn calculate(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub(crate) fn metadata_value(self) -> String {
        blake3::Hash::from_bytes(self.0).to_hex().to_string()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ObjectFormatVersion(NonZeroU64);

impl ObjectFormatVersion {
    pub fn new(value: u64) -> Result<Self, StorageModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(StorageModelError::ObjectFormatVersionMustBePositive)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ObjectMediaType {
    ParquetData,
    DeadLetter,
}

impl ObjectMediaType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ParquetData => "application/vnd.apache.parquet",
            Self::DeadLetter => "application/x-ndjson",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct TransferLimit(NonZeroU64);

impl TransferLimit {
    pub fn new(value: u64) -> Result<Self, StorageModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(StorageModelError::TransferLimitMustBePositive)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ObjectReadRange {
    start: u64,
    end: u64,
    object_size: ObjectByteSize,
}

impl ObjectReadRange {
    pub fn new(
        start: u64,
        end: u64,
        object_size: ObjectByteSize,
    ) -> Result<Self, StorageModelError> {
        if start >= end || end > object_size.get() {
            return Err(StorageModelError::InvalidObjectReadRange {
                start,
                end,
                object_size: object_size.get(),
            });
        }
        Ok(Self {
            start,
            end,
            object_size,
        })
    }

    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }

    #[must_use]
    pub const fn end(&self) -> u64 {
        self.end
    }

    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.end - self.start
    }

    pub(crate) const fn object_size(&self) -> ObjectByteSize {
        self.object_size
    }
}

pub(crate) fn byte_size(bytes: &Bytes) -> Result<ObjectByteSize, StorageModelError> {
    ObjectByteSize::from_usize(bytes.len())
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct RowCount(NonZeroU64);

impl RowCount {
    pub fn new(value: u64) -> Result<Self, StorageModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(StorageModelError::RowCountMustBePositive)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct UncompressedByteSize(NonZeroU64);

impl UncompressedByteSize {
    pub fn new(value: u64) -> Result<Self, StorageModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(StorageModelError::UncompressedByteSizeMustBePositive)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}
