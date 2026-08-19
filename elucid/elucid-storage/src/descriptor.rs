use bytes::Bytes;

use crate::value::byte_size;
use crate::{
    ManagedObjectKey, ManagedObjectKind, ObjectByteSize, ObjectDigest, ObjectFormatVersion,
    ObjectMediaType, StorageModelError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ObjectDescriptor {
    key: ManagedObjectKey,
    expected_byte_size: ObjectByteSize,
    digest: ObjectDigest,
    media_type: ObjectMediaType,
    format_version: ObjectFormatVersion,
}

impl ObjectDescriptor {
    pub fn new(
        key: ManagedObjectKey,
        expected_byte_size: ObjectByteSize,
        digest: ObjectDigest,
        media_type: ObjectMediaType,
        format_version: ObjectFormatVersion,
    ) -> Result<Self, StorageModelError> {
        let matching_kind = matches!(
            (key.kind(), media_type),
            (ManagedObjectKind::ParquetData, ObjectMediaType::ParquetData)
                | (ManagedObjectKind::DeadLetter, ObjectMediaType::DeadLetter)
        );
        if !matching_kind {
            return Err(StorageModelError::MediaTypeDoesNotMatchManagedKey);
        }
        Ok(Self {
            key,
            expected_byte_size,
            digest,
            media_type,
            format_version,
        })
    }

    pub fn for_bytes(
        key: ManagedObjectKey,
        bytes: &Bytes,
        media_type: ObjectMediaType,
        format_version: ObjectFormatVersion,
    ) -> Result<Self, StorageModelError> {
        Self::new(
            key,
            byte_size(bytes)?,
            ObjectDigest::calculate(bytes),
            media_type,
            format_version,
        )
    }

    #[must_use]
    pub const fn key(&self) -> &ManagedObjectKey {
        &self.key
    }

    #[must_use]
    pub const fn expected_byte_size(&self) -> ObjectByteSize {
        self.expected_byte_size
    }

    #[must_use]
    pub const fn digest(&self) -> ObjectDigest {
        self.digest
    }

    #[must_use]
    pub const fn media_type(&self) -> ObjectMediaType {
        self.media_type
    }

    #[must_use]
    pub const fn format_version(&self) -> ObjectFormatVersion {
        self.format_version
    }
}
