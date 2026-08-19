use std::fmt::{Display, Formatter};

use object_store::path::Path as ObjectPath;

use crate::{BatchId, SegmentId, StorageModelError, StoredObjectId};

const UUID_TEXT_BYTES: usize = 36;
const MAXIMUM_MANAGED_KEY_BYTES: usize = 1_024;
const MAXIMUM_MANAGED_SUFFIX_BYTES: usize =
    1 + "dead-letters".len() + 1 + UUID_TEXT_BYTES + 1 + UUID_TEXT_BYTES + ".ndjson".len();
const MAXIMUM_ROOT_PREFIX_BYTES: usize = MAXIMUM_MANAGED_KEY_BYTES - MAXIMUM_MANAGED_SUFFIX_BYTES;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ManagedRoot(ObjectPath);

impl ManagedRoot {
    pub fn parse(value: &str) -> Result<Self, StorageModelError> {
        if value.len() > MAXIMUM_ROOT_PREFIX_BYTES {
            return Err(StorageModelError::RootPrefixTooLong {
                maximum_bytes: MAXIMUM_ROOT_PREFIX_BYTES,
            });
        }
        if value.starts_with('/') || value.ends_with('/') {
            return Err(StorageModelError::RootPrefixNotCanonical);
        }
        ObjectPath::parse(value)
            .map(Self)
            .map_err(|source| StorageModelError::RootPrefixInvalid { source })
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ManagedObjectKind {
    ParquetData,
    DeadLetter,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ObjectOwner {
    Segment(SegmentId),
    DeadLetterBatch(BatchId),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct ManagedObjectKey {
    path: ObjectPath,
    kind: ManagedObjectKind,
    owner: ObjectOwner,
    object_id: StoredObjectId,
}

impl ManagedObjectKey {
    #[must_use]
    pub fn parquet(root: &ManagedRoot, segment_id: SegmentId, object_id: StoredObjectId) -> Self {
        Self::new(
            root,
            "segments",
            segment_id.to_string(),
            format!("{object_id}.parquet"),
            ManagedObjectKind::ParquetData,
            ObjectOwner::Segment(segment_id),
            object_id,
        )
    }

    #[must_use]
    pub fn dead_letter(root: &ManagedRoot, batch_id: BatchId, object_id: StoredObjectId) -> Self {
        Self::new(
            root,
            "dead-letters",
            batch_id.to_string(),
            format!("{object_id}.ndjson"),
            ManagedObjectKind::DeadLetter,
            ObjectOwner::DeadLetterBatch(batch_id),
            object_id,
        )
    }

    fn new(
        root: &ManagedRoot,
        namespace: &'static str,
        owner_id: String,
        filename: String,
        kind: ManagedObjectKind,
        owner: ObjectOwner,
        object_id: StoredObjectId,
    ) -> Self {
        let path = root.0.child(namespace).child(owner_id).child(filename);
        Self {
            path,
            kind,
            owner,
            object_id,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.path.as_ref()
    }

    #[must_use]
    pub const fn kind(&self) -> ManagedObjectKind {
        self.kind
    }

    #[must_use]
    pub const fn owner(&self) -> ObjectOwner {
        self.owner
    }

    #[must_use]
    pub const fn object_id(&self) -> StoredObjectId {
        self.object_id
    }

    pub(crate) const fn as_object_path(&self) -> &ObjectPath {
        &self.path
    }
}

impl Display for ManagedObjectKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.path, formatter)
    }
}
