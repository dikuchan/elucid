use elucid_catalog::InputId;

use crate::{BatchId, ManagedObjectKind, ObjectDescriptor, ObjectOwner, StorageModelError};

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeadLetterDescriptor {
    input_id: InputId,
    batch_id: BatchId,
    object: ObjectDescriptor,
}

impl DeadLetterDescriptor {
    pub fn new(
        input_id: InputId,
        batch_id: BatchId,
        object: ObjectDescriptor,
    ) -> Result<Self, StorageModelError> {
        if object.key().owner() != ObjectOwner::DeadLetterBatch(batch_id)
            || object.key().kind() != ManagedObjectKind::DeadLetter
        {
            return Err(StorageModelError::DeadLetterObjectOwnerMismatch);
        }
        Ok(Self {
            input_id,
            batch_id,
            object,
        })
    }

    #[must_use]
    pub const fn object(&self) -> &ObjectDescriptor {
        &self.object
    }

    #[must_use]
    pub const fn input_id(&self) -> InputId {
        self.input_id
    }

    #[must_use]
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }
}
