use std::fmt::{Display, Formatter};

use elucid_core::UuidV7;
use uuid::Uuid;

use crate::StorageModelError;

macro_rules! uuid_identity {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[non_exhaustive]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                Display::fmt(&self.0, formatter)
            }
        }
    };
}

uuid_identity!(SegmentId);
uuid_identity!(StoredObjectId);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct BatchId(UuidV7);

impl BatchId {
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0.as_uuid()
    }
}

impl TryFrom<Uuid> for BatchId {
    type Error = StorageModelError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        UuidV7::try_from(value)
            .map(Self)
            .map_err(|_| StorageModelError::BatchIdentityMustBeUuidV7 { value })
    }
}

impl From<BatchId> for Uuid {
    fn from(value: BatchId) -> Self {
        value.as_uuid()
    }
}

impl Display for BatchId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}
