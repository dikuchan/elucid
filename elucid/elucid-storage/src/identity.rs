use std::fmt::{Display, Formatter};

use uuid::{Uuid, Variant, Version};

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
pub struct BatchId(Uuid);

impl BatchId {
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl TryFrom<Uuid> for BatchId {
    type Error = StorageModelError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        if value.get_version() != Some(Version::SortRand) || value.get_variant() != Variant::RFC4122
        {
            return Err(StorageModelError::BatchIdentityMustBeUuidV7 { value });
        }
        Ok(Self(value))
    }
}

impl From<BatchId> for Uuid {
    fn from(value: BatchId) -> Self {
        value.0
    }
}

impl Display for BatchId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}
