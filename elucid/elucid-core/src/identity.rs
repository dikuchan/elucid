use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

use uuid::{Uuid, Variant, Version};

/// A UUID with version 7 and the RFC variant.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct UuidV7(Uuid);

impl UuidV7 {
    pub const fn new(value: Uuid) -> Result<Self, UuidV7Error> {
        if matches!(value.get_version(), Some(Version::SortRand))
            && matches!(value.get_variant(), Variant::RFC4122)
        {
            Ok(Self(value))
        } else {
            Err(UuidV7Error::InvalidVersionOrVariant { value })
        }
    }

    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl TryFrom<Uuid> for UuidV7 {
    type Error = UuidV7Error;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<UuidV7> for Uuid {
    fn from(value: UuidV7) -> Self {
        value.0
    }
}

impl FromStr for UuidV7 {
    type Err = UuidV7Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid = value
            .parse::<Uuid>()
            .map_err(|source| UuidV7Error::Malformed { source })?;
        Self::try_from(uuid)
    }
}

impl Display for UuidV7 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

impl Debug for UuidV7 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum UuidV7Error {
    #[error("invalid UUID: {source}")]
    Malformed {
        #[source]
        source: uuid::Error,
    },

    #[error("expected an RFC 9562 UUIDv7, got {value}")]
    InvalidVersionOrVariant { value: Uuid },
}
