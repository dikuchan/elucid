use std::fmt::{Display, Formatter};

use elucid_core::UuidV7;
use uuid::Uuid;

use crate::{CatalogModelError, IdentityKind};

macro_rules! catalog_identity {
    ($name:ident, $kind:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[non_exhaustive]
        pub struct $name(UuidV7);

        impl $name {
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0.as_uuid()
            }
        }

        impl TryFrom<Uuid> for $name {
            type Error = CatalogModelError;

            fn try_from(value: Uuid) -> Result<Self, Self::Error> {
                UuidV7::try_from(value).map(Self).map_err(|_| {
                    CatalogModelError::IdentityMustBeUuidV7 {
                        kind: IdentityKind::$kind,
                        value,
                    }
                })
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.as_uuid()
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                Display::fmt(&self.0, formatter)
            }
        }
    };
}

catalog_identity!(SourceId, Source);
catalog_identity!(SchemaId, Schema);
catalog_identity!(FieldId, Field);
catalog_identity!(InputId, Input);
catalog_identity!(IngestionProfileRevisionId, IngestionProfileRevision);

impl FieldId {
    pub const EVENT_TIME: Self = Self::system(0x0000_0000_0000_7000_8000_0000_0000_0001);
    pub const INGESTION_TIME: Self = Self::system(0x0000_0000_0000_7000_8000_0000_0000_0002);
    pub const EVENT_ID: Self = Self::system(0x0000_0000_0000_7000_8000_0000_0000_0003);
    pub const REMAINDER: Self = Self::system(0x0000_0000_0000_7000_8000_0000_0000_0004);

    #[must_use]
    pub const fn is_system(self) -> bool {
        matches!(
            self,
            Self::EVENT_TIME | Self::INGESTION_TIME | Self::EVENT_ID | Self::REMAINDER
        )
    }

    const fn system(value: u128) -> Self {
        match UuidV7::new(Uuid::from_u128(value)) {
            Ok(value) => Self(value),
            // System field identities are fixed RFC 9562 UUIDv7 constants.
            Err(_) => unreachable!(),
        }
    }
}
