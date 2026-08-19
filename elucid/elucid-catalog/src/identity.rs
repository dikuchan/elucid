use std::fmt::{Display, Formatter};

use uuid::{Uuid, Variant, Version};

use crate::{CatalogModelError, IdentityKind};

macro_rules! catalog_identity {
    ($name:ident, $kind:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[non_exhaustive]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl TryFrom<Uuid> for $name {
            type Error = CatalogModelError;

            fn try_from(value: Uuid) -> Result<Self, Self::Error> {
                validate_uuid_v7(value, IdentityKind::$kind)?;
                Ok(Self(value))
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

catalog_identity!(SourceId, Source);
catalog_identity!(SchemaId, Schema);
catalog_identity!(FieldId, Field);
catalog_identity!(InputId, Input);
catalog_identity!(IngestionProfileRevisionId, IngestionProfileRevision);

impl FieldId {
    pub const EVENT_TIME: Self = Self(Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0001));
    pub const INGESTION_TIME: Self =
        Self(Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0002));
    pub const EVENT_ID: Self = Self(Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0003));
    pub const REMAINDER: Self = Self(Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0004));

    #[must_use]
    pub const fn is_system(self) -> bool {
        matches!(
            self,
            Self::EVENT_TIME | Self::INGESTION_TIME | Self::EVENT_ID | Self::REMAINDER
        )
    }
}

fn validate_uuid_v7(value: Uuid, kind: IdentityKind) -> Result<(), CatalogModelError> {
    if value.get_version() != Some(Version::SortRand) || value.get_variant() != Variant::RFC4122 {
        return Err(CatalogModelError::IdentityMustBeUuidV7 { kind, value });
    }
    Ok(())
}
