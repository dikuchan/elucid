use std::fmt::{Display, Formatter};
use std::num::NonZeroU64;
use std::str::FromStr;

use crate::{CatalogModelError, NameKind, VersionKind};

macro_rules! catalog_name {
    ($name:ident, $kind:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[non_exhaustive]
        pub struct $name(Box<str>);

        impl $name {
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<&str> for $name {
            type Error = CatalogModelError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::try_from(value.to_owned())
            }
        }

        impl TryFrom<String> for $name {
            type Error = CatalogModelError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                if !is_valid_name(&value) {
                    return Err(CatalogModelError::InvalidName {
                        kind: NameKind::$kind,
                        value,
                    });
                }
                Ok(Self(value.into_boxed_str()))
            }
        }

        impl FromStr for $name {
            type Err = CatalogModelError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::try_from(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

catalog_name!(SourceName, Source);
catalog_name!(InputName, Input);
catalog_name!(UserFieldName, UserField);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct SchemaVersion(NonZeroU64);

impl SchemaVersion {
    pub fn new(value: u64) -> Result<Self, CatalogModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(CatalogModelError::VersionMustBePositive {
                kind: VersionKind::Schema,
            })
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl Display for SchemaVersion {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.get(), formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ProfileRevision(NonZeroU64);

impl ProfileRevision {
    pub fn new(value: u64) -> Result<Self, CatalogModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(CatalogModelError::VersionMustBePositive {
                kind: VersionKind::IngestProfileRevision,
            })
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl Display for ProfileRevision {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.get(), formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct DeclarationDigest([u8; 32]);

impl DeclarationDigest {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct MaterializedDigest([u8; 32]);

impl MaterializedDigest {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct DefinitionDigests {
    declaration: DeclarationDigest,
    materialized: MaterializedDigest,
}

impl DefinitionDigests {
    #[must_use]
    pub const fn new(declaration: DeclarationDigest, materialized: MaterializedDigest) -> Self {
        Self {
            declaration,
            materialized,
        }
    }

    #[must_use]
    pub const fn declaration(self) -> DeclarationDigest {
        self.declaration
    }

    #[must_use]
    pub const fn materialized(self) -> MaterializedDigest {
        self.materialized
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct MaximumRecordBytes(NonZeroU64);

impl MaximumRecordBytes {
    pub fn new(value: u64) -> Result<Self, CatalogModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(CatalogModelError::MaximumRecordBytesMustBePositive)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct FieldOrdinal(i32);

impl FieldOrdinal {
    pub(crate) fn from_index(index: usize) -> Result<Self, CatalogModelError> {
        i32::try_from(index)
            .map(Self)
            .map_err(|_| CatalogModelError::FieldOrdinalOverflow)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0 as u32
    }
}

fn is_valid_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
