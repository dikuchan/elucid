use std::fmt::{Display, Formatter};

use thiserror::Error;
use yaml_rust2::scanner::ScanError;

use crate::{CatalogModelError, SchemaIncompatibility, SchemaVersion};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CatalogErrorCode {
    ManifestInvalid,
    DefinitionConflict,
    SchemaIncompatible,
    ProfileInvalid,
    Corrupt,
}

impl CatalogErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManifestInvalid => "CATALOG_MANIFEST_INVALID",
            Self::DefinitionConflict => "CATALOG_DEFINITION_CONFLICT",
            Self::SchemaIncompatible => "CATALOG_SCHEMA_INCOMPATIBLE",
            Self::ProfileInvalid => "CATALOG_PROFILE_INVALID",
            Self::Corrupt => "CATALOG_CORRUPT",
        }
    }
}

impl Display for CatalogErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct CatalogPath(String);

impl CatalogPath {
    #[must_use]
    pub(crate) fn new(value: impl AsRef<str>) -> Self {
        Self(value.as_ref().into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for CatalogPath {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CatalogApplicationError {
    #[error("catalog manifest is not UTF-8 at {path}: {source}")]
    ManifestNotUtf8 {
        path: CatalogPath,
        #[source]
        source: std::str::Utf8Error,
    },

    #[error("catalog manifest contains invalid YAML at {path}: {source}")]
    ManifestYamlSyntax {
        path: CatalogPath,
        #[source]
        source: ScanError,
    },

    #[error("catalog manifest cannot be decoded at {path}: {source}")]
    ManifestYamlDecode {
        path: CatalogPath,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("catalog manifest is invalid at {path}: {message}")]
    ManifestInvalid { path: CatalogPath, message: String },

    #[error("catalog manifest is invalid at {path}: {source}")]
    ManifestModelInvalid {
        path: CatalogPath,
        #[source]
        source: CatalogModelError,
    },

    #[error("immutable catalog definition conflicts at {path}")]
    DefinitionConflict { path: CatalogPath },

    #[error("catalog history diverged at {path}: {message}")]
    HistoryDiverged { path: CatalogPath, message: String },

    #[error("ingestion profile is invalid at {path}: {message}")]
    ProfileInvalid { path: CatalogPath, message: String },

    #[error(
        "schema {earlier_schema_version} cannot evolve additively to schema {later_schema_version} at {path}: {reason}"
    )]
    SchemaIncompatible {
        path: CatalogPath,
        earlier_schema_version: SchemaVersion,
        later_schema_version: SchemaVersion,
        reason: Box<SchemaIncompatibility>,
    },

    #[error("catalog state is corrupt at {path}: {message}")]
    Corruption { path: CatalogPath, message: String },

    #[error("catalog canonical JSON encoding failed at {path}: {source}")]
    CanonicalJsonEncoding {
        path: CatalogPath,
        #[source]
        source: serde_json::Error,
    },
}

impl CatalogApplicationError {
    #[must_use]
    pub const fn code(&self) -> CatalogErrorCode {
        match self {
            Self::ManifestNotUtf8 { .. }
            | Self::ManifestYamlSyntax { .. }
            | Self::ManifestYamlDecode { .. }
            | Self::ManifestInvalid { .. }
            | Self::ManifestModelInvalid { .. } => CatalogErrorCode::ManifestInvalid,
            Self::DefinitionConflict { .. } => CatalogErrorCode::DefinitionConflict,
            Self::HistoryDiverged { .. } => CatalogErrorCode::DefinitionConflict,
            Self::ProfileInvalid { .. } => CatalogErrorCode::ProfileInvalid,
            Self::SchemaIncompatible { .. } => CatalogErrorCode::SchemaIncompatible,
            Self::Corruption { .. } | Self::CanonicalJsonEncoding { .. } => {
                CatalogErrorCode::Corrupt
            }
        }
    }

    #[must_use]
    pub const fn path(&self) -> &CatalogPath {
        match self {
            Self::ManifestNotUtf8 { path, .. }
            | Self::ManifestYamlSyntax { path, .. }
            | Self::ManifestYamlDecode { path, .. }
            | Self::ManifestInvalid { path, .. }
            | Self::ManifestModelInvalid { path, .. }
            | Self::DefinitionConflict { path }
            | Self::HistoryDiverged { path, .. }
            | Self::ProfileInvalid { path, .. }
            | Self::SchemaIncompatible { path, .. }
            | Self::Corruption { path, .. }
            | Self::CanonicalJsonEncoding { path, .. } => path,
        }
    }

    pub(crate) fn manifest(path: impl AsRef<str>, message: impl Into<String>) -> Self {
        Self::ManifestInvalid {
            path: CatalogPath::new(path),
            message: message.into(),
        }
    }

    pub(crate) fn manifest_model(path: impl AsRef<str>, source: CatalogModelError) -> Self {
        Self::ManifestModelInvalid {
            path: CatalogPath::new(path),
            source,
        }
    }

    pub(crate) fn profile_invalid(path: impl AsRef<str>, message: impl Into<String>) -> Self {
        Self::ProfileInvalid {
            path: CatalogPath::new(path),
            message: message.into(),
        }
    }

    pub(crate) fn corruption(path: impl AsRef<str>, message: impl Into<String>) -> Self {
        Self::Corruption {
            path: CatalogPath::new(path),
            message: message.into(),
        }
    }
}
