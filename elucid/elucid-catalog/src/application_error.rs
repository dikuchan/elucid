use std::fmt::{Display, Formatter};

use thiserror::Error;
use yaml_rust2::scanner::ScanError;

use crate::{CatalogModelError, FieldId, LogicalType, Nullability, SchemaVersion};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CatalogErrorCode {
    ManifestInvalid,
    DefinitionConflict,
    HistoryDiverged,
    ProfileTargetMismatch,
    SchemaIncompatible,
    Corruption,
}

impl CatalogErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManifestInvalid => "CATALOG_MANIFEST_INVALID",
            Self::DefinitionConflict => "CATALOG_DEFINITION_CONFLICT",
            Self::HistoryDiverged => "CATALOG_HISTORY_DIVERGED",
            Self::ProfileTargetMismatch => "CATALOG_PROFILE_TARGET_MISMATCH",
            Self::SchemaIncompatible => "CATALOG_SCHEMA_INCOMPATIBLE",
            Self::Corruption => "CATALOG_CORRUPTION",
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

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SchemaIncompatibility {
    #[error("active non-null field {field_name:?} ({field_id}) is absent")]
    RequiredFieldAbsent {
        field_id: FieldId,
        field_name: String,
    },

    #[error(
        "field {field_name:?} ({field_id}) cannot change logical type from {stored_type} to {active_type}"
    )]
    LogicalType {
        field_id: FieldId,
        field_name: String,
        stored_type: LogicalType,
        active_type: LogicalType,
    },

    #[error(
        "field {field_name:?} ({field_id}) cannot change nullability from {stored_nullability} to {active_nullability}"
    )]
    Nullability {
        field_id: FieldId,
        field_name: String,
        stored_nullability: Nullability,
        active_nullability: Nullability,
    },

    #[error("field {field_name:?} ({field_id}) changed role")]
    Role {
        field_id: FieldId,
        field_name: String,
    },
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

    #[error("ingestion profile target is invalid at {path}: {message}")]
    ProfileTargetMismatch { path: CatalogPath, message: String },

    #[error(
        "schema {stored_schema_version} cannot adapt to active schema {active_schema_version} at {path}: {reason}"
    )]
    SchemaIncompatible {
        path: CatalogPath,
        stored_schema_version: SchemaVersion,
        active_schema_version: SchemaVersion,
        reason: SchemaIncompatibility,
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
            Self::HistoryDiverged { .. } => CatalogErrorCode::HistoryDiverged,
            Self::ProfileTargetMismatch { .. } => CatalogErrorCode::ProfileTargetMismatch,
            Self::SchemaIncompatible { .. } => CatalogErrorCode::SchemaIncompatible,
            Self::Corruption { .. } | Self::CanonicalJsonEncoding { .. } => {
                CatalogErrorCode::Corruption
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
            | Self::ProfileTargetMismatch { path, .. }
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

    pub(crate) fn profile_target(path: impl AsRef<str>, message: impl Into<String>) -> Self {
        Self::ProfileTargetMismatch {
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
