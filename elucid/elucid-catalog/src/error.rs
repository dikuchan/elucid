use thiserror::Error;
use uuid::Uuid;

use crate::{FieldId, IngestProfileRevisionId, InputId, SchemaId, SourceId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IdentityKind {
    Source,
    Schema,
    Field,
    Input,
    IngestProfileRevision,
}

impl std::fmt::Display for IdentityKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Source => "source",
            Self::Schema => "schema",
            Self::Field => "field",
            Self::Input => "input",
            Self::IngestProfileRevision => "ingest profile revision",
        };
        formatter.write_str(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NameKind {
    Source,
    Input,
    UserField,
}

impl std::fmt::Display for NameKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Source => "source",
            Self::Input => "input",
            Self::UserField => "user field",
        };
        formatter.write_str(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum VersionKind {
    Schema,
    IngestProfileRevision,
}

impl std::fmt::Display for VersionKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Schema => "schema version",
            Self::IngestProfileRevision => "ingest profile revision",
        };
        formatter.write_str(value)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CatalogModelError {
    #[error("{kind} identity must be an RFC 9562 UUIDv7, got {value}")]
    IdentityMustBeUuidV7 { kind: IdentityKind, value: Uuid },

    #[error("{kind} name must match [A-Za-z_][A-Za-z0-9_]*, got {value:?}")]
    InvalidName { kind: NameKind, value: String },

    #[error("{kind} must be positive")]
    VersionMustBePositive { kind: VersionKind },

    #[error("{kind} history exceeds the supported version range")]
    HistoryLengthExceedsVersionRange { kind: VersionKind },

    #[error("maximum record bytes must be positive")]
    MaximumRecordBytesMustBePositive,

    #[error("a non-root JSON Pointer must start with '/'")]
    JsonPointerMustStartWithSlash,

    #[error("JSON Pointer token {token_index} contains an invalid escape at byte {byte_offset}")]
    InvalidJsonPointerEscape {
        token_index: usize,
        byte_offset: usize,
    },

    #[error("system field identity {field_id} is reserved")]
    SystemFieldIdentityIsReserved { field_id: FieldId },

    #[error("system field identity {field_id} cannot be an input mapping target")]
    SystemFieldCannotBeMapped { field_id: FieldId },

    #[error("field identity {field_id} occurs more than once in one schema")]
    DuplicateFieldIdentity { field_id: FieldId },

    #[error("field name {name:?} occurs more than once in one schema")]
    DuplicateFieldName { name: String },

    #[error("schema contains too many fields to assign a PostgreSQL INTEGER ordinal")]
    FieldOrdinalOverflow,

    #[error("profile maps field identity {field_id} more than once")]
    DuplicateProfileMappingTarget { field_id: FieldId },

    #[error("input {input_id} must contain at least one ingest profile revision")]
    EmptyProfileRevisionHistory { input_id: InputId },

    #[error(
        "ingest profile revision {profile_revision_id} belongs to input {actual_input_id}, expected {expected_input_id}"
    )]
    ProfileRevisionInputMismatch {
        profile_revision_id: IngestProfileRevisionId,
        expected_input_id: InputId,
        actual_input_id: InputId,
    },

    #[error("ingest profile revisions must be contiguous: expected {expected}, got {actual}")]
    ProfileRevisionsMustBeContiguous { expected: u64, actual: u64 },

    #[error("ingest profile revision identity {profile_revision_id} occurs more than once")]
    DuplicateProfileRevisionIdentity {
        profile_revision_id: IngestProfileRevisionId,
    },

    #[error("active ingest profile revision {profile_revision_id} is absent from input {input_id}")]
    ActiveProfileRevisionNotFound {
        input_id: InputId,
        profile_revision_id: IngestProfileRevisionId,
    },

    #[error("a source must contain at least one schema")]
    EmptySchemaHistory,

    #[error(
        "schema {schema_id} belongs to source {actual_source_id}, expected {expected_source_id}"
    )]
    SchemaSourceMismatch {
        schema_id: SchemaId,
        expected_source_id: SourceId,
        actual_source_id: SourceId,
    },

    #[error("schema versions must be contiguous: expected {expected}, got {actual}")]
    SchemaVersionsMustBeContiguous { expected: u64, actual: u64 },

    #[error("schema identity {schema_id} occurs more than once")]
    DuplicateSchemaIdentity { schema_id: SchemaId },

    #[error("active schema {schema_id} is absent from source {source_id}")]
    ActiveSchemaNotFound {
        source_id: SourceId,
        schema_id: SchemaId,
    },

    #[error(
        "field identity {field_id} was previously named {previous_name:?}, not {current_name:?}"
    )]
    FieldIdentityReusedWithDifferentName {
        field_id: FieldId,
        previous_name: String,
        current_name: String,
    },

    #[error(
        "field name {name:?} previously used identity {previous_field_id}, not {current_field_id}"
    )]
    FieldNameReusedWithDifferentIdentity {
        name: String,
        previous_field_id: FieldId,
        current_field_id: FieldId,
    },

    #[error("input {input_id} belongs to source {actual_source_id}, expected {expected_source_id}")]
    InputSourceMismatch {
        input_id: InputId,
        expected_source_id: SourceId,
        actual_source_id: SourceId,
    },

    #[error("input identity {input_id} occurs more than once")]
    DuplicateInputIdentity { input_id: InputId },

    #[error("input name {name:?} occurs more than once in one source")]
    DuplicateInputName { name: String },

    #[error(
        "ingest profile revision {profile_revision_id} targets schema {schema_id}, which is absent from the source"
    )]
    ProfileTargetSchemaNotFound {
        profile_revision_id: IngestProfileRevisionId,
        schema_id: SchemaId,
    },

    #[error(
        "ingest profile revision {profile_revision_id} maps field {field_id}, which is not a data field in schema {schema_id}"
    )]
    ProfileMappingTargetNotFound {
        profile_revision_id: IngestProfileRevisionId,
        schema_id: SchemaId,
        field_id: FieldId,
    },

    #[error(
        "ingest profile revision {profile_revision_id} has no mapping for field {field_id} in schema {schema_id}"
    )]
    ProfileMappingMissing {
        profile_revision_id: IngestProfileRevisionId,
        schema_id: SchemaId,
        field_id: FieldId,
    },
}
