//! Strongly typed, I/O-free catalog domain model.

mod error;
mod identity;
mod input;
mod json_pointer;
mod logical_type;
mod schema;
mod source;
mod value;

pub use error::{CatalogModelError, IdentityKind, NameKind, VersionKind};
pub use identity::{FieldId, IngestProfileRevisionId, InputId, SchemaId, SourceId};
pub use input::{
    ConversionPolicy, EventTimeFormat, EventTimeMapping, FieldMapping, IngestProfile,
    IngestProfileRevision, Input, InputEncoding, InputKind, LineBoundaryPolicy, ParserKind,
    UnknownFieldPolicy,
};
pub use json_pointer::{JsonPointer, JsonPointerToken};
pub use logical_type::{FieldRole, LogicalType, Nullability, UserLogicalType};
pub use schema::{Field, Schema, UserField};
pub use source::Source;
pub use value::{
    DeclarationDigest, DefinitionDigests, FieldOrdinal, InputName, MaterializedDigest,
    MaximumRecordBytes, ProfileRevision, SchemaVersion, SourceName, UserFieldName,
};
