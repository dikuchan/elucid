use serde_json::{Map, Number, Value};

use crate::manifest::{ManifestIngestProfileRevision, ManifestSchema};
use crate::{
    CanonicalJson, CatalogApplicationError, CatalogPath, DeclarationDigest, Field, FieldRole,
    IngestProfile, IngestProfileRevision, IngestProfileRevisionId, Input, InputId, InputKind,
    InputName, JsonPointer, LogicalType, MaterializedDigest, ProfileRevision, Schema, SchemaId,
    SchemaVersion, SourceId, SourceName,
};

const SOURCE_DECLARATION_DOMAIN: &[u8] = b"elucid:catalog:source:v1\0";
const SCHEMA_DECLARATION_DOMAIN: &[u8] = b"elucid:catalog:schema:v1\0";
const INPUT_DECLARATION_DOMAIN: &[u8] = b"elucid:catalog:input:v1\0";
const INGEST_PROFILE_DECLARATION_DOMAIN: &[u8] = b"elucid:catalog:ingest-profile:v1\0";
const SCHEMA_MATERIALIZED_DOMAIN: &[u8] = b"elucid:catalog:schema-materialized:v1\0";
const INPUT_MATERIALIZED_DOMAIN: &[u8] = b"elucid:catalog:input-materialized:v1\0";
const INGEST_PROFILE_MATERIALIZED_DOMAIN: &[u8] =
    b"elucid:catalog:ingest-profile-materialized:v1\0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeclarationDocument {
    pub(crate) json: CanonicalJson,
    pub(crate) digest: DeclarationDigest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaterializedDocument {
    pub(crate) json: CanonicalJson,
    pub(crate) digest: MaterializedDigest,
}

pub(crate) fn source_declaration(
    name: &SourceName,
) -> Result<DeclarationDocument, CatalogApplicationError> {
    declaration_document(
        SOURCE_DECLARATION_DOMAIN,
        object([("name", string(name.as_str()))]),
        "source",
    )
}

pub(crate) fn manifest_schema_declaration(
    schema: &ManifestSchema,
    path: &str,
) -> Result<DeclarationDocument, CatalogApplicationError> {
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            field_declaration(
                field.name.as_str(),
                field.logical_type.as_str(),
                field.nullability.as_str(),
                FieldRole::Data.as_str(),
                field.description.as_deref(),
            )
        })
        .collect();
    schema_declaration(schema.version.get(), fields, path)
}

pub(crate) fn stored_schema_declaration(
    schema: &Schema,
    path: &str,
) -> Result<DeclarationDocument, CatalogApplicationError> {
    let fields = schema
        .fields()
        .iter()
        .filter(|field| field.role() == FieldRole::Data)
        .map(|field| {
            field_declaration(
                field.name(),
                field.logical_type().as_str(),
                field.nullability().as_str(),
                field.role().as_str(),
                field.description(),
            )
        })
        .collect();
    schema_declaration(schema.version().get(), fields, path)
}

pub(crate) fn schema_materialized(
    schema: &Schema,
    path: &str,
) -> Result<MaterializedDocument, CatalogApplicationError> {
    schema_materialized_parts(
        schema.id(),
        schema.source_id(),
        schema.version(),
        schema.fields(),
        path,
    )
}

pub(crate) fn schema_materialized_parts(
    schema_id: SchemaId,
    source_id: SourceId,
    version: SchemaVersion,
    schema_fields: &[Field],
    path: &str,
) -> Result<MaterializedDocument, CatalogApplicationError> {
    let fields = schema_fields
        .iter()
        .map(materialized_field)
        .collect::<Vec<_>>();
    let arrow_fields = schema_fields
        .iter()
        .map(arrow_field_descriptor)
        .collect::<Vec<_>>();
    materialized_document(
        SCHEMA_MATERIALIZED_DOMAIN,
        object([
            (
                "arrow_schema_descriptor",
                object([("fields", Value::Array(arrow_fields))]),
            ),
            ("fields", Value::Array(fields)),
            ("schema_id", string(&schema_id.to_string())),
            ("source_id", string(&source_id.to_string())),
            ("version", unsigned(version.get())),
        ]),
        path,
    )
}

pub(crate) fn input_declaration(
    name: &InputName,
    kind: InputKind,
    path: &str,
) -> Result<DeclarationDocument, CatalogApplicationError> {
    declaration_document(
        INPUT_DECLARATION_DOMAIN,
        object([
            ("kind", string(kind.as_str())),
            ("name", string(name.as_str())),
        ]),
        path,
    )
}

pub(crate) fn input_materialized(
    input_id: InputId,
    source_id: SourceId,
    name: &InputName,
    kind: InputKind,
    path: &str,
) -> Result<MaterializedDocument, CatalogApplicationError> {
    materialized_document(
        INPUT_MATERIALIZED_DOMAIN,
        object([
            ("input_id", string(&input_id.to_string())),
            ("kind", string(kind.as_str())),
            ("name", string(name.as_str())),
            ("source_id", string(&source_id.to_string())),
        ]),
        path,
    )
}

pub(crate) fn stored_input_materialized(
    input: &Input,
    path: &str,
) -> Result<MaterializedDocument, CatalogApplicationError> {
    input_materialized(
        input.id(),
        input.source_id(),
        input.name(),
        input.kind(),
        path,
    )
}

pub(crate) fn manifest_profile_declaration(
    revision: &ManifestIngestProfileRevision,
    path: &str,
) -> Result<DeclarationDocument, CatalogApplicationError> {
    let mappings = revision
        .mappings
        .iter()
        .map(|mapping| {
            object([
                ("json_pointer", string(&mapping.json_pointer.to_string())),
                ("target_field", string(mapping.target_field.as_str())),
            ])
        })
        .collect::<Vec<_>>();
    profile_declaration(
        revision,
        revision.target_schema_version.get(),
        mappings,
        path,
    )
}

pub(crate) fn stored_profile_declaration(
    revision: &IngestProfileRevision,
    target_schema: &Schema,
    path: &str,
) -> Result<DeclarationDocument, CatalogApplicationError> {
    let mappings = revision
        .profile()
        .mappings()
        .iter()
        .map(|mapping| {
            let target = target_schema
                .field(mapping.target_field_id())
                .filter(|field| field.role() == FieldRole::Data)
                .ok_or_else(|| {
                    CatalogApplicationError::corruption(
                        path,
                        format!(
                            "mapping target {} is absent from schema {}",
                            mapping.target_field_id(),
                            target_schema.id()
                        ),
                    )
                })?;
            Ok(object([
                ("json_pointer", string(&mapping.json_pointer().to_string())),
                ("target_field", string(target.name())),
            ]))
        })
        .collect::<Result<Vec<_>, CatalogApplicationError>>()?;
    let profile = revision.profile();
    let declaration = object([
        (
            "conversion_policy",
            string(profile.conversion_policy().as_str()),
        ),
        ("encoding", string(profile.encoding().as_str())),
        (
            "event_time_mapping",
            object([
                (
                    "format",
                    string(profile.event_time_mapping().format().as_str()),
                ),
                (
                    "json_pointer",
                    string(&profile.event_time_mapping().json_pointer().to_string()),
                ),
            ]),
        ),
        (
            "line_boundary_policy",
            string(profile.line_boundary_policy().as_str()),
        ),
        ("mappings", Value::Array(mappings)),
        (
            "maximum_record_bytes",
            unsigned(profile.maximum_record_bytes().get()),
        ),
        ("parser_kind", string(profile.parser_kind().as_str())),
        ("revision", unsigned(revision.revision().get())),
        (
            "target_schema_version",
            unsigned(target_schema.version().get()),
        ),
        (
            "unknown_field_policy",
            string(profile.unknown_field_policy().as_str()),
        ),
    ]);
    declaration_document(INGEST_PROFILE_DECLARATION_DOMAIN, declaration, path)
}

pub(crate) fn profile_materialized(
    revision: &IngestProfileRevision,
    path: &str,
) -> Result<MaterializedDocument, CatalogApplicationError> {
    profile_materialized_parts(
        revision.id(),
        revision.input_id(),
        revision.revision(),
        revision.target_schema_id(),
        revision.profile(),
        path,
    )
}

pub(crate) fn profile_materialized_parts(
    revision_id: IngestProfileRevisionId,
    input_id: InputId,
    revision: ProfileRevision,
    target_schema_id: SchemaId,
    profile: &IngestProfile,
    path: &str,
) -> Result<MaterializedDocument, CatalogApplicationError> {
    let mappings = profile
        .mappings()
        .iter()
        .map(|mapping| {
            object([
                ("json_pointer", string(&mapping.json_pointer().to_string())),
                (
                    "json_pointer_tokens",
                    pointer_tokens(mapping.json_pointer()),
                ),
                (
                    "target_field_id",
                    string(&mapping.target_field_id().to_string()),
                ),
            ])
        })
        .collect::<Vec<_>>();
    materialized_document(
        INGEST_PROFILE_MATERIALIZED_DOMAIN,
        object([
            (
                "conversion_policy",
                string(profile.conversion_policy().as_str()),
            ),
            ("encoding", string(profile.encoding().as_str())),
            (
                "event_time_mapping",
                object([
                    (
                        "format",
                        string(profile.event_time_mapping().format().as_str()),
                    ),
                    (
                        "json_pointer",
                        string(&profile.event_time_mapping().json_pointer().to_string()),
                    ),
                    (
                        "json_pointer_tokens",
                        pointer_tokens(profile.event_time_mapping().json_pointer()),
                    ),
                ]),
            ),
            (
                "ingest_profile_revision_id",
                string(&revision_id.to_string()),
            ),
            ("input_id", string(&input_id.to_string())),
            (
                "line_boundary_policy",
                string(profile.line_boundary_policy().as_str()),
            ),
            ("mappings", Value::Array(mappings)),
            (
                "maximum_record_bytes",
                unsigned(profile.maximum_record_bytes().get()),
            ),
            ("parser_kind", string(profile.parser_kind().as_str())),
            ("revision", unsigned(revision.get())),
            ("target_schema_id", string(&target_schema_id.to_string())),
            (
                "unknown_field_policy",
                string(profile.unknown_field_policy().as_str()),
            ),
        ]),
        path,
    )
}

fn profile_declaration(
    revision: &ManifestIngestProfileRevision,
    target_schema_version: u64,
    mappings: Vec<Value>,
    path: &str,
) -> Result<DeclarationDocument, CatalogApplicationError> {
    declaration_document(
        INGEST_PROFILE_DECLARATION_DOMAIN,
        object([
            (
                "conversion_policy",
                string(revision.conversion_policy.as_str()),
            ),
            ("encoding", string(revision.encoding.as_str())),
            (
                "event_time_mapping",
                object([
                    (
                        "format",
                        string(revision.event_time_mapping.format.as_str()),
                    ),
                    (
                        "json_pointer",
                        string(&revision.event_time_mapping.json_pointer.to_string()),
                    ),
                ]),
            ),
            (
                "line_boundary_policy",
                string(revision.line_boundary_policy.as_str()),
            ),
            ("mappings", Value::Array(mappings)),
            (
                "maximum_record_bytes",
                unsigned(revision.maximum_record_bytes.get()),
            ),
            ("parser_kind", string(revision.parser_kind.as_str())),
            ("revision", unsigned(revision.revision.get())),
            ("target_schema_version", unsigned(target_schema_version)),
            (
                "unknown_field_policy",
                string(revision.unknown_field_policy.as_str()),
            ),
        ]),
        path,
    )
}

fn schema_declaration(
    version: u64,
    fields: Vec<Value>,
    path: &str,
) -> Result<DeclarationDocument, CatalogApplicationError> {
    declaration_document(
        SCHEMA_DECLARATION_DOMAIN,
        object([
            ("fields", Value::Array(fields)),
            ("format_version", unsigned(1)),
            ("version", unsigned(version)),
        ]),
        path,
    )
}

fn field_declaration(
    name: &str,
    logical_type: &str,
    nullability: &str,
    role: &str,
    description: Option<&str>,
) -> Value {
    let mut fields = Map::new();
    if let Some(description) = description {
        fields.insert("description".to_owned(), string(description));
    }
    fields.insert("logical_type".to_owned(), string(logical_type));
    fields.insert("name".to_owned(), string(name));
    fields.insert("nullability".to_owned(), string(nullability));
    fields.insert("role".to_owned(), string(role));
    Value::Object(fields)
}

fn materialized_field(field: &Field) -> Value {
    let mut fields = Map::new();
    if let Some(description) = field.description() {
        fields.insert("description".to_owned(), string(description));
    }
    fields.insert("field_id".to_owned(), string(&field.id().to_string()));
    fields.insert(
        "logical_metadata".to_owned(),
        logical_metadata(field.logical_type()),
    );
    fields.insert(
        "logical_type".to_owned(),
        string(field.logical_type().as_str()),
    );
    fields.insert("name".to_owned(), string(field.name()));
    fields.insert(
        "nullability".to_owned(),
        string(field.nullability().as_str()),
    );
    fields.insert(
        "ordinal".to_owned(),
        unsigned(u64::from(field.ordinal().get())),
    );
    fields.insert("role".to_owned(), string(field.role().as_str()));
    Value::Object(fields)
}

fn arrow_field_descriptor(field: &Field) -> Value {
    object([
        (
            "metadata",
            object(
                std::iter::once(("elucid.field_id", string(&field.id().to_string()))).chain(
                    field
                        .logical_type()
                        .metadata_value()
                        .map(|value| ("elucid.logical_type", string(value))),
                ),
            ),
        ),
        ("name", string(field.name())),
        ("nullable", Value::Bool(field.nullability().is_nullable())),
        ("type", arrow_type_descriptor(field.logical_type())),
    ])
}

fn arrow_type_descriptor(logical_type: LogicalType) -> Value {
    match logical_type {
        LogicalType::Bool => object([("kind", string("BOOLEAN"))]),
        LogicalType::Int32 => object([("kind", string("INT32"))]),
        LogicalType::Int64 => object([("kind", string("INT64"))]),
        LogicalType::UInt32 => object([("kind", string("UINT32"))]),
        LogicalType::UInt64 => object([("kind", string("UINT64"))]),
        LogicalType::Float32 => object([("kind", string("FLOAT32"))]),
        LogicalType::Float64 => object([("kind", string("FLOAT64"))]),
        LogicalType::Utf8 | LogicalType::Json => object([("kind", string("UTF8"))]),
        LogicalType::Datetime => object([
            ("kind", string("TIMESTAMP")),
            ("time_unit", string("MILLISECOND")),
            ("timezone", string("UTC")),
        ]),
        LogicalType::Eid => object([
            ("byte_width", unsigned(16)),
            ("kind", string("FIXED_SIZE_BINARY")),
        ]),
    }
}

fn logical_metadata(logical_type: LogicalType) -> Value {
    object(
        logical_type
            .metadata_value()
            .map(|value| ("elucid.logical_type", string(value))),
    )
}

fn pointer_tokens(pointer: &JsonPointer) -> Value {
    Value::Array(
        pointer
            .tokens()
            .iter()
            .map(|token| string(token.as_str()))
            .collect(),
    )
}

fn declaration_document(
    domain: &[u8],
    value: Value,
    path: &str,
) -> Result<DeclarationDocument, CatalogApplicationError> {
    let json = encode_canonical_json(value, path)?;
    let digest = DeclarationDigest::new(digest(domain, json.as_bytes()));
    Ok(DeclarationDocument { json, digest })
}

fn materialized_document(
    domain: &[u8],
    value: Value,
    path: &str,
) -> Result<MaterializedDocument, CatalogApplicationError> {
    let json = encode_canonical_json(value, path)?;
    let digest = MaterializedDigest::new(digest(domain, json.as_bytes()));
    Ok(MaterializedDocument { json, digest })
}

fn encode_canonical_json(
    value: Value,
    path: &str,
) -> Result<CanonicalJson, CatalogApplicationError> {
    serde_json::to_string(&sort_objects(value))
        .map(|json| CanonicalJson::new(json.into_boxed_str()))
        .map_err(|source| CatalogApplicationError::CanonicalJsonEncoding {
            path: CatalogPath::new(path),
            source,
        })
}

fn sort_objects(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_objects).collect()),
        Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, sort_objects(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

fn digest(domain: &[u8], document: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(document);
    *hasher.finalize().as_bytes()
}

fn object<K, I>(entries: I) -> Value
where
    K: Into<String>,
    I: IntoIterator<Item = (K, Value)>,
{
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.into(), value))
            .collect(),
    )
}

fn string(value: &str) -> Value {
    Value::String(value.to_owned())
}

fn unsigned(value: u64) -> Value {
    Value::Number(Number::from(value))
}
