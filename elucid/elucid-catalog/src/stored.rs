use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::canonical::{
    input_declaration, input_materialized, profile_materialized, schema_materialized,
    source_declaration, stored_profile_declaration, stored_schema_declaration,
};
use crate::{
    CatalogApplicationError, CatalogModelError, DeclarationDigest, DefinitionDigests,
    EventTimeFormat, EventTimeMapping, FieldId, FieldMapping, IngestionProfile,
    IngestionProfileRevision, IngestionProfileRevisionId, Input, InputId, InputName, JsonPointer,
    MaterializedDigest, MaximumRecordBytes, Nullability, ProfileRevision, Schema, SchemaId,
    SchemaVersion, Source, SourceId, SourceName, UserField, UserFieldName, UserLogicalType,
};

pub fn decode_stored_schema_definition(
    schema_id: SchemaId,
    source_id: SourceId,
    version: SchemaVersion,
    definition: Value,
) -> Result<Schema, CatalogApplicationError> {
    let path = format!("schema_versions[{schema_id}].definition");
    let document = decode_definition::<StoredSchemaDocument>(&definition, &path)?;
    verify_uuid(&document.schema_id, schema_id.as_uuid(), &path, "schema_id")?;
    verify_uuid(&document.source_id, source_id.as_uuid(), &path, "source_id")?;
    if document.version != version.get() {
        return corruption(&path, "embedded schema version does not match its row");
    }

    let user_fields = document
        .fields
        .into_iter()
        .enumerate()
        .filter(|(_, field)| field.role == "DATA")
        .map(|(index, field)| decode_user_field(field, &format!("{path}.fields[{index}]")))
        .collect::<Result<Vec<_>, _>>()?;
    let provisional = Schema::new(
        schema_id,
        source_id,
        version,
        empty_digests(),
        user_fields.clone(),
    )
    .map_err(|source| stored_model_error(&path, source))?;
    let declaration = stored_schema_declaration(&provisional, &path)?;
    let materialized = schema_materialized(&provisional, &path)?;
    verify_materialized_document(&definition, materialized.json.as_str(), &path)?;

    Schema::new(
        schema_id,
        source_id,
        version,
        DefinitionDigests::new(declaration.digest, materialized.digest),
        user_fields,
    )
    .map_err(|source| stored_model_error(&path, source))
}

pub fn decode_stored_profile_definition(
    revision_id: IngestionProfileRevisionId,
    input_id: InputId,
    revision: ProfileRevision,
    target_schema: &Schema,
    definition: Value,
) -> Result<IngestionProfileRevision, CatalogApplicationError> {
    let path = format!("ingestion_profile_revisions[{revision_id}].definition");
    let document = decode_definition::<StoredProfileDocument>(&definition, &path)?;
    verify_uuid(
        &document.ingestion_profile_revision_id,
        revision_id.as_uuid(),
        &path,
        "ingestion_profile_revision_id",
    )?;
    verify_uuid(&document.input_id, input_id.as_uuid(), &path, "input_id")?;
    verify_uuid(
        &document.target_schema_id,
        target_schema.id().as_uuid(),
        &path,
        "target_schema_id",
    )?;
    if document.revision != revision.get() {
        return corruption(
            &path,
            "embedded ingestion profile revision does not match its row",
        );
    }

    let maximum_record_bytes = MaximumRecordBytes::new(document.maximum_record_bytes)
        .map_err(|source| stored_model_error(&path, source))?;
    let event_time = EventTimeMapping::new(
        JsonPointer::parse(&document.event_time.json_pointer)
            .map_err(|source| stored_model_error(&path, source))?,
        decode_event_time_format(&document.event_time.format, &path)?,
    );
    let mappings = document
        .mappings
        .into_iter()
        .enumerate()
        .map(|(index, mapping)| {
            let mapping_path = format!("{path}.mappings[{index}]");
            let field_id = parse_field_id(&mapping.target_field_id, &mapping_path)?;
            let pointer = JsonPointer::parse(&mapping.json_pointer)
                .map_err(|source| stored_model_error(&mapping_path, source))?;
            FieldMapping::new(field_id, pointer)
                .map_err(|source| stored_model_error(&mapping_path, source))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let profile = IngestionProfile::new(maximum_record_bytes, event_time, mappings)
        .map_err(|source| stored_model_error(&path, source))?;
    let provisional = IngestionProfileRevision::new(
        revision_id,
        input_id,
        revision,
        target_schema.id(),
        empty_digests(),
        profile.clone(),
    );
    let declaration = stored_profile_declaration(&provisional, target_schema, &path)?;
    let materialized = profile_materialized(&provisional, &path)?;
    verify_materialized_document(&definition, materialized.json.as_str(), &path)?;

    Ok(IngestionProfileRevision::new(
        revision_id,
        input_id,
        revision,
        target_schema.id(),
        DefinitionDigests::new(declaration.digest, materialized.digest),
        profile,
    ))
}

pub fn assemble_stored_input(
    input_id: InputId,
    source_id: SourceId,
    name: InputName,
    active_profile_revision_id: IngestionProfileRevisionId,
    profile_revisions: Vec<IngestionProfileRevision>,
) -> Result<Input, CatalogApplicationError> {
    let path = format!("inputs[{input_id}]");
    let declaration = input_declaration(&name, &path)?;
    let materialized = input_materialized(input_id, source_id, &name, &path)?;
    Input::new(
        input_id,
        source_id,
        name,
        DefinitionDigests::new(declaration.digest, materialized.digest),
        active_profile_revision_id,
        profile_revisions,
    )
    .map_err(|source| stored_model_error(&path, source))
}

pub fn assemble_stored_source(
    source_id: SourceId,
    name: SourceName,
    display_name: String,
    active_schema_id: SchemaId,
    schemas: Vec<Schema>,
    inputs: Vec<Input>,
) -> Result<Source, CatalogApplicationError> {
    let path = format!("sources[{source_id}]");
    let declaration = source_declaration(&name)?;
    Source::new(
        source_id,
        name,
        display_name,
        declaration.digest,
        active_schema_id,
        schemas,
        inputs,
    )
    .map_err(|source| stored_model_error(&path, source))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSchemaDocument {
    #[serde(rename = "arrow_schema_descriptor")]
    _arrow_schema_descriptor: Value,
    fields: Vec<StoredFieldDocument>,
    schema_id: String,
    source_id: String,
    version: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredFieldDocument {
    field_id: String,
    #[serde(rename = "logical_metadata")]
    _logical_metadata: Value,
    logical_type: String,
    name: String,
    nullability: String,
    #[serde(rename = "ordinal")]
    _ordinal: u64,
    role: String,
    description: Option<String>,
    historical_remainder_pointer: Option<String>,
    #[serde(rename = "historical_remainder_pointer_tokens")]
    _historical_remainder_pointer_tokens: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredProfileDocument {
    event_time: StoredEventTimeDocument,
    ingestion_profile_revision_id: String,
    input_id: String,
    mappings: Vec<StoredMappingDocument>,
    maximum_record_bytes: u64,
    revision: u64,
    target_schema_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredEventTimeDocument {
    format: String,
    json_pointer: String,
    #[serde(rename = "json_pointer_tokens")]
    _json_pointer_tokens: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredMappingDocument {
    json_pointer: String,
    #[serde(rename = "json_pointer_tokens")]
    _json_pointer_tokens: Value,
    target_field_id: String,
}

fn decode_user_field(
    field: StoredFieldDocument,
    path: &str,
) -> Result<UserField, CatalogApplicationError> {
    let id = parse_field_id(&field.field_id, path)?;
    let name =
        UserFieldName::try_from(field.name).map_err(|source| stored_model_error(path, source))?;
    let logical_type = decode_user_logical_type(&field.logical_type, path)?;
    let nullability = decode_nullability(&field.nullability, path)?;
    let user_field = UserField::new(id, name, logical_type, nullability)
        .map_err(|source| stored_model_error(path, source))?;
    let user_field = match field.description {
        Some(description) => user_field.with_description(description),
        None => user_field,
    };
    match field.historical_remainder_pointer {
        Some(pointer) => user_field
            .with_historical_remainder_pointer(
                JsonPointer::parse(&pointer).map_err(|source| stored_model_error(path, source))?,
            )
            .map_err(|source| stored_model_error(path, source)),
        None => Ok(user_field),
    }
}

fn decode_user_logical_type(
    value: &str,
    path: &str,
) -> Result<UserLogicalType, CatalogApplicationError> {
    match value {
        "bool" => Ok(UserLogicalType::Bool),
        "int32" => Ok(UserLogicalType::Int32),
        "int64" => Ok(UserLogicalType::Int64),
        "uint32" => Ok(UserLogicalType::UInt32),
        "uint64" => Ok(UserLogicalType::UInt64),
        "float32" => Ok(UserLogicalType::Float32),
        "float64" => Ok(UserLogicalType::Float64),
        "utf8" => Ok(UserLogicalType::Utf8),
        "datetime" => Ok(UserLogicalType::Datetime),
        _ => corruption(path, "stored user field has an unknown logical type"),
    }
}

fn decode_nullability(value: &str, path: &str) -> Result<Nullability, CatalogApplicationError> {
    match value {
        "NON_NULL" => Ok(Nullability::NonNull),
        "NULLABLE" => Ok(Nullability::Nullable),
        _ => corruption(path, "stored field has an unknown nullability"),
    }
}

fn decode_event_time_format(
    value: &str,
    path: &str,
) -> Result<EventTimeFormat, CatalogApplicationError> {
    match value {
        "RFC3339" => Ok(EventTimeFormat::Rfc3339),
        "UNIX_MILLISECONDS" => Ok(EventTimeFormat::UnixMilliseconds),
        _ => corruption(
            path,
            "stored ingestion profile has an unknown event-time format",
        ),
    }
}

fn parse_field_id(value: &str, path: &str) -> Result<FieldId, CatalogApplicationError> {
    let uuid = Uuid::parse_str(value)
        .map_err(|_| CatalogApplicationError::corruption(path, "stored field ID is not a UUID"))?;
    FieldId::try_from(uuid).map_err(|source| stored_model_error(path, source))
}

fn verify_uuid(
    value: &str,
    expected: Uuid,
    path: &str,
    field: &str,
) -> Result<(), CatalogApplicationError> {
    let actual = Uuid::parse_str(value).map_err(|_| {
        CatalogApplicationError::corruption(path, format!("embedded {field} is not a UUID"))
    })?;
    if actual == expected {
        Ok(())
    } else {
        corruption(path, format!("embedded {field} does not match its row"))
    }
}

fn decode_definition<'de, T>(
    definition: &'de Value,
    path: &str,
) -> Result<T, CatalogApplicationError>
where
    T: Deserialize<'de>,
{
    T::deserialize(definition)
        .map_err(|_| CatalogApplicationError::corruption(path, "definition shape is invalid"))
}

fn verify_materialized_document(
    stored: &Value,
    canonical: &str,
    path: &str,
) -> Result<(), CatalogApplicationError> {
    let canonical = serde_json::from_str::<Value>(canonical).map_err(|_| {
        CatalogApplicationError::corruption(path, "generated canonical definition is invalid")
    })?;
    if stored == &canonical {
        Ok(())
    } else {
        corruption(
            path,
            "definition does not match its canonical materialization",
        )
    }
}

const fn empty_digests() -> DefinitionDigests {
    DefinitionDigests::new(
        DeclarationDigest::new([0; 32]),
        MaterializedDigest::new([0; 32]),
    )
}

fn stored_model_error(path: &str, source: CatalogModelError) -> CatalogApplicationError {
    CatalogApplicationError::corruption(path, source.to_string())
}

fn corruption<T>(path: &str, message: impl Into<String>) -> Result<T, CatalogApplicationError> {
    Err(CatalogApplicationError::corruption(path, message))
}
