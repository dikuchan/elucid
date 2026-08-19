use std::collections::HashSet;

use serde::{Deserialize, Deserializer};
use yaml_rust2::scanner::{Scanner, Token, TokenType};

use crate::canonical::{
    DeclarationDocument, input_declaration, manifest_profile_declaration,
    manifest_schema_declaration, source_declaration,
};
use crate::{
    CatalogApplicationError, CatalogPath, ConversionPolicy, EventTimeFormat, InputEncoding,
    InputKind, InputName, JsonPointer, LineBoundaryPolicy, MaximumRecordBytes, Nullability,
    ParserKind, ProfileRevision, SchemaVersion, SourceName, UnknownFieldPolicy, UserFieldName,
    UserLogicalType,
};

const MANIFEST_FORMAT_VERSION: u64 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CatalogManifest {
    pub(crate) source: ManifestSource,
    pub(crate) declarations: ManifestDeclarations,
}

impl CatalogManifest {
    pub fn decode(bytes: &[u8]) -> Result<Self, CatalogApplicationError> {
        let document = std::str::from_utf8(bytes).map_err(|source| {
            CatalogApplicationError::ManifestNotUtf8 {
                path: CatalogPath::new("$"),
                source,
            }
        })?;
        audit_yaml_syntax(document)?;
        let raw: RawManifest = serde_yaml::from_str(document).map_err(|source| {
            CatalogApplicationError::ManifestYamlDecode {
                path: CatalogPath::new("$"),
                source,
            }
        })?;
        Self::try_from(raw)
    }

    #[must_use]
    pub fn source_name(&self) -> &SourceName {
        &self.source.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestSource {
    pub(crate) name: SourceName,
    pub(crate) display_name: String,
    pub(crate) active_schema_version: SchemaVersion,
    pub(crate) schemas: Vec<ManifestSchema>,
    pub(crate) inputs: Vec<ManifestInput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestSchema {
    pub(crate) version: SchemaVersion,
    pub(crate) fields: Vec<ManifestUserField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestUserField {
    pub(crate) name: UserFieldName,
    pub(crate) logical_type: UserLogicalType,
    pub(crate) nullability: Nullability,
    pub(crate) description: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestInput {
    pub(crate) name: InputName,
    pub(crate) kind: InputKind,
    pub(crate) active_ingestion_profile_revision: ProfileRevision,
    pub(crate) ingestion_profile_revisions: Vec<ManifestIngestionProfileRevision>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestIngestionProfileRevision {
    pub(crate) revision: ProfileRevision,
    pub(crate) target_schema_version: SchemaVersion,
    pub(crate) parser_kind: ParserKind,
    pub(crate) encoding: InputEncoding,
    pub(crate) line_boundary_policy: LineBoundaryPolicy,
    pub(crate) maximum_record_bytes: MaximumRecordBytes,
    pub(crate) conversion_policy: ConversionPolicy,
    pub(crate) unknown_field_policy: UnknownFieldPolicy,
    pub(crate) event_time_mapping: ManifestEventTimeMapping,
    pub(crate) mappings: Vec<ManifestFieldMapping>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestEventTimeMapping {
    pub(crate) json_pointer: JsonPointer,
    pub(crate) format: EventTimeFormat,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestFieldMapping {
    pub(crate) target_field: UserFieldName,
    pub(crate) json_pointer: JsonPointer,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestDeclarations {
    pub(crate) source: DeclarationDocument,
    pub(crate) schemas: Vec<DeclarationDocument>,
    pub(crate) inputs: Vec<ManifestInputDeclarations>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestInputDeclarations {
    pub(crate) input: DeclarationDocument,
    pub(crate) ingestion_profiles: Vec<DeclarationDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawManifest {
    format_version: u64,
    source: RawSource,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSource {
    name: String,
    display_name: String,
    active_schema_version: u64,
    schemas: Vec<RawSchema>,
    inputs: Vec<RawInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSchema {
    version: u64,
    fields: Vec<RawUserField>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUserField {
    name: String,
    logical_type: RawUserLogicalType,
    nullability: RawNullability,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInput {
    name: String,
    kind: RawInputKind,
    active_ingestion_profile_revision: u64,
    ingestion_profile_revisions: Vec<RawIngestionProfileRevision>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIngestionProfileRevision {
    revision: u64,
    target_schema_version: u64,
    parser_kind: RawParserKind,
    encoding: RawInputEncoding,
    line_boundary_policy: RawLineBoundaryPolicy,
    maximum_record_bytes: u64,
    conversion_policy: RawConversionPolicy,
    unknown_field_policy: RawUnknownFieldPolicy,
    event_time_mapping: RawEventTimeMapping,
    mappings: Vec<RawFieldMapping>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEventTimeMapping {
    json_pointer: String,
    format: RawEventTimeFormat,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFieldMapping {
    target_field: String,
    json_pointer: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum RawUserLogicalType {
    #[serde(rename = "bool")]
    Bool,
    #[serde(rename = "int32")]
    Int32,
    #[serde(rename = "int64")]
    Int64,
    #[serde(rename = "uint32")]
    UInt32,
    #[serde(rename = "uint64")]
    UInt64,
    #[serde(rename = "float32")]
    Float32,
    #[serde(rename = "float64")]
    Float64,
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "datetime")]
    Datetime,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum RawNullability {
    #[serde(rename = "NON_NULL")]
    NonNull,
    #[serde(rename = "NULLABLE")]
    Nullable,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum RawInputKind {
    #[serde(rename = "HTTP_NDJSON")]
    HttpNdjson,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum RawParserKind {
    #[serde(rename = "NDJSON")]
    Ndjson,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum RawInputEncoding {
    #[serde(rename = "UTF8")]
    Utf8,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum RawLineBoundaryPolicy {
    #[serde(rename = "LF_WITH_OPTIONAL_CR")]
    LfWithOptionalCr,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum RawConversionPolicy {
    #[serde(rename = "STRICT")]
    Strict,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum RawUnknownFieldPolicy {
    #[serde(rename = "CAPTURE_TOP_LEVEL_REMAINDER")]
    CaptureTopLevelRemainder,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum RawEventTimeFormat {
    #[serde(rename = "RFC3339")]
    Rfc3339,
    #[serde(rename = "UNIX_MILLISECONDS")]
    UnixMilliseconds,
}

impl TryFrom<RawManifest> for CatalogManifest {
    type Error = CatalogApplicationError;

    fn try_from(raw: RawManifest) -> Result<Self, Self::Error> {
        if raw.format_version != MANIFEST_FORMAT_VERSION {
            return Err(CatalogApplicationError::manifest(
                "format_version",
                format!(
                    "format version must be {MANIFEST_FORMAT_VERSION}, got {}",
                    raw.format_version
                ),
            ));
        }
        let source = ManifestSource::try_from(raw.source)?;
        let declarations = ManifestDeclarations::new(&source)?;
        Ok(Self {
            source,
            declarations,
        })
    }
}

impl ManifestDeclarations {
    fn new(source: &ManifestSource) -> Result<Self, CatalogApplicationError> {
        let source_declaration = source_declaration(&source.name)?;
        let schemas = source
            .schemas
            .iter()
            .enumerate()
            .map(|(index, schema)| {
                manifest_schema_declaration(schema, &format!("source.schemas[{index}]"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let inputs = source
            .inputs
            .iter()
            .enumerate()
            .map(|(input_index, input)| {
                let path = format!("source.inputs[{input_index}]");
                let input_declaration = input_declaration(&input.name, input.kind, &path)?;
                let ingestion_profiles = input
                    .ingestion_profile_revisions
                    .iter()
                    .enumerate()
                    .map(|(revision_index, revision)| {
                        manifest_profile_declaration(
                            revision,
                            &format!("{path}.ingestion_profile_revisions[{revision_index}]"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ManifestInputDeclarations {
                    input: input_declaration,
                    ingestion_profiles,
                })
            })
            .collect::<Result<Vec<_>, CatalogApplicationError>>()?;
        Ok(Self {
            source: source_declaration,
            schemas,
            inputs,
        })
    }
}

impl TryFrom<RawSource> for ManifestSource {
    type Error = CatalogApplicationError;

    fn try_from(raw: RawSource) -> Result<Self, Self::Error> {
        let name = SourceName::try_from(raw.name)
            .map_err(|source| CatalogApplicationError::manifest_model("source.name", source))?;
        if raw.schemas.is_empty() {
            return Err(CatalogApplicationError::manifest(
                "source.schemas",
                "schema history must not be empty",
            ));
        }

        let schemas = raw
            .schemas
            .into_iter()
            .enumerate()
            .map(|(index, schema)| ManifestSchema::try_from_raw(schema, index))
            .collect::<Result<Vec<_>, _>>()?;
        let active_schema_version =
            SchemaVersion::new(raw.active_schema_version).map_err(|source| {
                CatalogApplicationError::manifest_model("source.active_schema_version", source)
            })?;
        if !schemas
            .iter()
            .any(|schema| schema.version == active_schema_version)
        {
            return Err(CatalogApplicationError::manifest(
                "source.active_schema_version",
                format!(
                    "schema version {} is absent from source.schemas",
                    active_schema_version.get()
                ),
            ));
        }

        let mut input_names = HashSet::with_capacity(raw.inputs.len());
        let inputs = raw
            .inputs
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                let input = ManifestInput::try_from_raw(input, index, &schemas)?;
                if !input_names.insert(input.name.clone()) {
                    return Err(CatalogApplicationError::manifest(
                        format!("source.inputs[{index}].name"),
                        format!("input name {:?} occurs more than once", input.name.as_str()),
                    ));
                }
                Ok(input)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            name,
            display_name: raw.display_name,
            active_schema_version,
            schemas,
            inputs,
        })
    }
}

impl ManifestSchema {
    fn try_from_raw(raw: RawSchema, index: usize) -> Result<Self, CatalogApplicationError> {
        let path = format!("source.schemas[{index}]");
        let version = SchemaVersion::new(raw.version).map_err(|source| {
            CatalogApplicationError::manifest_model(format!("{path}.version"), source)
        })?;
        let expected = sequence_number(index, &format!("{path}.version"))?;
        if version.get() != expected {
            return Err(CatalogApplicationError::manifest(
                format!("{path}.version"),
                format!(
                    "schema versions must be contiguous: expected {expected}, got {}",
                    version.get()
                ),
            ));
        }

        let mut names = HashSet::with_capacity(raw.fields.len());
        let fields = raw
            .fields
            .into_iter()
            .enumerate()
            .map(|(field_index, field)| {
                let field_path = format!("{path}.fields[{field_index}]");
                let name = UserFieldName::try_from(field.name).map_err(|source| {
                    CatalogApplicationError::manifest_model(format!("{field_path}.name"), source)
                })?;
                if !names.insert(name.clone()) {
                    return Err(CatalogApplicationError::manifest(
                        format!("{field_path}.name"),
                        format!("field name {:?} occurs more than once", name.as_str()),
                    ));
                }
                Ok(ManifestUserField {
                    name,
                    logical_type: field.logical_type.into(),
                    nullability: field.nullability.into(),
                    description: field.description,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { version, fields })
    }
}

impl ManifestInput {
    fn try_from_raw(
        raw: RawInput,
        input_index: usize,
        schemas: &[ManifestSchema],
    ) -> Result<Self, CatalogApplicationError> {
        let path = format!("source.inputs[{input_index}]");
        let name = InputName::try_from(raw.name).map_err(|source| {
            CatalogApplicationError::manifest_model(format!("{path}.name"), source)
        })?;
        if raw.ingestion_profile_revisions.is_empty() {
            return Err(CatalogApplicationError::manifest(
                format!("{path}.ingestion_profile_revisions"),
                "ingestion profile history must not be empty",
            ));
        }
        let revisions = raw
            .ingestion_profile_revisions
            .into_iter()
            .enumerate()
            .map(|(revision_index, revision)| {
                ManifestIngestionProfileRevision::try_from_raw(
                    revision,
                    input_index,
                    revision_index,
                    schemas,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let active_ingestion_profile_revision =
            ProfileRevision::new(raw.active_ingestion_profile_revision).map_err(|source| {
                CatalogApplicationError::manifest_model(
                    format!("{path}.active_ingestion_profile_revision"),
                    source,
                )
            })?;
        if !revisions
            .iter()
            .any(|revision| revision.revision == active_ingestion_profile_revision)
        {
            return Err(CatalogApplicationError::manifest(
                format!("{path}.active_ingestion_profile_revision"),
                format!(
                    "ingestion profile revision {} is absent from {path}.ingestion_profile_revisions",
                    active_ingestion_profile_revision.get()
                ),
            ));
        }
        Ok(Self {
            name,
            kind: raw.kind.into(),
            active_ingestion_profile_revision,
            ingestion_profile_revisions: revisions,
        })
    }
}

impl ManifestIngestionProfileRevision {
    fn try_from_raw(
        raw: RawIngestionProfileRevision,
        input_index: usize,
        revision_index: usize,
        schemas: &[ManifestSchema],
    ) -> Result<Self, CatalogApplicationError> {
        let path =
            format!("source.inputs[{input_index}].ingestion_profile_revisions[{revision_index}]");
        let revision = ProfileRevision::new(raw.revision).map_err(|source| {
            CatalogApplicationError::manifest_model(format!("{path}.revision"), source)
        })?;
        let expected = sequence_number(revision_index, &format!("{path}.revision"))?;
        if revision.get() != expected {
            return Err(CatalogApplicationError::manifest(
                format!("{path}.revision"),
                format!(
                    "ingestion profile revisions must be contiguous: expected {expected}, got {}",
                    revision.get()
                ),
            ));
        }

        let target_schema_version =
            SchemaVersion::new(raw.target_schema_version).map_err(|source| {
                CatalogApplicationError::manifest_model(
                    format!("{path}.target_schema_version"),
                    source,
                )
            })?;
        let Some(target_schema) = schemas
            .iter()
            .find(|schema| schema.version == target_schema_version)
        else {
            return Err(CatalogApplicationError::profile_target(
                format!("{path}.target_schema_version"),
                format!(
                    "schema version {} is absent from source.schemas",
                    target_schema_version.get()
                ),
            ));
        };

        let event_time_mapping = ManifestEventTimeMapping {
            json_pointer: JsonPointer::parse(&raw.event_time_mapping.json_pointer).map_err(
                |source| {
                    CatalogApplicationError::manifest_model(
                        format!("{path}.event_time_mapping.json_pointer"),
                        source,
                    )
                },
            )?,
            format: raw.event_time_mapping.format.into(),
        };

        let target_names = target_schema
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect::<HashSet<_>>();
        let mut mapped_names = HashSet::with_capacity(raw.mappings.len());
        let mappings = raw
            .mappings
            .into_iter()
            .enumerate()
            .map(|(mapping_index, mapping)| {
                let mapping_path = format!("{path}.mappings[{mapping_index}]");
                let target_field =
                    UserFieldName::try_from(mapping.target_field).map_err(|source| {
                        CatalogApplicationError::manifest_model(
                            format!("{mapping_path}.target_field"),
                            source,
                        )
                    })?;
                if !target_names.contains(&target_field) {
                    return Err(CatalogApplicationError::profile_target(
                        format!("{mapping_path}.target_field"),
                        format!(
                            "field {:?} is absent from target schema version {}",
                            target_field.as_str(),
                            target_schema_version.get()
                        ),
                    ));
                }
                if !mapped_names.insert(target_field.clone()) {
                    return Err(CatalogApplicationError::profile_target(
                        format!("{mapping_path}.target_field"),
                        format!("field {:?} is mapped more than once", target_field.as_str()),
                    ));
                }
                let json_pointer = JsonPointer::parse(&mapping.json_pointer).map_err(|source| {
                    CatalogApplicationError::manifest_model(
                        format!("{mapping_path}.json_pointer"),
                        source,
                    )
                })?;
                Ok(ManifestFieldMapping {
                    target_field,
                    json_pointer,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(missing) = target_schema
            .fields
            .iter()
            .find(|field| !mapped_names.contains(&field.name))
        {
            return Err(CatalogApplicationError::profile_target(
                format!("{path}.mappings"),
                format!(
                    "target schema field {:?} has no mapping",
                    missing.name.as_str()
                ),
            ));
        }

        Ok(Self {
            revision,
            target_schema_version,
            parser_kind: raw.parser_kind.into(),
            encoding: raw.encoding.into(),
            line_boundary_policy: raw.line_boundary_policy.into(),
            maximum_record_bytes: MaximumRecordBytes::new(raw.maximum_record_bytes).map_err(
                |source| {
                    CatalogApplicationError::manifest_model(
                        format!("{path}.maximum_record_bytes"),
                        source,
                    )
                },
            )?,
            conversion_policy: raw.conversion_policy.into(),
            unknown_field_policy: raw.unknown_field_policy.into(),
            event_time_mapping,
            mappings,
        })
    }
}

fn audit_yaml_syntax(document: &str) -> Result<(), CatalogApplicationError> {
    let mut scanner = Scanner::new(document.chars());
    while let Some(Token(_, token)) =
        scanner
            .next_token()
            .map_err(|source| CatalogApplicationError::ManifestYamlSyntax {
                path: CatalogPath::new("$"),
                source,
            })?
    {
        match token {
            TokenType::VersionDirective(1, 2) => {}
            TokenType::VersionDirective(major, minor) => {
                return Err(CatalogApplicationError::manifest(
                    "$",
                    format!("YAML version must be 1.2, got {major}.{minor}"),
                ));
            }
            TokenType::Alias(_) => {
                return Err(CatalogApplicationError::manifest(
                    "$",
                    "YAML aliases are not allowed",
                ));
            }
            TokenType::Tag(_, _) => {
                return Err(CatalogApplicationError::manifest(
                    "$",
                    "explicit YAML tags are not allowed",
                ));
            }
            TokenType::StreamStart(_)
            | TokenType::StreamEnd
            | TokenType::TagDirective(_, _)
            | TokenType::DocumentStart
            | TokenType::DocumentEnd
            | TokenType::BlockSequenceStart
            | TokenType::BlockMappingStart
            | TokenType::BlockEnd
            | TokenType::FlowSequenceStart
            | TokenType::FlowSequenceEnd
            | TokenType::FlowMappingStart
            | TokenType::FlowMappingEnd
            | TokenType::BlockEntry
            | TokenType::FlowEntry
            | TokenType::Key
            | TokenType::Value
            | TokenType::Anchor(_)
            | TokenType::Scalar(_, _) => {}
        }
    }
    Ok(())
}

fn sequence_number(index: usize, path: &str) -> Result<u64, CatalogApplicationError> {
    u64::try_from(index)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| CatalogApplicationError::manifest(path, "history is too long"))
}

fn deserialize_optional_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer).map(Some)
}

impl From<RawUserLogicalType> for UserLogicalType {
    fn from(value: RawUserLogicalType) -> Self {
        match value {
            RawUserLogicalType::Bool => Self::Bool,
            RawUserLogicalType::Int32 => Self::Int32,
            RawUserLogicalType::Int64 => Self::Int64,
            RawUserLogicalType::UInt32 => Self::UInt32,
            RawUserLogicalType::UInt64 => Self::UInt64,
            RawUserLogicalType::Float32 => Self::Float32,
            RawUserLogicalType::Float64 => Self::Float64,
            RawUserLogicalType::Utf8 => Self::Utf8,
            RawUserLogicalType::Datetime => Self::Datetime,
        }
    }
}

impl From<RawNullability> for Nullability {
    fn from(value: RawNullability) -> Self {
        match value {
            RawNullability::NonNull => Self::NonNull,
            RawNullability::Nullable => Self::Nullable,
        }
    }
}

impl From<RawInputKind> for InputKind {
    fn from(value: RawInputKind) -> Self {
        match value {
            RawInputKind::HttpNdjson => Self::HttpNdjson,
        }
    }
}

impl From<RawParserKind> for ParserKind {
    fn from(value: RawParserKind) -> Self {
        match value {
            RawParserKind::Ndjson => Self::Ndjson,
        }
    }
}

impl From<RawInputEncoding> for InputEncoding {
    fn from(value: RawInputEncoding) -> Self {
        match value {
            RawInputEncoding::Utf8 => Self::Utf8,
        }
    }
}

impl From<RawLineBoundaryPolicy> for LineBoundaryPolicy {
    fn from(value: RawLineBoundaryPolicy) -> Self {
        match value {
            RawLineBoundaryPolicy::LfWithOptionalCr => Self::LfWithOptionalCr,
        }
    }
}

impl From<RawConversionPolicy> for ConversionPolicy {
    fn from(value: RawConversionPolicy) -> Self {
        match value {
            RawConversionPolicy::Strict => Self::Strict,
        }
    }
}

impl From<RawUnknownFieldPolicy> for UnknownFieldPolicy {
    fn from(value: RawUnknownFieldPolicy) -> Self {
        match value {
            RawUnknownFieldPolicy::CaptureTopLevelRemainder => Self::CaptureTopLevelRemainder,
        }
    }
}

impl From<RawEventTimeFormat> for EventTimeFormat {
    fn from(value: RawEventTimeFormat) -> Self {
        match value {
            RawEventTimeFormat::Rfc3339 => Self::Rfc3339,
            RawEventTimeFormat::UnixMilliseconds => Self::UnixMilliseconds,
        }
    }
}
