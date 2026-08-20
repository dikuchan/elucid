use std::fmt::{Display, Formatter};
use std::str::FromStr as _;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Timelike as _, Utc};
use elucid_catalog::{
    EventTimeFormat, FieldId, FieldRole, IngestionProfile, JsonPointer, LogicalType, Nullability,
    Schema, Source,
};
use serde_json::{Map, Number, Value};

use crate::{BatchId, BatchMetadata, IngestionTime};

const EVENT_ID_DOMAIN: &[u8] = b"elucid:event\0";
const MAXIMUM_JSON_CONTAINER_DEPTH: usize = 128;

/// Maximum number of original payload bytes retained in a dead-letter entry.
pub const DEAD_LETTER_PAYLOAD_PREFIX_BYTES: usize = 4 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct RecordLocation {
    line_number: u64,
    input_position: u64,
}

impl RecordLocation {
    const fn new(line_number: u64, input_position: u64) -> Self {
        Self {
            line_number,
            input_position,
        }
    }

    #[must_use]
    pub const fn line_number(self) -> u64 {
        self.line_number
    }

    #[must_use]
    pub const fn input_position(self) -> u64 {
        self.input_position
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct EventTime(i64);

impl EventTime {
    const fn from_unix_milliseconds(value: i64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn unix_milliseconds(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct EventId([u8; 16]);

impl EventId {
    fn for_occurrence(batch_id: BatchId, input_position: u64) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(EVENT_ID_DOMAIN);
        hasher.update(batch_id.as_uuid().as_bytes());
        hasher.update(&input_position.to_be_bytes());
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct RecordPayloadDigest([u8; 32]);

impl RecordPayloadDigest {
    fn calculate(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum NormalizedValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Datetime(i64),
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct NormalizedField {
    field_id: FieldId,
    value: NormalizedValue,
}

impl NormalizedField {
    #[must_use]
    pub const fn field_id(&self) -> FieldId {
        self.field_id
    }

    #[must_use]
    pub const fn value(&self) -> &NormalizedValue {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct JsonObject(Map<String, Value>);

impl JsonObject {
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    #[must_use]
    pub const fn as_map(&self) -> &Map<String, Value> {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct AcceptedRow {
    location: RecordLocation,
    event_time: EventTime,
    ingestion_time: IngestionTime,
    event_id: EventId,
    fields: Vec<NormalizedField>,
    remainder: Option<JsonObject>,
}

impl AcceptedRow {
    #[must_use]
    pub const fn location(&self) -> RecordLocation {
        self.location
    }

    #[must_use]
    pub const fn event_time(&self) -> EventTime {
        self.event_time
    }

    #[must_use]
    pub const fn ingestion_time(&self) -> IngestionTime {
        self.ingestion_time
    }

    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    #[must_use]
    pub fn fields(&self) -> &[NormalizedField] {
        &self.fields
    }

    #[must_use]
    pub const fn remainder(&self) -> Option<&JsonObject> {
        self.remainder.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum DeadLetterCode {
    InvalidUtf8,
    TooLarge,
    ParseFailed,
    FieldMissing,
    FieldNull,
    ConversionFailed,
    EventTimeInvalid,
    EventDayLimitExceeded,
}

impl DeadLetterCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidUtf8 => "RECORD_INVALID_UTF8",
            Self::TooLarge => "RECORD_TOO_LARGE",
            Self::ParseFailed => "RECORD_PARSE_FAILED",
            Self::FieldMissing => "RECORD_FIELD_MISSING",
            Self::FieldNull => "RECORD_FIELD_NULL",
            Self::ConversionFailed => "RECORD_CONVERSION_FAILED",
            Self::EventTimeInvalid => "RECORD_EVENT_TIME_INVALID",
            Self::EventDayLimitExceeded => "RECORD_EVENT_DAY_LIMIT_EXCEEDED",
        }
    }

    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::InvalidUtf8 => "record is not valid UTF-8",
            Self::TooLarge => "record exceeds the pinned profile byte limit",
            Self::ParseFailed => "record is not exactly one JSON object with unique keys",
            Self::FieldMissing => "a required mapped field is absent",
            Self::FieldNull => "a required mapped field is null",
            Self::ConversionFailed => "a mapped field cannot be converted to its declared type",
            Self::EventTimeInvalid => "event time is absent, null, or invalid",
            Self::EventDayLimitExceeded => "record introduces too many distinct event days",
        }
    }
}

impl Display for DeadLetterCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum PayloadEncoding {
    Utf8,
    Base64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum PayloadExtent {
    Complete,
    Prefix,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeadLetterPayload {
    content: String,
    encoding: PayloadEncoding,
    extent: PayloadExtent,
}

impl DeadLetterPayload {
    fn from_record(record: &[u8]) -> Self {
        let extent = if record.len() <= DEAD_LETTER_PAYLOAD_PREFIX_BYTES {
            PayloadExtent::Complete
        } else {
            PayloadExtent::Prefix
        };
        let prefix_bytes = record.len().min(DEAD_LETTER_PAYLOAD_PREFIX_BYTES);
        match std::str::from_utf8(record) {
            Ok(text) => {
                let mut prefix_end = prefix_bytes;
                while !text.is_char_boundary(prefix_end) {
                    prefix_end -= 1;
                }
                Self {
                    content: text[..prefix_end].to_owned(),
                    encoding: PayloadEncoding::Utf8,
                    extent,
                }
            }
            Err(_) => Self {
                content: BASE64.encode(&record[..prefix_bytes]),
                encoding: PayloadEncoding::Base64,
                extent,
            },
        }
    }

    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    #[must_use]
    pub const fn encoding(&self) -> PayloadEncoding {
        self.encoding
    }

    #[must_use]
    pub const fn extent(&self) -> PayloadExtent {
        self.extent
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeadLetterEntry {
    batch_id: BatchId,
    location: RecordLocation,
    code: DeadLetterCode,
    payload_byte_count: u64,
    payload_digest: RecordPayloadDigest,
    payload: DeadLetterPayload,
}

impl DeadLetterEntry {
    fn new(
        batch_id: BatchId,
        location: RecordLocation,
        code: DeadLetterCode,
        record: &[u8],
        payload_byte_count: u64,
    ) -> Self {
        Self {
            batch_id,
            location,
            code,
            payload_byte_count,
            payload_digest: RecordPayloadDigest::calculate(record),
            payload: DeadLetterPayload::from_record(record),
        }
    }

    #[must_use]
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    #[must_use]
    pub const fn location(&self) -> RecordLocation {
        self.location
    }

    #[must_use]
    pub const fn code(&self) -> DeadLetterCode {
        self.code
    }

    #[must_use]
    pub const fn message(&self) -> &'static str {
        self.code.message()
    }

    #[must_use]
    pub const fn payload_byte_count(&self) -> u64 {
        self.payload_byte_count
    }

    #[must_use]
    pub const fn payload_digest(&self) -> RecordPayloadDigest {
        self.payload_digest
    }

    #[must_use]
    pub const fn payload(&self) -> &DeadLetterPayload {
        &self.payload
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum NormalizedRecord {
    Accepted(AcceptedRow),
    DeadLetter(DeadLetterEntry),
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct NormalizedBatch {
    metadata: BatchMetadata,
    records: Vec<NormalizedRecord>,
    ignored_records: u64,
}

impl NormalizedBatch {
    #[must_use]
    pub const fn metadata(&self) -> BatchMetadata {
        self.metadata
    }

    #[must_use]
    pub fn records(&self) -> &[NormalizedRecord] {
        &self.records
    }

    #[must_use]
    pub const fn ignored_records(&self) -> u64 {
        self.ignored_records
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NormalizationError {
    #[error("pinned source {pinned} does not match supplied source {supplied}")]
    SourceMismatch {
        pinned: elucid_catalog::SourceId,
        supplied: elucid_catalog::SourceId,
    },

    #[error("pinned input {input_id} is absent from source {source_id}")]
    InputNotFound {
        source_id: elucid_catalog::SourceId,
        input_id: elucid_catalog::InputId,
    },

    #[error(
        "pinned ingestion profile revision {profile_revision_id} is absent from input {input_id}"
    )]
    ProfileRevisionNotFound {
        input_id: elucid_catalog::InputId,
        profile_revision_id: elucid_catalog::IngestionProfileRevisionId,
    },

    #[error("pinned schema {pinned_schema_id} does not match profile target {profile_schema_id}")]
    ProfileTargetMismatch {
        pinned_schema_id: elucid_catalog::SchemaId,
        profile_schema_id: elucid_catalog::SchemaId,
    },

    #[error("pinned schema {schema_id} is absent from source {source_id}")]
    SchemaNotFound {
        source_id: elucid_catalog::SourceId,
        schema_id: elucid_catalog::SchemaId,
    },

    #[error("stored data field {field_id} has an unsupported logical type")]
    UnsupportedDataFieldType { field_id: FieldId },

    #[error("required stored data field {field_id} has no pinned mapping")]
    RequiredFieldMappingMissing { field_id: FieldId },

    #[error("batch is too large to represent record positions and counts")]
    PositionOverflow,
}

/// Normalizes one durably admitted body against the catalog identities pinned with that body.
///
/// # Errors
///
/// Returns an error only when the pinned catalog contract cannot be resolved or record positions
/// cannot be represented. Malformed records are returned as dead-letter entries and do not stop
/// normalization of later records.
pub fn normalize_records(
    metadata: BatchMetadata,
    body: &[u8],
    source: &Source,
) -> Result<NormalizedBatch, NormalizationError> {
    let plan = NormalizationPlan::resolve(metadata, source)?;
    u64::try_from(body.len()).map_err(|_| NormalizationError::PositionOverflow)?;

    let mut records = Vec::new();
    let mut ignored_records = 0_u64;
    let mut record_start = 0_usize;
    let mut line_number = 1_u64;

    for (delimiter_position, byte) in body.iter().copied().enumerate() {
        if byte != b'\n' {
            continue;
        }
        let payload_end =
            if delimiter_position > record_start && body[delimiter_position - 1] == b'\r' {
                delimiter_position - 1
            } else {
                delimiter_position
            };
        normalize_occurrence(
            &plan,
            &body[record_start..payload_end],
            record_start,
            line_number,
            &mut records,
            &mut ignored_records,
        )?;
        record_start = delimiter_position + 1;
        line_number = line_number
            .checked_add(1)
            .ok_or(NormalizationError::PositionOverflow)?;
    }

    if record_start < body.len() {
        normalize_occurrence(
            &plan,
            &body[record_start..],
            record_start,
            line_number,
            &mut records,
            &mut ignored_records,
        )?;
    }

    Ok(NormalizedBatch {
        metadata,
        records,
        ignored_records,
    })
}

struct NormalizationPlan<'a> {
    metadata: BatchMetadata,
    profile: &'a IngestionProfile,
    fields: Vec<FieldPlan<'a>>,
    removed_top_level_properties: Vec<&'a str>,
}

impl<'a> NormalizationPlan<'a> {
    fn resolve(metadata: BatchMetadata, source: &'a Source) -> Result<Self, NormalizationError> {
        let pinned = metadata.catalog();
        if pinned.source_id() != source.id() {
            return Err(NormalizationError::SourceMismatch {
                pinned: pinned.source_id(),
                supplied: source.id(),
            });
        }
        let input = source
            .input(pinned.input_id())
            .ok_or(NormalizationError::InputNotFound {
                source_id: source.id(),
                input_id: pinned.input_id(),
            })?;
        let revision = input
            .profile_revisions()
            .iter()
            .find(|revision| revision.id() == pinned.profile_revision_id())
            .ok_or(NormalizationError::ProfileRevisionNotFound {
                input_id: input.id(),
                profile_revision_id: pinned.profile_revision_id(),
            })?;
        if revision.target_schema_id() != pinned.target_schema_id() {
            return Err(NormalizationError::ProfileTargetMismatch {
                pinned_schema_id: pinned.target_schema_id(),
                profile_schema_id: revision.target_schema_id(),
            });
        }
        let schema =
            source
                .schema(pinned.target_schema_id())
                .ok_or(NormalizationError::SchemaNotFound {
                    source_id: source.id(),
                    schema_id: pinned.target_schema_id(),
                })?;
        let profile = revision.profile();
        let fields = field_plans(schema, profile)?;
        let mut removed_top_level_properties = Vec::with_capacity(profile.mappings().len() + 1);
        add_top_level_property(
            &mut removed_top_level_properties,
            profile.event_time().json_pointer(),
        );
        for mapping in profile.mappings() {
            add_top_level_property(&mut removed_top_level_properties, mapping.json_pointer());
        }
        Ok(Self {
            metadata,
            profile,
            fields,
            removed_top_level_properties,
        })
    }
}

struct FieldPlan<'a> {
    field_id: FieldId,
    nullability: Nullability,
    conversion: Conversion,
    pointer: Option<&'a JsonPointer>,
}

#[derive(Clone, Copy)]
enum Conversion {
    Bool,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Datetime,
}

fn field_plans<'a>(
    schema: &Schema,
    profile: &'a IngestionProfile,
) -> Result<Vec<FieldPlan<'a>>, NormalizationError> {
    schema
        .fields()
        .iter()
        .filter(|field| field.role() == FieldRole::Data)
        .map(|field| {
            let conversion = match field.logical_type() {
                LogicalType::Bool => Conversion::Bool,
                LogicalType::Int32 => Conversion::Int32,
                LogicalType::Int64 => Conversion::Int64,
                LogicalType::UInt32 => Conversion::UInt32,
                LogicalType::UInt64 => Conversion::UInt64,
                LogicalType::Float32 => Conversion::Float32,
                LogicalType::Float64 => Conversion::Float64,
                LogicalType::Utf8 => Conversion::Utf8,
                LogicalType::Datetime => Conversion::Datetime,
                LogicalType::Eid | LogicalType::Json => {
                    return Err(NormalizationError::UnsupportedDataFieldType {
                        field_id: field.id(),
                    });
                }
                _ => {
                    return Err(NormalizationError::UnsupportedDataFieldType {
                        field_id: field.id(),
                    });
                }
            };
            let pointer = profile
                .mappings()
                .iter()
                .find(|mapping| mapping.target_field_id() == field.id())
                .map(|mapping| mapping.json_pointer());
            if pointer.is_none() && field.nullability() == Nullability::NonNull {
                return Err(NormalizationError::RequiredFieldMappingMissing {
                    field_id: field.id(),
                });
            }
            Ok(FieldPlan {
                field_id: field.id(),
                nullability: field.nullability(),
                conversion,
                pointer,
            })
        })
        .collect()
}

fn add_top_level_property<'a>(properties: &mut Vec<&'a str>, pointer: &'a JsonPointer) {
    if let [token] = pointer.tokens() {
        let property = token.as_str();
        if !properties.contains(&property) {
            properties.push(property);
        }
    }
}

fn normalize_occurrence(
    plan: &NormalizationPlan<'_>,
    record: &[u8],
    input_position: usize,
    line_number: u64,
    records: &mut Vec<NormalizedRecord>,
    ignored_records: &mut u64,
) -> Result<(), NormalizationError> {
    if record.iter().all(u8::is_ascii_whitespace) {
        *ignored_records = ignored_records
            .checked_add(1)
            .ok_or(NormalizationError::PositionOverflow)?;
        return Ok(());
    }

    let input_position =
        u64::try_from(input_position).map_err(|_| NormalizationError::PositionOverflow)?;
    let payload_byte_count =
        u64::try_from(record.len()).map_err(|_| NormalizationError::PositionOverflow)?;
    let location = RecordLocation::new(line_number, input_position);
    let maximum_record_bytes = plan.profile.maximum_record_bytes().get();
    let outcome = if payload_byte_count > maximum_record_bytes {
        NormalizedRecord::DeadLetter(DeadLetterEntry::new(
            plan.metadata.batch_id(),
            location,
            DeadLetterCode::TooLarge,
            record,
            payload_byte_count,
        ))
    } else if std::str::from_utf8(record).is_err() {
        NormalizedRecord::DeadLetter(DeadLetterEntry::new(
            plan.metadata.batch_id(),
            location,
            DeadLetterCode::InvalidUtf8,
            record,
            payload_byte_count,
        ))
    } else {
        match normalize_record(plan, record, location) {
            Ok(row) => NormalizedRecord::Accepted(row),
            Err(code) => NormalizedRecord::DeadLetter(DeadLetterEntry::new(
                plan.metadata.batch_id(),
                location,
                code,
                record,
                payload_byte_count,
            )),
        }
    };
    records.push(outcome);
    Ok(())
}

fn normalize_record(
    plan: &NormalizationPlan<'_>,
    record: &[u8],
    location: RecordLocation,
) -> Result<AcceptedRow, DeadLetterCode> {
    let object = JsonParser::parse_object(record).map_err(|_| DeadLetterCode::ParseFailed)?;
    let root = Value::Object(object);
    let event_time = resolve_pointer(&root, plan.profile.event_time().json_pointer())
        .and_then(|value| parse_datetime(value, plan.profile.event_time().format()))
        .map(EventTime::from_unix_milliseconds)
        .ok_or(DeadLetterCode::EventTimeInvalid)?;
    let mut fields = Vec::with_capacity(plan.fields.len());
    for field in &plan.fields {
        let value = match field
            .pointer
            .and_then(|pointer| resolve_pointer(&root, pointer))
        {
            None => nullable_value(field.nullability, DeadLetterCode::FieldMissing)?,
            Some(Value::Null) => nullable_value(field.nullability, DeadLetterCode::FieldNull)?,
            Some(value) => {
                convert_value(value, field.conversion, plan.profile.event_time().format())
                    .ok_or(DeadLetterCode::ConversionFailed)?
            }
        };
        fields.push(NormalizedField {
            field_id: field.field_id,
            value,
        });
    }
    let Value::Object(mut object) = root else {
        return Err(DeadLetterCode::ParseFailed);
    };
    for property in &plan.removed_top_level_properties {
        object.remove(*property);
    }
    let remainder = if object.is_empty() {
        None
    } else {
        Some(JsonObject(object))
    };
    Ok(AcceptedRow {
        location,
        event_time,
        ingestion_time: plan.metadata.ingestion_time(),
        event_id: EventId::for_occurrence(plan.metadata.batch_id(), location.input_position()),
        fields,
        remainder,
    })
}

fn nullable_value(
    nullability: Nullability,
    non_null_error: DeadLetterCode,
) -> Result<NormalizedValue, DeadLetterCode> {
    match nullability {
        Nullability::Nullable => Ok(NormalizedValue::Null),
        Nullability::NonNull => Err(non_null_error),
        _ => Err(non_null_error),
    }
}

fn resolve_pointer<'a>(root: &'a Value, pointer: &JsonPointer) -> Option<&'a Value> {
    let mut current = root;
    for token in pointer.tokens() {
        current = match current {
            Value::Object(object) => object.get(token.as_str())?,
            Value::Array(array) => {
                let index = parse_array_index(token.as_str())?;
                array.get(index)?
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => return None,
        };
    }
    Some(current)
}

fn parse_array_index(token: &str) -> Option<usize> {
    if token.starts_with('+') || (token.len() > 1 && token.starts_with('0')) {
        return None;
    }
    token.parse().ok()
}

fn convert_value(
    value: &Value,
    conversion: Conversion,
    datetime_format: EventTimeFormat,
) -> Option<NormalizedValue> {
    match conversion {
        Conversion::Bool => value.as_bool().map(NormalizedValue::Bool),
        Conversion::Int32 => integer_i64(value)
            .and_then(|value| i32::try_from(value).ok())
            .map(NormalizedValue::Int32),
        Conversion::Int64 => integer_i64(value).map(NormalizedValue::Int64),
        Conversion::UInt32 => integer_u64(value)
            .and_then(|value| u32::try_from(value).ok())
            .map(NormalizedValue::UInt32),
        Conversion::UInt64 => integer_u64(value).map(NormalizedValue::UInt64),
        Conversion::Float32 => number_text(value)
            .and_then(|value| value.parse::<f32>().ok())
            .filter(|value| value.is_finite())
            .map(NormalizedValue::Float32),
        Conversion::Float64 => number_text(value)
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite())
            .map(NormalizedValue::Float64),
        Conversion::Utf8 => value
            .as_str()
            .map(ToOwned::to_owned)
            .map(NormalizedValue::Utf8),
        Conversion::Datetime => {
            parse_datetime(value, datetime_format).map(NormalizedValue::Datetime)
        }
    }
}

fn integer_i64(value: &Value) -> Option<i64> {
    let number = value.as_number()?;
    number
        .as_i64()
        .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn integer_u64(value: &Value) -> Option<u64> {
    let number = value.as_number()?;
    number
        .as_u64()
        .or_else(|| number.as_i64().and_then(|value| u64::try_from(value).ok()))
}

fn number_text(value: &Value) -> Option<String> {
    value.as_number().map(ToString::to_string)
}

fn parse_datetime(value: &Value, format: EventTimeFormat) -> Option<i64> {
    match format {
        EventTimeFormat::Rfc3339 => {
            let parsed = DateTime::parse_from_rfc3339(value.as_str()?).ok()?;
            if parsed.nanosecond() % 1_000_000 != 0 {
                return None;
            }
            let milliseconds = parsed.timestamp_millis();
            DateTime::<Utc>::from_timestamp_millis(milliseconds).map(|_| milliseconds)
        }
        EventTimeFormat::UnixMilliseconds => {
            let milliseconds = integer_i64(value)?;
            DateTime::<Utc>::from_timestamp_millis(milliseconds).map(|_| milliseconds)
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct JsonParseError;

struct JsonParser<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> JsonParser<'a> {
    fn parse_object(bytes: &'a [u8]) -> Result<Map<String, Value>, JsonParseError> {
        let mut parser = Self { bytes, position: 0 };
        parser.skip_whitespace();
        let value = parser.parse_value(0)?;
        parser.skip_whitespace();
        if parser.position != bytes.len() {
            return Err(JsonParseError);
        }
        match value {
            Value::Object(object) => Ok(object),
            Value::Null
            | Value::Bool(_)
            | Value::Number(_)
            | Value::String(_)
            | Value::Array(_) => Err(JsonParseError),
        }
    }

    fn parse_value(&mut self, depth: usize) -> Result<Value, JsonParseError> {
        self.skip_whitespace();
        match self.peek().ok_or(JsonParseError)? {
            b'n' => {
                self.consume_literal(b"null")?;
                Ok(Value::Null)
            }
            b't' => {
                self.consume_literal(b"true")?;
                Ok(Value::Bool(true))
            }
            b'f' => {
                self.consume_literal(b"false")?;
                Ok(Value::Bool(false))
            }
            b'"' => self.parse_string().map(Value::String),
            b'[' => self.parse_array(depth),
            b'{' => self.parse_map(depth),
            b'-' | b'0'..=b'9' => self.parse_number().map(Value::Number),
            _ => Err(JsonParseError),
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<Value, JsonParseError> {
        let child_depth = next_depth(depth)?;
        self.consume(b'[')?;
        self.skip_whitespace();
        let mut values = Vec::new();
        if self.consume_if(b']') {
            return Ok(Value::Array(values));
        }
        loop {
            values.push(self.parse_value(child_depth)?);
            self.skip_whitespace();
            if self.consume_if(b']') {
                return Ok(Value::Array(values));
            }
            self.consume(b',')?;
        }
    }

    fn parse_map(&mut self, depth: usize) -> Result<Value, JsonParseError> {
        let child_depth = next_depth(depth)?;
        self.consume(b'{')?;
        self.skip_whitespace();
        let mut values = Map::new();
        if self.consume_if(b'}') {
            return Ok(Value::Object(values));
        }
        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            if values.contains_key(&key) {
                return Err(JsonParseError);
            }
            self.skip_whitespace();
            self.consume(b':')?;
            let value = self.parse_value(child_depth)?;
            values.insert(key, value);
            self.skip_whitespace();
            if self.consume_if(b'}') {
                return Ok(Value::Object(values));
            }
            self.consume(b',')?;
        }
    }

    fn parse_string(&mut self) -> Result<String, JsonParseError> {
        let start = self.position;
        self.consume(b'"')?;
        let mut escaped = false;
        while let Some(byte) = self.peek() {
            self.position += 1;
            if escaped {
                escaped = false;
                continue;
            }
            match byte {
                b'\\' => escaped = true,
                b'"' => {
                    return serde_json::from_slice(&self.bytes[start..self.position])
                        .map_err(|_| JsonParseError);
                }
                _ => {}
            }
        }
        Err(JsonParseError)
    }

    fn parse_number(&mut self) -> Result<Number, JsonParseError> {
        let start = self.position;
        while let Some(byte) = self.peek() {
            if is_value_delimiter(byte) {
                break;
            }
            self.position += 1;
        }
        let text =
            std::str::from_utf8(&self.bytes[start..self.position]).map_err(|_| JsonParseError)?;
        Number::from_str(text).map_err(|_| JsonParseError)
    }

    fn consume_literal(&mut self, literal: &[u8]) -> Result<(), JsonParseError> {
        let end = self
            .position
            .checked_add(literal.len())
            .ok_or(JsonParseError)?;
        if self.bytes.get(self.position..end) != Some(literal) {
            return Err(JsonParseError);
        }
        self.position = end;
        Ok(())
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(is_json_whitespace) {
            self.position += 1;
        }
    }

    fn consume(&mut self, expected: u8) -> Result<(), JsonParseError> {
        if !self.consume_if(expected) {
            return Err(JsonParseError);
        }
        Ok(())
    }

    fn consume_if(&mut self, expected: u8) -> bool {
        if self.peek() != Some(expected) {
            return false;
        }
        self.position += 1;
        true
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.position).copied()
    }
}

fn next_depth(depth: usize) -> Result<usize, JsonParseError> {
    let next = depth.checked_add(1).ok_or(JsonParseError)?;
    if next > MAXIMUM_JSON_CONTAINER_DEPTH {
        return Err(JsonParseError);
    }
    Ok(next)
}

const fn is_value_delimiter(byte: u8) -> bool {
    is_json_whitespace(byte) || matches!(byte, b',' | b']' | b'}')
}

const fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}
