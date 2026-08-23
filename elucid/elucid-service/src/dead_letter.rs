use std::path::{Path, PathBuf};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt as _;
use utoipa::ToSchema;

use elucid_ingestion::{DeadLetterCode, DeadLetterEntry, PayloadEncoding, PayloadExtent};
use elucid_storage::{
    ManagedObjectKey, ObjectDescriptor, ObjectDigest, ObjectFormatVersion, ObjectMediaType,
    StorageModelError,
};

const DEAD_LETTER_FORMAT_VERSION: u64 = 1;
const STAGING_NAMESPACE: &str = "dead-letters";

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeadLetterObjectError {
    #[error("dead-letter object must contain at least one entry")]
    Empty,
    #[error("dead-letter object exceeds its local byte limit")]
    CapacityExceeded,
    #[error("dead-letter object cannot be encoded")]
    Encode(#[source] serde_json::Error),
    #[error("dead-letter object cannot be decoded")]
    Decode(#[source] serde_json::Error),
    #[error("dead-letter object has invalid internal data")]
    Invalid,
    #[error("dead-letter object descriptor is invalid")]
    Model(#[source] StorageModelError),
    #[error("dead-letter staging I/O failed")]
    Io(#[source] std::io::Error),
}

#[derive(Debug)]
pub(crate) struct StagedDeadLetterObject {
    descriptor: ObjectDescriptor,
}

impl StagedDeadLetterObject {
    pub(crate) const fn descriptor(&self) -> &ObjectDescriptor {
        &self.descriptor
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeadLetterDocumentEntry {
    #[schema(format = Uuid)]
    batch_id: String,
    line_number: u64,
    input_position: u64,
    code: DeadLetterDocumentCode,
    message: String,
    payload_byte_count: u64,
    payload_blake3: String,
    payload: DeadLetterDocumentPayload,
}

impl DeadLetterDocumentEntry {
    fn from_entry(entry: &DeadLetterEntry) -> Result<Self, DeadLetterObjectError> {
        Ok(Self {
            batch_id: entry.batch_id().to_string(),
            line_number: entry.location().line_number(),
            input_position: entry.location().input_position(),
            code: DeadLetterDocumentCode::try_from(entry.code())?,
            message: entry.message().to_owned(),
            payload_byte_count: entry.payload_byte_count(),
            payload_blake3: blake3::Hash::from_bytes(*entry.payload_digest().as_bytes())
                .to_hex()
                .to_string(),
            payload: DeadLetterDocumentPayload {
                encoding: DeadLetterDocumentEncoding::try_from(entry.payload().encoding())?,
                extent: DeadLetterDocumentExtent::try_from(entry.payload().extent())?,
                content: entry.payload().content().to_owned(),
            },
        })
    }

    fn validate(&self) -> bool {
        self.message == self.code.message()
            && uuid::Uuid::parse_str(&self.batch_id).is_ok()
            && self.payload_blake3.len() == 64
            && self
                .payload_blake3
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
enum DeadLetterDocumentCode {
    #[serde(rename = "RECORD_INVALID_UTF8")]
    InvalidUtf8,
    #[serde(rename = "RECORD_TOO_LARGE")]
    TooLarge,
    #[serde(rename = "RECORD_PARSE_FAILED")]
    ParseFailed,
    #[serde(rename = "RECORD_FIELD_MISSING")]
    FieldMissing,
    #[serde(rename = "RECORD_FIELD_NULL")]
    FieldNull,
    #[serde(rename = "RECORD_CONVERSION_FAILED")]
    ConversionFailed,
    #[serde(rename = "RECORD_EVENT_TIME_INVALID")]
    EventTimeInvalid,
    #[serde(rename = "RECORD_EVENT_DAY_LIMIT_EXCEEDED")]
    EventDayLimitExceeded,
}

impl DeadLetterDocumentCode {
    const fn message(self) -> &'static str {
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

impl TryFrom<DeadLetterCode> for DeadLetterDocumentCode {
    type Error = DeadLetterObjectError;

    fn try_from(value: DeadLetterCode) -> Result<Self, Self::Error> {
        match value {
            DeadLetterCode::InvalidUtf8 => Ok(Self::InvalidUtf8),
            DeadLetterCode::TooLarge => Ok(Self::TooLarge),
            DeadLetterCode::ParseFailed => Ok(Self::ParseFailed),
            DeadLetterCode::FieldMissing => Ok(Self::FieldMissing),
            DeadLetterCode::FieldNull => Ok(Self::FieldNull),
            DeadLetterCode::ConversionFailed => Ok(Self::ConversionFailed),
            DeadLetterCode::EventTimeInvalid => Ok(Self::EventTimeInvalid),
            DeadLetterCode::EventDayLimitExceeded => Ok(Self::EventDayLimitExceeded),
            _ => Err(DeadLetterObjectError::Invalid),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct DeadLetterDocumentPayload {
    encoding: DeadLetterDocumentEncoding,
    extent: DeadLetterDocumentExtent,
    content: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum DeadLetterDocumentEncoding {
    Utf8,
    Base64,
}

impl TryFrom<PayloadEncoding> for DeadLetterDocumentEncoding {
    type Error = DeadLetterObjectError;

    fn try_from(value: PayloadEncoding) -> Result<Self, Self::Error> {
        match value {
            PayloadEncoding::Utf8 => Ok(Self::Utf8),
            PayloadEncoding::Base64 => Ok(Self::Base64),
            _ => Err(DeadLetterObjectError::Invalid),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum DeadLetterDocumentExtent {
    Complete,
    Prefix,
}

impl TryFrom<PayloadExtent> for DeadLetterDocumentExtent {
    type Error = DeadLetterObjectError;

    fn try_from(value: PayloadExtent) -> Result<Self, Self::Error> {
        match value {
            PayloadExtent::Complete => Ok(Self::Complete),
            PayloadExtent::Prefix => Ok(Self::Prefix),
            _ => Err(DeadLetterObjectError::Invalid),
        }
    }
}

pub(crate) async fn stage_dead_letters(
    staging_root: &Path,
    key: ManagedObjectKey,
    entries: &[DeadLetterEntry],
    maximum_bytes: u64,
) -> Result<StagedDeadLetterObject, DeadLetterObjectError> {
    let bytes = encode_dead_letters(entries, maximum_bytes)?;
    let format_version = ObjectFormatVersion::new(DEAD_LETTER_FORMAT_VERSION)
        .map_err(DeadLetterObjectError::Model)?;
    let descriptor =
        ObjectDescriptor::for_bytes(key, &bytes, ObjectMediaType::DeadLetter, format_version)
            .map_err(DeadLetterObjectError::Model)?;
    let path = dead_letter_staging_path(staging_root, &descriptor);
    let parent = path.parent().ok_or(DeadLetterObjectError::Invalid)?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(DeadLetterObjectError::Io)?;
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .await
        .map_err(DeadLetterObjectError::Io)?;
    file.write_all(&bytes)
        .await
        .map_err(DeadLetterObjectError::Io)?;
    file.sync_all().await.map_err(DeadLetterObjectError::Io)?;
    drop(file);
    Ok(StagedDeadLetterObject { descriptor })
}

pub(crate) async fn read_staged_dead_letter(
    path: &Path,
    descriptor: &ObjectDescriptor,
    maximum_bytes: u64,
) -> Result<Bytes, DeadLetterObjectError> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(DeadLetterObjectError::Io)?;
    if metadata.len() > maximum_bytes || metadata.len() != descriptor.expected_byte_size().get() {
        return Err(DeadLetterObjectError::CapacityExceeded);
    }
    let bytes = tokio::fs::read(path)
        .await
        .map(Bytes::from)
        .map_err(DeadLetterObjectError::Io)?;
    if ObjectDigest::calculate(&bytes) != descriptor.digest() {
        return Err(DeadLetterObjectError::Invalid);
    }
    decode_dead_letters(&bytes)?;
    Ok(bytes)
}

pub(crate) fn decode_dead_letters(
    bytes: &[u8],
) -> Result<Vec<DeadLetterDocumentEntry>, DeadLetterObjectError> {
    if bytes.is_empty() || bytes.last() != Some(&b'\n') {
        return Err(DeadLetterObjectError::Invalid);
    }
    let mut entries = Vec::new();
    for line in bytes[..bytes.len() - 1].split(|byte| *byte == b'\n') {
        if line.is_empty() {
            return Err(DeadLetterObjectError::Invalid);
        }
        let entry: DeadLetterDocumentEntry =
            serde_json::from_slice(line).map_err(DeadLetterObjectError::Decode)?;
        if !entry.validate() {
            return Err(DeadLetterObjectError::Invalid);
        }
        entries.push(entry);
    }
    if entries.is_empty() {
        return Err(DeadLetterObjectError::Empty);
    }
    Ok(entries)
}

pub(crate) fn dead_letter_staging_path(
    staging_root: &Path,
    descriptor: &ObjectDescriptor,
) -> PathBuf {
    staging_root
        .join(STAGING_NAMESPACE)
        .join(descriptor.key().object_id().to_string())
        .join("entries.ndjson")
}

fn encode_dead_letters(
    entries: &[DeadLetterEntry],
    maximum_bytes: u64,
) -> Result<Bytes, DeadLetterObjectError> {
    if entries.is_empty() {
        return Err(DeadLetterObjectError::Empty);
    }
    let maximum_bytes =
        usize::try_from(maximum_bytes).map_err(|_| DeadLetterObjectError::CapacityExceeded)?;
    let mut output = Vec::new();
    for entry in entries {
        let document = DeadLetterDocumentEntry::from_entry(entry)?;
        let encoded = serde_json::to_vec(&document).map_err(DeadLetterObjectError::Encode)?;
        let projected = output
            .len()
            .checked_add(encoded.len())
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or(DeadLetterObjectError::CapacityExceeded)?;
        if projected > maximum_bytes {
            return Err(DeadLetterObjectError::CapacityExceeded);
        }
        output
            .try_reserve_exact(encoded.len() + 1)
            .map_err(|_| DeadLetterObjectError::CapacityExceeded)?;
        output.extend_from_slice(&encoded);
        output.push(b'\n');
    }
    Ok(Bytes::from(output))
}
