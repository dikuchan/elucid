use elucid_catalog::{IngestionProfileRevisionId, InputId, SchemaId, SourceId};
use uuid::Uuid;

use crate::{
    AppendBodyLimit, BatchByteSize, BatchId, BatchMetadata, BodyDigest, IngestionTime,
    PinnedCatalogIdentities, SpoolError,
};

const FRAME_MAGIC: &[u8; 8] = b"ELUCSP01";
const COMMIT_MAGIC: &[u8; 8] = b"ELUCCM01";
const FORMAT_VERSION: u16 = 1;
pub(crate) const HEADER_BYTES: usize = 148;
pub(crate) const FOOTER_BYTES: usize = 48;
pub(crate) const FRAME_OVERHEAD_BYTES: u64 = (HEADER_BYTES + FOOTER_BYTES) as u64;

#[derive(Debug)]
pub(crate) struct PreparedFrame {
    header: Vec<u8>,
    footer: Vec<u8>,
    body_bytes: BatchByteSize,
    body_digest: BodyDigest,
    stored_bytes: u64,
}

impl PreparedFrame {
    pub(crate) fn new(metadata: BatchMetadata, body: &[u8]) -> Result<Self, SpoolError> {
        let body_length = u64::try_from(body.len())
            .map_err(|_| SpoolError::invariant("batch body length exceeds u64"))?;
        let stored_length = body_length
            .checked_add(FRAME_OVERHEAD_BYTES)
            .ok_or_else(|| SpoolError::invariant("spool frame length overflow"))?;
        let body_bytes = BatchByteSize::new(body_length);
        let body_digest = BodyDigest::calculate(body);

        let mut header = Vec::with_capacity(HEADER_BYTES);
        header.extend_from_slice(FRAME_MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        header.extend_from_slice(&(HEADER_BYTES as u16).to_be_bytes());
        header.extend_from_slice(&stored_length.to_be_bytes());
        header.extend_from_slice(&body_length.to_be_bytes());
        header.extend_from_slice(metadata.batch_id().as_uuid().as_bytes());
        header.extend_from_slice(metadata.catalog().source_id().as_uuid().as_bytes());
        header.extend_from_slice(metadata.catalog().input_id().as_uuid().as_bytes());
        header.extend_from_slice(
            metadata
                .catalog()
                .profile_revision_id()
                .as_uuid()
                .as_bytes(),
        );
        header.extend_from_slice(metadata.catalog().target_schema_id().as_uuid().as_bytes());
        header.extend_from_slice(&metadata.ingestion_time().unix_milliseconds().to_be_bytes());
        header.extend_from_slice(body_digest.as_bytes());
        if header.len() != HEADER_BYTES {
            return Err(SpoolError::invariant("spool frame header size mismatch"));
        }

        let mut frame_hasher = blake3::Hasher::new();
        frame_hasher.update(&header);
        frame_hasher.update(body);
        let frame_digest = frame_hasher.finalize();
        let mut footer = Vec::with_capacity(FOOTER_BYTES);
        footer.extend_from_slice(frame_digest.as_bytes());
        footer.extend_from_slice(COMMIT_MAGIC);
        footer.extend_from_slice(&stored_length.to_be_bytes());
        if footer.len() != FOOTER_BYTES {
            return Err(SpoolError::invariant("spool frame footer size mismatch"));
        }

        Ok(Self {
            header,
            footer,
            body_bytes,
            body_digest,
            stored_bytes: stored_length,
        })
    }

    pub(crate) fn header(&self) -> &[u8] {
        &self.header
    }

    pub(crate) fn footer(&self) -> &[u8] {
        &self.footer
    }

    pub(crate) const fn body_bytes(&self) -> BatchByteSize {
        self.body_bytes
    }

    pub(crate) const fn body_digest(&self) -> BodyDigest {
        self.body_digest
    }

    pub(crate) const fn stored_bytes(&self) -> u64 {
        self.stored_bytes
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DecodedHeader {
    raw: [u8; HEADER_BYTES],
    metadata: BatchMetadata,
    body_bytes: BatchByteSize,
    body_digest: BodyDigest,
    stored_bytes: u64,
}

impl DecodedHeader {
    pub(crate) const fn raw(&self) -> &[u8; HEADER_BYTES] {
        &self.raw
    }

    pub(crate) const fn metadata(&self) -> BatchMetadata {
        self.metadata
    }

    pub(crate) const fn body_bytes(&self) -> BatchByteSize {
        self.body_bytes
    }

    pub(crate) const fn body_digest(&self) -> BodyDigest {
        self.body_digest
    }

    pub(crate) const fn stored_bytes(&self) -> u64 {
        self.stored_bytes
    }
}

pub(crate) fn decode_header(
    raw: [u8; HEADER_BYTES],
    body_limit: AppendBodyLimit,
) -> Result<DecodedHeader, SpoolError> {
    if &raw[..8] != FRAME_MAGIC {
        return Err(SpoolError::corrupt("spool frame magic is invalid"));
    }
    if read_u16(&raw, 8)? != FORMAT_VERSION {
        return Err(SpoolError::corrupt("spool frame version is unsupported"));
    }
    if usize::from(read_u16(&raw, 10)?) != HEADER_BYTES {
        return Err(SpoolError::corrupt("spool frame header length is invalid"));
    }
    let stored_bytes = read_u64(&raw, 12)?;
    let body_length = read_u64(&raw, 20)?;
    validate_lengths(stored_bytes, body_length, body_limit)?;

    let batch_id = BatchId::try_from(read_uuid(&raw, 28)?)
        .map_err(|_| SpoolError::corrupt("spool batch identity is not UUIDv7"))?;
    let source_id = SourceId::try_from(read_uuid(&raw, 44)?)
        .map_err(|_| SpoolError::corrupt("spool source identity is not UUIDv7"))?;
    let input_id = InputId::try_from(read_uuid(&raw, 60)?)
        .map_err(|_| SpoolError::corrupt("spool input identity is not UUIDv7"))?;
    let profile_revision_id = IngestionProfileRevisionId::try_from(read_uuid(&raw, 76)?)
        .map_err(|_| SpoolError::corrupt("spool profile identity is not UUIDv7"))?;
    let target_schema_id = SchemaId::try_from(read_uuid(&raw, 92)?)
        .map_err(|_| SpoolError::corrupt("spool schema identity is not UUIDv7"))?;
    let ingestion_time = IngestionTime::from_unix_milliseconds(read_i64(&raw, 108)?)
        .map_err(|_| SpoolError::corrupt("spool ingestion time is outside the UTC range"))?;
    let body_digest = BodyDigest::from_bytes(read_array(&raw, 116)?);

    Ok(DecodedHeader {
        raw,
        metadata: BatchMetadata::new(
            batch_id,
            PinnedCatalogIdentities::new(
                source_id,
                input_id,
                profile_revision_id,
                target_schema_id,
            ),
            ingestion_time,
        ),
        body_bytes: BatchByteSize::new(body_length),
        body_digest,
        stored_bytes,
    })
}

pub(crate) fn validate_incomplete_header(
    prefix: &[u8],
    body_limit: AppendBodyLimit,
) -> Result<(), SpoolError> {
    let magic_bytes = prefix.len().min(FRAME_MAGIC.len());
    if prefix[..magic_bytes] != FRAME_MAGIC[..magic_bytes] {
        return Err(SpoolError::corrupt(
            "incomplete spool tail is not a frame prefix",
        ));
    }
    if prefix.len() >= 10 && read_u16(prefix, 8)? != FORMAT_VERSION {
        return Err(SpoolError::corrupt(
            "incomplete spool tail has an unsupported version",
        ));
    }
    if prefix.len() >= 12 && usize::from(read_u16(prefix, 10)?) != HEADER_BYTES {
        return Err(SpoolError::corrupt(
            "incomplete spool tail has invalid framing",
        ));
    }
    if prefix.len() >= 28 {
        validate_lengths(read_u64(prefix, 12)?, read_u64(prefix, 20)?, body_limit)?;
    }
    if prefix.len() >= 44 {
        BatchId::try_from(read_uuid(prefix, 28)?)
            .map_err(|_| SpoolError::corrupt("incomplete tail batch identity is not UUIDv7"))?;
    }
    if prefix.len() >= 60 {
        SourceId::try_from(read_uuid(prefix, 44)?)
            .map_err(|_| SpoolError::corrupt("incomplete tail source identity is not UUIDv7"))?;
    }
    if prefix.len() >= 76 {
        InputId::try_from(read_uuid(prefix, 60)?)
            .map_err(|_| SpoolError::corrupt("incomplete tail input identity is not UUIDv7"))?;
    }
    if prefix.len() >= 92 {
        IngestionProfileRevisionId::try_from(read_uuid(prefix, 76)?)
            .map_err(|_| SpoolError::corrupt("incomplete tail profile identity is not UUIDv7"))?;
    }
    if prefix.len() >= 108 {
        SchemaId::try_from(read_uuid(prefix, 92)?)
            .map_err(|_| SpoolError::corrupt("incomplete tail schema identity is not UUIDv7"))?;
    }
    if prefix.len() >= 116 {
        IngestionTime::from_unix_milliseconds(read_i64(prefix, 108)?).map_err(|_| {
            SpoolError::corrupt("incomplete tail ingestion time is outside the UTC range")
        })?;
    }
    Ok(())
}

pub(crate) fn validate_digests_and_footer(
    header: &DecodedHeader,
    calculated_body_digest: [u8; 32],
    calculated_frame_digest: [u8; 32],
    footer: &[u8; FOOTER_BYTES],
) -> Result<(), SpoolError> {
    if header.body_digest().as_bytes() != &calculated_body_digest {
        return Err(SpoolError::corrupt("spool body digest does not match"));
    }
    if footer[..32] != calculated_frame_digest {
        return Err(SpoolError::corrupt("spool frame digest does not match"));
    }
    if &footer[32..40] != COMMIT_MAGIC {
        return Err(SpoolError::corrupt("spool commit footer is invalid"));
    }
    if read_u64(footer, 40)? != header.stored_bytes() {
        return Err(SpoolError::corrupt("spool footer length does not match"));
    }
    Ok(())
}

pub(crate) fn reserved_frame_bytes(limit: AppendBodyLimit) -> Result<u64, SpoolError> {
    limit
        .get()
        .checked_add(FRAME_OVERHEAD_BYTES)
        .ok_or_else(|| SpoolError::invariant("spool reservation size overflow"))
}

fn validate_lengths(
    stored_bytes: u64,
    body_bytes: u64,
    body_limit: AppendBodyLimit,
) -> Result<(), SpoolError> {
    if body_bytes > body_limit.get() {
        return Err(SpoolError::corrupt(
            "spool frame body exceeds the configured recovery bound",
        ));
    }
    let expected_stored_bytes = body_bytes
        .checked_add(FRAME_OVERHEAD_BYTES)
        .ok_or_else(|| SpoolError::corrupt("spool frame length overflows u64"))?;
    if stored_bytes != expected_stored_bytes {
        return Err(SpoolError::corrupt("spool frame lengths are inconsistent"));
    }
    Ok(())
}

fn read_uuid(bytes: &[u8], offset: usize) -> Result<Uuid, SpoolError> {
    Ok(Uuid::from_bytes(read_array(bytes, offset)?))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, SpoolError> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, SpoolError> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64, SpoolError> {
    Ok(i64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_array<const SIZE: usize>(bytes: &[u8], offset: usize) -> Result<[u8; SIZE], SpoolError> {
    let end = offset
        .checked_add(SIZE)
        .ok_or_else(|| SpoolError::invariant("fixed-size spool field offset overflow"))?;
    bytes
        .get(offset..end)
        .ok_or_else(|| SpoolError::invariant("fixed-size spool field is truncated"))?
        .try_into()
        .map_err(|_| SpoolError::invariant("fixed-size spool field has the wrong length"))
}
