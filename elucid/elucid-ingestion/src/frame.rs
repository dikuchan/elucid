use crate::{BatchByteSize, BatchMetadata, BodyDigest, SpoolError};

const FRAME_MAGIC: &[u8; 8] = b"ELUCSP01";
const COMMIT_MAGIC: &[u8; 8] = b"ELUCCM01";
const FORMAT_VERSION: u16 = 1;
const HEADER_BYTES: usize = 148;
const FOOTER_BYTES: usize = 48;
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

pub(crate) fn reserved_frame_bytes(limit: crate::AppendBodyLimit) -> Result<u64, SpoolError> {
    limit
        .get()
        .checked_add(FRAME_OVERHEAD_BYTES)
        .ok_or_else(|| SpoolError::invariant("spool reservation size overflow"))
}
