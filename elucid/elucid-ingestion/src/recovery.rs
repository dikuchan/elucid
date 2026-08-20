use std::fs::{File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::frame::{
    FOOTER_BYTES, HEADER_BYTES, decode_header, reserved_frame_bytes, validate_digests_and_footer,
    validate_incomplete_header,
};
use crate::spool::DATA_FILE_NAME;
use crate::{
    AppendBodyLimit, MaximumBatchAdmission, RecoveredBatch, RecoveryReport, Spool, SpoolCapacity,
    SpoolCheckpoint, SpoolError,
};

const SCAN_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug)]
#[non_exhaustive]
pub struct SpoolRecovery {
    spool: Spool,
    batches: RecoveredBatches,
    report: RecoveryReport,
}

impl SpoolRecovery {
    #[must_use]
    pub const fn report(&self) -> RecoveryReport {
        self.report
    }

    #[must_use]
    pub fn into_parts(self) -> (Spool, RecoveredBatches, RecoveryReport) {
        (self.spool, self.batches, self.report)
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct RecoveredBatches {
    reader: Arc<Mutex<File>>,
    position: u64,
    end: u64,
    body_limit: AppendBodyLimit,
    state: RecoveredBatchState,
}

impl RecoveredBatches {
    /// Reads and validates the next batch that was not covered by the recovered checkpoint.
    ///
    /// The reader holds at most one configured maximum-size batch body in memory. It is a
    /// snapshot of the committed range observed during recovery and does not include later
    /// appends. Cancelling an in-progress read does not advance the durable batch position.
    ///
    /// # Errors
    ///
    /// Returns `SPOOL_CORRUPT` if the recovered range changes or no longer validates, and
    /// `SPOOL_UNAVAILABLE` after an I/O or blocking-task failure. A failed reader cannot resume.
    pub async fn next_batch(&mut self) -> Result<Option<RecoveredBatch>, SpoolError> {
        match self.state {
            RecoveredBatchState::Reading => {}
            RecoveredBatchState::Complete => return Ok(None),
            RecoveredBatchState::Failed => {
                return Err(SpoolError::invariant(
                    "recovered spool reader is unavailable after a previous failure",
                ));
            }
        }

        if self.position == self.end {
            self.state = RecoveredBatchState::Complete;
            return Ok(None);
        }

        let reader = Arc::clone(&self.reader);
        let position = self.position;
        let end = self.end;
        let body_limit = self.body_limit;
        let result = tokio::task::spawn_blocking(move || {
            read_recovered_batch(&reader, position, end, body_limit)
        })
        .await
        .map_err(SpoolError::task)
        .and_then(|result| result);
        match result {
            Ok((batch, next_position)) => {
                self.position = next_position;
                Ok(Some(batch))
            }
            Err(error) => {
                self.state = RecoveredBatchState::Failed;
                Err(error)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveredBatchState {
    Reading,
    Complete,
    Failed,
}

pub(crate) async fn recover(
    directory: &Path,
    capacity: SpoolCapacity,
    maximum_batch: AppendBodyLimit,
) -> Result<SpoolRecovery, SpoolError> {
    let directory = directory.to_owned();
    let recovered =
        tokio::task::spawn_blocking(move || recover_files(&directory, capacity, maximum_batch))
            .await
            .map_err(SpoolError::task)??;

    let spool = Spool::from_recovered(
        recovered.writer,
        capacity,
        recovered.report.committed_bytes(),
    );
    let batches = RecoveredBatches {
        reader: Arc::new(Mutex::new(recovered.reader)),
        position: recovered.report.checkpoint().position(),
        end: recovered.report.committed_bytes(),
        body_limit: maximum_batch,
        state: RecoveredBatchState::Reading,
    };
    Ok(SpoolRecovery {
        spool,
        batches,
        report: recovered.report,
    })
}

#[derive(Debug)]
struct RecoveredFiles {
    writer: File,
    reader: File,
    report: RecoveryReport,
}

fn recover_files(
    directory: &Path,
    capacity: SpoolCapacity,
    maximum_batch: AppendBodyLimit,
) -> Result<RecoveredFiles, SpoolError> {
    let directory_metadata = std::fs::metadata(directory)
        .map_err(|source| SpoolError::io("inspect the spool directory", source))?;
    if !directory_metadata.is_dir() {
        return Err(SpoolError::invariant(
            "configured spool path is not a directory",
        ));
    }

    let checkpoint = crate::checkpoint::load(directory)?;
    let data_path = directory.join(DATA_FILE_NAME);
    let mut writer = OpenOptions::new()
        .append(true)
        .read(true)
        .open(&data_path)
        .map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                SpoolError::corrupt("spool data file is missing")
            } else {
                SpoolError::io("open the spool data file", source)
            }
        })?;
    let file_bytes = writer
        .metadata()
        .map_err(|source| SpoolError::io("inspect the spool data file", source))?
        .len();
    let scan = scan_committed_frames(&mut writer, file_bytes, checkpoint, maximum_batch)?;

    if scan.discarded_tail_bytes > 0 {
        writer
            .set_len(scan.committed_bytes)
            .map_err(|source| SpoolError::io("discard the incomplete spool append", source))?;
        writer.sync_all().map_err(|source| {
            SpoolError::io("synchronize the recovered spool data file", source)
        })?;
    }

    let required_bytes = reserved_frame_bytes(maximum_batch)?;
    let available_bytes = capacity.get().saturating_sub(scan.committed_bytes);
    let maximum_batch_admission = if required_bytes <= available_bytes {
        MaximumBatchAdmission::Available
    } else {
        MaximumBatchAdmission::CapacityExhausted
    };
    let report = RecoveryReport::new(
        checkpoint,
        scan.committed_batches,
        scan.pending_batches,
        scan.committed_bytes,
        scan.discarded_tail_bytes,
        maximum_batch_admission,
    );

    let reader = File::open(data_path)
        .map_err(|source| SpoolError::io("open the recovered spool data file", source))?;

    Ok(RecoveredFiles {
        writer,
        reader,
        report,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FrameScan {
    committed_batches: u64,
    pending_batches: u64,
    committed_bytes: u64,
    discarded_tail_bytes: u64,
}

fn scan_committed_frames(
    file: &mut File,
    file_bytes: u64,
    checkpoint: SpoolCheckpoint,
    body_limit: AppendBodyLimit,
) -> Result<FrameScan, SpoolError> {
    let mut position = 0_u64;
    let mut committed_batches = 0_u64;
    let mut pending_batches = 0_u64;
    let mut checkpoint_is_boundary = checkpoint.position() == 0;
    let mut discarded_tail_bytes = 0_u64;
    let mut body_buffer = [0_u8; SCAN_BUFFER_BYTES];

    while position < file_bytes {
        let remaining_bytes = file_bytes - position;
        file.seek(SeekFrom::Start(position))
            .map_err(|source| SpoolError::io("seek through the spool data file", source))?;

        if remaining_bytes < HEADER_BYTES as u64 {
            let prefix_bytes = usize::try_from(remaining_bytes).map_err(|_| {
                SpoolError::invariant("incomplete spool header length does not fit usize")
            })?;
            let mut prefix = [0_u8; HEADER_BYTES];
            file.read_exact(&mut prefix[..prefix_bytes])
                .map_err(|source| SpoolError::io("read the incomplete spool header", source))?;
            validate_incomplete_header(&prefix[..prefix_bytes], body_limit)?;
            discarded_tail_bytes = remaining_bytes;
            break;
        }

        let mut raw_header = [0_u8; HEADER_BYTES];
        file.read_exact(&mut raw_header)
            .map_err(|source| SpoolError::io("read a spool frame header", source))?;
        let header = decode_header(raw_header, body_limit)?;
        let frame_end = position
            .checked_add(header.stored_bytes())
            .ok_or_else(|| SpoolError::corrupt("spool frame position overflows u64"))?;
        if frame_end > file_bytes {
            discarded_tail_bytes = remaining_bytes;
            break;
        }

        let mut body_hasher = blake3::Hasher::new();
        let mut frame_hasher = blake3::Hasher::new();
        frame_hasher.update(header.raw());
        let mut unread_body_bytes = header.body_bytes().get();
        while unread_body_bytes > 0 {
            let chunk_bytes = usize::try_from(unread_body_bytes.min(SCAN_BUFFER_BYTES as u64))
                .map_err(|_| SpoolError::invariant("spool scan chunk does not fit usize"))?;
            let chunk = &mut body_buffer[..chunk_bytes];
            file.read_exact(chunk)
                .map_err(|source| SpoolError::io("read a spool frame body", source))?;
            body_hasher.update(chunk);
            frame_hasher.update(chunk);
            unread_body_bytes -= chunk_bytes as u64;
        }

        let mut footer = [0_u8; FOOTER_BYTES];
        file.read_exact(&mut footer)
            .map_err(|source| SpoolError::io("read a spool frame footer", source))?;
        validate_digests_and_footer(
            &header,
            *body_hasher.finalize().as_bytes(),
            *frame_hasher.finalize().as_bytes(),
            &footer,
        )?;

        committed_batches = committed_batches
            .checked_add(1)
            .ok_or_else(|| SpoolError::corrupt("spool batch count overflows u64"))?;
        if position >= checkpoint.position() {
            pending_batches = pending_batches
                .checked_add(1)
                .ok_or_else(|| SpoolError::corrupt("pending spool batch count overflows u64"))?;
        }
        position = frame_end;
        if position == checkpoint.position() {
            checkpoint_is_boundary = true;
        }
    }

    if checkpoint.position() > position || !checkpoint_is_boundary {
        return Err(SpoolError::corrupt(
            "spool checkpoint is not a committed frame boundary",
        ));
    }

    Ok(FrameScan {
        committed_batches,
        pending_batches,
        committed_bytes: position,
        discarded_tail_bytes,
    })
}

fn read_recovered_batch(
    reader: &Mutex<File>,
    position: u64,
    end: u64,
    body_limit: AppendBodyLimit,
) -> Result<(RecoveredBatch, u64), SpoolError> {
    let mut reader = reader
        .lock()
        .map_err(|_| SpoolError::invariant("recovered spool reader lock is poisoned"))?;
    reader
        .seek(SeekFrom::Start(position))
        .map_err(|source| SpoolError::io("seek to a recovered spool batch", source))?;

    let mut raw_header = [0_u8; HEADER_BYTES];
    read_exact_committed(&mut reader, &mut raw_header)?;
    let header = decode_header(raw_header, body_limit)?;
    let next_position = position
        .checked_add(header.stored_bytes())
        .ok_or_else(|| SpoolError::corrupt("recovered spool frame position overflows u64"))?;
    if next_position > end {
        return Err(SpoolError::corrupt(
            "recovered spool frame exceeds its committed range",
        ));
    }

    let body_bytes = usize::try_from(header.body_bytes().get()).map_err(|_| {
        SpoolError::corrupt("recovered spool body does not fit the platform address space")
    })?;
    let mut body = Vec::new();
    body.try_reserve_exact(body_bytes).map_err(|_| {
        SpoolError::invariant("cannot allocate the configured recovered batch body bound")
    })?;
    body.resize(body_bytes, 0);
    read_exact_committed(&mut reader, &mut body)?;
    let mut footer = [0_u8; FOOTER_BYTES];
    read_exact_committed(&mut reader, &mut footer)?;

    let body_digest = *blake3::hash(&body).as_bytes();
    let mut frame_hasher = blake3::Hasher::new();
    frame_hasher.update(header.raw());
    frame_hasher.update(&body);
    let frame_digest = *frame_hasher.finalize().as_bytes();
    validate_digests_and_footer(&header, body_digest, frame_digest, &footer)?;
    Ok((
        RecoveredBatch::new(header.metadata(), Bytes::from(body), header.body_digest()),
        next_position,
    ))
}

fn read_exact_committed(reader: &mut File, bytes: &mut [u8]) -> Result<(), SpoolError> {
    match reader.read_exact(bytes) {
        Ok(_) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::UnexpectedEof => Err(
            SpoolError::corrupt("committed spool data changed after recovery"),
        ),
        Err(source) => Err(SpoolError::io("read a recovered spool batch", source)),
    }
}
