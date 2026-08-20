use std::fs::{File, OpenOptions};
use std::io::{Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;

use crate::checkpoint;
use crate::frame::{PreparedFrame, reserved_frame_bytes};
use crate::{
    AppendBodyLimit, BatchMetadata, DurableAppend, MaximumBatchAdmission, SpoolCapacity,
    SpoolError, SpoolRecovery, SpoolUsage,
};

pub(crate) const DATA_FILE_NAME: &str = "spool.data";

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Spool {
    inner: Arc<SpoolInner>,
}

#[derive(Debug)]
struct SpoolInner {
    capacity: SpoolCapacity,
    writer: Mutex<File>,
    accounting: Mutex<Accounting>,
    checkpoint: Mutex<crate::SpoolCheckpoint>,
    directory: PathBuf,
    failed: AtomicBool,
}

#[derive(Debug)]
struct Accounting {
    committed_bytes: u64,
    reserved_bytes: u64,
}

impl Spool {
    /// Opens the spool in an existing directory, initializing it when no spool state exists.
    ///
    /// Existing state is always recovered and validated before the returned writer can accept an
    /// append. A directory containing only one of the data and checkpoint files is corrupt.
    ///
    /// # Errors
    ///
    /// Returns `SPOOL_CORRUPT` for incomplete or invalid existing state and `SPOOL_UNAVAILABLE`
    /// when the directory or its files cannot be accessed or synchronized.
    pub async fn open(
        directory: impl AsRef<Path>,
        capacity: SpoolCapacity,
        maximum_batch: AppendBodyLimit,
    ) -> Result<SpoolRecovery, SpoolError> {
        let directory = directory.as_ref().to_owned();
        let inspection_directory = directory.clone();
        let files = tokio::task::spawn_blocking(move || inspect_spool_files(&inspection_directory))
            .await
            .map_err(SpoolError::task)??;
        match files {
            SpoolFiles::Absent => drop(Self::create_new(&directory, capacity).await?),
            SpoolFiles::Complete => {}
            SpoolFiles::Incomplete => {
                return Err(SpoolError::corrupt(
                    "spool data and checkpoint files are incomplete",
                ));
            }
        }
        Self::recover(directory, capacity, maximum_batch).await
    }

    /// Creates a new append-only spool in an existing directory.
    ///
    /// # Errors
    ///
    /// Returns `SPOOL_UNAVAILABLE` if the directory is inaccessible, is not a directory, or
    /// already contains spool data or checkpoint state.
    pub async fn create_new(
        directory: impl AsRef<Path>,
        capacity: SpoolCapacity,
    ) -> Result<Self, SpoolError> {
        let directory = directory.as_ref().to_owned();
        let creation_directory = directory.clone();
        let writer = tokio::task::spawn_blocking(move || create_spool_files(&creation_directory))
            .await
            .map_err(SpoolError::task)??;
        Ok(Self::from_recovered(
            writer,
            directory,
            capacity,
            0,
            crate::SpoolCheckpoint::INITIAL,
        ))
    }

    /// Recovers a previously created append-only spool and its pending committed batches.
    ///
    /// Recovery scans the complete file with bounded memory, validates every committed frame and
    /// the durable checkpoint, and discards only a valid incomplete final append.
    ///
    /// # Errors
    ///
    /// Returns `SPOOL_CORRUPT` for invalid committed data or checkpoint state and
    /// `SPOOL_UNAVAILABLE` when recovery cannot access or synchronize the spool.
    pub async fn recover(
        directory: impl AsRef<Path>,
        capacity: SpoolCapacity,
        maximum_batch: AppendBodyLimit,
    ) -> Result<SpoolRecovery, SpoolError> {
        crate::recovery::recover(directory.as_ref(), capacity, maximum_batch).await
    }

    pub(crate) fn from_recovered(
        writer: File,
        directory: PathBuf,
        capacity: SpoolCapacity,
        committed_bytes: u64,
        checkpoint: crate::SpoolCheckpoint,
    ) -> Self {
        Self {
            inner: Arc::new(SpoolInner {
                capacity,
                writer: Mutex::new(writer),
                accounting: Mutex::new(Accounting {
                    committed_bytes,
                    reserved_bytes: 0,
                }),
                checkpoint: Mutex::new(checkpoint),
                directory,
                failed: AtomicBool::new(false),
            }),
        }
    }

    /// Reserves the worst-case physical spool space before the caller reads a request body.
    ///
    /// Dropping the returned reservation releases all reserved capacity.
    ///
    /// # Errors
    ///
    /// Returns `CAPACITY_EXHAUSTED` when the complete framed batch cannot fit or
    /// `SPOOL_UNAVAILABLE` after an I/O or internal consistency failure.
    pub fn reserve(&self, body_limit: AppendBodyLimit) -> Result<SpoolReservation, SpoolError> {
        self.inner.reserve(body_limit)
    }

    /// Returns exact in-process capacity accounting for committed and reserved spool bytes.
    ///
    /// # Errors
    ///
    /// Returns `SPOOL_UNAVAILABLE` if the accounting state cannot be trusted.
    pub fn usage(&self) -> Result<SpoolUsage, SpoolError> {
        self.inner.usage()
    }

    /// Reports whether another maximum-size batch can be reserved without changing accounting.
    ///
    /// # Errors
    ///
    /// Returns `SPOOL_UNAVAILABLE` if the writer or its capacity accounting cannot be trusted.
    pub fn maximum_batch_admission(
        &self,
        body_limit: AppendBodyLimit,
    ) -> Result<MaximumBatchAdmission, SpoolError> {
        self.inner.maximum_batch_admission(body_limit)
    }

    /// Returns the checkpoint durably installed by this spool instance.
    ///
    /// # Errors
    ///
    /// Returns `SPOOL_UNAVAILABLE` when local synchronization state cannot be trusted.
    pub fn checkpoint(&self) -> Result<crate::SpoolCheckpoint, SpoolError> {
        self.inner.checkpoint()
    }

    /// Durably advances the checkpoint described by an exact completion plan.
    ///
    /// # Errors
    ///
    /// Returns `SPOOL_UNAVAILABLE` if the plan is stale, exceeds committed data, or cannot be
    /// synchronized. A failed spool requires restart and recovery before further writes.
    pub async fn advance_checkpoint(
        &self,
        plan: crate::CheckpointPlan,
    ) -> Result<crate::SpoolCheckpoint, SpoolError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || inner.advance_checkpoint(plan))
            .await
            .map_err(|source| {
                self.inner.mark_failed();
                SpoolError::task(source)
            })?
    }

    /// Reclaims the complete data file only when every committed frame is checkpointed.
    ///
    /// Partial-prefix rewriting is intentionally deferred because it would renumber live recovery
    /// ranges. A deferred result leaves data and capacity accounting unchanged.
    ///
    /// # Errors
    ///
    /// Returns `SPOOL_UNAVAILABLE` when local state cannot be synchronized safely.
    pub async fn reclaim_checkpointed(&self) -> Result<crate::SpoolReclamation, SpoolError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || inner.reclaim_checkpointed())
            .await
            .map_err(|source| {
                self.inner.mark_failed();
                SpoolError::task(source)
            })?
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpoolFiles {
    Absent,
    Complete,
    Incomplete,
}

fn inspect_spool_files(directory: &Path) -> Result<SpoolFiles, SpoolError> {
    let data_exists = directory
        .join(DATA_FILE_NAME)
        .try_exists()
        .map_err(|source| SpoolError::io("inspect the spool data file", source))?;
    let checkpoint_exists = checkpoint::exists(directory)?;
    Ok(match (data_exists, checkpoint_exists) {
        (false, false) => SpoolFiles::Absent,
        (true, true) => SpoolFiles::Complete,
        (true, false) | (false, true) => SpoolFiles::Incomplete,
    })
}

#[derive(Debug)]
#[non_exhaustive]
pub struct SpoolReservation {
    inner: Arc<SpoolInner>,
    body_limit: AppendBodyLimit,
    reserved_bytes: u64,
    active: bool,
}

impl SpoolReservation {
    /// Appends one complete frame and returns only after its data and framing are synchronized.
    ///
    /// # Errors
    ///
    /// Returns `INGESTION_BATCH_LIMIT_EXCEEDED` before writing when the body exceeds the
    /// reservation, or `SPOOL_UNAVAILABLE` when the durable append cannot complete.
    pub async fn append(
        mut self,
        metadata: BatchMetadata,
        body: Bytes,
    ) -> Result<DurableAppend, SpoolError> {
        let body_bytes = u64::try_from(body.len())
            .map_err(|_| SpoolError::invariant("batch body length exceeds u64"))?;
        if body_bytes > self.body_limit.get() {
            return Err(SpoolError::batch_limit(body_bytes, self.body_limit.get()));
        }
        self.inner.ensure_writable()?;

        self.active = false;
        let inner = Arc::clone(&self.inner);
        let reserved_bytes = self.reserved_bytes;
        tokio::task::spawn_blocking(move || inner.append_reserved(metadata, body, reserved_bytes))
            .await
            .map_err(|source| {
                self.inner.mark_failed();
                SpoolError::task(source)
            })?
    }
}

impl Drop for SpoolReservation {
    fn drop(&mut self) {
        if self.active {
            self.inner.release_reservation(self.reserved_bytes);
        }
    }
}

impl SpoolInner {
    fn reserve(
        self: &Arc<Self>,
        body_limit: AppendBodyLimit,
    ) -> Result<SpoolReservation, SpoolError> {
        self.ensure_writable()?;
        let required_bytes = reserved_frame_bytes(body_limit)?;
        let mut accounting = self.lock_accounting()?;
        self.ensure_writable()?;
        let occupied_bytes = accounting
            .committed_bytes
            .checked_add(accounting.reserved_bytes)
            .ok_or_else(|| SpoolError::invariant("spool accounting overflow"))?;
        let available_bytes = self.capacity.get().saturating_sub(occupied_bytes);
        if required_bytes > available_bytes {
            return Err(SpoolError::capacity(required_bytes, available_bytes));
        }
        accounting.reserved_bytes = accounting
            .reserved_bytes
            .checked_add(required_bytes)
            .ok_or_else(|| SpoolError::invariant("spool reservation accounting overflow"))?;
        Ok(SpoolReservation {
            inner: Arc::clone(self),
            body_limit,
            reserved_bytes: required_bytes,
            active: true,
        })
    }

    fn usage(&self) -> Result<SpoolUsage, SpoolError> {
        self.ensure_writable()?;
        let accounting = self.lock_accounting()?;
        SpoolUsage::new(
            self.capacity.get(),
            accounting.committed_bytes,
            accounting.reserved_bytes,
        )
    }

    fn maximum_batch_admission(
        &self,
        body_limit: AppendBodyLimit,
    ) -> Result<MaximumBatchAdmission, SpoolError> {
        self.ensure_writable()?;
        let required_bytes = reserved_frame_bytes(body_limit)?;
        let accounting = self.lock_accounting()?;
        self.ensure_writable()?;
        let occupied_bytes = accounting
            .committed_bytes
            .checked_add(accounting.reserved_bytes)
            .ok_or_else(|| SpoolError::invariant("spool accounting overflow"))?;
        let available_bytes = self.capacity.get().saturating_sub(occupied_bytes);
        Ok(if required_bytes <= available_bytes {
            MaximumBatchAdmission::Available
        } else {
            MaximumBatchAdmission::CapacityExhausted
        })
    }

    fn append_reserved(
        &self,
        metadata: BatchMetadata,
        body: Bytes,
        reserved_bytes: u64,
    ) -> Result<DurableAppend, SpoolError> {
        let result = self.write_frame(metadata, &body);
        match result {
            Ok((prepared, range)) => {
                self.commit_reservation(reserved_bytes, prepared.stored_bytes())?;
                Ok(DurableAppend::new(
                    metadata,
                    prepared.body_bytes(),
                    prepared.body_digest(),
                    range,
                ))
            }
            Err(error) => {
                self.mark_failed();
                self.release_reservation(reserved_bytes);
                Err(error)
            }
        }
    }

    fn write_frame(
        &self,
        metadata: BatchMetadata,
        body: &[u8],
    ) -> Result<(PreparedFrame, crate::SpoolBatchRange), SpoolError> {
        self.ensure_writable()?;
        let prepared = PreparedFrame::new(metadata, body)?;
        let mut writer = self.lock_writer()?;
        self.ensure_writable()?;
        let start = writer
            .metadata()
            .map_err(|source| self.fail_io("inspect before append", source))?
            .len();
        writer
            .write_all(prepared.header())
            .map_err(|source| self.fail_io("write", source))?;
        writer
            .write_all(body)
            .map_err(|source| self.fail_io("write", source))?;
        writer
            .write_all(prepared.footer())
            .map_err(|source| self.fail_io("write", source))?;
        writer
            .sync_all()
            .map_err(|source| self.fail_io("synchronize", source))?;
        let end = start
            .checked_add(prepared.stored_bytes())
            .ok_or_else(|| SpoolError::invariant("spool append position overflow"))?;
        Ok((prepared, crate::SpoolBatchRange::new(start, end)?))
    }

    fn commit_reservation(
        &self,
        reserved_bytes: u64,
        committed_bytes: u64,
    ) -> Result<(), SpoolError> {
        let mut accounting = self.lock_accounting()?;
        if committed_bytes > reserved_bytes || accounting.reserved_bytes < reserved_bytes {
            self.mark_failed();
            return Err(SpoolError::invariant(
                "durable append does not fit its capacity reservation",
            ));
        }
        let new_committed = accounting
            .committed_bytes
            .checked_add(committed_bytes)
            .ok_or_else(|| {
                self.mark_failed();
                SpoolError::invariant("committed spool byte count overflow")
            })?;
        accounting.committed_bytes = new_committed;
        accounting.reserved_bytes -= reserved_bytes;
        Ok(())
    }

    fn release_reservation(&self, reserved_bytes: u64) {
        let mut accounting = match self.accounting.lock() {
            Ok(accounting) => accounting,
            Err(poisoned) => {
                self.mark_failed();
                poisoned.into_inner()
            }
        };
        if accounting.reserved_bytes < reserved_bytes {
            self.mark_failed();
            accounting.reserved_bytes = 0;
            return;
        }
        accounting.reserved_bytes -= reserved_bytes;
    }

    fn lock_writer(&self) -> Result<MutexGuard<'_, File>, SpoolError> {
        self.writer.lock().map_err(|_| {
            self.mark_failed();
            SpoolError::invariant("spool writer lock is poisoned")
        })
    }

    fn lock_accounting(&self) -> Result<MutexGuard<'_, Accounting>, SpoolError> {
        self.accounting.lock().map_err(|_| {
            self.mark_failed();
            SpoolError::invariant("spool accounting lock is poisoned")
        })
    }

    fn lock_checkpoint(&self) -> Result<MutexGuard<'_, crate::SpoolCheckpoint>, SpoolError> {
        self.checkpoint.lock().map_err(|_| {
            self.mark_failed();
            SpoolError::invariant("spool checkpoint lock is poisoned")
        })
    }

    fn checkpoint(&self) -> Result<crate::SpoolCheckpoint, SpoolError> {
        self.ensure_writable()?;
        let checkpoint = *self.lock_checkpoint()?;
        self.ensure_writable()?;
        Ok(checkpoint)
    }

    fn advance_checkpoint(
        &self,
        plan: crate::CheckpointPlan,
    ) -> Result<crate::SpoolCheckpoint, SpoolError> {
        self.ensure_writable()?;
        let accounting = self.lock_accounting()?;
        let mut checkpoint = self.lock_checkpoint()?;
        if plan.current() != *checkpoint {
            return Err(SpoolError::invariant("checkpoint plan is stale"));
        }
        if plan.target().position() > accounting.committed_bytes {
            return Err(SpoolError::invariant(
                "checkpoint plan exceeds committed spool data",
            ));
        }
        checkpoint::store(&self.directory, plan.target())
            .map_err(|error| self.fail_existing(error))?;
        *checkpoint = plan.target();
        Ok(*checkpoint)
    }

    fn reclaim_checkpointed(&self) -> Result<crate::SpoolReclamation, SpoolError> {
        self.ensure_writable()?;
        let mut writer = self.lock_writer()?;
        let mut accounting = self.lock_accounting()?;
        let mut checkpoint = self.lock_checkpoint()?;
        if accounting.reserved_bytes != 0
            || checkpoint.position() == 0
            || checkpoint.position() != accounting.committed_bytes
        {
            return Ok(crate::SpoolReclamation::DEFERRED);
        }
        let reclaimed_bytes = accounting.committed_bytes;

        // Reset first. A crash before truncation replays already-published occurrences through
        // retained output records; truncating first could leave the durable checkpoint beyond EOF.
        checkpoint::store(&self.directory, crate::SpoolCheckpoint::INITIAL)
            .map_err(|error| self.fail_existing(error))?;
        *checkpoint = crate::SpoolCheckpoint::INITIAL;
        writer
            .set_len(0)
            .map_err(|source| self.fail_io("truncate checkpointed spool data", source))?;
        writer
            .seek(SeekFrom::Start(0))
            .map_err(|source| self.fail_io("reset the spool writer position", source))?;
        writer
            .sync_all()
            .map_err(|source| self.fail_io("synchronize reclaimed spool data", source))?;
        accounting.committed_bytes = 0;
        Ok(crate::SpoolReclamation::reclaimed(reclaimed_bytes))
    }

    fn ensure_writable(&self) -> Result<(), SpoolError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(SpoolError::invariant(
                "spool is unavailable until process restart and recovery",
            ));
        }
        Ok(())
    }

    fn fail_io(&self, operation: &'static str, source: std::io::Error) -> SpoolError {
        self.mark_failed();
        SpoolError::io(operation, source)
    }

    fn fail_existing(&self, error: SpoolError) -> SpoolError {
        self.mark_failed();
        error
    }

    fn mark_failed(&self) {
        self.failed.store(true, Ordering::Release);
    }
}

fn create_spool_files(directory: &Path) -> Result<File, SpoolError> {
    let metadata = std::fs::metadata(directory)
        .map_err(|source| SpoolError::io("inspect the spool directory", source))?;
    if !metadata.is_dir() {
        return Err(SpoolError::invariant(
            "configured spool path is not a directory",
        ));
    }

    let data_path: PathBuf = directory.join(DATA_FILE_NAME);
    let writer = OpenOptions::new()
        .create_new(true)
        .append(true)
        .read(true)
        .open(data_path)
        .map_err(|source| SpoolError::io("create the spool data file", source))?;
    writer
        .sync_all()
        .map_err(|source| SpoolError::io("synchronize the new spool data file", source))?;
    checkpoint::create_new(directory)?;
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| SpoolError::io("synchronize the spool directory", source))?;
    Ok(writer)
}
