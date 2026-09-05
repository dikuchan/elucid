use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{DateTime, NaiveDate, Utc};
use elucid_catalog::{InputId, SchemaId, SourceId};
use elucid_core::UuidV7;
use elucid_metastore::{
    DeadLetterRegistration, IngestionSegmentRegistration, IngestionSegmentTimes,
};
use elucid_storage::{
    BatchId, ManagedObjectKey, ManagedRoot, ObjectByteSize, ObjectDescriptor, ObjectDigest,
    ObjectFormatVersion, ObjectMediaType, SegmentId, StoredObjectId,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    NormalizedBatch, NormalizedRecord, SpoolBatchRange, SpoolCheckpoint, SpoolReclamation,
};

const RECOVERY_FILE_NAME: &str = "output-recovery.log";
const NEXT_RECOVERY_FILE_NAME: &str = "output-recovery.log.next";
const RECOVERY_MAGIC: &[u8; 8] = b"ELUCOR01";
const RECOVERY_COMMIT_MAGIC: &[u8; 8] = b"ELUCOC01";
const RECOVERY_FORMAT_VERSION: u16 = 1;
const RECOVERY_HEADER_BYTES: usize = 76;
const RECOVERY_FOOTER_BYTES: usize = 48;
const MAXIMUM_RECOVERY_PAYLOAD_BYTES: u64 = 8 * 1024 * 1024;
const MAXIMUM_RECOVERY_RECORDS: usize = 1_000_000;
const MAXIMUM_COVERED_BATCHES: usize = 50_000;
const MAXIMUM_COVERED_POSITIONS: usize = 100_000;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct OutputRecoveryId(UuidV7);

impl OutputRecoveryId {
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0.as_uuid()
    }
}

impl TryFrom<Uuid> for OutputRecoveryId {
    type Error = OutputRecoveryModelError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        UuidV7::try_from(value)
            .map(Self)
            .map_err(|_| OutputRecoveryModelError::RecoveryIdentityMustBeUuidV7)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BatchPositionCoverage {
    batch_id: BatchId,
    spool_range: SpoolBatchRange,
    positions: Vec<u64>,
}

impl BatchPositionCoverage {
    pub fn new(
        batch_id: BatchId,
        spool_range: SpoolBatchRange,
        positions: Vec<u64>,
    ) -> Result<Self, OutputRecoveryModelError> {
        validate_positions(&positions)?;
        Ok(Self {
            batch_id,
            spool_range,
            positions,
        })
    }

    #[must_use]
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    #[must_use]
    pub const fn spool_range(&self) -> SpoolBatchRange {
        self.spool_range
    }

    #[must_use]
    pub fn positions(&self) -> &[u64] {
        &self.positions
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecoveryOutput {
    Segment(IngestionSegmentRegistration),
    DeadLetter(DeadLetterRegistration),
}

impl RecoveryOutput {
    #[must_use]
    pub const fn object(&self) -> &ObjectDescriptor {
        match self {
            Self::Segment(registration) => registration.object(),
            Self::DeadLetter(registration) => registration.object(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct OutputRecoveryRecord {
    id: OutputRecoveryId,
    revision: NonZeroU64,
    output: RecoveryOutput,
    coverage: Vec<BatchPositionCoverage>,
}

impl OutputRecoveryRecord {
    pub fn segment(
        id: OutputRecoveryId,
        registration: IngestionSegmentRegistration,
        mut coverage: Vec<BatchPositionCoverage>,
    ) -> Result<Self, OutputRecoveryModelError> {
        coverage.sort_unstable_by_key(|item| item.spool_range);
        validate_coverage(&coverage)?;
        let covered_rows = coverage.iter().try_fold(0_u64, |total, item| {
            let item_positions = u64::try_from(item.positions.len())
                .map_err(|_| OutputRecoveryModelError::CoveredPositionLimitExceeded)?;
            total
                .checked_add(item_positions)
                .ok_or(OutputRecoveryModelError::CoveredPositionLimitExceeded)
        })?;
        if covered_rows != registration.row_count() {
            return Err(OutputRecoveryModelError::SegmentRowCoverageMismatch);
        }
        Ok(Self {
            id,
            revision: NonZeroU64::MIN,
            output: RecoveryOutput::Segment(registration),
            coverage,
        })
    }

    pub fn dead_letter(
        id: OutputRecoveryId,
        registration: DeadLetterRegistration,
        coverage: BatchPositionCoverage,
    ) -> Result<Self, OutputRecoveryModelError> {
        if registration.batch_id() != coverage.batch_id() {
            return Err(OutputRecoveryModelError::DeadLetterBatchMismatch);
        }
        Ok(Self {
            id,
            revision: NonZeroU64::MIN,
            output: RecoveryOutput::DeadLetter(registration),
            coverage: vec![coverage],
        })
    }

    #[must_use]
    pub const fn id(&self) -> OutputRecoveryId {
        self.id
    }

    #[must_use]
    pub const fn revision(&self) -> NonZeroU64 {
        self.revision
    }

    #[must_use]
    pub const fn output(&self) -> &RecoveryOutput {
        &self.output
    }

    #[must_use]
    pub fn coverage(&self) -> &[BatchPositionCoverage] {
        &self.coverage
    }

    pub fn replacement_segment(
        &self,
        registration: IngestionSegmentRegistration,
    ) -> Result<Self, OutputRecoveryModelError> {
        let RecoveryOutput::Segment(current) = &self.output else {
            return Err(OutputRecoveryModelError::OutputKindCannotChange);
        };
        if current.segment_id() == registration.segment_id()
            || current.object().key().object_id() == registration.object().key().object_id()
        {
            return Err(OutputRecoveryModelError::ReplacementIdentitiesMustChange);
        }
        let revision = self
            .revision
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(OutputRecoveryModelError::RecoveryRevisionExhausted)?;
        let mut replacement = Self::segment(self.id, registration, self.coverage.clone())?;
        replacement.revision = revision;
        Ok(replacement)
    }

    pub fn recovery_action(
        &self,
        observation: OutputRecoveryObservation,
        retained_spool: RetainedSpoolBytes,
    ) -> Result<OutputRecoveryAction, OutputRecoveryModelError> {
        let action = match observation {
            OutputRecoveryObservation::Unregistered(UnregisteredOutputBytes::Available) => {
                OutputRecoveryAction::Register
            }
            OutputRecoveryObservation::Unregistered(UnregisteredOutputBytes::Missing) => {
                OutputRecoveryAction::Rebuild
            }
            OutputRecoveryObservation::Planned(PlannedOutputBytes::Available) => {
                OutputRecoveryAction::Upload
            }
            OutputRecoveryObservation::Planned(PlannedOutputBytes::MissingUnverified) => {
                OutputRecoveryAction::VerifyExactObject
            }
            OutputRecoveryObservation::Planned(PlannedOutputBytes::MissingVerified) => {
                OutputRecoveryAction::RecordVerifiedUpload
            }
            OutputRecoveryObservation::Planned(PlannedOutputBytes::MissingAbsent) => {
                OutputRecoveryAction::AbandonAndRebuild
            }
            OutputRecoveryObservation::Uploaded => OutputRecoveryAction::Publish,
            OutputRecoveryObservation::Published => OutputRecoveryAction::Complete,
            OutputRecoveryObservation::Abandoned => OutputRecoveryAction::Rebuild,
        };
        if matches!(
            action,
            OutputRecoveryAction::Rebuild | OutputRecoveryAction::AbandonAndRebuild
        ) && retained_spool == RetainedSpoolBytes::Missing
        {
            return Err(OutputRecoveryModelError::RetainedSpoolBytesMissing);
        }
        Ok(action)
    }

    fn object_identity(&self) -> StoredObjectId {
        self.output.object().key().object_id()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UnregisteredOutputBytes {
    Available,
    Missing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PlannedOutputBytes {
    Available,
    MissingUnverified,
    MissingVerified,
    MissingAbsent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OutputRecoveryObservation {
    Unregistered(UnregisteredOutputBytes),
    Planned(PlannedOutputBytes),
    Uploaded,
    Published,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OutputRecoveryAction {
    Register,
    Rebuild,
    Upload,
    VerifyExactObject,
    RecordVerifiedUpload,
    Publish,
    Complete,
    AbandonAndRebuild,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RetainedSpoolBytes {
    Available,
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PublishedOutput(OutputRecoveryRecord);

impl PublishedOutput {
    pub fn resolve(
        record: OutputRecoveryRecord,
        observation: OutputRecoveryObservation,
    ) -> Result<Self, OutputRecoveryModelError> {
        if observation != OutputRecoveryObservation::Published {
            return Err(OutputRecoveryModelError::OutputIsNotPublished);
        }
        Ok(Self(record))
    }

    fn coverage(&self) -> &[BatchPositionCoverage] {
        self.0.coverage()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BatchOutputRequirements {
    batch_id: BatchId,
    spool_range: SpoolBatchRange,
    positions: Vec<u64>,
}

impl BatchOutputRequirements {
    pub fn new(
        batch_id: BatchId,
        spool_range: SpoolBatchRange,
        positions: Vec<u64>,
    ) -> Result<Self, OutputRecoveryModelError> {
        if positions.len() > MAXIMUM_COVERED_POSITIONS {
            return Err(OutputRecoveryModelError::CoveredPositionLimitExceeded);
        }
        if positions.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(OutputRecoveryModelError::PositionsMustIncrease);
        }
        Ok(Self {
            batch_id,
            spool_range,
            positions,
        })
    }

    pub fn from_normalized(
        batch: &NormalizedBatch,
        spool_range: SpoolBatchRange,
    ) -> Result<Self, OutputRecoveryModelError> {
        let positions = batch
            .records()
            .iter()
            .map(|record| match record {
                NormalizedRecord::Accepted(row) => row.location().input_position(),
                NormalizedRecord::DeadLetter(entry) => entry.location().input_position(),
            })
            .collect();
        Self::new(batch.metadata().batch_id(), spool_range, positions)
    }

    #[must_use]
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    #[must_use]
    pub const fn spool_range(&self) -> SpoolBatchRange {
        self.spool_range
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CheckpointPlan {
    current: SpoolCheckpoint,
    target: SpoolCheckpoint,
}

impl CheckpointPlan {
    #[must_use]
    pub const fn current(self) -> SpoolCheckpoint {
        self.current
    }

    #[must_use]
    pub const fn target(self) -> SpoolCheckpoint {
        self.target
    }
}

pub fn plan_checkpoint(
    current: SpoolCheckpoint,
    batches: &[BatchOutputRequirements],
    outputs: &[PublishedOutput],
) -> Result<Option<CheckpointPlan>, OutputRecoveryModelError> {
    let known_batches = batches
        .iter()
        .map(|batch| batch.batch_id)
        .collect::<BTreeSet<_>>();
    if known_batches.len() != batches.len() {
        return Err(OutputRecoveryModelError::DuplicateBatchRequirements);
    }

    let required = batches
        .iter()
        .flat_map(|batch| {
            batch
                .positions
                .iter()
                .copied()
                .map(move |position| ((batch.batch_id, position), batch.spool_range))
        })
        .collect::<BTreeMap<_, _>>();
    let mut covered = BTreeMap::new();
    for coverage in outputs.iter().flat_map(PublishedOutput::coverage) {
        for position in &coverage.positions {
            let key = (coverage.batch_id, *position);
            if known_batches.contains(&coverage.batch_id) {
                let Some(required_range) = required.get(&key) else {
                    return Err(OutputRecoveryModelError::PublishedCoverageIsUnknown);
                };
                if *required_range != coverage.spool_range {
                    return Err(OutputRecoveryModelError::PublishedCoverageRangeMismatch);
                }
                if covered.insert(key, coverage.spool_range).is_some() {
                    return Err(OutputRecoveryModelError::PublishedCoverageOverlaps);
                }
            }
        }
    }

    let mut target = current;
    for batch in batches {
        if batch.spool_range.start() != target {
            return Err(OutputRecoveryModelError::BatchRequirementsAreNotContiguous);
        }
        if batch
            .positions
            .iter()
            .any(|position| !covered.contains_key(&(batch.batch_id, *position)))
        {
            break;
        }
        target = batch.spool_range.end();
    }
    Ok((target != current).then_some(CheckpointPlan { current, target }))
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct OutputRecoveryLog {
    inner: Arc<Mutex<OutputRecoveryLogInner>>,
}

#[derive(Debug)]
struct OutputRecoveryLogInner {
    writer: File,
    directory: PathBuf,
    root: ManagedRoot,
    records: BTreeMap<OutputRecoveryId, OutputRecoveryRecord>,
}

impl OutputRecoveryLog {
    pub async fn open(
        directory: impl AsRef<Path>,
        root: ManagedRoot,
    ) -> Result<Self, OutputRecoveryError> {
        let directory = directory.as_ref().to_owned();
        let inner = tokio::task::spawn_blocking(move || recover_log(&directory, root))
            .await
            .map_err(OutputRecoveryError::task)??;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub async fn record(
        &self,
        record: OutputRecoveryRecord,
    ) -> Result<OutputRecoveryRecordOutcome, OutputRecoveryError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut inner = lock_log(&inner)?;
            inner.record(record)
        })
        .await
        .map_err(OutputRecoveryError::task)?
    }

    pub fn records(&self) -> Result<Vec<OutputRecoveryRecord>, OutputRecoveryError> {
        let inner = lock_log(&self.inner)?;
        Ok(inner.records.values().cloned().collect())
    }

    /// Removes recovery records only after the spool has durably reclaimed its complete file.
    ///
    /// # Errors
    ///
    /// Returns a conflict if any retained record points beyond the reclaimed spool tail, or an
    /// availability error when the replacement log cannot be synchronized atomically.
    pub async fn reclaim(
        &self,
        reclamation: SpoolReclamation,
    ) -> Result<OutputRecoveryReclamation, OutputRecoveryError> {
        let Some(bytes) = reclamation.reclaimed_bytes() else {
            return Ok(OutputRecoveryReclamation::Deferred);
        };
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut inner = lock_log(&inner)?;
            inner.reclaim(bytes)
        })
        .await
        .map_err(OutputRecoveryError::task)?
    }
}

impl OutputRecoveryLogInner {
    fn record(
        &mut self,
        record: OutputRecoveryRecord,
    ) -> Result<OutputRecoveryRecordOutcome, OutputRecoveryError> {
        validate_record_root(&record, &self.root)?;
        if self.records.get(&record.id) == Some(&record) {
            return Ok(OutputRecoveryRecordOutcome::AlreadyRecorded);
        }
        validate_transition(self.records.get(&record.id), &record)?;
        ensure_unique_object_identity(&self.records, &record)?;
        let payload = encode_record(&record)?;
        let frame = encode_frame(&record, &payload)?;
        self.writer
            .write_all(&frame)
            .map_err(|source| OutputRecoveryError::io("append output recovery record", source))?;
        self.writer.sync_all().map_err(|source| {
            OutputRecoveryError::io("synchronize output recovery record", source)
        })?;
        self.records.insert(record.id, record);
        Ok(OutputRecoveryRecordOutcome::Recorded)
    }

    fn reclaim(
        &mut self,
        reclaimed_bytes: u64,
    ) -> Result<OutputRecoveryReclamation, OutputRecoveryError> {
        if self.records.values().any(|record| {
            record
                .coverage
                .iter()
                .any(|coverage| coverage.spool_range.end().position() > reclaimed_bytes)
        }) {
            return Err(OutputRecoveryError::Conflict);
        }
        let reclaimed_records = self.records.len();
        let next_path = self.directory.join(NEXT_RECOVERY_FILE_NAME);
        let next = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&next_path)
            .map_err(|source| {
                OutputRecoveryError::io("create the next output recovery log", source)
            })?;
        next.sync_all().map_err(|source| {
            OutputRecoveryError::io("synchronize the next output recovery log", source)
        })?;
        drop(next);
        let path = self.directory.join(RECOVERY_FILE_NAME);
        std::fs::rename(next_path, &path)
            .map_err(|source| OutputRecoveryError::io("replace the output recovery log", source))?;
        File::open(&self.directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| {
                OutputRecoveryError::io("synchronize output recovery directory", source)
            })?;
        self.writer = OpenOptions::new()
            .append(true)
            .read(true)
            .open(path)
            .map_err(|source| OutputRecoveryError::io("reopen the output recovery log", source))?;
        self.records.clear();
        Ok(OutputRecoveryReclamation::Reclaimed {
            records: reclaimed_records,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OutputRecoveryRecordOutcome {
    Recorded,
    AlreadyRecorded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OutputRecoveryReclamation {
    Deferred,
    Reclaimed { records: usize },
}

impl OutputRecoveryReclamation {
    #[must_use]
    pub const fn reclaimed_records(self) -> Option<usize> {
        match self {
            Self::Deferred => None,
            Self::Reclaimed { records } => Some(records),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum OutputRecoveryModelError {
    #[error("output recovery identity must be UUIDv7")]
    RecoveryIdentityMustBeUuidV7,
    #[error("covered positions must be non-empty")]
    CoveredPositionsMustNotBeEmpty,
    #[error("covered positions must increase strictly")]
    PositionsMustIncrease,
    #[error("an output recovery record covers too many batches")]
    CoveredBatchLimitExceeded,
    #[error("an output recovery record covers too many positions")]
    CoveredPositionLimitExceeded,
    #[error("covered batch identities or spool ranges overlap")]
    CoveredBatchesOverlap,
    #[error("segment row count does not match its exact occurrence coverage")]
    SegmentRowCoverageMismatch,
    #[error("dead-letter registration and coverage refer to different batches")]
    DeadLetterBatchMismatch,
    #[error("an output replacement cannot change output kind")]
    OutputKindCannotChange,
    #[error("rebuilt output must use fresh segment and object identities")]
    ReplacementIdentitiesMustChange,
    #[error("output recovery revision is exhausted")]
    RecoveryRevisionExhausted,
    #[error("output has not reached a published durable state")]
    OutputIsNotPublished,
    #[error("output cannot be rebuilt because its covered spool bytes are not retained")]
    RetainedSpoolBytesMissing,
    #[error("checkpoint requirements contain the same batch more than once")]
    DuplicateBatchRequirements,
    #[error("checkpoint batch requirements are not contiguous")]
    BatchRequirementsAreNotContiguous,
    #[error("published output covers an occurrence absent from its recovered batch")]
    PublishedCoverageIsUnknown,
    #[error("published output coverage refers to a different spool frame")]
    PublishedCoverageRangeMismatch,
    #[error("published outputs cover the same occurrence more than once")]
    PublishedCoverageOverlaps,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OutputRecoveryError {
    #[error("output recovery record is invalid")]
    Model(#[from] OutputRecoveryModelError),
    #[error("output recovery log is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("cannot {operation}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot encode output recovery metadata")]
    Encode(#[source] serde_json::Error),
    #[error("blocking output recovery operation failed")]
    Task(#[source] tokio::task::JoinError),
    #[error("output recovery log lock is poisoned")]
    LockPoisoned,
    #[error("output recovery record conflicts with durable local state")]
    Conflict,
}

impl OutputRecoveryError {
    fn corrupt(message: &'static str) -> Self {
        Self::Corrupt(message)
    }

    fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self::Io { operation, source }
    }

    fn task(source: tokio::task::JoinError) -> Self {
        Self::Task(source)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableRecoveryRecord {
    recovery_id: String,
    revision: u64,
    output: DurableRecoveryOutput,
    coverage: Vec<DurableBatchCoverage>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
enum DurableRecoveryOutput {
    Segment {
        segment_id: String,
        source_id: String,
        schema_id: String,
        event_day: String,
        minimum_event_time_micros: i64,
        maximum_event_time_micros: i64,
        minimum_ingestion_time_micros: i64,
        maximum_ingestion_time_micros: i64,
        row_count: u64,
        uncompressed_bytes: u64,
        object: DurableObjectDescriptor,
    },
    DeadLetter {
        input_id: String,
        batch_id: String,
        object: DurableObjectDescriptor,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableObjectDescriptor {
    object_id: String,
    object_key: String,
    expected_byte_size: u64,
    digest: [u8; 32],
    media_type: String,
    format_version: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableBatchCoverage {
    batch_id: String,
    spool_start: u64,
    spool_end: u64,
    positions: Vec<u64>,
}

fn validate_positions(positions: &[u64]) -> Result<(), OutputRecoveryModelError> {
    if positions.is_empty() {
        return Err(OutputRecoveryModelError::CoveredPositionsMustNotBeEmpty);
    }
    if positions.len() > MAXIMUM_COVERED_POSITIONS {
        return Err(OutputRecoveryModelError::CoveredPositionLimitExceeded);
    }
    if positions.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(OutputRecoveryModelError::PositionsMustIncrease);
    }
    Ok(())
}

fn validate_coverage(coverage: &[BatchPositionCoverage]) -> Result<(), OutputRecoveryModelError> {
    if coverage.is_empty() {
        return Err(OutputRecoveryModelError::CoveredPositionsMustNotBeEmpty);
    }
    if coverage.len() > MAXIMUM_COVERED_BATCHES {
        return Err(OutputRecoveryModelError::CoveredBatchLimitExceeded);
    }
    let mut positions = 0_usize;
    let mut batch_ids = BTreeSet::new();
    for item in coverage {
        validate_positions(&item.positions)?;
        positions = positions
            .checked_add(item.positions.len())
            .ok_or(OutputRecoveryModelError::CoveredPositionLimitExceeded)?;
        if positions > MAXIMUM_COVERED_POSITIONS || !batch_ids.insert(item.batch_id) {
            return Err(OutputRecoveryModelError::CoveredBatchesOverlap);
        }
    }
    if coverage
        .windows(2)
        .any(|pair| pair[0].spool_range.end() > pair[1].spool_range.start())
    {
        return Err(OutputRecoveryModelError::CoveredBatchesOverlap);
    }
    Ok(())
}

fn recover_log(
    directory: &Path,
    root: ManagedRoot,
) -> Result<OutputRecoveryLogInner, OutputRecoveryError> {
    let metadata = std::fs::metadata(directory)
        .map_err(|source| OutputRecoveryError::io("inspect output recovery directory", source))?;
    if !metadata.is_dir() {
        return Err(OutputRecoveryError::corrupt(
            "output recovery path is not a directory",
        ));
    }
    let path = directory.join(RECOVERY_FILE_NAME);
    let mut writer = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&path)
        .map_err(|source| OutputRecoveryError::io("open output recovery log", source))?;
    let file_bytes = writer
        .metadata()
        .map_err(|source| OutputRecoveryError::io("inspect output recovery log", source))?
        .len();
    let (records, committed_bytes) = scan_log(&mut writer, file_bytes, &root)?;
    if committed_bytes != file_bytes {
        writer.set_len(committed_bytes).map_err(|source| {
            OutputRecoveryError::io("discard incomplete recovery tail", source)
        })?;
        writer.sync_all().map_err(|source| {
            OutputRecoveryError::io("synchronize recovered output log", source)
        })?;
    }
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            OutputRecoveryError::io("synchronize output recovery directory", source)
        })?;
    Ok(OutputRecoveryLogInner {
        writer,
        directory: directory.to_owned(),
        root,
        records,
    })
}

fn scan_log(
    file: &mut File,
    file_bytes: u64,
    root: &ManagedRoot,
) -> Result<(BTreeMap<OutputRecoveryId, OutputRecoveryRecord>, u64), OutputRecoveryError> {
    let mut position = 0_u64;
    let mut records = BTreeMap::new();
    while position < file_bytes {
        file.seek(SeekFrom::Start(position)).map_err(|source| {
            OutputRecoveryError::io("seek through output recovery log", source)
        })?;
        let remaining = file_bytes - position;
        if remaining < RECOVERY_HEADER_BYTES as u64 {
            let prefix_len = usize::try_from(remaining)
                .map_err(|_| OutputRecoveryError::corrupt("recovery tail length is invalid"))?;
            let mut prefix = [0_u8; RECOVERY_HEADER_BYTES];
            file.read_exact(&mut prefix[..prefix_len])
                .map_err(|source| {
                    OutputRecoveryError::io("read incomplete recovery header", source)
                })?;
            validate_incomplete_header(&prefix[..prefix_len])?;
            break;
        }

        let mut header = [0_u8; RECOVERY_HEADER_BYTES];
        file.read_exact(&mut header)
            .map_err(|source| OutputRecoveryError::io("read recovery header", source))?;
        let decoded = decode_recovery_header(&header)?;
        let frame_bytes = (RECOVERY_HEADER_BYTES as u64)
            .checked_add(decoded.payload_bytes)
            .and_then(|bytes| bytes.checked_add(RECOVERY_FOOTER_BYTES as u64))
            .ok_or_else(|| OutputRecoveryError::corrupt("recovery frame length overflows"))?;
        let frame_end = position
            .checked_add(frame_bytes)
            .ok_or_else(|| OutputRecoveryError::corrupt("recovery frame position overflows"))?;
        if frame_end > file_bytes {
            break;
        }

        let payload_len = usize::try_from(decoded.payload_bytes)
            .map_err(|_| OutputRecoveryError::corrupt("recovery payload does not fit memory"))?;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(payload_len)
            .map_err(|_| OutputRecoveryError::corrupt("recovery payload allocation failed"))?;
        payload.resize(payload_len, 0);
        file.read_exact(&mut payload)
            .map_err(|source| OutputRecoveryError::io("read recovery payload", source))?;
        let mut footer = [0_u8; RECOVERY_FOOTER_BYTES];
        file.read_exact(&mut footer)
            .map_err(|source| OutputRecoveryError::io("read recovery footer", source))?;
        validate_recovery_frame(
            &header,
            &payload,
            &footer,
            frame_bytes,
            decoded.payload_digest,
        )?;
        let record = decode_record(&payload, root)?;
        if record.id.as_uuid() != decoded.recovery_id || record.revision.get() != decoded.revision {
            return Err(OutputRecoveryError::corrupt(
                "recovery header and payload identities disagree",
            ));
        }
        validate_transition(records.get(&record.id), &record)
            .map_err(|_| OutputRecoveryError::corrupt("recovery revisions are inconsistent"))?;
        ensure_unique_object_identity(&records, &record)
            .map_err(|_| OutputRecoveryError::corrupt("recovery object identity is reused"))?;
        records.insert(record.id, record);
        if records.len() > MAXIMUM_RECOVERY_RECORDS {
            return Err(OutputRecoveryError::corrupt(
                "output recovery record limit is exceeded",
            ));
        }
        position = frame_end;
    }
    Ok((records, position))
}

#[derive(Clone, Copy, Debug)]
struct DecodedRecoveryHeader {
    payload_bytes: u64,
    recovery_id: Uuid,
    revision: u64,
    payload_digest: [u8; 32],
}

fn encode_frame(
    record: &OutputRecoveryRecord,
    payload: &[u8],
) -> Result<Vec<u8>, OutputRecoveryError> {
    let payload_bytes = u64::try_from(payload.len())
        .map_err(|_| OutputRecoveryError::corrupt("recovery payload length exceeds u64"))?;
    if payload_bytes > MAXIMUM_RECOVERY_PAYLOAD_BYTES {
        return Err(OutputRecoveryError::Conflict);
    }
    let payload_digest = blake3::hash(payload);
    let mut header = Vec::with_capacity(RECOVERY_HEADER_BYTES);
    header.extend_from_slice(RECOVERY_MAGIC);
    header.extend_from_slice(&RECOVERY_FORMAT_VERSION.to_be_bytes());
    header.extend_from_slice(&(RECOVERY_HEADER_BYTES as u16).to_be_bytes());
    header.extend_from_slice(&payload_bytes.to_be_bytes());
    header.extend_from_slice(record.id.as_uuid().as_bytes());
    header.extend_from_slice(&record.revision.get().to_be_bytes());
    header.extend_from_slice(payload_digest.as_bytes());
    if header.len() != RECOVERY_HEADER_BYTES {
        return Err(OutputRecoveryError::corrupt(
            "recovery header length is inconsistent",
        ));
    }
    let frame_bytes = payload_bytes
        .checked_add((RECOVERY_HEADER_BYTES + RECOVERY_FOOTER_BYTES) as u64)
        .ok_or_else(|| OutputRecoveryError::corrupt("recovery frame length overflows"))?;
    let mut frame_hasher = blake3::Hasher::new();
    frame_hasher.update(&header);
    frame_hasher.update(payload);
    let mut frame = Vec::with_capacity(
        usize::try_from(frame_bytes)
            .map_err(|_| OutputRecoveryError::corrupt("recovery frame does not fit memory"))?,
    );
    frame.extend_from_slice(&header);
    frame.extend_from_slice(payload);
    frame.extend_from_slice(frame_hasher.finalize().as_bytes());
    frame.extend_from_slice(RECOVERY_COMMIT_MAGIC);
    frame.extend_from_slice(&frame_bytes.to_be_bytes());
    Ok(frame)
}

fn decode_recovery_header(
    header: &[u8; RECOVERY_HEADER_BYTES],
) -> Result<DecodedRecoveryHeader, OutputRecoveryError> {
    if &header[..8] != RECOVERY_MAGIC {
        return Err(OutputRecoveryError::corrupt("recovery magic is invalid"));
    }
    if u16::from_be_bytes(copy_array(&header[8..10])?) != RECOVERY_FORMAT_VERSION {
        return Err(OutputRecoveryError::corrupt(
            "recovery format version is unsupported",
        ));
    }
    if usize::from(u16::from_be_bytes(copy_array(&header[10..12])?)) != RECOVERY_HEADER_BYTES {
        return Err(OutputRecoveryError::corrupt(
            "recovery header length is invalid",
        ));
    }
    let payload_bytes = u64::from_be_bytes(copy_array(&header[12..20])?);
    if payload_bytes == 0 || payload_bytes > MAXIMUM_RECOVERY_PAYLOAD_BYTES {
        return Err(OutputRecoveryError::corrupt(
            "recovery payload length is invalid",
        ));
    }
    Ok(DecodedRecoveryHeader {
        payload_bytes,
        recovery_id: Uuid::from_bytes(copy_array(&header[20..36])?),
        revision: u64::from_be_bytes(copy_array(&header[36..44])?),
        payload_digest: copy_array(&header[44..76])?,
    })
}

fn validate_incomplete_header(prefix: &[u8]) -> Result<(), OutputRecoveryError> {
    let magic_bytes = prefix.len().min(RECOVERY_MAGIC.len());
    if prefix[..magic_bytes] != RECOVERY_MAGIC[..magic_bytes] {
        return Err(OutputRecoveryError::corrupt(
            "incomplete recovery tail is not a frame prefix",
        ));
    }
    if prefix.len() >= 10
        && u16::from_be_bytes(copy_array(&prefix[8..10])?) != RECOVERY_FORMAT_VERSION
    {
        return Err(OutputRecoveryError::corrupt(
            "incomplete recovery tail has an unsupported version",
        ));
    }
    if prefix.len() >= 12
        && usize::from(u16::from_be_bytes(copy_array(&prefix[10..12])?)) != RECOVERY_HEADER_BYTES
    {
        return Err(OutputRecoveryError::corrupt(
            "incomplete recovery tail has invalid framing",
        ));
    }
    if prefix.len() >= 20 {
        let payload_bytes = u64::from_be_bytes(copy_array(&prefix[12..20])?);
        if payload_bytes == 0 || payload_bytes > MAXIMUM_RECOVERY_PAYLOAD_BYTES {
            return Err(OutputRecoveryError::corrupt(
                "incomplete recovery tail has an invalid payload length",
            ));
        }
    }
    Ok(())
}

fn validate_recovery_frame(
    header: &[u8; RECOVERY_HEADER_BYTES],
    payload: &[u8],
    footer: &[u8; RECOVERY_FOOTER_BYTES],
    expected_frame_bytes: u64,
    expected_payload_digest: [u8; 32],
) -> Result<(), OutputRecoveryError> {
    if blake3::hash(payload).as_bytes() != &expected_payload_digest {
        return Err(OutputRecoveryError::corrupt(
            "recovery payload digest does not match",
        ));
    }
    let mut frame_hasher = blake3::Hasher::new();
    frame_hasher.update(header);
    frame_hasher.update(payload);
    if &footer[..32] != frame_hasher.finalize().as_bytes()
        || &footer[32..40] != RECOVERY_COMMIT_MAGIC
        || u64::from_be_bytes(copy_array(&footer[40..48])?) != expected_frame_bytes
    {
        return Err(OutputRecoveryError::corrupt(
            "recovery commit footer does not match",
        ));
    }
    Ok(())
}

fn encode_record(record: &OutputRecoveryRecord) -> Result<Vec<u8>, OutputRecoveryError> {
    let durable = DurableRecoveryRecord::from_record(record);
    serde_json::to_vec(&durable).map_err(OutputRecoveryError::Encode)
}

fn decode_record(
    payload: &[u8],
    root: &ManagedRoot,
) -> Result<OutputRecoveryRecord, OutputRecoveryError> {
    let durable: DurableRecoveryRecord = serde_json::from_slice(payload)
        .map_err(|_| OutputRecoveryError::corrupt("recovery payload is not canonical metadata"))?;
    durable.into_record(root)
}

impl DurableRecoveryRecord {
    fn from_record(record: &OutputRecoveryRecord) -> Self {
        Self {
            recovery_id: record.id.to_string(),
            revision: record.revision.get(),
            output: DurableRecoveryOutput::from_output(&record.output),
            coverage: record
                .coverage
                .iter()
                .map(DurableBatchCoverage::from_coverage)
                .collect(),
        }
    }

    fn into_record(self, root: &ManagedRoot) -> Result<OutputRecoveryRecord, OutputRecoveryError> {
        let id = OutputRecoveryId::try_from(parse_uuid(&self.recovery_id)?)
            .map_err(|_| OutputRecoveryError::corrupt("recovery identity is invalid"))?;
        let revision = NonZeroU64::new(self.revision)
            .ok_or_else(|| OutputRecoveryError::corrupt("recovery revision is zero"))?;
        let coverage = self
            .coverage
            .into_iter()
            .map(DurableBatchCoverage::into_coverage)
            .collect::<Result<Vec<_>, _>>()?;
        validate_coverage(&coverage)
            .map_err(|_| OutputRecoveryError::corrupt("recovery coverage is invalid"))?;
        let output = self.output.into_output(root)?;
        let record = OutputRecoveryRecord {
            id,
            revision,
            output,
            coverage,
        };
        validate_record_semantics(&record)?;
        Ok(record)
    }
}

impl DurableRecoveryOutput {
    fn from_output(output: &RecoveryOutput) -> Self {
        match output {
            RecoveryOutput::Segment(registration) => {
                let times = registration.times();
                Self::Segment {
                    segment_id: registration.segment_id().to_string(),
                    source_id: registration.source_id().to_string(),
                    schema_id: registration.schema_id().to_string(),
                    event_day: times.event_day().to_string(),
                    minimum_event_time_micros: times.minimum_event_time().timestamp_micros(),
                    maximum_event_time_micros: times.maximum_event_time().timestamp_micros(),
                    minimum_ingestion_time_micros: times
                        .minimum_ingestion_time()
                        .timestamp_micros(),
                    maximum_ingestion_time_micros: times
                        .maximum_ingestion_time()
                        .timestamp_micros(),
                    row_count: registration.row_count(),
                    uncompressed_bytes: registration.uncompressed_bytes(),
                    object: DurableObjectDescriptor::from_descriptor(registration.object()),
                }
            }
            RecoveryOutput::DeadLetter(registration) => Self::DeadLetter {
                input_id: registration.input_id().to_string(),
                batch_id: registration.batch_id().to_string(),
                object: DurableObjectDescriptor::from_descriptor(registration.object()),
            },
        }
    }

    fn into_output(self, root: &ManagedRoot) -> Result<RecoveryOutput, OutputRecoveryError> {
        match self {
            Self::Segment {
                segment_id,
                source_id,
                schema_id,
                event_day,
                minimum_event_time_micros,
                maximum_event_time_micros,
                minimum_ingestion_time_micros,
                maximum_ingestion_time_micros,
                row_count,
                uncompressed_bytes,
                object,
            } => {
                let segment_id = SegmentId::from(parse_uuid(&segment_id)?);
                let source_id = SourceId::try_from(parse_uuid(&source_id)?).map_err(|_| {
                    OutputRecoveryError::corrupt("recovery source identity is invalid")
                })?;
                let schema_id = SchemaId::try_from(parse_uuid(&schema_id)?).map_err(|_| {
                    OutputRecoveryError::corrupt("recovery schema identity is invalid")
                })?;
                let times = IngestionSegmentTimes::new(
                    NaiveDate::parse_from_str(&event_day, "%Y-%m-%d").map_err(|_| {
                        OutputRecoveryError::corrupt("recovery event day is invalid")
                    })?,
                    timestamp_from_micros(minimum_event_time_micros)?,
                    timestamp_from_micros(maximum_event_time_micros)?,
                    timestamp_from_micros(minimum_ingestion_time_micros)?,
                    timestamp_from_micros(maximum_ingestion_time_micros)?,
                )
                .map_err(|_| OutputRecoveryError::corrupt("recovery segment times are invalid"))?;
                let object_id = object.object_id()?;
                let descriptor = object.into_descriptor(
                    ManagedObjectKey::parquet(root, segment_id, object_id),
                    ObjectMediaType::ParquetData,
                )?;
                let registration = IngestionSegmentRegistration::new(
                    segment_id,
                    source_id,
                    schema_id,
                    times,
                    nonzero(row_count, "recovery segment row count is zero")?,
                    nonzero(
                        uncompressed_bytes,
                        "recovery segment uncompressed byte count is zero",
                    )?,
                    descriptor,
                )
                .map_err(|_| {
                    OutputRecoveryError::corrupt("recovery segment metadata is invalid")
                })?;
                Ok(RecoveryOutput::Segment(registration))
            }
            Self::DeadLetter {
                input_id,
                batch_id,
                object,
            } => {
                let input_id = InputId::try_from(parse_uuid(&input_id)?).map_err(|_| {
                    OutputRecoveryError::corrupt("recovery input identity is invalid")
                })?;
                let batch_id = BatchId::try_from(parse_uuid(&batch_id)?).map_err(|_| {
                    OutputRecoveryError::corrupt("recovery batch identity is invalid")
                })?;
                let object_id = object.object_id()?;
                let descriptor = object.into_descriptor(
                    ManagedObjectKey::dead_letter(root, batch_id, object_id),
                    ObjectMediaType::DeadLetter,
                )?;
                DeadLetterRegistration::new(input_id, batch_id, descriptor)
                    .map(RecoveryOutput::DeadLetter)
                    .map_err(|_| {
                        OutputRecoveryError::corrupt("recovery dead-letter metadata is invalid")
                    })
            }
        }
    }
}

impl DurableObjectDescriptor {
    fn from_descriptor(descriptor: &ObjectDescriptor) -> Self {
        Self {
            object_id: descriptor.key().object_id().to_string(),
            object_key: descriptor.key().as_str().to_owned(),
            expected_byte_size: descriptor.expected_byte_size().get(),
            digest: *descriptor.digest().as_bytes(),
            media_type: descriptor.media_type().as_str().to_owned(),
            format_version: descriptor.format_version().get(),
        }
    }

    fn object_id(&self) -> Result<StoredObjectId, OutputRecoveryError> {
        parse_uuid(&self.object_id).map(StoredObjectId::from)
    }

    fn into_descriptor(
        self,
        key: ManagedObjectKey,
        media_type: ObjectMediaType,
    ) -> Result<ObjectDescriptor, OutputRecoveryError> {
        if self.object_key != key.as_str() || self.media_type != media_type.as_str() {
            return Err(OutputRecoveryError::corrupt(
                "recovery object key or media type is inconsistent",
            ));
        }
        ObjectDescriptor::new(
            key,
            ObjectByteSize::new(self.expected_byte_size),
            ObjectDigest::new(self.digest),
            media_type,
            ObjectFormatVersion::new(self.format_version)
                .map_err(|_| OutputRecoveryError::corrupt("recovery object format is invalid"))?,
        )
        .map_err(|_| OutputRecoveryError::corrupt("recovery object descriptor is invalid"))
    }
}

impl DurableBatchCoverage {
    fn from_coverage(coverage: &BatchPositionCoverage) -> Self {
        Self {
            batch_id: coverage.batch_id.to_string(),
            spool_start: coverage.spool_range.start().position(),
            spool_end: coverage.spool_range.end().position(),
            positions: coverage.positions.clone(),
        }
    }

    fn into_coverage(self) -> Result<BatchPositionCoverage, OutputRecoveryError> {
        let batch_id = BatchId::try_from(parse_uuid(&self.batch_id)?)
            .map_err(|_| OutputRecoveryError::corrupt("recovery batch identity is invalid"))?;
        let range = SpoolBatchRange::new(self.spool_start, self.spool_end)
            .map_err(|_| OutputRecoveryError::corrupt("recovery spool range is invalid"))?;
        BatchPositionCoverage::new(batch_id, range, self.positions)
            .map_err(|_| OutputRecoveryError::corrupt("recovery positions are invalid"))
    }
}

fn validate_record_semantics(record: &OutputRecoveryRecord) -> Result<(), OutputRecoveryError> {
    match &record.output {
        RecoveryOutput::Segment(registration) => {
            let covered_rows = record.coverage.iter().try_fold(0_u64, |total, coverage| {
                total.checked_add(coverage.positions.len() as u64)
            });
            if covered_rows != Some(registration.row_count()) {
                return Err(OutputRecoveryError::corrupt(
                    "recovery segment row coverage does not match",
                ));
            }
        }
        RecoveryOutput::DeadLetter(registration) => {
            if record.coverage.len() != 1 || record.coverage[0].batch_id != registration.batch_id()
            {
                return Err(OutputRecoveryError::corrupt(
                    "recovery dead-letter coverage does not match",
                ));
            }
        }
    }
    Ok(())
}

fn validate_record_root(
    record: &OutputRecoveryRecord,
    root: &ManagedRoot,
) -> Result<(), OutputRecoveryError> {
    let object = record.output.object();
    let expected = match &record.output {
        RecoveryOutput::Segment(registration) => {
            ManagedObjectKey::parquet(root, registration.segment_id(), object.key().object_id())
        }
        RecoveryOutput::DeadLetter(registration) => {
            ManagedObjectKey::dead_letter(root, registration.batch_id(), object.key().object_id())
        }
    };
    if expected != *object.key() {
        return Err(OutputRecoveryError::Conflict);
    }
    Ok(())
}

fn validate_transition(
    current: Option<&OutputRecoveryRecord>,
    next: &OutputRecoveryRecord,
) -> Result<(), OutputRecoveryError> {
    let Some(current) = current else {
        if next.revision != NonZeroU64::MIN {
            return Err(OutputRecoveryError::Conflict);
        }
        return Ok(());
    };
    if current.coverage != next.coverage
        || std::mem::discriminant(&current.output) != std::mem::discriminant(&next.output)
        || current.revision.get().checked_add(1) != Some(next.revision.get())
    {
        return Err(OutputRecoveryError::Conflict);
    }
    Ok(())
}

fn ensure_unique_object_identity(
    records: &BTreeMap<OutputRecoveryId, OutputRecoveryRecord>,
    next: &OutputRecoveryRecord,
) -> Result<(), OutputRecoveryError> {
    if records
        .iter()
        .any(|(id, record)| *id != next.id && record.object_identity() == next.object_identity())
    {
        return Err(OutputRecoveryError::Conflict);
    }
    Ok(())
}

fn parse_uuid(value: &str) -> Result<Uuid, OutputRecoveryError> {
    Uuid::parse_str(value)
        .map_err(|_| OutputRecoveryError::corrupt("recovery UUID text is invalid"))
}

fn timestamp_from_micros(value: i64) -> Result<DateTime<Utc>, OutputRecoveryError> {
    DateTime::from_timestamp_micros(value)
        .ok_or_else(|| OutputRecoveryError::corrupt("recovery timestamp is outside UTC range"))
}

fn nonzero(value: u64, message: &'static str) -> Result<NonZeroU64, OutputRecoveryError> {
    NonZeroU64::new(value).ok_or_else(|| OutputRecoveryError::corrupt(message))
}

fn copy_array<const SIZE: usize>(bytes: &[u8]) -> Result<[u8; SIZE], OutputRecoveryError> {
    bytes
        .try_into()
        .map_err(|_| OutputRecoveryError::corrupt("fixed-size recovery field is invalid"))
}

fn lock_log(
    inner: &Mutex<OutputRecoveryLogInner>,
) -> Result<MutexGuard<'_, OutputRecoveryLogInner>, OutputRecoveryError> {
    inner.lock().map_err(|_| OutputRecoveryError::LockPoisoned)
}

impl std::fmt::Display for OutputRecoveryId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}
