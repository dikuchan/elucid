use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use elucid_ingestion::{
    BatchId, BatchOutputRequirements, BatchPositionCoverage, NormalizationError, NormalizedRecord,
    OutputRecoveryAction, OutputRecoveryError, OutputRecoveryId, OutputRecoveryLog,
    OutputRecoveryModelError, OutputRecoveryObservation, OutputRecoveryRecord, PlannedOutputBytes,
    PublishedOutput, RecoveredBatch, RecoveredBatches, RecoveryOutput, RetainedSpoolBytes,
    SegmentBuildError, SegmentBuildOutcome, SegmentBuildSummary, SegmentBuilders,
    SegmentMaterializationError, SegmentStagingCapacity, Spool, SpoolBatchRange, SpoolError,
    UnregisteredOutputBytes, materialize_segment_record_batch, normalize_records, plan_checkpoint,
};
use elucid_metastore::{
    DeadLetterRegistration, IngestionSegmentRegistration, IngestionSegmentTimes,
    ObjectPublicationState, OperationalStore, OrphanGracePeriod, PublicationError,
    PublicationErrorKind, PublicationStore, ReconciliationLimit, RetentionPeriod,
};
use elucid_storage::{
    ImmutableObjectStore, ManagedObjectKey, ManagedRoot, ObjectDescriptor, ObjectUploadOutcome,
    ObjectVerificationOutcome, ParquetSegmentExpectation, ParquetSegmentInput, ParquetWriteLimit,
    SegmentId, StorageError, StorageErrorKind, StoredObjectId, TransferLimit,
    validate_parquet_segment, write_parquet_segment,
};

use crate::dead_letter::{
    DeadLetterObjectError, dead_letter_staging_path, read_staged_dead_letter, stage_dead_letters,
};
use crate::ingestion::IngestionBoundary;
use crate::metrics::ServiceMetrics;

const PUBLICATION_RETRY_DELAY: Duration = Duration::from_secs(1);
const ORPHAN_GRACE_SECONDS: u64 = 300;
const RECONCILIATION_ITEMS: u64 = 1_000;

pub(crate) struct ProcessingDependencies<'a> {
    pub(crate) catalog: &'a elucid_metastore::CatalogStore,
    pub(crate) publication: &'a PublicationStore,
    pub(crate) operations: &'a OperationalStore,
    pub(crate) objects: &'a ImmutableObjectStore,
    pub(crate) root: &'a ManagedRoot,
    pub(crate) spool_path: &'a Path,
    pub(crate) scratch_path: &'a Path,
    pub(crate) scratch_bytes: u64,
    pub(crate) event_retention_seconds: u64,
    pub(crate) dead_letter_retention_seconds: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum IngestionProcessingError {
    #[error("ingestion processing was cancelled")]
    Cancelled,
    #[error("ingestion spool processing failed")]
    Spool(#[from] SpoolError),
    #[error("ingestion output recovery failed")]
    Recovery(#[from] OutputRecoveryError),
    #[error("ingestion output recovery model is invalid")]
    RecoveryModel(#[from] OutputRecoveryModelError),
    #[error("spooled batch cannot be normalized")]
    Normalization(#[from] NormalizationError),
    #[error("normalized rows cannot be built into segments")]
    SegmentBuild(#[from] SegmentBuildError),
    #[error("sealed segment cannot be materialized")]
    SegmentMaterialization(#[from] SegmentMaterializationError),
    #[error("dead-letter output cannot be staged")]
    DeadLetter(#[from] DeadLetterObjectError),
    #[error("local output staging failed while {operation}")]
    LocalStaging {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("immutable output storage failed")]
    Storage(#[from] StorageError),
    #[error("publication metadata failed")]
    Publication(#[from] PublicationError),
    #[error("ingestion processing invariant failed: {0}")]
    Invariant(&'static str),
}

impl IngestionProcessingError {
    pub(crate) const fn reason(&self) -> &'static str {
        match self {
            Self::Cancelled => "ingestion processing was cancelled",
            Self::Spool(_) => "ingestion spool processing failed",
            Self::Recovery(_) => "ingestion output recovery failed",
            Self::RecoveryModel(_) => "ingestion output recovery model is invalid",
            Self::Normalization(_) => "spooled batch normalization failed",
            Self::SegmentBuild(_) => "segment building failed",
            Self::SegmentMaterialization(_) => "segment materialization failed",
            Self::DeadLetter(_) => "dead-letter staging failed",
            Self::LocalStaging { .. } => "local output staging failed",
            Self::Storage(_) => "immutable output storage failed",
            Self::Publication(_) => "publication metadata failed",
            Self::Invariant(reason) => reason,
        }
    }
}

pub(crate) async fn run(
    boundary: &IngestionBoundary,
    dependencies: ProcessingDependencies<'_>,
) -> Result<(), IngestionProcessingError> {
    let result = Processor::start(boundary, dependencies).await;
    boundary.set_worker_operational(false);
    match result {
        Err(IngestionProcessingError::Cancelled) => Ok(()),
        result => result,
    }
}

struct Processor<'a> {
    boundary: &'a IngestionBoundary,
    dependencies: ProcessingDependencies<'a>,
    spool: Spool,
    batches: RecoveredBatches,
    output_log: OutputRecoveryLog,
    builders: SegmentBuilders,
    pending: VecDeque<PendingBatch>,
    ranges: HashMap<BatchId, SpoolBatchRange>,
    published: Vec<PublishedOutput>,
    recovered_coverage: HashMap<BatchId, BTreeSet<u64>>,
    event_retention: RetentionPeriod,
    dead_letter_retention: RetentionPeriod,
    transfer_limit: TransferLimit,
    parquet_write_limit: ParquetWriteLimit,
    cancellation: CancellationToken,
    metrics: Arc<ServiceMetrics>,
}

#[derive(Clone, Debug)]
struct PendingBatch {
    requirements: BatchOutputRequirements,
    ingestion_time: elucid_ingestion::IngestionTime,
}

#[derive(Debug)]
enum ProcessingWakeup {
    Batch(RecoveredBatch),
    SegmentExpiration,
}

impl<'a> Processor<'a> {
    async fn start(
        boundary: &'a IngestionBoundary,
        dependencies: ProcessingDependencies<'a>,
    ) -> Result<(), IngestionProcessingError> {
        let batches =
            boundary
                .take_recovered_batches()
                .await
                .ok_or(IngestionProcessingError::Invariant(
                    "ingestion worker was started more than once",
                ))?;
        let output_log =
            OutputRecoveryLog::open(dependencies.spool_path, dependencies.root.clone()).await?;
        let staging_capacity = SegmentStagingCapacity::new(dependencies.scratch_bytes)
            .map_err(|_| IngestionProcessingError::Invariant("scratch capacity is invalid"))?;
        let event_retention = RetentionPeriod::new(dependencies.event_retention_seconds)
            .map_err(|_| IngestionProcessingError::Invariant("event retention is invalid"))?;
        let dead_letter_retention =
            RetentionPeriod::new(dependencies.dead_letter_retention_seconds).map_err(|_| {
                IngestionProcessingError::Invariant("dead-letter retention is invalid")
            })?;
        let transfer_limit = TransferLimit::new(dependencies.scratch_bytes)
            .map_err(|_| IngestionProcessingError::Invariant("scratch capacity is invalid"))?;
        let parquet_write_limit = ParquetWriteLimit::new(dependencies.scratch_bytes)
            .map_err(|_| IngestionProcessingError::Invariant("scratch capacity is invalid"))?;
        let mut processor = Self {
            boundary,
            dependencies,
            spool: boundary.spool(),
            batches,
            output_log,
            builders: SegmentBuilders::new(staging_capacity),
            pending: VecDeque::new(),
            ranges: HashMap::new(),
            published: Vec::new(),
            recovered_coverage: HashMap::new(),
            event_retention,
            dead_letter_retention,
            transfer_limit,
            parquet_write_limit,
            cancellation: boundary.shutdown_token(),
            metrics: Arc::clone(boundary.metrics()),
        };
        processor.recover_outputs().await?;
        boundary.set_worker_operational(true);
        processor.run_loop().await
    }

    async fn recover_outputs(&mut self) -> Result<(), IngestionProcessingError> {
        let records = self.output_log.records()?;
        let mut resumable = Vec::with_capacity(records.len());
        for record in records {
            match self.recovery_disposition(&record).await? {
                RecoveryDisposition::Resume => resumable.push(record),
                RecoveryDisposition::Rebuild => {}
            }
        }
        let referenced_segments = resumable
            .iter()
            .filter_map(|record| match record.output() {
                RecoveryOutput::Segment(registration) => Some(registration.segment_id()),
                RecoveryOutput::DeadLetter(_) => None,
                _ => None,
            })
            .collect::<Vec<_>>();
        let referenced_dead_letters = resumable
            .iter()
            .filter_map(|record| match record.output() {
                RecoveryOutput::DeadLetter(registration) => {
                    Some(registration.object().key().object_id())
                }
                RecoveryOutput::Segment(_) => None,
                _ => None,
            })
            .collect::<Vec<_>>();
        let grace = OrphanGracePeriod::new(ORPHAN_GRACE_SECONDS)
            .map_err(|_| IngestionProcessingError::Invariant("orphan grace is invalid"))?;
        let limit = ReconciliationLimit::new(RECONCILIATION_ITEMS)
            .map_err(|_| IngestionProcessingError::Invariant("reconciliation limit is invalid"))?;
        loop {
            match self
                .dependencies
                .publication
                .reconcile_unreferenced_outputs(
                    &referenced_segments,
                    &referenced_dead_letters,
                    grace,
                    limit,
                )
                .await
            {
                Ok(_) => break,
                Err(error) if error.kind() == PublicationErrorKind::Unavailable => {
                    self.metrics.record_publication_retry();
                    self.retry_delay().await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        for record in resumable {
            let published = self
                .publish_record(&record, PublicationMetric::Recovery)
                .await?;
            self.remember_coverage(&record)?;
            self.published.push(published);
        }
        self.refresh_publication_backlog().await;
        Ok(())
    }

    async fn recovery_disposition(
        &self,
        record: &OutputRecoveryRecord,
    ) -> Result<RecoveryDisposition, IngestionProcessingError> {
        let state = loop {
            match self.output_state(record).await {
                Ok(state) => break state,
                Err(error) if transient_publication(&error) => {
                    self.metrics.record_publication_retry();
                    self.retry_delay().await?;
                }
                Err(error) => return Err(error),
            }
        };
        let staged = match state {
            ObjectPublicationState::Unregistered | ObjectPublicationState::Planned => {
                Some(self.staged_output_exists(record).await?)
            }
            ObjectPublicationState::Uploaded
            | ObjectPublicationState::Published
            | ObjectPublicationState::Abandoned => None,
            _ => {
                return Err(IngestionProcessingError::Invariant(
                    "publication state is unknown",
                ));
            }
        };
        let observation = match (state, staged) {
            (ObjectPublicationState::Unregistered, Some(true)) => {
                OutputRecoveryObservation::Unregistered(UnregisteredOutputBytes::Available)
            }
            (ObjectPublicationState::Unregistered, Some(false)) => {
                OutputRecoveryObservation::Unregistered(UnregisteredOutputBytes::Missing)
            }
            (ObjectPublicationState::Planned, Some(true)) => {
                OutputRecoveryObservation::Planned(PlannedOutputBytes::Available)
            }
            (ObjectPublicationState::Planned, Some(false)) => {
                OutputRecoveryObservation::Planned(PlannedOutputBytes::MissingUnverified)
            }
            (ObjectPublicationState::Uploaded, None) => OutputRecoveryObservation::Uploaded,
            (ObjectPublicationState::Published, None) => OutputRecoveryObservation::Published,
            (ObjectPublicationState::Abandoned, None) => OutputRecoveryObservation::Abandoned,
            _ => {
                return Err(IngestionProcessingError::Invariant(
                    "publication state and staged output disagree",
                ));
            }
        };
        let action = record.recovery_action(observation, RetainedSpoolBytes::Available)?;
        match action {
            OutputRecoveryAction::Register
            | OutputRecoveryAction::Upload
            | OutputRecoveryAction::Publish
            | OutputRecoveryAction::Complete => Ok(RecoveryDisposition::Resume),
            OutputRecoveryAction::Rebuild | OutputRecoveryAction::AbandonAndRebuild => {
                Ok(RecoveryDisposition::Rebuild)
            }
            OutputRecoveryAction::VerifyExactObject => loop {
                match self
                    .dependencies
                    .objects
                    .verify(record.output().object())
                    .await
                {
                    Ok(ObjectVerificationOutcome::Verified) => {
                        let verified_action = record.recovery_action(
                            OutputRecoveryObservation::Planned(PlannedOutputBytes::MissingVerified),
                            RetainedSpoolBytes::Available,
                        )?;
                        if verified_action != OutputRecoveryAction::RecordVerifiedUpload {
                            return Err(IngestionProcessingError::Invariant(
                                "verified output produced an invalid recovery action",
                            ));
                        }
                        self.record_verified_upload(record).await?;
                        return Ok(RecoveryDisposition::Resume);
                    }
                    Ok(ObjectVerificationOutcome::Absent) => {
                        let absent_action = record.recovery_action(
                            OutputRecoveryObservation::Planned(PlannedOutputBytes::MissingAbsent),
                            RetainedSpoolBytes::Available,
                        )?;
                        if absent_action != OutputRecoveryAction::AbandonAndRebuild {
                            return Err(IngestionProcessingError::Invariant(
                                "absent output produced an invalid recovery action",
                            ));
                        }
                        return Ok(RecoveryDisposition::Rebuild);
                    }
                    Err(error) if transient_storage(&error) => {
                        self.metrics.record_publication_retry();
                        self.retry_delay().await?;
                    }
                    Err(error) => return Err(error.into()),
                    Ok(_) => {
                        return Err(IngestionProcessingError::Invariant(
                            "object verification returned an unknown outcome",
                        ));
                    }
                }
            },
            OutputRecoveryAction::RecordVerifiedUpload => Err(IngestionProcessingError::Invariant(
                "unverified output requested an upload-state transition",
            )),
            _ => Err(IngestionProcessingError::Invariant(
                "output recovery action is unknown",
            )),
        }
    }

    async fn run_loop(&mut self) -> Result<(), IngestionProcessingError> {
        loop {
            while let Some(batch) = self.batches.next_batch().await? {
                self.process_batch(batch).await?;
                self.drain_sealed_segments().await?;
            }
            self.advance_checkpoint().await?;
            match self.wait_for_work().await? {
                ProcessingWakeup::Batch(batch) => self.process_batch(batch).await?,
                ProcessingWakeup::SegmentExpiration => {
                    self.builders.seal_expired(Instant::now())?;
                }
            }
            self.drain_sealed_segments().await?;
        }
    }

    async fn wait_for_work(&mut self) -> Result<ProcessingWakeup, IngestionProcessingError> {
        let expiration = self.builders.next_expiration_in(Instant::now())?;
        let cancellation = &self.cancellation;
        let batches = &mut self.batches;
        match expiration {
            Some(expiration) => tokio::select! {
                () = cancellation.cancelled() => Err(IngestionProcessingError::Cancelled),
                result = batches.wait_next_batch() => Ok(ProcessingWakeup::Batch(result?)),
                () = tokio::time::sleep(expiration) => Ok(ProcessingWakeup::SegmentExpiration),
            },
            None => tokio::select! {
                () = cancellation.cancelled() => Err(IngestionProcessingError::Cancelled),
                result = batches.wait_next_batch() => Ok(ProcessingWakeup::Batch(result?)),
            },
        }
    }

    async fn process_batch(
        &mut self,
        batch: RecoveredBatch,
    ) -> Result<(), IngestionProcessingError> {
        let metadata = batch.metadata();
        self.boundary
            .observe_oldest_batch(metadata.ingestion_time());
        let catalog = self.dependencies.catalog.snapshot();
        let source = catalog.source_by_id(metadata.catalog().source_id()).ok_or(
            IngestionProcessingError::Invariant(
                "pinned source is absent from the retained catalog",
            ),
        )?;
        let mut normalized = normalize_records(metadata, batch.body(), source)?;
        let requirements = BatchOutputRequirements::from_normalized(&normalized, batch.range())?;
        let (accepted, rejected) = normalized.records().iter().try_fold(
            (0_u64, 0_u64),
            |(accepted, rejected), record| match record {
                NormalizedRecord::Accepted(_) => Ok((accepted.saturating_add(1), rejected)),
                NormalizedRecord::DeadLetter(_) => Ok((accepted, rejected.saturating_add(1))),
                _ => Err(IngestionProcessingError::Invariant(
                    "normalization returned an unknown record kind",
                )),
            },
        )?;
        self.metrics
            .record_normalized(accepted, rejected, normalized.ignored_records());
        if let Some(covered) = self.recovered_coverage.get(&metadata.batch_id()) {
            normalized.remove_covered_positions(covered);
        }
        if self
            .ranges
            .insert(metadata.batch_id(), batch.range())
            .is_some()
        {
            return Err(IngestionProcessingError::Invariant(
                "spool contains a duplicate batch identity",
            ));
        }
        self.pending.push_back(PendingBatch {
            requirements,
            ingestion_time: metadata.ingestion_time(),
        });

        let mut candidate = normalized;
        loop {
            match self.builders.push_batch(candidate, Instant::now())? {
                SegmentBuildOutcome::Accepted(summary) => {
                    self.stage_dead_letter(summary).await?;
                    break;
                }
                SegmentBuildOutcome::Deferred { batch, .. } => {
                    self.drain_sealed_segments().await?;
                    candidate = batch;
                }
                _ => {
                    return Err(IngestionProcessingError::Invariant(
                        "segment builder returned an unknown outcome",
                    ));
                }
            }
        }
        Ok(())
    }

    async fn stage_dead_letter(
        &mut self,
        summary: SegmentBuildSummary,
    ) -> Result<(), IngestionProcessingError> {
        if summary.dead_letters().is_empty() {
            return Ok(());
        }
        let metadata = summary.metadata();
        let range = self.ranges.get(&metadata.batch_id()).copied().ok_or(
            IngestionProcessingError::Invariant("dead-letter batch has no retained spool range"),
        )?;
        let mut positions = summary
            .dead_letters()
            .iter()
            .map(|entry| entry.location().input_position())
            .collect::<Vec<_>>();
        positions.sort_unstable();
        let coverage = BatchPositionCoverage::new(metadata.batch_id(), range, positions)?;
        let object_id = StoredObjectId::from(Uuid::now_v7());
        let key =
            ManagedObjectKey::dead_letter(self.dependencies.root, metadata.batch_id(), object_id);
        let staged = stage_dead_letters(
            self.dependencies.scratch_path,
            key,
            summary.dead_letters(),
            self.dependencies.scratch_bytes,
        )
        .await?;
        let registration = DeadLetterRegistration::new(
            metadata.catalog().input_id(),
            metadata.batch_id(),
            staged.descriptor().clone(),
        )
        .map_err(|_| IngestionProcessingError::Invariant("dead-letter registration is invalid"))?;
        let recovery_id = OutputRecoveryId::try_from(Uuid::now_v7()).map_err(|_| {
            IngestionProcessingError::Invariant("generated recovery identity is invalid")
        })?;
        let record = OutputRecoveryRecord::dead_letter(recovery_id, registration, coverage)?;
        self.output_log.record(record.clone()).await?;
        let published = self
            .publish_record(&record, PublicationMetric::DeadLetter)
            .await?;
        self.published.push(published);
        Ok(())
    }

    async fn drain_sealed_segments(&mut self) -> Result<(), IngestionProcessingError> {
        while let Some(segment) = self.builders.take_next_sealed()? {
            let catalog = self.dependencies.catalog.snapshot();
            let source = catalog.source_by_id(segment.source_id()).ok_or(
                IngestionProcessingError::Invariant(
                    "sealed segment source is absent from the retained catalog",
                ),
            )?;
            let schema =
                source
                    .schema(segment.schema_id())
                    .ok_or(IngestionProcessingError::Invariant(
                        "sealed segment schema is absent from the retained catalog",
                    ))?;
            let segment_id = SegmentId::from(Uuid::now_v7());
            let object_id = StoredObjectId::from(Uuid::now_v7());
            let key = ManagedObjectKey::parquet(self.dependencies.root, segment_id, object_id);
            let batch = materialize_segment_record_batch(&segment, schema)?;
            let input = ParquetSegmentInput::new(key, schema, batch).map_err(|_| {
                IngestionProcessingError::Invariant("Parquet segment input is invalid")
            })?;
            let staged = write_parquet_segment(
                self.dependencies.scratch_path,
                input,
                self.parquet_write_limit,
            )
            .await?;
            let bounds = segment.bounds();
            let times = IngestionSegmentTimes::new(
                segment.event_day().as_date(),
                timestamp(bounds.minimum_event_time().unix_milliseconds())?,
                timestamp(bounds.maximum_event_time().unix_milliseconds())?,
                timestamp(bounds.minimum_ingestion_time().unix_milliseconds())?,
                timestamp(bounds.maximum_ingestion_time().unix_milliseconds())?,
            )
            .map_err(|_| {
                IngestionProcessingError::Invariant("sealed segment time bounds are invalid")
            })?;
            let row_count = NonZeroU64::new(segment.row_count()).ok_or(
                IngestionProcessingError::Invariant("sealed segment has no rows"),
            )?;
            let uncompressed_bytes = NonZeroU64::new(segment.estimated_uncompressed_bytes())
                .ok_or(IngestionProcessingError::Invariant(
                    "sealed segment has no estimated bytes",
                ))?;
            let registration = IngestionSegmentRegistration::new(
                segment_id,
                segment.source_id(),
                segment.schema_id(),
                times,
                row_count,
                uncompressed_bytes,
                staged.object_descriptor().clone(),
            )
            .map_err(|_| IngestionProcessingError::Invariant("segment registration is invalid"))?;
            let coverage = segment_coverage(&segment, &self.ranges)?;
            let recovery_id = OutputRecoveryId::try_from(Uuid::now_v7()).map_err(|_| {
                IngestionProcessingError::Invariant("generated recovery identity is invalid")
            })?;
            let record = OutputRecoveryRecord::segment(recovery_id, registration, coverage)?;
            self.output_log.record(record.clone()).await?;
            let published = self
                .publish_record(&record, PublicationMetric::Segment)
                .await?;
            self.published.push(published);
        }
        Ok(())
    }

    async fn publish_record(
        &self,
        record: &OutputRecoveryRecord,
        metric: PublicationMetric,
    ) -> Result<PublishedOutput, IngestionProcessingError> {
        let mut published_here = false;
        loop {
            let state = match self.output_state(record).await {
                Ok(state) => state,
                Err(error) if transient_publication(&error) => {
                    self.metrics.record_publication_retry();
                    self.retry_delay().await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match state {
                ObjectPublicationState::Unregistered => {
                    if !self.staged_output_exists(record).await? {
                        return Err(IngestionProcessingError::Invariant(
                            "unregistered recovery output has no staged bytes",
                        ));
                    }
                    if let Err(error) = self.register_output(record).await {
                        if transient_publication(&error) {
                            self.metrics.record_publication_retry();
                            self.retry_delay().await?;
                            continue;
                        }
                        return Err(error);
                    }
                }
                ObjectPublicationState::Planned => {
                    if !self.staged_output_exists(record).await? {
                        match self
                            .dependencies
                            .objects
                            .verify(record.output().object())
                            .await
                        {
                            Ok(ObjectVerificationOutcome::Verified) => {
                                self.record_verified_upload(record).await?;
                                continue;
                            }
                            Ok(ObjectVerificationOutcome::Absent) => {
                                return Err(IngestionProcessingError::Invariant(
                                    "planned recovery output has neither staged nor uploaded bytes",
                                ));
                            }
                            Err(error) if transient_storage(&error) => {
                                self.metrics.record_publication_retry();
                                self.retry_delay().await?;
                                continue;
                            }
                            Err(error) => return Err(error.into()),
                            Ok(_) => {
                                return Err(IngestionProcessingError::Invariant(
                                    "object verification returned an unknown outcome",
                                ));
                            }
                        }
                    }
                    let bytes = self.read_staged_output(record).await?;
                    match self
                        .dependencies
                        .objects
                        .upload(record.output().object(), bytes, self.transfer_limit)
                        .await
                    {
                        Ok(ObjectUploadOutcome::Uploaded | ObjectUploadOutcome::AlreadyPresent) => {
                        }
                        Err(error) if transient_storage(&error) => {
                            self.metrics.record_publication_retry();
                            self.retry_delay().await?;
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                        Ok(_) => {
                            return Err(IngestionProcessingError::Invariant(
                                "object upload returned an unknown outcome",
                            ));
                        }
                    }
                    self.record_verified_upload(record).await?;
                }
                ObjectPublicationState::Uploaded => match self.publish_output(record).await {
                    Ok(()) => published_here = true,
                    Err(error) if transient_publication(&error) => {
                        self.metrics.record_publication_retry();
                        self.retry_delay().await?;
                        continue;
                    }
                    Err(error) => return Err(error),
                },
                ObjectPublicationState::Published => {
                    if published_here {
                        match metric {
                            PublicationMetric::Segment => self.metrics.record_segment_published(),
                            PublicationMetric::DeadLetter => {
                                self.metrics.record_dead_letter_published();
                            }
                            PublicationMetric::Recovery => {}
                        }
                    }
                    let published = PublishedOutput::resolve(
                        record.clone(),
                        elucid_ingestion::OutputRecoveryObservation::Published,
                    )
                    .map_err(OutputRecoveryError::from)
                    .map_err(IngestionProcessingError::from)?;
                    self.remove_staged_output(record).await?;
                    return Ok(published);
                }
                ObjectPublicationState::Abandoned => {
                    return Err(IngestionProcessingError::Invariant(
                        "retained output was abandoned before publication",
                    ));
                }
                _ => {
                    return Err(IngestionProcessingError::Invariant(
                        "publication state is unknown",
                    ));
                }
            }
        }
    }

    async fn output_state(
        &self,
        record: &OutputRecoveryRecord,
    ) -> Result<ObjectPublicationState, IngestionProcessingError> {
        match record.output() {
            RecoveryOutput::Segment(registration) => Ok(self
                .dependencies
                .publication
                .ingestion_output_state(registration)
                .await?),
            RecoveryOutput::DeadLetter(registration) => Ok(self
                .dependencies
                .publication
                .dead_letter_output_state(registration)
                .await?),
            _ => Err(IngestionProcessingError::Invariant(
                "recovery output kind is unsupported",
            )),
        }
    }

    async fn register_output(
        &self,
        record: &OutputRecoveryRecord,
    ) -> Result<(), IngestionProcessingError> {
        match record.output() {
            RecoveryOutput::Segment(registration) => {
                self.dependencies
                    .publication
                    .register_ingestion_segment(registration)
                    .await?;
            }
            RecoveryOutput::DeadLetter(registration) => {
                self.dependencies
                    .publication
                    .register_dead_letter(registration)
                    .await?;
            }
            _ => {
                return Err(IngestionProcessingError::Invariant(
                    "recovery output kind is unsupported",
                ));
            }
        }
        Ok(())
    }

    async fn record_verified_upload(
        &self,
        record: &OutputRecoveryRecord,
    ) -> Result<(), IngestionProcessingError> {
        loop {
            match self
                .dependencies
                .publication
                .record_verified_upload(record.output().object())
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == PublicationErrorKind::Unavailable => {
                    self.metrics.record_publication_retry();
                    self.retry_delay().await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn publish_output(
        &self,
        record: &OutputRecoveryRecord,
    ) -> Result<(), IngestionProcessingError> {
        match record.output() {
            RecoveryOutput::Segment(registration) => {
                self.dependencies
                    .publication
                    .publish_ingestion_segment(registration.segment_id(), self.event_retention)
                    .await?;
            }
            RecoveryOutput::DeadLetter(registration) => {
                self.dependencies
                    .publication
                    .publish_dead_letter(
                        registration.object().key().object_id(),
                        self.dead_letter_retention,
                    )
                    .await?;
            }
            _ => {
                return Err(IngestionProcessingError::Invariant(
                    "recovery output kind is unsupported",
                ));
            }
        }
        Ok(())
    }

    async fn staged_output_exists(
        &self,
        record: &OutputRecoveryRecord,
    ) -> Result<bool, IngestionProcessingError> {
        tokio::fs::try_exists(self.staged_output_path(record)?)
            .await
            .map_err(DeadLetterObjectError::Io)
            .map_err(IngestionProcessingError::from)
    }

    async fn remove_staged_output(
        &self,
        record: &OutputRecoveryRecord,
    ) -> Result<(), IngestionProcessingError> {
        match tokio::fs::remove_file(self.staged_output_path(record)?).await {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(IngestionProcessingError::LocalStaging {
                operation: "removing a published output",
                source,
            }),
        }
    }

    fn staged_output_path(
        &self,
        record: &OutputRecoveryRecord,
    ) -> Result<PathBuf, IngestionProcessingError> {
        match record.output() {
            RecoveryOutput::Segment(registration) => {
                let catalog = self.dependencies.catalog.snapshot();
                let source = catalog.source_by_id(registration.source_id()).ok_or(
                    IngestionProcessingError::Invariant(
                        "recovered segment source is absent from the catalog",
                    ),
                )?;
                let schema = source.schema(registration.schema_id()).ok_or(
                    IngestionProcessingError::Invariant(
                        "recovered segment schema is absent from the catalog",
                    ),
                )?;
                let expectation = ParquetSegmentExpectation::new(
                    registration.object().key().clone(),
                    schema,
                    registration.row_count(),
                )
                .map_err(|_| {
                    IngestionProcessingError::Invariant("recovered Parquet expectation is invalid")
                })?;
                Ok(expectation.staging_path(self.dependencies.scratch_path))
            }
            RecoveryOutput::DeadLetter(registration) => Ok(dead_letter_staging_path(
                self.dependencies.scratch_path,
                registration.object(),
            )),
            _ => Err(IngestionProcessingError::Invariant(
                "recovery output kind is unsupported",
            )),
        }
    }

    async fn read_staged_output(
        &self,
        record: &OutputRecoveryRecord,
    ) -> Result<Bytes, IngestionProcessingError> {
        match record.output() {
            RecoveryOutput::Segment(registration) => {
                let catalog = self.dependencies.catalog.snapshot();
                let source = catalog.source_by_id(registration.source_id()).ok_or(
                    IngestionProcessingError::Invariant(
                        "recovered segment source is absent from the catalog",
                    ),
                )?;
                let schema = source.schema(registration.schema_id()).ok_or(
                    IngestionProcessingError::Invariant(
                        "recovered segment schema is absent from the catalog",
                    ),
                )?;
                let expectation = ParquetSegmentExpectation::new(
                    registration.object().key().clone(),
                    schema,
                    registration.row_count(),
                )
                .map_err(|_| {
                    IngestionProcessingError::Invariant("recovered Parquet expectation is invalid")
                })?;
                let path = expectation.staging_path(self.dependencies.scratch_path);
                let staged = validate_parquet_segment(&path, expectation).await?;
                if staged.object_descriptor() != registration.object() {
                    return Err(IngestionProcessingError::Invariant(
                        "recovered Parquet bytes do not match durable output metadata",
                    ));
                }
                read_bounded_file(
                    &path,
                    registration.object(),
                    self.dependencies.scratch_bytes,
                )
                .await
            }
            RecoveryOutput::DeadLetter(registration) => read_staged_dead_letter(
                &dead_letter_staging_path(self.dependencies.scratch_path, registration.object()),
                registration.object(),
                self.dependencies.scratch_bytes,
            )
            .await
            .map_err(IngestionProcessingError::from),
            _ => Err(IngestionProcessingError::Invariant(
                "recovery output kind is unsupported",
            )),
        }
    }

    async fn advance_checkpoint(&mut self) -> Result<(), IngestionProcessingError> {
        let requirements = self
            .pending
            .iter()
            .map(|pending| pending.requirements.clone())
            .collect::<Vec<_>>();
        let Some(plan) = plan_checkpoint(self.spool.checkpoint()?, &requirements, &self.published)?
        else {
            return Ok(());
        };
        let target = plan.target();
        self.spool.advance_checkpoint(plan).await?;
        let mut completed = 0_u64;
        while self
            .pending
            .front()
            .is_some_and(|pending| pending.requirements.spool_range().end() <= target)
        {
            let pending = self
                .pending
                .pop_front()
                .ok_or(IngestionProcessingError::Invariant(
                    "pending batch queue changed unexpectedly",
                ))?;
            self.ranges.remove(&pending.requirements.batch_id());
            completed = completed
                .checked_add(1)
                .ok_or(IngestionProcessingError::Invariant(
                    "completed batch count overflow",
                ))?;
        }
        let next_oldest = self.pending.front().map(|pending| pending.ingestion_time);
        if !self
            .boundary
            .complete_pending_batches(completed, next_oldest)
        {
            return Err(IngestionProcessingError::Invariant(
                "spool backlog accounting underflowed",
            ));
        }
        self.boundary.refresh_spool_usage()?;
        let reclamation = self.spool.reclaim_checkpointed().await?;
        if reclamation.reclaimed_bytes().is_some() {
            self.output_log.reclaim(reclamation).await?;
            self.published.clear();
            self.recovered_coverage.clear();
            self.ranges.clear();
            self.boundary.refresh_spool_usage()?;
        }
        self.refresh_publication_backlog().await;
        Ok(())
    }

    fn remember_coverage(
        &mut self,
        record: &OutputRecoveryRecord,
    ) -> Result<(), IngestionProcessingError> {
        for coverage in record.coverage() {
            let positions = self
                .recovered_coverage
                .entry(coverage.batch_id())
                .or_default();
            for position in coverage.positions() {
                if !positions.insert(*position) {
                    return Err(IngestionProcessingError::Invariant(
                        "recovered output coverage overlaps",
                    ));
                }
            }
        }
        Ok(())
    }

    async fn refresh_publication_backlog(&self) {
        if let Ok(backlog) = self.dependencies.operations.publication_backlog().await {
            self.metrics.update_publication_backlog(backlog);
        }
    }

    async fn retry_delay(&self) -> Result<(), IngestionProcessingError> {
        tokio::select! {
            () = self.cancellation.cancelled() => Err(IngestionProcessingError::Cancelled),
            () = tokio::time::sleep(PUBLICATION_RETRY_DELAY) => Ok(()),
        }
    }
}

#[derive(Clone, Copy)]
enum PublicationMetric {
    Segment,
    DeadLetter,
    Recovery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryDisposition {
    Resume,
    Rebuild,
}

fn segment_coverage(
    segment: &elucid_ingestion::SealedSegment,
    ranges: &HashMap<BatchId, SpoolBatchRange>,
) -> Result<Vec<BatchPositionCoverage>, IngestionProcessingError> {
    let mut positions = BTreeMap::<BatchId, Vec<u64>>::new();
    for row in segment.rows() {
        positions
            .entry(row.batch_id())
            .or_default()
            .push(row.row().location().input_position());
    }
    positions
        .into_iter()
        .map(|(batch_id, mut positions)| {
            positions.sort_unstable();
            let range =
                ranges
                    .get(&batch_id)
                    .copied()
                    .ok_or(IngestionProcessingError::Invariant(
                        "sealed segment row has no retained spool range",
                    ))?;
            BatchPositionCoverage::new(batch_id, range, positions)
                .map_err(OutputRecoveryError::from)
                .map_err(IngestionProcessingError::from)
        })
        .collect()
}

fn timestamp(milliseconds: i64) -> Result<DateTime<Utc>, IngestionProcessingError> {
    DateTime::<Utc>::from_timestamp_millis(milliseconds).ok_or(IngestionProcessingError::Invariant(
        "normalized timestamp is outside the UTC range",
    ))
}

async fn read_bounded_file(
    path: &Path,
    descriptor: &ObjectDescriptor,
    maximum_bytes: u64,
) -> Result<Bytes, IngestionProcessingError> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(DeadLetterObjectError::Io)?;
    if metadata.len() > maximum_bytes || metadata.len() != descriptor.expected_byte_size().get() {
        return Err(IngestionProcessingError::Invariant(
            "staged output size is outside its durable descriptor",
        ));
    }
    let bytes = tokio::fs::read(path)
        .await
        .map(Bytes::from)
        .map_err(DeadLetterObjectError::Io)?;
    if elucid_storage::ObjectDigest::calculate(&bytes) != descriptor.digest() {
        return Err(IngestionProcessingError::Invariant(
            "staged output digest differs from its durable descriptor",
        ));
    }
    Ok(bytes)
}

fn transient_storage(error: &StorageError) -> bool {
    matches!(
        error.kind(),
        StorageErrorKind::ObjectStoreUnavailable
            | StorageErrorKind::ObjectUploadFailed
            | StorageErrorKind::ObjectVerificationFailed
    )
}

fn transient_publication(error: &IngestionProcessingError) -> bool {
    matches!(
        error,
        IngestionProcessingError::Publication(error)
            if error.kind() == PublicationErrorKind::Unavailable
    )
}
