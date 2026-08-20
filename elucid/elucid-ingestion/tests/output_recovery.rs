use std::fs::OpenOptions;
use std::io::Write as _;
use std::num::NonZeroU64;

use bytes::Bytes;
use chrono::{NaiveDate, TimeZone as _, Utc};
use elucid_catalog::{IngestionProfileRevisionId, InputId, SchemaId, SourceId};
use elucid_ingestion::{
    AppendBodyLimit, BatchId, BatchMetadata, BatchOutputRequirements, BatchPositionCoverage,
    IngestionTime, OutputRecoveryAction, OutputRecoveryId, OutputRecoveryLog,
    OutputRecoveryObservation, OutputRecoveryRecord, PinnedCatalogIdentities, PlannedOutputBytes,
    PublishedOutput, RetainedSpoolBytes, Spool, SpoolCapacity, UnregisteredOutputBytes,
    plan_checkpoint,
};
use elucid_metastore::{
    DeadLetterRegistration, IngestionSegmentRegistration, IngestionSegmentTimes,
};
use elucid_storage::{
    ManagedObjectKey, ManagedRoot, ObjectByteSize, ObjectDescriptor, ObjectDigest,
    ObjectFormatVersion, ObjectMediaType, SegmentId, StoredObjectId,
};
use uuid::Uuid;

#[tokio::test(flavor = "current_thread")]
async fn sealed_output_records_survive_restart_and_identity_replacement() {
    let directory = tempfile::tempdir().expect("temporary recovery directory");
    let root = ManagedRoot::parse("recovery-test").expect("managed root");
    let range = append_batch(directory.path(), 1).await;
    let coverage = vec![
        BatchPositionCoverage::new(batch_id(1), range, vec![0, 42]).expect("valid exact coverage"),
    ];
    let recovery_id = OutputRecoveryId::try_from(identity(10)).expect("recovery identity");
    let original =
        OutputRecoveryRecord::segment(recovery_id, segment_registration(&root, 11, 12), coverage)
            .expect("valid segment recovery record");

    let log = OutputRecoveryLog::open(directory.path(), root.clone())
        .await
        .expect("open output recovery log");
    log.record(original.clone())
        .await
        .expect("durably record output before external I/O");
    drop(log);

    let reopened = OutputRecoveryLog::open(directory.path(), root.clone())
        .await
        .expect("recover output log");
    assert_eq!(
        reopened.records().expect("recovered records"),
        vec![original.clone()]
    );

    let replacement = original
        .replacement_segment(segment_registration(&root, 13, 14))
        .expect("replace missing output with fresh identities");
    reopened
        .record(replacement.clone())
        .await
        .expect("durably replace output record");
    let torn = OutputRecoveryRecord::dead_letter(
        OutputRecoveryId::try_from(identity(15)).expect("recovery identity"),
        dead_letter_registration(&root, 1, 16),
        BatchPositionCoverage::new(batch_id(1), range, vec![84]).expect("torn record coverage"),
    )
    .expect("torn dead-letter record");
    reopened
        .record(torn)
        .await
        .expect("write record that will be torn");
    drop(reopened);

    let log_path = directory.path().join("output-recovery.log");
    let complete_bytes = std::fs::metadata(&log_path)
        .expect("output log metadata")
        .len();
    OpenOptions::new()
        .write(true)
        .open(&log_path)
        .expect("open output log for truncation")
        .set_len(complete_bytes - 11)
        .expect("tear final output record");

    let recovered = OutputRecoveryLog::open(directory.path(), root)
        .await
        .expect("recover replaced output log");
    assert_eq!(
        recovered.records().expect("latest records"),
        vec![replacement]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn output_recovery_rejects_an_arbitrary_tail_without_truncating_it() {
    let directory = tempfile::tempdir().expect("temporary recovery directory");
    let root = ManagedRoot::parse("corrupt-recovery-test").expect("managed root");
    let range = append_batch(directory.path(), 1).await;
    let record = OutputRecoveryRecord::segment(
        OutputRecoveryId::try_from(identity(17)).expect("recovery identity"),
        segment_registration(&root, 18, 19),
        vec![BatchPositionCoverage::new(batch_id(1), range, vec![0, 42]).expect("valid coverage")],
    )
    .expect("valid segment recovery record");
    let log = OutputRecoveryLog::open(directory.path(), root.clone())
        .await
        .expect("open output recovery log");
    log.record(record).await.expect("record output");
    drop(log);

    let log_path = directory.path().join("output-recovery.log");
    OpenOptions::new()
        .append(true)
        .open(&log_path)
        .expect("open output log")
        .write_all(b"not a recovery frame")
        .expect("append corrupt tail");
    let corrupt_bytes = std::fs::metadata(&log_path)
        .expect("corrupt output log metadata")
        .len();

    let error = OutputRecoveryLog::open(directory.path(), root)
        .await
        .expect_err("arbitrary recovery tail must be corruption");
    assert!(matches!(
        error,
        elucid_ingestion::OutputRecoveryError::Corrupt(_)
    ));
    assert_eq!(
        std::fs::metadata(log_path)
            .expect("unchanged corrupt log metadata")
            .len(),
        corrupt_bytes
    );
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_waits_for_every_accepted_and_rejected_occurrence() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let capacity = SpoolCapacity::new(64 * 1024).expect("spool capacity");
    let maximum_batch = AppendBodyLimit::new(1_024).expect("maximum batch");
    let spool = Spool::create_new(directory.path(), capacity)
        .await
        .expect("create spool");
    let first = append(&spool, 1).await;
    let second = append(&spool, 2).await;
    let root = ManagedRoot::parse("checkpoint-test").expect("managed root");

    let segment = OutputRecoveryRecord::segment(
        OutputRecoveryId::try_from(identity(20)).expect("recovery identity"),
        segment_registration(&root, 21, 22),
        vec![
            BatchPositionCoverage::new(batch_id(1), first, vec![0])
                .expect("first accepted coverage"),
            BatchPositionCoverage::new(batch_id(2), second, vec![0])
                .expect("second accepted coverage"),
        ],
    )
    .expect("segment record");
    let first_dead_letter = OutputRecoveryRecord::dead_letter(
        OutputRecoveryId::try_from(identity(23)).expect("recovery identity"),
        dead_letter_registration(&root, 1, 24),
        BatchPositionCoverage::new(batch_id(1), first, vec![17]).expect("first rejected coverage"),
    )
    .expect("dead-letter record");
    let second_dead_letter = OutputRecoveryRecord::dead_letter(
        OutputRecoveryId::try_from(identity(25)).expect("recovery identity"),
        dead_letter_registration(&root, 2, 26),
        BatchPositionCoverage::new(batch_id(2), second, vec![17])
            .expect("second rejected coverage"),
    )
    .expect("dead-letter record");
    let requirements = vec![
        BatchOutputRequirements::new(batch_id(1), first, vec![0, 17]).expect("first requirements"),
        BatchOutputRequirements::new(batch_id(2), second, vec![0, 17])
            .expect("second requirements"),
    ];
    let output_log = OutputRecoveryLog::open(directory.path(), root.clone())
        .await
        .expect("open output log");
    for record in [
        segment.clone(),
        first_dead_letter.clone(),
        second_dead_letter.clone(),
    ] {
        output_log
            .record(record)
            .await
            .expect("durably record output");
    }
    let published_segment = PublishedOutput::resolve(segment, OutputRecoveryObservation::Published)
        .expect("published segment");
    let published_first_dead_letter =
        PublishedOutput::resolve(first_dead_letter, OutputRecoveryObservation::Published)
            .expect("published dead letter");

    let first_plan = plan_checkpoint(
        spool.checkpoint().expect("current checkpoint"),
        &requirements,
        &[published_segment.clone(), published_first_dead_letter],
    )
    .expect("plan first checkpoint")
    .expect("first batch is complete");
    assert_eq!(first_plan.target(), first.end());
    spool
        .advance_checkpoint(first_plan)
        .await
        .expect("advance through first batch");
    drop(spool);

    let recovery = Spool::recover(directory.path(), capacity, maximum_batch)
        .await
        .expect("recover checkpointed spool");
    assert_eq!(recovery.report().checkpoint(), first.end());
    assert_eq!(recovery.report().pending_batches(), 1);
    let (spool, mut batches, _) = recovery.into_parts();
    assert_eq!(
        batches
            .next_batch()
            .await
            .expect("read pending batch")
            .expect("second batch")
            .metadata()
            .batch_id(),
        batch_id(2)
    );

    let published_second_dead_letter =
        PublishedOutput::resolve(second_dead_letter, OutputRecoveryObservation::Published)
            .expect("published second dead letter");
    let final_plan = plan_checkpoint(
        spool.checkpoint().expect("recovered checkpoint"),
        &requirements[1..],
        &[published_segment, published_second_dead_letter],
    )
    .expect("plan final checkpoint")
    .expect("second batch is complete");
    assert_eq!(final_plan.target(), second.end());
    spool
        .advance_checkpoint(final_plan)
        .await
        .expect("advance through second batch");
    let reclaimed = spool
        .reclaim_checkpointed()
        .await
        .expect("reclaim completed spool file");
    assert_eq!(reclaimed.reclaimed_bytes(), Some(second.end().position()));
    assert_eq!(
        output_log
            .reclaim(reclaimed)
            .await
            .expect("reclaim covered output records")
            .reclaimed_records(),
        Some(3)
    );
    assert_eq!(spool.checkpoint().expect("reset checkpoint").position(), 0);
    assert_eq!(spool.usage().expect("reclaimed usage").committed_bytes(), 0);
    drop(batches);
    drop(spool);
    drop(output_log);
    let recovered_output_log = OutputRecoveryLog::open(directory.path(), root)
        .await
        .expect("recover reclaimed output log");
    assert!(
        recovered_output_log
            .records()
            .expect("recovered output records")
            .is_empty()
    );
    let empty_recovery = Spool::recover(directory.path(), capacity, maximum_batch)
        .await
        .expect("recover reclaimed spool");
    assert_eq!(empty_recovery.report().committed_batches(), 0);
    assert_eq!(empty_recovery.report().pending_batches(), 0);
}

#[test]
fn recovery_actions_never_rebuild_a_published_output() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("test runtime");
    let range = runtime.block_on(append_batch(directory.path(), 1));
    let root = ManagedRoot::parse("action-test").expect("managed root");
    let record = OutputRecoveryRecord::segment(
        OutputRecoveryId::try_from(identity(30)).expect("recovery identity"),
        segment_registration(&root, 31, 32),
        vec![BatchPositionCoverage::new(batch_id(1), range, vec![0, 1]).expect("coverage")],
    )
    .expect("segment record");

    assert_eq!(
        record
            .recovery_action(
                OutputRecoveryObservation::Published,
                RetainedSpoolBytes::Missing,
            )
            .expect("published resolution"),
        OutputRecoveryAction::Complete
    );
    assert_eq!(
        record
            .recovery_action(
                OutputRecoveryObservation::Planned(PlannedOutputBytes::MissingAbsent),
                RetainedSpoolBytes::Available,
            )
            .expect("missing planned resolution"),
        OutputRecoveryAction::AbandonAndRebuild
    );
    assert_eq!(
        record
            .recovery_action(
                OutputRecoveryObservation::Unregistered(UnregisteredOutputBytes::Missing),
                RetainedSpoolBytes::Available,
            )
            .expect("missing unregistered resolution"),
        OutputRecoveryAction::Rebuild
    );
    assert_eq!(
        record.recovery_action(
            OutputRecoveryObservation::Planned(PlannedOutputBytes::MissingAbsent),
            RetainedSpoolBytes::Missing,
        ),
        Err(elucid_ingestion::OutputRecoveryModelError::RetainedSpoolBytesMissing)
    );
}

async fn append_batch(
    directory: &std::path::Path,
    sequence: u128,
) -> elucid_ingestion::SpoolBatchRange {
    let spool = Spool::open(
        directory,
        SpoolCapacity::new(64 * 1024).expect("spool capacity"),
        AppendBodyLimit::new(1_024).expect("maximum batch"),
    )
    .await
    .expect("open spool")
    .into_parts()
    .0;
    append(&spool, sequence).await
}

async fn append(spool: &Spool, sequence: u128) -> elucid_ingestion::SpoolBatchRange {
    let body = Bytes::from(format!("batch {sequence}\n"));
    spool
        .reserve(AppendBodyLimit::new(body.len() as u64).expect("body limit"))
        .expect("reserve append")
        .append(batch_metadata(sequence), body)
        .await
        .expect("append batch")
        .range()
}

fn batch_metadata(sequence: u128) -> BatchMetadata {
    BatchMetadata::new(
        batch_id(sequence),
        PinnedCatalogIdentities::new(
            SourceId::try_from(identity(100)).expect("source identity"),
            InputId::try_from(identity(101)).expect("input identity"),
            IngestionProfileRevisionId::try_from(identity(102)).expect("profile identity"),
            SchemaId::try_from(identity(103)).expect("schema identity"),
        ),
        IngestionTime::from_unix_milliseconds(1_776_945_600_000 + sequence as i64)
            .expect("ingestion time"),
    )
}

fn segment_registration(
    root: &ManagedRoot,
    segment_sequence: u128,
    object_sequence: u128,
) -> IngestionSegmentRegistration {
    let segment_id = SegmentId::from(identity(segment_sequence));
    let object = descriptor(
        ManagedObjectKey::parquet(
            root,
            segment_id,
            StoredObjectId::from(identity(object_sequence)),
        ),
        b"parquet fixture",
        ObjectMediaType::ParquetData,
    );
    IngestionSegmentRegistration::new(
        segment_id,
        SourceId::try_from(identity(100)).expect("source identity"),
        SchemaId::try_from(identity(103)).expect("schema identity"),
        IngestionSegmentTimes::new(
            NaiveDate::from_ymd_opt(2026, 8, 20).expect("event day"),
            Utc.with_ymd_and_hms(2026, 8, 20, 10, 0, 0)
                .single()
                .expect("minimum event time"),
            Utc.with_ymd_and_hms(2026, 8, 20, 10, 1, 0)
                .single()
                .expect("maximum event time"),
            Utc.with_ymd_and_hms(2026, 8, 20, 10, 2, 0)
                .single()
                .expect("minimum ingestion time"),
            Utc.with_ymd_and_hms(2026, 8, 20, 10, 2, 1)
                .single()
                .expect("maximum ingestion time"),
        )
        .expect("segment times"),
        NonZeroU64::new(2).expect("row count"),
        NonZeroU64::new(256).expect("uncompressed bytes"),
        object,
    )
    .expect("segment registration")
}

fn dead_letter_registration(
    root: &ManagedRoot,
    batch_sequence: u128,
    object_sequence: u128,
) -> DeadLetterRegistration {
    let batch_id = batch_id(batch_sequence);
    DeadLetterRegistration::new(
        InputId::try_from(identity(101)).expect("input identity"),
        batch_id,
        descriptor(
            ManagedObjectKey::dead_letter(
                root,
                batch_id,
                StoredObjectId::from(identity(object_sequence)),
            ),
            b"{\"error\":\"invalid\"}\n",
            ObjectMediaType::DeadLetter,
        ),
    )
    .expect("dead-letter registration")
}

fn descriptor(
    key: ManagedObjectKey,
    bytes: &[u8],
    media_type: ObjectMediaType,
) -> ObjectDescriptor {
    ObjectDescriptor::new(
        key,
        ObjectByteSize::new(bytes.len() as u64),
        ObjectDigest::calculate(bytes),
        media_type,
        ObjectFormatVersion::new(1).expect("format version"),
    )
    .expect("object descriptor")
}

fn batch_id(sequence: u128) -> BatchId {
    BatchId::try_from(identity(sequence)).expect("batch identity")
}

fn identity(sequence: u128) -> Uuid {
    Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 | sequence)
}
