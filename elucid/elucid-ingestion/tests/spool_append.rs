use std::fs;

use elucid_core::{CodedError, ErrorCode};

use bytes::Bytes;
use elucid_catalog::{IngestionProfileRevisionId, InputId, SchemaId, SourceId};
use elucid_ingestion::{
    AppendBodyLimit, BatchId, BatchMetadata, BodyDigest, IngestionTime, PinnedCatalogIdentities,
    Spool, SpoolCapacity,
};
use uuid::Uuid;

#[tokio::test(flavor = "current_thread")]
async fn durable_append_persists_the_exact_body_and_pinned_metadata() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let spool = Spool::create_new(
        directory.path(),
        SpoolCapacity::new(16_384).expect("spool capacity"),
    )
    .await
    .expect("create spool");
    let metadata = batch_metadata();
    let body = Bytes::from_static(
        br#"{"timestamp":"2026-08-20T12:00:00.000Z","message":"exact body"}
"#,
    );

    let reservation = spool
        .reserve(AppendBodyLimit::new(body.len() as u64).expect("body limit"))
        .expect("reserve spool capacity");
    let durable = reservation
        .append(metadata, body.clone())
        .await
        .expect("durable append");

    assert_eq!(durable.metadata(), metadata);
    assert_eq!(durable.body_bytes().get(), body.len() as u64);
    assert_eq!(durable.body_digest(), BodyDigest::calculate(&body));

    let files = fs::read_dir(directory.path())
        .expect("read spool directory")
        .collect::<Result<Vec<_>, _>>()
        .expect("read spool entries");
    assert_eq!(files.len(), 2, "data and checkpoint files");
    let persisted = files
        .iter()
        .map(|entry| fs::read(entry.path()).expect("read spool file"))
        .find(|bytes| contains(bytes, &body))
        .expect("append-only data file");
    let usage = spool.usage().expect("spool usage");
    assert_eq!(usage.committed_bytes(), persisted.len() as u64);
    assert_eq!(usage.reserved_bytes(), 0);
    assert!(
        contains(&persisted, &body),
        "body bytes must be stored exactly"
    );
    assert!(contains(
        &persisted,
        metadata.batch_id().as_uuid().as_bytes()
    ));
    assert!(contains(
        &persisted,
        metadata.catalog().source_id().as_uuid().as_bytes()
    ));
    assert!(contains(
        &persisted,
        metadata.catalog().input_id().as_uuid().as_bytes()
    ));
    assert!(contains(
        &persisted,
        metadata
            .catalog()
            .profile_revision_id()
            .as_uuid()
            .as_bytes()
    ));
    assert!(contains(
        &persisted,
        metadata.catalog().target_schema_id().as_uuid().as_bytes()
    ));
    assert!(contains(
        &persisted,
        &metadata.ingestion_time().unix_milliseconds().to_be_bytes()
    ));
    assert!(contains(&persisted, durable.body_digest().as_bytes()));
}

#[tokio::test(flavor = "current_thread")]
async fn reservation_prevents_overcommit_and_releases_unused_capacity() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let spool = Spool::create_new(
        directory.path(),
        SpoolCapacity::new(10_000).expect("spool capacity"),
    )
    .await
    .expect("create spool");
    let limit = AppendBodyLimit::new(8_000).expect("body limit");

    let reservation = spool.reserve(limit).expect("first reservation");
    let error = spool.reserve(limit).expect_err("capacity must be reserved");
    assert_eq!(error.error_code(), ErrorCode::CapacityExhausted);
    assert!(spool.usage().expect("usage").reserved_bytes() >= limit.get());

    drop(reservation);

    assert_eq!(spool.usage().expect("usage").reserved_bytes(), 0);
    let _reservation = spool.reserve(limit).expect("released capacity is reusable");
}

#[tokio::test(flavor = "current_thread")]
async fn body_larger_than_the_reservation_is_not_appended() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let spool = Spool::create_new(
        directory.path(),
        SpoolCapacity::new(4_096).expect("spool capacity"),
    )
    .await
    .expect("create spool");
    let reservation = spool
        .reserve(AppendBodyLimit::new(3).expect("body limit"))
        .expect("reservation");

    let error = reservation
        .append(batch_metadata(), Bytes::from_static(b"four"))
        .await
        .expect_err("oversized body must fail");

    assert_eq!(error.error_code(), ErrorCode::IngestionBatchLimitExceeded);
    let usage = spool.usage().expect("usage");
    assert_eq!(usage.committed_bytes(), 0);
    assert_eq!(usage.reserved_bytes(), 0);
}

fn batch_metadata() -> BatchMetadata {
    BatchMetadata::new(
        BatchId::try_from(identity(1)).expect("batch identity"),
        PinnedCatalogIdentities::new(
            SourceId::try_from(identity(2)).expect("source identity"),
            InputId::try_from(identity(3)).expect("input identity"),
            IngestionProfileRevisionId::try_from(identity(4)).expect("profile identity"),
            SchemaId::try_from(identity(5)).expect("schema identity"),
        ),
        IngestionTime::from_unix_milliseconds(1_776_945_600_000).expect("ingestion time"),
    )
}

fn identity(sequence: u128) -> Uuid {
    Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 | sequence)
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|candidate| candidate == needle)
}
