use bytes::Bytes;
use elucid_catalog::{IngestionProfileRevisionId, InputId, SchemaId, SourceId};
use elucid_ingestion::{
    AppendBodyLimit, BatchId, BatchMetadata, IngestionTime, PinnedCatalogIdentities, Spool,
    SpoolCapacity,
};
use uuid::Uuid;

#[tokio::test(flavor = "current_thread")]
async fn open_initializes_an_empty_spool_and_recovers_its_committed_batches() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let capacity = SpoolCapacity::new(16_384).expect("spool capacity");
    let maximum_batch = AppendBodyLimit::new(1_024).expect("maximum batch");

    let initial = Spool::open(directory.path(), capacity, maximum_batch)
        .await
        .expect("initialize spool");
    assert_eq!(initial.report().committed_batches(), 0);
    assert_eq!(initial.report().pending_batches(), 0);
    let (spool, mut batches, _) = initial.into_parts();
    assert!(
        batches
            .next_batch()
            .await
            .expect("read empty spool")
            .is_none()
    );

    let body = Bytes::from_static(b"persisted batch\n");
    let metadata = batch_metadata();
    spool
        .reserve(maximum_batch)
        .expect("reserve batch")
        .append(metadata, body.clone())
        .await
        .expect("append batch");
    drop(spool);

    let recovered = Spool::open(directory.path(), capacity, maximum_batch)
        .await
        .expect("reopen spool");
    assert_eq!(recovered.report().committed_batches(), 1);
    assert_eq!(recovered.report().pending_batches(), 1);
    let (_, mut batches, _) = recovered.into_parts();
    let recovered_batch = batches
        .next_batch()
        .await
        .expect("read recovered batch")
        .expect("committed batch");
    assert_eq!(recovered_batch.metadata(), metadata);
    assert_eq!(recovered_batch.body(), &body);
    assert!(
        batches
            .next_batch()
            .await
            .expect("finish recovered batches")
            .is_none()
    );
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
