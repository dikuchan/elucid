use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use elucid_core::{CodedError, ErrorCode};

use bytes::Bytes;
use elucid_catalog::{IngestionProfileRevisionId, InputId, SchemaId, SourceId};
use elucid_ingestion::{
    AppendBodyLimit, BatchId, BatchMetadata, BodyDigest, IngestionTime, MaximumBatchAdmission,
    PinnedCatalogIdentities, Spool, SpoolCapacity,
};
use uuid::Uuid;

#[tokio::test(flavor = "current_thread")]
async fn recovery_loads_committed_batches_and_the_initial_checkpoint() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let capacity = SpoolCapacity::new(16_384).expect("spool capacity");
    let maximum_batch = AppendBodyLimit::new(1_024).expect("maximum batch");
    let spool = Spool::create_new(directory.path(), capacity)
        .await
        .expect("create spool");
    let first_body = Bytes::from_static(b"first batch\n");
    let second_body = Bytes::from_static(b"second batch\n");
    append(&spool, 1, first_body.clone()).await;
    append(&spool, 2, second_body.clone()).await;
    drop(spool);

    let recovery = Spool::recover(directory.path(), capacity, maximum_batch)
        .await
        .expect("recover spool");
    let report = recovery.report();
    assert_eq!(report.checkpoint().position(), 0);
    assert_eq!(report.committed_batches(), 2);
    assert_eq!(report.pending_batches(), 2);
    assert_eq!(report.discarded_tail_bytes(), 0);
    assert_eq!(
        report.maximum_batch_admission(),
        MaximumBatchAdmission::Available
    );

    let (spool, mut batches, recovered_report) = recovery.into_parts();
    assert_eq!(recovered_report, report);
    let third_body = Bytes::from_static(b"later batch\n");
    append(&spool, 3, third_body).await;
    let first = batches
        .next_batch()
        .await
        .expect("read first recovered batch")
        .expect("first recovered batch");
    assert_eq!(first.metadata(), batch_metadata(1));
    assert_eq!(first.body(), &first_body);
    assert_eq!(first.body_digest(), BodyDigest::calculate(&first_body));
    let second = batches
        .next_batch()
        .await
        .expect("read second recovered batch")
        .expect("second recovered batch");
    assert_eq!(second.metadata(), batch_metadata(2));
    assert_eq!(second.body(), &second_body);
    assert!(
        batches
            .next_batch()
            .await
            .expect("finish recovered batches")
            .is_none()
    );
    assert!(spool.usage().expect("recovered usage").committed_bytes() > report.committed_bytes());
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_discards_only_an_incomplete_final_append() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let capacity = SpoolCapacity::new(16_384).expect("spool capacity");
    let maximum_batch = AppendBodyLimit::new(1_024).expect("maximum batch");
    let spool = Spool::create_new(directory.path(), capacity)
        .await
        .expect("create spool");
    let first_body = Bytes::from_static(b"first committed batch\n");
    let second_body = Bytes::from_static(b"second committed batch\n");
    append(&spool, 1, first_body.clone()).await;
    append(&spool, 2, second_body.clone()).await;
    drop(spool);
    let data_path = file_containing(directory.path(), &second_body);
    let committed_bytes = fs::metadata(&data_path).expect("data metadata").len();

    let torn_directory = tempfile::tempdir().expect("temporary torn frame directory");
    let torn_spool = Spool::create_new(torn_directory.path(), capacity)
        .await
        .expect("create torn source spool");
    let torn_body = Bytes::from_static(b"uncommitted torn batch\n");
    append(&torn_spool, 3, torn_body.clone()).await;
    drop(torn_spool);
    let complete_frame =
        fs::read(file_containing(torn_directory.path(), &torn_body)).expect("read complete frame");
    let torn_length = complete_frame.len() - 11;
    OpenOptions::new()
        .append(true)
        .open(&data_path)
        .expect("open data file for torn append")
        .write_all(&complete_frame[..torn_length])
        .expect("append torn frame");

    let recovery = Spool::recover(directory.path(), capacity, maximum_batch)
        .await
        .expect("recover torn tail");
    assert_eq!(recovery.report().committed_batches(), 2);
    assert_eq!(recovery.report().discarded_tail_bytes(), torn_length as u64);
    assert_eq!(
        fs::metadata(&data_path).expect("truncated metadata").len(),
        committed_bytes
    );

    let (_, mut batches, _) = recovery.into_parts();
    assert_eq!(
        batches
            .next_batch()
            .await
            .expect("first batch")
            .expect("first batch")
            .body(),
        &first_body
    );
    assert_eq!(
        batches
            .next_batch()
            .await
            .expect("second batch")
            .expect("second batch")
            .body(),
        &second_body
    );
    assert!(
        batches
            .next_batch()
            .await
            .expect("end of batches")
            .is_none()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_rejects_corruption_without_truncating_committed_data() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let capacity = SpoolCapacity::new(16_384).expect("spool capacity");
    let maximum_batch = AppendBodyLimit::new(1_024).expect("maximum batch");
    let spool = Spool::create_new(directory.path(), capacity)
        .await
        .expect("create spool");
    let body = Bytes::from_static(b"committed body must not be discarded\n");
    append(&spool, 1, body.clone()).await;
    drop(spool);
    let data_path = file_containing(directory.path(), &body);
    let mut corrupted = fs::read(&data_path).expect("read data file");
    let body_offset = find(&corrupted, &body).expect("body offset");
    corrupted[body_offset] ^= 0xff;
    fs::write(&data_path, &corrupted).expect("write corruption");

    let error = Spool::recover(directory.path(), capacity, maximum_batch)
        .await
        .expect_err("committed corruption must fail recovery");

    assert_eq!(error.error_code(), ErrorCode::SpoolCorrupt);
    assert_eq!(
        fs::metadata(&data_path).expect("corrupt metadata").len(),
        corrupted.len() as u64
    );
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_rejects_an_arbitrary_tail_without_truncating_it() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let capacity = SpoolCapacity::new(16_384).expect("spool capacity");
    let maximum_batch = AppendBodyLimit::new(1_024).expect("maximum batch");
    let spool = Spool::create_new(directory.path(), capacity)
        .await
        .expect("create spool");
    let body = Bytes::from_static(b"committed body before invalid tail\n");
    append(&spool, 1, body.clone()).await;
    drop(spool);
    let data_path = file_containing(directory.path(), &body);
    let invalid_tail = b"not a spool frame";
    OpenOptions::new()
        .append(true)
        .open(&data_path)
        .expect("open data file for invalid tail")
        .write_all(invalid_tail)
        .expect("append invalid tail");
    let corrupted_bytes = fs::metadata(&data_path)
        .expect("invalid tail metadata")
        .len();

    let error = Spool::recover(directory.path(), capacity, maximum_batch)
        .await
        .expect_err("an arbitrary tail is corruption, not a torn append");

    assert_eq!(error.error_code(), ErrorCode::SpoolCorrupt);
    assert_eq!(
        fs::metadata(&data_path)
            .expect("unchanged invalid tail metadata")
            .len(),
        corrupted_bytes
    );
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_reports_when_another_maximum_batch_will_not_fit() {
    let directory = tempfile::tempdir().expect("temporary spool directory");
    let capacity = SpoolCapacity::new(4_096).expect("spool capacity");
    let maximum_batch = AppendBodyLimit::new(2_000).expect("maximum batch");
    let spool = Spool::create_new(directory.path(), capacity)
        .await
        .expect("create spool");
    append(&spool, 1, Bytes::from(vec![b'a'; 2_000])).await;
    append(&spool, 2, Bytes::from(vec![b'b'; 1_000])).await;
    drop(spool);

    let recovery = Spool::recover(directory.path(), capacity, maximum_batch)
        .await
        .expect("recover full spool");

    assert_eq!(
        recovery.report().maximum_batch_admission(),
        MaximumBatchAdmission::CapacityExhausted
    );
    let (spool, _, _) = recovery.into_parts();
    let error = spool
        .reserve(maximum_batch)
        .expect_err("maximum batch must remain rejected");
    assert_eq!(error.error_code(), ErrorCode::CapacityExhausted);
}

async fn append(spool: &Spool, sequence: u128, body: Bytes) {
    spool
        .reserve(AppendBodyLimit::new(body.len() as u64).expect("body limit"))
        .expect("reserve append")
        .append(batch_metadata(sequence), body)
        .await
        .expect("append batch");
}

fn batch_metadata(sequence: u128) -> BatchMetadata {
    BatchMetadata::new(
        BatchId::try_from(identity(sequence)).expect("batch identity"),
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

fn identity(sequence: u128) -> Uuid {
    Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 | sequence)
}

fn file_containing(directory: &Path, needle: &[u8]) -> PathBuf {
    fs::read_dir(directory)
        .expect("read spool directory")
        .map(|entry| entry.expect("spool entry").path())
        .find(|path| {
            fs::read(path)
                .map(|bytes| find(&bytes, needle).is_some())
                .unwrap_or(false)
        })
        .expect("file containing bytes")
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|candidate| candidate == needle)
}
