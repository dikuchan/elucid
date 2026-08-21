use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array as _, ArrayRef, FixedSizeBinaryArray, StringArray, TimestampMillisecondArray,
};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, TimeZone as _, Utc};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use elucid_catalog::{CatalogManifest, LogicalType, Schema};
use elucid_engine::{
    EngineErrorCode, HistoricalConversionMetrics, QueryObjectStore, SnapshotTableProvider,
};
use elucid_metastore::{
    CatalogApplyOutcome, CatalogStore, IngestionSegmentRegistration, IngestionSegmentTimes,
    PublicationStore, QueryRequestTimeRange, QuerySnapshotLimits, QuerySnapshotStore,
    RetentionPeriod, install,
};
use elucid_storage::{
    ImmutableObjectStore, ManagedObjectKey, ManagedRoot, ObjectDescriptor, ParquetSegmentInput,
    ParquetWriteLimit, SegmentId, StoredObjectId, TransferLimit, write_parquet_segment,
};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use sqlx::postgres::PgPoolOptions;
use tempfile::TempDir;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ImageExt as _, runners::AsyncRunner as _};
use uuid::Uuid;

const CATALOG: &str = r#"
format_version: 1
source:
  name: logs
  display_name: Application logs
  active_schema_version: 2
  schemas:
    - version: 1
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
    - version: 2
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
        - name: region
          logical_type: utf8
          nullability: NULLABLE
          historical_remainder_pointer: /region
  inputs:
    - name: vector
      active_ingestion_profile_revision: 1
      ingestion_profile_revisions:
        - revision: 1
          target_schema_version: 1
          maximum_record_bytes: 1048576
          event_time:
            json_pointer: /timestamp
            format: RFC3339
          mappings:
            - target_field: message
              json_pointer: /message
"#;

#[tokio::test]
#[ignore = "requires Docker"]
async fn exact_snapshot_scan_adapts_history_and_rejects_object_drift() {
    let container = Postgres::default()
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("start PostgreSQL container");
    let host = container.get_host().await.expect("PostgreSQL host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("PostgreSQL port");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&format!(
            "postgresql://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("connect to PostgreSQL");
    install(&pool).await.expect("install metastore");

    let catalog_store = CatalogStore::load(pool.clone())
        .await
        .expect("load empty catalog");
    let manifest = CatalogManifest::decode(CATALOG.as_bytes()).expect("decode catalog fixture");
    let source = match catalog_store
        .apply(&manifest)
        .await
        .expect("apply catalog fixture")
    {
        CatalogApplyOutcome::Applied { source } => source,
        CatalogApplyOutcome::Unchanged { .. } => panic!("new catalog unexpectedly existed"),
        _ => panic!("unknown catalog application outcome"),
    };
    let stored_schema = &source.schemas()[0];
    let active_schema = &source.schemas()[1];

    let raw_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let immutable_store = ImmutableObjectStore::new(Arc::clone(&raw_store));
    let publication = PublicationStore::new(pool.clone());
    let staging = TempDir::new().expect("create staging directory");
    let root = ManagedRoot::parse("exact-snapshot-test").expect("managed root");

    let old = write_upload_and_publish(
        &publication,
        &immutable_store,
        staging.path(),
        &root,
        FixtureIdentity::new(1, 101),
        stored_schema,
        old_batch(stored_schema),
    )
    .await;
    let current = write_upload_and_publish(
        &publication,
        &immutable_store,
        staging.path(),
        &root,
        FixtureIdentity::new(2, 102),
        active_schema,
        current_batch(active_schema, "selected", "us", timestamp(11, 45)),
    )
    .await;
    let stray = write_upload(
        &immutable_store,
        staging.path(),
        &root,
        FixtureIdentity::new(3, 103),
        active_schema,
        current_batch(active_schema, "stray", "stray", timestamp(11, 50)),
    )
    .await;
    assert_ne!(stray.descriptor, current.descriptor);

    let request_range = QueryRequestTimeRange::new(timestamp(10, 30), timestamp(12, 0))
        .expect("ordered request range");
    let snapshot = QuerySnapshotStore::new(pool)
        .select(
            "source logs",
            request_range,
            QuerySnapshotLimits::new(16 * 1024 * 1024).expect("snapshot byte limit"),
        )
        .await
        .expect("select exact query snapshot");
    assert_eq!(snapshot.segments().len(), 2);

    let query_objects = QueryObjectStore::new(
        ObjectStoreUrl::parse("memory://elucid").expect("object-store URL"),
        Arc::clone(&raw_store),
    );
    let metrics = Arc::new(HistoricalConversionMetrics::default());
    let provider =
        SnapshotTableProvider::open(&snapshot, query_objects.clone(), Arc::clone(&metrics))
            .await
            .expect("open validated snapshot table");

    let context = SessionContext::new();
    context
        .register_table("logs", Arc::new(provider))
        .expect("register snapshot table");
    let batches = context
        .sql("SELECT message, region FROM logs")
        .await
        .expect("plan provider scan")
        .collect()
        .await
        .expect("execute provider scan");
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let messages = batch
                .column_by_name("message")
                .expect("message column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("UTF-8 messages");
            let regions = batch
                .column_by_name("region")
                .expect("region column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("UTF-8 regions");
            (0..batch.num_rows())
                .map(|index| {
                    (
                        messages.value(index).to_owned(),
                        (!regions.is_null(index)).then(|| regions.value(index).to_owned()),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("historical".to_owned(), Some("eu".to_owned())),
            ("invalid".to_owned(), None),
            ("selected".to_owned(), Some("us".to_owned())),
        ]
    );
    assert_eq!(metrics.failures(LogicalType::Utf8), 1);

    raw_store
        .delete(&ObjectPath::from(old.descriptor.key().as_str()))
        .await
        .expect("delete selected object");
    let error = SnapshotTableProvider::open(&snapshot, query_objects.clone(), Arc::clone(&metrics))
        .await
        .expect_err("missing selected object must fail the complete snapshot");
    assert_eq!(error.code(), EngineErrorCode::PublishedObjectMissing);

    immutable_store
        .upload(
            &old.descriptor,
            old.bytes.clone(),
            transfer_limit(&old.bytes),
        )
        .await
        .expect("restore selected object");
    raw_store
        .put(
            &ObjectPath::from(current.descriptor.key().as_str()),
            PutPayload::from_bytes(Bytes::from_static(b"not parquet")),
        )
        .await
        .expect("replace selected object with corrupt bytes");
    let error = SnapshotTableProvider::open(&snapshot, query_objects, metrics)
        .await
        .expect_err("changed selected object must fail the complete snapshot");
    assert_eq!(error.code(), EngineErrorCode::PublishedObjectCorrupt);
}

#[derive(Debug)]
struct UploadedSegment {
    descriptor: ObjectDescriptor,
    bytes: Bytes,
}

#[derive(Clone, Copy, Debug)]
struct FixtureIdentity {
    segment_sequence: u64,
    object_sequence: u64,
}

impl FixtureIdentity {
    const fn new(segment_sequence: u64, object_sequence: u64) -> Self {
        Self {
            segment_sequence,
            object_sequence,
        }
    }
}

async fn write_upload_and_publish(
    publication: &PublicationStore,
    store: &ImmutableObjectStore,
    staging_root: &std::path::Path,
    root: &ManagedRoot,
    identity: FixtureIdentity,
    schema: &Schema,
    batch: RecordBatch,
) -> UploadedSegment {
    let row_count = batch.num_rows();
    let minimum_event_time = event_time(&batch, 0);
    let maximum_event_time = event_time(&batch, row_count - 1);
    let uploaded = write_upload(store, staging_root, root, identity, schema, batch).await;
    let segment_id = SegmentId::from(uuid(identity.segment_sequence));
    let times = IngestionSegmentTimes::new(
        minimum_event_time.date_naive(),
        minimum_event_time,
        maximum_event_time,
        maximum_event_time,
        maximum_event_time,
    )
    .expect("valid segment times");
    let registration = IngestionSegmentRegistration::new(
        segment_id,
        schema.source_id(),
        schema.id(),
        times,
        NonZeroU64::new(u64::try_from(row_count).expect("row count fits u64"))
            .expect("positive row count"),
        NonZeroU64::new(1).expect("positive uncompressed byte count"),
        uploaded.descriptor.clone(),
    )
    .expect("valid segment registration");
    publication
        .register_ingestion_segment(&registration)
        .await
        .expect("register segment");
    publication
        .record_verified_upload(&uploaded.descriptor)
        .await
        .expect("record verified upload");
    publication
        .publish_ingestion_segment(segment_id, RetentionPeriod::new(3_600).expect("retention"))
        .await
        .expect("publish segment");
    uploaded
}

async fn write_upload(
    store: &ImmutableObjectStore,
    staging_root: &std::path::Path,
    root: &ManagedRoot,
    identity: FixtureIdentity,
    schema: &Schema,
    batch: RecordBatch,
) -> UploadedSegment {
    let key = ManagedObjectKey::parquet(
        root,
        SegmentId::from(uuid(identity.segment_sequence)),
        StoredObjectId::from(uuid(identity.object_sequence)),
    );
    let input = ParquetSegmentInput::new(key, schema, batch).expect("valid Parquet input");
    let staged = write_parquet_segment(
        staging_root,
        input,
        ParquetWriteLimit::new(16 * 1024 * 1024).expect("Parquet write limit"),
    )
    .await
    .expect("write Parquet segment");
    let descriptor = staged.object_descriptor().clone();
    let bytes = Bytes::from(std::fs::read(staged.path()).expect("read staged Parquet"));
    store
        .upload(&descriptor, bytes.clone(), transfer_limit(&bytes))
        .await
        .expect("upload exact Parquet object");
    UploadedSegment { descriptor, bytes }
}

fn old_batch(schema: &Schema) -> RecordBatch {
    let times = [timestamp(10, 0), timestamp(11, 0), timestamp(11, 30)];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(
            TimestampMillisecondArray::from(times.map(|value| value.timestamp_millis()).to_vec())
                .with_timezone("UTC"),
        ),
        Arc::new(
            TimestampMillisecondArray::from(vec![timestamp(12, 0).timestamp_millis(); 3])
                .with_timezone("UTC"),
        ),
        Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                [1_u128, 2, 3].iter().map(|value| value.to_be_bytes()),
            )
            .expect("fixed-size event identities"),
        ),
        Arc::new(StringArray::from(vec![
            "outside-range",
            "historical",
            "invalid",
        ])),
        Arc::new(StringArray::from(vec![
            Some(r#"{"region":"outside"}"#),
            Some(r#"{"region":"eu"}"#),
            Some(r#"{"region":42}"#),
        ])),
    ];
    RecordBatch::try_new(Arc::new(schema.arrow_schema().clone()), arrays)
        .expect("old-schema record batch")
}

fn current_batch(schema: &Schema, message: &str, region: &str, time: DateTime<Utc>) -> RecordBatch {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(
            TimestampMillisecondArray::from(vec![time.timestamp_millis()]).with_timezone("UTC"),
        ),
        Arc::new(
            TimestampMillisecondArray::from(vec![timestamp(12, 0).timestamp_millis()])
                .with_timezone("UTC"),
        ),
        Arc::new(
            FixedSizeBinaryArray::try_from_iter([10_u128.to_be_bytes()].iter())
                .expect("fixed-size event identity"),
        ),
        Arc::new(StringArray::from(vec![message])),
        Arc::new(StringArray::from(vec![region])),
        Arc::new(StringArray::from(vec![None::<&str>])),
    ];
    RecordBatch::try_new(Arc::new(schema.arrow_schema().clone()), arrays)
        .expect("active-schema record batch")
}

fn event_time(batch: &RecordBatch, index: usize) -> DateTime<Utc> {
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("event-time column");
    DateTime::from_timestamp_millis(column.value(index)).expect("valid event timestamp")
}

fn transfer_limit(bytes: &Bytes) -> TransferLimit {
    TransferLimit::new(u64::try_from(bytes.len()).expect("object size fits u64"))
        .expect("positive object transfer limit")
}

fn timestamp(hour: u32, minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 20, hour, minute, 0)
        .single()
        .expect("fixture timestamp")
}

fn uuid(sequence: u64) -> Uuid {
    Uuid::from_u128(0x019d_0000_0000_7000_8000_0000_0000_0000 | u128::from(sequence))
}
