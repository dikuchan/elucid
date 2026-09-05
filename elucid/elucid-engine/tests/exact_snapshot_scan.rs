use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use elucid_core::{CodedError, ErrorCode};

use arrow::array::{
    Array as _, ArrayRef, FixedSizeBinaryArray, Int64Array, StringArray, TimestampMillisecondArray,
};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, TimeZone as _, Utc};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use elucid_catalog::{CatalogManifest, LogicalType, Schema};
use elucid_engine::{
    HistoricalConversionMetrics, MAXIMUM_ENCODED_QUERY_ROW_BYTES, QueryCancellation,
    QueryCompletion, QueryEngine, QueryExecutionLimitConfiguration, QueryExecutionLimits,
    QueryObjectStore, QueryResourceLimitExceeded, QueryTruncationReason, SnapshotTableProvider,
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
use serde_json::{Value, json};
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
        - name: attempts
          logical_type: int64
          nullability: NON_NULL
    - version: 2
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
        - name: attempts
          logical_type: int64
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
            - target_field: attempts
              json_pointer: /attempts
"#;

#[tokio::test]
#[ignore = "requires Docker"]
async fn exact_snapshot_executes_typed_pipelines_and_rejects_runtime_or_object_failures() {
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
    let oversized_message = "x".repeat(
        usize::try_from(MAXIMUM_ENCODED_QUERY_ROW_BYTES).expect("encoded-row limit fits usize"),
    );
    write_upload_and_publish(
        &publication,
        &immutable_store,
        staging.path(),
        &root,
        FixtureIdentity::new(4, 104),
        active_schema,
        current_batch(
            active_schema,
            &oversized_message,
            "oversized",
            timestamp(12, 30),
        ),
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
    let snapshot_store = QuerySnapshotStore::new(pool);
    let snapshot = snapshot_store
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

    let typed_snapshot = snapshot_store
        .select(
            r#"source logs | filter attempts + 1 >= 3 and region != null | project message, adjusted = attempts + 1, original_region = try_cast(rest("region") as utf8), attempts_text = cast(attempts as utf8) | sort -adjusted | take 2"#,
            request_range,
            QuerySnapshotLimits::new(16 * 1024 * 1024).expect("snapshot byte limit"),
        )
        .await
        .expect("select typed pipeline snapshot");
    let query_scratch = TempDir::new().expect("create query scratch directory");
    let engine = QueryEngine::new(
        query_objects.clone(),
        Arc::clone(&metrics),
        query_limits(query_scratch.path()),
    )
    .expect("initialize bounded query engine");
    let result = engine
        .execute(
            &typed_snapshot,
            &QueryCancellation::new(),
            engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect("execute typed pipeline");
    assert_eq!(
        result.rows(),
        vec![
            vec![json!("selected"), json!("5"), Value::Null, json!("4")],
            vec![json!("historical"), json!("3"), json!("eu"), json!("2")],
        ]
    );
    assert_eq!(result.completion(), QueryCompletion::Complete);
    assert!(result.diagnostics().is_empty());
    assert_eq!(result.statistics().selected_segments(), 2);
    assert_eq!(
        result.statistics().selected_parquet_bytes(),
        typed_snapshot.selected_parquet_bytes()
    );
    assert_eq!(result.statistics().output_rows(), 2);
    assert_eq!(
        result.statistics().output_bytes(),
        u64::try_from(
            serde_json::to_vec(result.rows())
                .expect("encode expected rows")
                .len()
        )
        .expect("encoded rows fit u64")
    );

    let aggregate_snapshot = snapshot_store
        .select(
            "source logs | summarize events = count(), known = count(region), total = sum(attempts), mean = avg(attempts), first = min(message), last = max(message) by region | sort region",
            request_range,
            QuerySnapshotLimits::new(16 * 1024 * 1024).expect("snapshot byte limit"),
        )
        .await
        .expect("select aggregate pipeline snapshot");
    let result = engine
        .execute(
            &aggregate_snapshot,
            &QueryCancellation::new(),
            engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect("execute aggregate pipeline");
    assert_eq!(
        result.rows(),
        vec![
            vec![
                json!("eu"),
                json!("1"),
                json!("1"),
                json!("2"),
                json!(2.0),
                json!("historical"),
                json!("historical")
            ],
            vec![
                json!("us"),
                json!("1"),
                json!("1"),
                json!("4"),
                json!(4.0),
                json!("selected"),
                json!("selected")
            ],
            vec![
                Value::Null,
                json!("1"),
                json!("0"),
                json!("3"),
                json!(3.0),
                json!("invalid"),
                json!("invalid")
            ],
        ]
    );

    let cast_failure_snapshot = snapshot_store
        .select(
            r#"source logs | filter message == "invalid" | project region = cast(rest("region") as utf8)"#,
            request_range,
            QuerySnapshotLimits::new(16 * 1024 * 1024).expect("snapshot byte limit"),
        )
        .await
        .expect("select strict-cast failure snapshot");
    let error = engine
        .execute(
            &cast_failure_snapshot,
            &QueryCancellation::new(),
            engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect_err("a JSON number cannot be strictly cast to UTF-8");
    assert_eq!(error.error_code(), ErrorCode::QueryCastFailed);

    let overflow_range = QueryRequestTimeRange::new(timestamp(9, 30), timestamp(10, 30))
        .expect("ordered overflow range");
    let overflow_snapshot = snapshot_store
        .select(
            "source logs | project overflow = attempts + 1",
            overflow_range,
            QuerySnapshotLimits::new(16 * 1024 * 1024).expect("snapshot byte limit"),
        )
        .await
        .expect("select arithmetic-overflow snapshot");
    let error = engine
        .execute(
            &overflow_snapshot,
            &QueryCancellation::new(),
            engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect_err("integer overflow must terminate execution");
    assert_eq!(error.error_code(), ErrorCode::QueryEvaluationFailed);

    let sum_overflow_range = QueryRequestTimeRange::new(timestamp(9, 30), timestamp(11, 15))
        .expect("ordered sum-overflow range");
    let sum_overflow_snapshot = snapshot_store
        .select(
            "source logs | summarize total = sum(attempts)",
            sum_overflow_range,
            QuerySnapshotLimits::new(16 * 1024 * 1024).expect("snapshot byte limit"),
        )
        .await
        .expect("select sum-overflow snapshot");
    let error = engine
        .execute(
            &sum_overflow_snapshot,
            &QueryCancellation::new(),
            engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect_err("integer sum overflow must terminate execution");
    assert_eq!(error.error_code(), ErrorCode::QueryEvaluationFailed);

    let empty_range =
        QueryRequestTimeRange::new(timestamp(8, 0), timestamp(9, 0)).expect("ordered empty range");
    let empty_snapshot = snapshot_store
        .select(
            "source logs | summarize events = count(), total = sum(attempts), mean = avg(attempts)",
            empty_range,
            QuerySnapshotLimits::new(16 * 1024 * 1024).expect("snapshot byte limit"),
        )
        .await
        .expect("select empty aggregate snapshot");
    let result = engine
        .execute(
            &empty_snapshot,
            &QueryCancellation::new(),
            engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect("execute empty aggregate pipeline");
    assert_eq!(
        result.rows(),
        vec![vec![json!("0"), Value::Null, Value::Null]]
    );

    let encoded_snapshot = snapshot_store
        .select(
            "source logs | project @event_time, @event_id, attempts, @rest | sort @event_time",
            request_range,
            QuerySnapshotLimits::new(16 * 1024 * 1024).expect("snapshot byte limit"),
        )
        .await
        .expect("select typed-result snapshot");
    let encoded = engine
        .execute(
            &encoded_snapshot,
            &QueryCancellation::new(),
            engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect("encode typed query result");
    assert_eq!(
        encoded
            .columns()
            .iter()
            .map(|column| (column.name(), column.logical_type()))
            .collect::<Vec<_>>(),
        vec![
            ("@event_time", LogicalType::Datetime),
            ("@event_id", LogicalType::Eid),
            ("attempts", LogicalType::Int64),
            ("@rest", LogicalType::Json),
        ]
    );
    assert_eq!(
        encoded.rows(),
        vec![
            vec![
                json!("2026-08-20T11:00:00.000Z"),
                json!("00000000000000000000000000000002"),
                json!("2"),
                json!({"region": "eu"})
            ],
            vec![
                json!("2026-08-20T11:30:00.000Z"),
                json!("00000000000000000000000000000003"),
                json!("3"),
                json!({"region": 42})
            ],
            vec![
                json!("2026-08-20T11:45:00.000Z"),
                json!("0000000000000000000000000000000a"),
                json!("4"),
                Value::Null
            ],
        ]
    );

    let eid_snapshot = snapshot_store
        .select(
            r#"source logs | project literal = eid("0123456789abcdef1032547698badcfe"), parsed = cast("0123456789abcdef1032547698badcfe" as eid), text = cast(eid("0123456789abcdef1032547698badcfe") as utf8), document = cast(eid("0123456789abcdef1032547698badcfe") as json), invalid = try_cast("0123456789ABCDEF1032547698BADCFE" as eid) | take 1"#,
            request_range,
            QuerySnapshotLimits::new(16 * 1024 * 1024).expect("snapshot byte limit"),
        )
        .await
        .expect("select event-ID conversion snapshot");
    let eid_result = engine
        .execute(
            &eid_snapshot,
            &QueryCancellation::new(),
            engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect("execute event-ID conversions");
    assert_eq!(
        eid_result.rows(),
        vec![vec![
            json!("0123456789abcdef1032547698badcfe"),
            json!("0123456789abcdef1032547698badcfe"),
            json!("0123456789abcdef1032547698badcfe"),
            json!("0123456789abcdef1032547698badcfe"),
            Value::Null,
        ]]
    );

    let requested_output_rows = engine
        .limits()
        .output_row_limit(1)
        .expect("requested output row limit");
    let truncated = engine
        .execute(
            &encoded_snapshot,
            &QueryCancellation::new(),
            requested_output_rows,
        )
        .await
        .expect("truncate query by row count");
    assert_eq!(truncated.rows().len(), 1);
    assert_eq!(
        truncated.completion(),
        QueryCompletion::Truncated {
            reason: QueryTruncationReason::OutputRows,
        }
    );

    let mut byte_limit_configuration = query_limit_configuration(query_scratch.path());
    byte_limit_configuration.maximum_result_bytes = 2;
    let byte_limited_engine = QueryEngine::new(
        query_objects.clone(),
        Arc::clone(&metrics),
        QueryExecutionLimits::new(byte_limit_configuration).expect("byte limits"),
    )
    .expect("initialize byte-limited query engine");
    let truncated = byte_limited_engine
        .execute(
            &encoded_snapshot,
            &QueryCancellation::new(),
            byte_limited_engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect("truncate query by encoded bytes");
    assert!(truncated.rows().is_empty());
    assert_eq!(truncated.statistics().output_bytes(), 2);
    assert_eq!(
        truncated.completion(),
        QueryCompletion::Truncated {
            reason: QueryTruncationReason::OutputBytes,
        }
    );

    let mut scan_limit_configuration = query_limit_configuration(query_scratch.path());
    scan_limit_configuration.maximum_scan_bytes = 1;
    let scan_limited_engine = QueryEngine::new(
        query_objects.clone(),
        Arc::clone(&metrics),
        QueryExecutionLimits::new(scan_limit_configuration).expect("scan limits"),
    )
    .expect("initialize scan-limited query engine");
    let error = scan_limited_engine
        .execute(
            &encoded_snapshot,
            &QueryCancellation::new(),
            scan_limited_engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect_err("selected bytes must be checked again before execution");
    assert_eq!(error.error_code(), ErrorCode::QueryResourceLimitExceeded);
    assert_eq!(
        error.resource_limit_exceeded(),
        Some(QueryResourceLimitExceeded::ScanBytes { maximum: 1 })
    );

    let cancellation = QueryCancellation::new();
    cancellation.cancel();
    let error = engine
        .execute(
            &encoded_snapshot,
            &cancellation,
            engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect_err("pre-cancelled query must not begin execution");
    assert_eq!(error.error_code(), ErrorCode::QueryCancelled);

    let mut timeout_configuration = query_limit_configuration(query_scratch.path());
    timeout_configuration.timeout = Duration::from_nanos(1);
    let timeout_engine = QueryEngine::new(
        query_objects.clone(),
        Arc::clone(&metrics),
        QueryExecutionLimits::new(timeout_configuration).expect("timeout limits"),
    )
    .expect("initialize timeout-limited query engine");
    let error = timeout_engine
        .execute(
            &encoded_snapshot,
            &QueryCancellation::new(),
            timeout_engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect_err("query execution must honor its deadline");
    assert_eq!(error.error_code(), ErrorCode::QueryTimeout);

    let oversized_range = QueryRequestTimeRange::new(timestamp(12, 15), timestamp(12, 45))
        .expect("ordered oversized-row range");
    let oversized_snapshot = snapshot_store
        .select(
            "source logs | project message",
            oversized_range,
            QuerySnapshotLimits::new(16 * 1024 * 1024).expect("snapshot byte limit"),
        )
        .await
        .expect("select oversized-row snapshot");
    let error = engine
        .execute(
            &oversized_snapshot,
            &QueryCancellation::new(),
            engine.limits().maximum_output_row_limit(),
        )
        .await
        .expect_err("one encoded row must not exceed the reported implementation limit");
    assert_eq!(error.error_code(), ErrorCode::QueryResourceLimitExceeded);
    assert_eq!(
        error.resource_limit_exceeded(),
        Some(QueryResourceLimitExceeded::EncodedRowBytes {
            maximum: MAXIMUM_ENCODED_QUERY_ROW_BYTES,
        })
    );

    raw_store
        .delete(&ObjectPath::from(old.descriptor.key().as_str()))
        .await
        .expect("delete selected object");
    let error = SnapshotTableProvider::open(&snapshot, query_objects.clone(), Arc::clone(&metrics))
        .await
        .expect_err("missing selected object must fail the complete snapshot");
    assert_eq!(error.error_code(), ErrorCode::PublishedObjectMissing);

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
    assert_eq!(error.error_code(), ErrorCode::PublishedObjectCorrupt);
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
        Arc::new(Int64Array::from(vec![i64::MAX, 2, 3])),
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
        Arc::new(Int64Array::from(vec![4])),
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

fn query_limits(scratch_path: &Path) -> QueryExecutionLimits {
    QueryExecutionLimits::new(query_limit_configuration(scratch_path)).expect("query limits")
}

fn query_limit_configuration(scratch_path: &Path) -> QueryExecutionLimitConfiguration {
    QueryExecutionLimitConfiguration {
        timeout: Duration::from_secs(30),
        maximum_scan_bytes: 16 * 1024 * 1024,
        memory_bytes: 64 * 1024 * 1024,
        scratch_path: scratch_path.to_path_buf(),
        scratch_capacity_bytes: 64 * 1024 * 1024,
        maximum_result_rows: 1_000,
        maximum_result_bytes: 16 * 1024 * 1024,
    }
}
