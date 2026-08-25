use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array as _, ArrayRef, FixedSizeBinaryArray, StringArray, TimestampMillisecondArray,
};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, TimeZone as _, Utc};
use elucid_catalog::{CatalogManifest, Schema};
use elucid_compaction::{
    CompactionBuildLimitConfiguration, CompactionBuildLimits, CompactionWorker,
};
use elucid_metastore::{
    CatalogApplyOutcome, CatalogStore, CompactionClaimLimitConfiguration, CompactionClaimLimits,
    CompactionStore, IngestionSegmentRegistration, IngestionSegmentTimes, MaintenanceOwnership,
    PublicationStore, RetentionPeriod, install,
};
use elucid_storage::{
    ImmutableObjectStore, ManagedObjectKey, ManagedRoot, ObjectByteSize, ObjectDescriptor,
    ObjectDigest, ObjectFormatVersion, ObjectMediaType, ParquetSegmentInput, ParquetWriteLimit,
    SegmentId, StoredObjectId, TransferLimit, write_parquet_segment,
};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlx::FromRow;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tempfile::TempDir;
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::GenericImage;
use testcontainers_modules::testcontainers::core::{ImageExt as _, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner as _;
use uuid::Uuid;

const BUCKET: &str = "elucid-compaction-test";
const MINIO_CLIENT_TAG: &str = "RELEASE.2025-02-21T16-00-46Z";
const CATALOG: &str = r#"
format_version: 1
source:
  name: logs
  display_name: Application logs
  active_schema_version: 1
  schemas:
    - version: 1
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
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
async fn bounded_streaming_merge_preserves_interleaved_rows_and_registers_uploaded_outputs() {
    let test_identity = Uuid::now_v7().simple().to_string();
    let network = format!("elucid-compaction-{test_identity}");
    let server_name = format!("elucid-compaction-minio-{test_identity}");
    let minio = MinIO::default()
        .with_network(network.clone())
        .with_container_name(server_name.clone())
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("start MinIO");
    let minio_alias = format!("http://minioadmin:minioadmin@{server_name}:9000");
    let bucket_path = format!("local/{BUCKET}");
    let _bucket = GenericImage::new("minio/mc", MINIO_CLIENT_TAG)
        .with_wait_for(WaitFor::message_on_stdout("Bucket created successfully"))
        .with_network(network)
        .with_env_var("MC_HOST_local", minio_alias)
        .with_cmd(["mb", bucket_path.as_str()])
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("create MinIO bucket");
    let minio_host = minio.get_host().await.expect("MinIO host");
    let minio_port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("MinIO API port");
    let raw_objects = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://{minio_host}:{minio_port}"))
            .with_bucket_name(BUCKET)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .expect("build S3 client"),
    ) as Arc<dyn ObjectStore>;
    let objects = ImmutableObjectStore::new(Arc::clone(&raw_objects));

    let postgres = Postgres::default()
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("start PostgreSQL container");
    let postgres_host = postgres.get_host().await.expect("PostgreSQL host");
    let postgres_port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("PostgreSQL port");
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&format!(
            "postgresql://postgres:postgres@{postgres_host}:{postgres_port}/postgres"
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
    let schema = source.active_schema();
    let root = ManagedRoot::parse("compaction-test").expect("managed root");
    let staging = TempDir::new().expect("create staging directory");
    let publication = PublicationStore::new(pool.clone());
    for (index, seconds) in [[0, 3, 6], [1, 4, 7], [2, 5, 8]].into_iter().enumerate() {
        write_upload_and_publish(
            &publication,
            &objects,
            staging.path(),
            &root,
            schema,
            u64::try_from(index + 1).expect("segment sequence"),
            seconds,
        )
        .await;
    }

    let compaction = CompactionStore::new(pool.clone());
    let mut owner = match compaction
        .try_acquire_maintenance()
        .await
        .expect("acquire maintenance owner")
    {
        MaintenanceOwnership::Acquired(owner) => owner,
        MaintenanceOwnership::HeldElsewhere => panic!("maintenance owner already exists"),
        _ => panic!("unknown maintenance ownership outcome"),
    };
    let claim_limits = CompactionClaimLimits::new(CompactionClaimLimitConfiguration {
        maximum_candidate_segments: 100,
        maximum_input_segments: 4,
        maximum_input_rows: 12,
        maximum_input_parquet_bytes: 16 * 1024 * 1024,
        maximum_input_uncompressed_bytes: 120,
        target_output_rows: 5,
        target_output_uncompressed_bytes: 100,
        minimum_retention: Duration::from_secs(60),
    })
    .expect("valid claim limits");
    let claim = owner
        .claim(&claim_limits)
        .await
        .expect("claim compaction")
        .expect("beneficial compaction claim");
    drop(owner);

    let build_limits = CompactionBuildLimits::new(CompactionBuildLimitConfiguration {
        maximum_input_segments: 4,
        maximum_input_rows: 12,
        maximum_input_parquet_bytes: 16 * 1024 * 1024,
        maximum_input_uncompressed_bytes: 120,
        reader_batch_rows: 2,
        target_output_rows: 5,
        target_output_uncompressed_bytes: 100,
        maximum_output_parquet_bytes: 16 * 1024 * 1024,
        maximum_staging_bytes: 32 * 1024 * 1024,
        maximum_duration: Duration::from_secs(30),
    })
    .expect("valid build limits");
    let worker = CompactionWorker::new(
        compaction,
        publication,
        Arc::clone(&raw_objects),
        root,
        staging.path(),
        build_limits,
    );
    let result = worker
        .build_register_and_upload(&claim)
        .await
        .expect("build and upload compaction outputs");

    assert_eq!(result.run_id(), claim.run_id());
    assert_eq!(result.input_segments(), 3);
    assert_eq!(result.output_segments(), 2);
    assert_eq!(result.rows(), 9);
    assert!(result.output_parquet_bytes() > 0);
    assert_eq!(
        run_state(&pool, claim.run_id().as_uuid()).await,
        "UPLOADING"
    );
    assert_eq!(active_input_count(&pool, claim.run_id().as_uuid()).await, 3);

    let output_objects = load_output_objects(&pool, claim.run_id().as_uuid()).await;
    assert_eq!(output_objects.len(), 2);
    assert!(
        output_objects
            .iter()
            .all(|output| output.segment_state == "PREPARED" && output.object_state == "UPLOADED")
    );
    let rows = read_output_rows(&objects, &output_objects).await;
    assert_eq!(
        rows,
        (0..9)
            .map(|second| {
                (
                    timestamp(second).timestamp_millis(),
                    (u128::try_from(second).expect("event identity") + 100).to_be_bytes(),
                    format!("event-{second}"),
                )
            })
            .collect::<Vec<_>>()
    );
}

async fn write_upload_and_publish(
    publication: &PublicationStore,
    objects: &ImmutableObjectStore,
    staging_root: &std::path::Path,
    root: &ManagedRoot,
    schema: &Schema,
    sequence: u64,
    seconds: [i64; 3],
) {
    let segment_id = SegmentId::from(Uuid::from_u128(u128::from(sequence)));
    let key = ManagedObjectKey::parquet(
        root,
        segment_id,
        StoredObjectId::from(Uuid::from_u128(u128::from(sequence + 100))),
    );
    let batch = event_batch(schema, seconds);
    let input = ParquetSegmentInput::new(key, schema, batch).expect("valid Parquet input");
    let staged = write_parquet_segment(
        staging_root,
        input,
        ParquetWriteLimit::new(16 * 1024 * 1024).expect("Parquet write limit"),
    )
    .await
    .expect("write input Parquet");
    let descriptor = staged.object_descriptor().clone();
    let bytes = Bytes::from(
        tokio::fs::read(staged.path())
            .await
            .expect("read input Parquet"),
    );
    objects
        .upload(
            &descriptor,
            bytes,
            TransferLimit::new(descriptor.expected_byte_size().get()).expect("transfer limit"),
        )
        .await
        .expect("upload input Parquet");
    let times = IngestionSegmentTimes::new(
        timestamp(seconds[0]).date_naive(),
        timestamp(seconds[0]),
        timestamp(seconds[2]),
        timestamp(seconds[0] + 100),
        timestamp(seconds[2] + 100),
    )
    .expect("input segment bounds");
    let registration = IngestionSegmentRegistration::new(
        segment_id,
        schema.source_id(),
        schema.id(),
        times,
        NonZeroU64::new(3).expect("row count"),
        NonZeroU64::new(30).expect("uncompressed bytes"),
        descriptor.clone(),
    )
    .expect("input registration");
    publication
        .register_ingestion_segment(&registration)
        .await
        .expect("register input segment");
    publication
        .record_verified_upload(&descriptor)
        .await
        .expect("record input upload");
    publication
        .publish_ingestion_segment(segment_id, RetentionPeriod::new(3_600).expect("retention"))
        .await
        .expect("publish input segment");
}

fn event_batch(schema: &Schema, seconds: [i64; 3]) -> RecordBatch {
    let event_times = seconds
        .map(|second| timestamp(second).timestamp_millis())
        .to_vec();
    let ingestion_times = seconds
        .map(|second| timestamp(second + 100).timestamp_millis())
        .to_vec();
    let event_ids = seconds
        .map(|second| (u128::try_from(second).expect("event identity") + 100).to_be_bytes())
        .to_vec();
    let messages = seconds.map(|second| format!("event-{second}")).to_vec();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampMillisecondArray::from(event_times).with_timezone("UTC")),
        Arc::new(TimestampMillisecondArray::from(ingestion_times).with_timezone("UTC")),
        Arc::new(FixedSizeBinaryArray::try_from_iter(event_ids.iter()).expect("event identities")),
        Arc::new(StringArray::from(messages)),
        Arc::new(StringArray::from(vec![None::<&str>; 3])),
    ];
    RecordBatch::try_new(Arc::new(schema.arrow_schema().clone()), arrays).expect("record batch")
}

#[derive(Debug, FromRow)]
struct OutputObjectRow {
    segment_id: Uuid,
    object_id: Uuid,
    object_key: String,
    expected_byte_size: i64,
    blake3_digest: Vec<u8>,
    media_type: String,
    format_version: i64,
    segment_state: String,
    object_state: String,
}

async fn load_output_objects(pool: &PgPool, run_id: Uuid) -> Vec<OutputObjectRow> {
    sqlx::query_as(
        r#"
        SELECT
            segment.segment_id,
            object.object_id,
            object.object_key,
            object.expected_byte_size,
            object.blake3_digest,
            object.media_type,
            object.format_version,
            segment.state AS segment_state,
            object.state AS object_state
        FROM segments AS segment
        JOIN stored_objects AS object USING (segment_id)
        WHERE segment.produced_by_compaction_run_id = $1
        ORDER BY segment.minimum_event_time, segment.segment_id
        "#,
    )
    .bind(run_id)
    .fetch_all(pool)
    .await
    .expect("load output objects")
}

async fn read_output_rows(
    objects: &ImmutableObjectStore,
    outputs: &[OutputObjectRow],
) -> Vec<(i64, [u8; 16], String)> {
    let mut rows = Vec::new();
    for output in outputs {
        let segment_id = SegmentId::from(output.segment_id);
        let object_id = StoredObjectId::from(output.object_id);
        let key = ManagedObjectKey::parse_parquet(&output.object_key, segment_id, object_id)
            .expect("parse output key");
        assert_eq!(output.media_type, ObjectMediaType::ParquetData.as_str());
        let digest = output
            .blake3_digest
            .as_slice()
            .try_into()
            .map(ObjectDigest::new)
            .expect("output digest length");
        let descriptor = ObjectDescriptor::new(
            key,
            ObjectByteSize::new(u64::try_from(output.expected_byte_size).expect("output size")),
            digest,
            ObjectMediaType::ParquetData,
            ObjectFormatVersion::new(u64::try_from(output.format_version).expect("format version"))
                .expect("positive format version"),
        )
        .expect("output descriptor");
        let bytes = objects
            .read_exact(
                &descriptor,
                TransferLimit::new(descriptor.expected_byte_size().get()).expect("read limit"),
            )
            .await
            .expect("read exact output");
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("open output Parquet")
            .with_batch_size(2)
            .build()
            .expect("build output reader");
        for batch in reader {
            let batch = batch.expect("read output batch");
            let event_times = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("event times");
            let event_ids = batch
                .column(2)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("event identities");
            let messages = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("messages");
            for index in 0..batch.num_rows() {
                rows.push((
                    event_times.value(index),
                    event_ids
                        .value(index)
                        .try_into()
                        .expect("16-byte event identity"),
                    messages.value(index).to_owned(),
                ));
            }
        }
    }
    rows
}

async fn run_state(pool: &PgPool, run_id: Uuid) -> String {
    sqlx::query_scalar("SELECT state FROM compaction_runs WHERE compaction_run_id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await
        .expect("load compaction run state")
}

async fn active_input_count(pool: &PgPool, run_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM segments WHERE claimed_by_compaction_run_id = $1 AND state = 'ACTIVE'",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .expect("count active inputs")
}

fn timestamp(second: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 20, 10, 0, 0)
        .single()
        .expect("fixture date")
        + chrono::TimeDelta::seconds(second)
}
