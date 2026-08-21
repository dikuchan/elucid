use std::num::NonZeroU64;
use std::time::Duration;

use chrono::{DateTime, TimeZone as _, Utc};
use elucid_catalog::CatalogManifest;
use elucid_language::DiagnosticCode;
use elucid_metastore::{
    CatalogApplyOutcome, CatalogStore, IngestionSegmentRegistration, IngestionSegmentTimes,
    PublicationStore, QueryRequestTimeRange, QuerySnapshotErrorKind, QuerySnapshotLimitExceeded,
    QuerySnapshotLimits, QuerySnapshotStore, RetentionPeriod, install,
};
use elucid_storage::{
    ManagedObjectKey, ManagedRoot, ObjectByteSize, ObjectDescriptor, ObjectDigest,
    ObjectFormatVersion, ObjectMediaType, SegmentId, StoredObjectId,
};
use sqlx::postgres::PgPoolOptions;
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
async fn query_snapshot_is_bounded_exact_and_schema_complete() {
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

    let root = ManagedRoot::parse("query-snapshot-test").expect("managed root");
    let publication = PublicationStore::new(pool.clone());
    let first = publish_segment(
        &publication,
        &root,
        PublishedSegment {
            segment_sequence: 1,
            object_sequence: 101,
            source_id: source.id(),
            schema_id: stored_schema.id(),
            minimum_event_time: timestamp(2026, 8, 20, 9, 59),
            maximum_event_time: timestamp(2026, 8, 20, 10, 0),
            bytes: b"first parquet",
        },
    )
    .await;
    let second = publish_segment(
        &publication,
        &root,
        PublishedSegment {
            segment_sequence: 2,
            object_sequence: 102,
            source_id: source.id(),
            schema_id: active_schema.id(),
            minimum_event_time: timestamp(2026, 8, 20, 11, 59),
            maximum_event_time: timestamp(2026, 8, 20, 11, 59),
            bytes: b"second parquet",
        },
    )
    .await;
    publish_segment(
        &publication,
        &root,
        PublishedSegment {
            segment_sequence: 3,
            object_sequence: 103,
            source_id: source.id(),
            schema_id: active_schema.id(),
            minimum_event_time: timestamp(2026, 8, 20, 12, 0),
            maximum_event_time: timestamp(2026, 8, 20, 12, 1),
            bytes: b"outside parquet",
        },
    )
    .await;
    register_prepared_segment(
        &publication,
        &root,
        PublishedSegment {
            segment_sequence: 4,
            object_sequence: 104,
            source_id: source.id(),
            schema_id: active_schema.id(),
            minimum_event_time: timestamp(2026, 8, 20, 11, 0),
            maximum_event_time: timestamp(2026, 8, 20, 11, 1),
            bytes: b"prepared parquet",
        },
    )
    .await;

    let range =
        QueryRequestTimeRange::new(timestamp(2026, 8, 20, 10, 0), timestamp(2026, 8, 20, 12, 0))
            .expect("ordered request range");
    let store = QuerySnapshotStore::new(pool.clone());
    let snapshot = store
        .select(
            "source logs | project @event_time, message, region",
            range,
            QuerySnapshotLimits::new(1_024).expect("query snapshot limits"),
        )
        .await
        .expect("select query snapshot");

    assert_eq!(snapshot.source_id(), source.id());
    assert_eq!(snapshot.active_schema().id(), active_schema.id());
    assert_eq!(snapshot.stored_schemas().len(), 2);
    assert_eq!(snapshot.stored_schemas()[0].id(), stored_schema.id());
    assert_eq!(snapshot.stored_schemas()[1].id(), active_schema.id());
    assert_eq!(
        snapshot.time_range().start_inclusive().unix_milliseconds(),
        range.start_inclusive().timestamp_millis()
    );
    assert_eq!(
        snapshot.time_range().end_exclusive().unix_milliseconds(),
        range.end_exclusive().timestamp_millis()
    );
    assert_eq!(snapshot.segments().len(), 2);
    assert_eq!(
        snapshot.segments()[0].segment_id(),
        SegmentId::from(uuid(1))
    );
    assert_eq!(snapshot.segments()[0].schema_id(), stored_schema.id());
    assert_eq!(snapshot.segments()[0].object(), &first);
    assert_eq!(
        snapshot.segments()[1].segment_id(),
        SegmentId::from(uuid(2))
    );
    assert_eq!(snapshot.segments()[1].schema_id(), active_schema.id());
    assert_eq!(snapshot.segments()[1].object(), &second);
    assert_eq!(
        snapshot.selected_parquet_bytes(),
        first.expected_byte_size().get() + second.expected_byte_size().get()
    );

    let error = store
        .select(
            "source logs",
            range,
            QuerySnapshotLimits::new(first.expected_byte_size().get()).expect("byte limit"),
        )
        .await
        .expect_err("selected object bytes must exceed the limit");
    assert_eq!(error.kind(), QuerySnapshotErrorKind::ResourceLimit);
    assert_eq!(
        error.limit_exceeded(),
        Some(QuerySnapshotLimitExceeded::ParquetBytes {
            maximum: first.expected_byte_size().get()
        })
    );

    let error = store
        .select(
            "source missing",
            range,
            QuerySnapshotLimits::new(1_024).expect("query snapshot limits"),
        )
        .await
        .expect_err("unknown source must fail analysis");
    assert_eq!(error.kind(), QuerySnapshotErrorKind::Analysis);
    assert_eq!(
        error
            .analysis_error()
            .expect("analysis error")
            .diagnostics()[0]
            .code(),
        DiagnosticCode::SourceNotFound
    );

    sqlx::query("UPDATE stored_objects SET object_key = 'wrong/key.parquet' WHERE object_id = $1")
        .bind(first.key().object_id().as_uuid())
        .execute(&pool)
        .await
        .expect("corrupt stored object key");
    let error = store
        .select(
            "source logs",
            range,
            QuerySnapshotLimits::new(1_024).expect("query snapshot limits"),
        )
        .await
        .expect_err("mismatched exact object key must fail");
    assert_eq!(error.kind(), QuerySnapshotErrorKind::Corrupt);
}

#[derive(Clone, Copy)]
struct PublishedSegment<'a> {
    segment_sequence: u64,
    object_sequence: u64,
    source_id: elucid_catalog::SourceId,
    schema_id: elucid_catalog::SchemaId,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    bytes: &'a [u8],
}

async fn publish_segment(
    publication: &PublicationStore,
    root: &ManagedRoot,
    fixture: PublishedSegment<'_>,
) -> ObjectDescriptor {
    let segment_id = SegmentId::from(uuid(fixture.segment_sequence));
    let object = descriptor(
        ManagedObjectKey::parquet(
            root,
            segment_id,
            StoredObjectId::from(uuid(fixture.object_sequence)),
        ),
        fixture.bytes,
    );
    let segment = segment_registration(
        segment_id,
        fixture.source_id,
        fixture.schema_id,
        fixture.minimum_event_time,
        fixture.maximum_event_time,
        object.clone(),
    );
    publication
        .register_ingestion_segment(&segment)
        .await
        .expect("register segment");
    publication
        .record_verified_upload(&object)
        .await
        .expect("record verified upload");
    publication
        .publish_ingestion_segment(segment_id, RetentionPeriod::new(3_600).expect("retention"))
        .await
        .expect("publish segment");
    object
}

async fn register_prepared_segment(
    publication: &PublicationStore,
    root: &ManagedRoot,
    fixture: PublishedSegment<'_>,
) {
    let segment_id = SegmentId::from(uuid(fixture.segment_sequence));
    let object = descriptor(
        ManagedObjectKey::parquet(
            root,
            segment_id,
            StoredObjectId::from(uuid(fixture.object_sequence)),
        ),
        fixture.bytes,
    );
    let segment = segment_registration(
        segment_id,
        fixture.source_id,
        fixture.schema_id,
        fixture.minimum_event_time,
        fixture.maximum_event_time,
        object,
    );
    publication
        .register_ingestion_segment(&segment)
        .await
        .expect("register prepared segment");
}

fn segment_registration(
    segment_id: SegmentId,
    source_id: elucid_catalog::SourceId,
    schema_id: elucid_catalog::SchemaId,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    object: ObjectDescriptor,
) -> IngestionSegmentRegistration {
    let times = IngestionSegmentTimes::new(
        minimum_event_time.date_naive(),
        minimum_event_time,
        maximum_event_time,
        maximum_event_time,
        maximum_event_time,
    )
    .expect("valid segment times");
    IngestionSegmentRegistration::new(
        segment_id,
        source_id,
        schema_id,
        times,
        NonZeroU64::new(1).expect("positive rows"),
        NonZeroU64::new(128).expect("positive bytes"),
        object,
    )
    .expect("valid segment registration")
}

fn descriptor(key: ManagedObjectKey, bytes: &[u8]) -> ObjectDescriptor {
    ObjectDescriptor::new(
        key,
        ObjectByteSize::new(u64::try_from(bytes.len()).expect("fixture size fits u64")),
        ObjectDigest::calculate(bytes),
        ObjectMediaType::ParquetData,
        ObjectFormatVersion::new(1).expect("positive format version"),
    )
    .expect("valid object descriptor")
}

fn timestamp(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .single()
        .expect("fixture timestamp")
}

fn uuid(sequence: u64) -> Uuid {
    Uuid::from_u128(0x019d_0000_0000_7000_8000_0000_0000_0000 | u128::from(sequence))
}
