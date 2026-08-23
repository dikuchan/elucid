use std::num::NonZeroU64;
use std::time::Duration;

use chrono::{DateTime, TimeZone as _, Utc};
use elucid_catalog::{CatalogManifest, InputId, SchemaId, SourceId};
use elucid_metastore::{
    CatalogApplyOutcome, CatalogStore, DeadLetterRegistration, IngestionSegmentRegistration,
    IngestionSegmentTimes, ObjectUploadRecordOutcome, OperationalLimit, OperationalStore,
    PublicationOutcome, PublicationStore, QueryRequestTimeRange, QuerySnapshotLimits,
    QuerySnapshotStore, ReclamationGracePeriod, RegistrationOutcome, RetentionPeriod,
    RetentionScanLimit, RetentionStore, StoredObjectState, install,
};
use elucid_storage::{
    BatchId, ManagedObjectKey, ManagedRoot, ObjectByteSize, ObjectDescriptor, ObjectDigest,
    ObjectFormatVersion, ObjectMediaType, SegmentId, StoredObjectId,
};
use sqlx::postgres::{PgPool, PgPoolOptions};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ImageExt as _, runners::AsyncRunner as _};
use uuid::Uuid;

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
async fn retention_expiration_is_bounded_snapshot_safe_and_postgres_timed() {
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
        .max_connections(5)
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
    let source_id = source.id();
    let schema_id = source.active_schema().id();
    let input_id = source.inputs()[0].id();
    let root = ManagedRoot::parse("retention-test").expect("managed root");
    let publication = PublicationStore::new(pool.clone());

    for fixture in [
        SegmentFixture::new(1, 101, 10, 0, 2),
        SegmentFixture::new(2, 102, 10, 1, 3),
        SegmentFixture::new(3, 103, 10, 2, 4),
        SegmentFixture::new(4, 104, 10, 3, 5),
    ] {
        publish_segment(&publication, &root, source_id, schema_id, fixture).await;
    }
    make_segments_due(&pool, &[1, 2, 4]).await;
    claim_segment_for_compaction(&pool, source_id, schema_id, 4).await;

    let range =
        QueryRequestTimeRange::new(timestamp(2026, 8, 20, 9, 0), timestamp(2026, 8, 20, 11, 0))
            .expect("ordered request range");
    let snapshots = QuerySnapshotStore::new(pool.clone());
    let before_expiration = snapshots
        .select(
            "source logs",
            range,
            QuerySnapshotLimits::new(1_024).expect("snapshot limits"),
        )
        .await
        .expect("select snapshot before expiration");
    assert_eq!(
        snapshot_segment_ids(&before_expiration),
        [segment_id(1), segment_id(2), segment_id(3), segment_id(4)]
    );

    let maximum_query_lifetime_seconds = 60;
    let safety_margin_seconds = 1;
    let grace = ReclamationGracePeriod::new(maximum_query_lifetime_seconds, safety_margin_seconds)
        .expect("reclamation grace");
    let retention = RetentionStore::new(pool.clone());
    let first = retention
        .expire_segments(grace, RetentionScanLimit::new(1).expect("single-item scan"))
        .await
        .expect("expire first bounded batch");
    assert_eq!(first.expired_segments(), 1);
    assert_eq!(first.expired_rows(), 2);
    assert_eq!(segment_state(&pool, 1).await, "EXPIRED");
    assert_eq!(segment_state(&pool, 2).await, "ACTIVE");
    assert_eq!(reclamation_grace_seconds(&pool, 1).await, 61);

    let second = retention
        .expire_segments(grace, RetentionScanLimit::new(10).expect("bounded scan"))
        .await
        .expect("expire remaining eligible segments");
    assert_eq!(second.expired_segments(), 1);
    assert_eq!(second.expired_rows(), 3);
    assert_eq!(segment_state(&pool, 2).await, "EXPIRED");
    assert_eq!(segment_state(&pool, 3).await, "ACTIVE");
    assert_eq!(segment_state(&pool, 4).await, "ACTIVE");

    let after_expiration = snapshots
        .select(
            "source logs",
            range,
            QuerySnapshotLimits::new(1_024).expect("snapshot limits"),
        )
        .await
        .expect("select snapshot after expiration");
    assert_eq!(
        snapshot_segment_ids(&after_expiration),
        [segment_id(3), segment_id(4)]
    );
    assert_eq!(
        snapshot_segment_ids(&before_expiration),
        [segment_id(1), segment_id(2), segment_id(3), segment_id(4)]
    );
    assert_eq!(published_object_count(&pool, &[1, 2]).await, 2);

    let third = retention
        .expire_segments(grace, RetentionScanLimit::new(10).expect("bounded scan"))
        .await
        .expect("repeat expiration scan");
    assert_eq!(third.expired_segments(), 0);
    assert_eq!(third.expired_rows(), 0);

    let due_dead_letter = publish_dead_letter(&publication, &root, input_id, 201, 301).await;
    let future_dead_letter = publish_dead_letter(&publication, &root, input_id, 202, 302).await;
    sqlx::query(
        "UPDATE stored_objects SET retention_deadline = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE object_id = $1",
    )
    .bind(due_dead_letter.as_uuid())
    .execute(&pool)
    .await
    .expect("make dead letter due");

    let operations = OperationalStore::new(pool.clone(), root);
    let visible_dead_letters = operations
        .dead_letters(
            source_id,
            OperationalLimit::new(10).expect("operational limit"),
        )
        .await
        .expect("list unexpired dead letters");
    assert_eq!(visible_dead_letters.items().len(), 1);
    assert_eq!(
        visible_dead_letters.items()[0].object_id(),
        future_dead_letter
    );
    assert!(
        operations
            .dead_letter(due_dead_letter)
            .await
            .expect("look up expired dead letter")
            .is_none()
    );
    assert_eq!(
        publication
            .stored_object_state(due_dead_letter)
            .await
            .expect("load expired dead-letter object state"),
        Some(StoredObjectState::Published)
    );
}

#[derive(Clone, Copy)]
struct SegmentFixture {
    segment_sequence: u64,
    object_sequence: u64,
    hour: u32,
    minute: u32,
    rows: u64,
}

impl SegmentFixture {
    const fn new(
        segment_sequence: u64,
        object_sequence: u64,
        hour: u32,
        minute: u32,
        rows: u64,
    ) -> Self {
        Self {
            segment_sequence,
            object_sequence,
            hour,
            minute,
            rows,
        }
    }
}

async fn publish_segment(
    publication: &PublicationStore,
    root: &ManagedRoot,
    source_id: SourceId,
    schema_id: SchemaId,
    fixture: SegmentFixture,
) {
    let segment_id = SegmentId::from(uuid(fixture.segment_sequence));
    let bytes = format!("parquet {}", fixture.segment_sequence);
    let object = descriptor(
        ManagedObjectKey::parquet(
            root,
            segment_id,
            StoredObjectId::from(uuid(fixture.object_sequence)),
        ),
        bytes.as_bytes(),
        ObjectMediaType::ParquetData,
    );
    let event_time = timestamp(2026, 8, 20, fixture.hour, fixture.minute);
    let times = IngestionSegmentTimes::new(
        event_time.date_naive(),
        event_time,
        event_time,
        event_time,
        event_time,
    )
    .expect("valid segment times");
    let segment = IngestionSegmentRegistration::new(
        segment_id,
        source_id,
        schema_id,
        times,
        NonZeroU64::new(fixture.rows).expect("positive rows"),
        NonZeroU64::new(128).expect("positive bytes"),
        object.clone(),
    )
    .expect("valid segment registration");
    assert_eq!(
        publication
            .register_ingestion_segment(&segment)
            .await
            .expect("register segment"),
        RegistrationOutcome::Registered
    );
    assert_eq!(
        publication
            .record_verified_upload(&object)
            .await
            .expect("record verified upload"),
        ObjectUploadRecordOutcome::Recorded
    );
    assert_eq!(
        publication
            .publish_ingestion_segment(segment_id, RetentionPeriod::new(3_600).expect("retention"))
            .await
            .expect("publish segment"),
        PublicationOutcome::Published
    );
}

async fn publish_dead_letter(
    publication: &PublicationStore,
    root: &ManagedRoot,
    input_id: InputId,
    batch_sequence: u64,
    object_sequence: u64,
) -> StoredObjectId {
    let batch_id = BatchId::try_from(uuid(batch_sequence)).expect("batch identity");
    let object_id = StoredObjectId::from(uuid(object_sequence));
    let object = descriptor(
        ManagedObjectKey::dead_letter(root, batch_id, object_id),
        b"{\"error\":\"invalid\"}\n",
        ObjectMediaType::DeadLetter,
    );
    let registration = DeadLetterRegistration::new(input_id, batch_id, object.clone())
        .expect("valid dead-letter registration");
    publication
        .register_dead_letter(&registration)
        .await
        .expect("register dead letter");
    publication
        .record_verified_upload(&object)
        .await
        .expect("record dead-letter upload");
    publication
        .publish_dead_letter(object_id, RetentionPeriod::new(3_600).expect("retention"))
        .await
        .expect("publish dead letter");
    object_id
}

async fn make_segments_due(pool: &PgPool, sequences: &[u64]) {
    let segment_ids = sequences
        .iter()
        .map(|sequence| uuid(*sequence))
        .collect::<Vec<_>>();
    sqlx::query(
        "UPDATE segments SET data_expires_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE segment_id = ANY($1::uuid[])",
    )
    .bind(&segment_ids)
    .execute(pool)
    .await
    .expect("make segments due");
}

async fn claim_segment_for_compaction(
    pool: &PgPool,
    source_id: SourceId,
    schema_id: SchemaId,
    segment_sequence: u64,
) {
    let run_id = uuid(401);
    sqlx::query(
        r#"
        INSERT INTO compaction_runs (
            compaction_run_id, source_id, schema_id, event_day, state
        ) VALUES ($1, $2, $3, '2026-08-20', 'BUILDING')
        "#,
    )
    .bind(run_id)
    .bind(source_id.as_uuid())
    .bind(schema_id.as_uuid())
    .execute(pool)
    .await
    .expect("create compaction run");
    sqlx::query(
        "UPDATE segments SET claimed_by_compaction_run_id = $2, updated_at = CURRENT_TIMESTAMP WHERE segment_id = $1",
    )
    .bind(uuid(segment_sequence))
    .bind(run_id)
    .execute(pool)
    .await
    .expect("claim segment for compaction");
}

fn descriptor(
    key: ManagedObjectKey,
    bytes: &[u8],
    media_type: ObjectMediaType,
) -> ObjectDescriptor {
    ObjectDescriptor::new(
        key,
        ObjectByteSize::new(u64::try_from(bytes.len()).expect("fixture size fits u64")),
        ObjectDigest::calculate(bytes),
        media_type,
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

fn snapshot_segment_ids(snapshot: &elucid_metastore::QuerySnapshot) -> Vec<SegmentId> {
    snapshot
        .segments()
        .iter()
        .map(elucid_metastore::QuerySegment::segment_id)
        .collect()
}

fn segment_id(sequence: u64) -> SegmentId {
    SegmentId::from(uuid(sequence))
}

async fn segment_state(pool: &PgPool, sequence: u64) -> String {
    sqlx::query_scalar("SELECT state FROM segments WHERE segment_id = $1")
        .bind(uuid(sequence))
        .fetch_one(pool)
        .await
        .expect("load segment state")
}

async fn reclamation_grace_seconds(pool: &PgPool, sequence: u64) -> i64 {
    sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (reclaim_after - retired_at))::BIGINT FROM segments WHERE segment_id = $1",
    )
    .bind(uuid(sequence))
    .fetch_one(pool)
    .await
    .expect("load reclamation grace")
}

async fn published_object_count(pool: &PgPool, segment_sequences: &[u64]) -> i64 {
    let segment_ids = segment_sequences
        .iter()
        .map(|sequence| uuid(*sequence))
        .collect::<Vec<_>>();
    sqlx::query_scalar(
        "SELECT count(*) FROM stored_objects WHERE segment_id = ANY($1::uuid[]) AND state = 'PUBLISHED'",
    )
    .bind(&segment_ids)
    .fetch_one(pool)
    .await
    .expect("count published segment objects")
}
