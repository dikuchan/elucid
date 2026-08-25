use std::num::NonZeroU64;
use std::time::Duration;

use chrono::{DateTime, TimeZone as _, Utc};
use elucid_catalog::{CatalogManifest, SchemaId, SourceId};
use elucid_metastore::{
    CatalogApplyOutcome, CatalogStore, CompactionClaimLimitConfiguration, CompactionClaimLimits,
    CompactionOutputRegistration, CompactionOutputRegistrationConfiguration,
    CompactionOutputRegistrationOutcome, CompactionStore, IngestionSegmentRegistration,
    IngestionSegmentTimes, MaintenanceOwnership, PublicationStore, RetentionPeriod, install,
};
use elucid_storage::{
    ManagedObjectKey, ManagedRoot, ObjectByteSize, ObjectDescriptor, ObjectDigest,
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
async fn maintenance_owner_claims_one_bounded_run_and_registers_only_a_smaller_exact_replacement() {
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
    let root = ManagedRoot::parse("compaction-test").expect("managed root");
    let publication = PublicationStore::new(pool.clone());

    for fixture in [
        SegmentFixture::eligible(1, 101, 0),
        SegmentFixture::eligible(2, 102, 2),
        SegmentFixture::eligible(3, 103, 4),
        SegmentFixture::different_day(4, 104),
        SegmentFixture::too_large(5, 105),
        SegmentFixture::eligible(6, 106, 6),
    ] {
        publish_segment(&publication, &root, source_id, schema_id, fixture).await;
    }
    sqlx::query(
        "UPDATE segments SET data_expires_at = CURRENT_TIMESTAMP + INTERVAL '30 seconds' WHERE segment_id = $1",
    )
    .bind(segment_id(6).as_uuid())
    .execute(&pool)
    .await
    .expect("move one segment close to expiration");

    let compaction = CompactionStore::new(pool.clone());
    let mut owner = match compaction
        .try_acquire_maintenance()
        .await
        .expect("attempt maintenance ownership")
    {
        MaintenanceOwnership::Acquired(owner) => owner,
        MaintenanceOwnership::HeldElsewhere => panic!("first maintenance owner was rejected"),
        _ => panic!("unknown maintenance ownership outcome"),
    };
    assert!(matches!(
        compaction
            .try_acquire_maintenance()
            .await
            .expect("attempt competing maintenance ownership"),
        MaintenanceOwnership::HeldElsewhere
    ));

    let limits = CompactionClaimLimits::new(CompactionClaimLimitConfiguration {
        maximum_candidate_segments: 100,
        maximum_input_segments: 4,
        maximum_input_rows: 12,
        maximum_input_parquet_bytes: 1_000,
        maximum_input_uncompressed_bytes: 120,
        target_output_rows: 10,
        target_output_uncompressed_bytes: 100,
        minimum_retention: Duration::from_secs(60),
    })
    .expect("valid compaction limits");
    let claim = owner
        .claim(&limits)
        .await
        .expect("claim compaction run")
        .expect("eligible compaction run");

    assert_eq!(claim.source_id(), source_id);
    assert_eq!(claim.schema().id(), schema_id);
    assert_eq!(claim.event_day(), timestamp(0).date_naive());
    assert_eq!(claim.input_rows(), 9);
    assert_eq!(claim.input_uncompressed_bytes(), 90);
    assert_eq!(claim.input_parquet_bytes(), 30);
    assert_eq!(
        claim
            .inputs()
            .iter()
            .map(|input| input.segment_id())
            .collect::<Vec<_>>(),
        [segment_id(1), segment_id(2), segment_id(3)]
    );
    assert_eq!(
        claimed_input_count(&pool, claim.run_id().as_uuid()).await,
        3
    );
    assert_eq!(run_state(&pool, claim.run_id().as_uuid()).await, "BUILDING");

    let output_deadline = claim.data_expires_at();
    let outputs = vec![
        output_registration(
            &root,
            claim.run_id(),
            source_id,
            schema_id,
            OutputFixture {
                segment_sequence: 201,
                object_sequence: 301,
                times: IngestionSegmentTimes::new(
                    timestamp(0).date_naive(),
                    timestamp(0),
                    timestamp(3),
                    timestamp(10),
                    timestamp(13),
                )
                .expect("first output bounds"),
                rows: 5,
                uncompressed_bytes: 50,
            },
            output_deadline,
        ),
        output_registration(
            &root,
            claim.run_id(),
            source_id,
            schema_id,
            OutputFixture {
                segment_sequence: 202,
                object_sequence: 302,
                times: IngestionSegmentTimes::new(
                    timestamp(0).date_naive(),
                    timestamp(4),
                    timestamp(6),
                    timestamp(14),
                    timestamp(16),
                )
                .expect("second output bounds"),
                rows: 4,
                uncompressed_bytes: 40,
            },
            output_deadline,
        ),
    ];

    assert_eq!(
        compaction
            .register_outputs(claim.run_id(), &outputs)
            .await
            .expect("register compaction outputs"),
        CompactionOutputRegistrationOutcome::Registered
    );
    assert_eq!(
        compaction
            .register_outputs(claim.run_id(), &outputs)
            .await
            .expect("repeat output registration"),
        CompactionOutputRegistrationOutcome::AlreadyRegistered
    );
    assert_eq!(
        run_state(&pool, claim.run_id().as_uuid()).await,
        "UPLOADING"
    );
    assert_eq!(
        prepared_output_count(&pool, claim.run_id().as_uuid()).await,
        2
    );
    assert_eq!(
        planned_output_count(&pool, claim.run_id().as_uuid()).await,
        2
    );
    assert_eq!(
        active_claimed_input_count(&pool, claim.run_id().as_uuid()).await,
        3
    );

    owner
        .release()
        .await
        .expect("release maintenance ownership");
    assert!(matches!(
        compaction
            .try_acquire_maintenance()
            .await
            .expect("reacquire maintenance ownership"),
        MaintenanceOwnership::Acquired(_)
    ));
}

#[derive(Clone, Copy)]
struct SegmentFixture {
    segment_sequence: u64,
    object_sequence: u64,
    first_second: i64,
    rows: u64,
    uncompressed_bytes: u64,
    day_offset: i64,
}

impl SegmentFixture {
    const fn eligible(segment_sequence: u64, object_sequence: u64, first_second: i64) -> Self {
        Self {
            segment_sequence,
            object_sequence,
            first_second,
            rows: 3,
            uncompressed_bytes: 30,
            day_offset: 0,
        }
    }

    const fn different_day(segment_sequence: u64, object_sequence: u64) -> Self {
        Self {
            segment_sequence,
            object_sequence,
            first_second: 0,
            rows: 3,
            uncompressed_bytes: 30,
            day_offset: 86_400,
        }
    }

    const fn too_large(segment_sequence: u64, object_sequence: u64) -> Self {
        Self {
            segment_sequence,
            object_sequence,
            first_second: 8,
            rows: 10,
            uncompressed_bytes: 90,
            day_offset: 0,
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
    let first_second = fixture.first_second + fixture.day_offset;
    let last_second = first_second + i64::try_from(fixture.rows).expect("row count fits i64") - 1;
    let segment_id = segment_id(fixture.segment_sequence);
    let descriptor = descriptor(root, segment_id, fixture.object_sequence);
    let times = IngestionSegmentTimes::new(
        timestamp(first_second).date_naive(),
        timestamp(first_second),
        timestamp(last_second),
        timestamp(first_second + 10),
        timestamp(last_second + 10),
    )
    .expect("valid segment times");
    let registration = IngestionSegmentRegistration::new(
        segment_id,
        source_id,
        schema_id,
        times,
        NonZeroU64::new(fixture.rows).expect("positive rows"),
        NonZeroU64::new(fixture.uncompressed_bytes).expect("positive bytes"),
        descriptor.clone(),
    )
    .expect("valid segment registration");
    publication
        .register_ingestion_segment(&registration)
        .await
        .expect("register ingestion segment");
    publication
        .record_verified_upload(&descriptor)
        .await
        .expect("record verified upload");
    publication
        .publish_ingestion_segment(segment_id, RetentionPeriod::new(3_600).expect("retention"))
        .await
        .expect("publish ingestion segment");
}

struct OutputFixture {
    segment_sequence: u64,
    object_sequence: u64,
    times: IngestionSegmentTimes,
    rows: u64,
    uncompressed_bytes: u64,
}

fn output_registration(
    root: &ManagedRoot,
    run_id: elucid_metastore::CompactionRunId,
    source_id: SourceId,
    schema_id: SchemaId,
    fixture: OutputFixture,
    data_expires_at: DateTime<Utc>,
) -> CompactionOutputRegistration {
    let segment_id = segment_id(fixture.segment_sequence);
    CompactionOutputRegistration::new(CompactionOutputRegistrationConfiguration {
        run_id,
        segment_id,
        source_id,
        schema_id,
        times: fixture.times,
        row_count: NonZeroU64::new(fixture.rows).expect("positive rows"),
        uncompressed_bytes: NonZeroU64::new(fixture.uncompressed_bytes).expect("positive bytes"),
        data_expires_at,
        object: descriptor(root, segment_id, fixture.object_sequence),
    })
    .expect("valid compaction output registration")
}

fn descriptor(root: &ManagedRoot, segment_id: SegmentId, object_sequence: u64) -> ObjectDescriptor {
    ObjectDescriptor::new(
        ManagedObjectKey::parquet(root, segment_id, object_id(object_sequence)),
        ObjectByteSize::new(10),
        ObjectDigest::new([u8::try_from(object_sequence).unwrap_or(255); 32]),
        ObjectMediaType::ParquetData,
        ObjectFormatVersion::new(1).expect("format version"),
    )
    .expect("valid object descriptor")
}

async fn run_state(pool: &PgPool, run_id: Uuid) -> String {
    sqlx::query_scalar("SELECT state FROM compaction_runs WHERE compaction_run_id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await
        .expect("load compaction run state")
}

async fn claimed_input_count(pool: &PgPool, run_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM segments WHERE claimed_by_compaction_run_id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await
        .expect("count claimed inputs")
}

async fn active_claimed_input_count(pool: &PgPool, run_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM segments WHERE claimed_by_compaction_run_id = $1 AND state = 'ACTIVE'",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .expect("count active claimed inputs")
}

async fn prepared_output_count(pool: &PgPool, run_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM segments WHERE produced_by_compaction_run_id = $1 AND state = 'PREPARED'",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .expect("count prepared outputs")
}

async fn planned_output_count(pool: &PgPool, run_id: Uuid) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM stored_objects AS object
        JOIN segments AS segment USING (segment_id)
        WHERE segment.produced_by_compaction_run_id = $1
          AND object.state = 'PLANNED'
        "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .expect("count planned outputs")
}

fn timestamp(second: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 20, 10, 0, 0)
        .single()
        .expect("fixture date")
        + chrono::TimeDelta::seconds(second)
}

fn segment_id(sequence: u64) -> SegmentId {
    SegmentId::from(Uuid::from_u128(u128::from(sequence)))
}

fn object_id(sequence: u64) -> StoredObjectId {
    StoredObjectId::from(Uuid::from_u128(u128::from(sequence)))
}
