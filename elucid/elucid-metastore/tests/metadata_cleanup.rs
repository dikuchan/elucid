use std::time::Duration;

use elucid_catalog::CatalogManifest;
use elucid_metastore::{
    CatalogApplyOutcome, CatalogStore, MetadataCleanupLimit, RetentionStore, install,
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
async fn terminal_metadata_cleanup_is_bounded_and_preserves_live_compaction_relationships() {
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

    let catalog = CatalogStore::load(pool.clone())
        .await
        .expect("load empty catalog");
    let manifest = CatalogManifest::decode(CATALOG.as_bytes()).expect("decode catalog fixture");
    let source = match catalog
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

    let fixtures = fixture_ids();
    insert_compaction_runs(
        &pool,
        source_id.as_uuid(),
        schema_id.as_uuid(),
        &fixtures.runs,
    )
    .await;

    insert_segments(
        &pool,
        source_id.as_uuid(),
        schema_id.as_uuid(),
        &fixtures.runs,
        &fixtures.segments,
    )
    .await;

    insert_stored_objects(
        &pool,
        input_id.as_uuid(),
        &fixtures.segments,
        &fixtures.objects,
    )
    .await;

    let retention = RetentionStore::new(pool.clone());
    let first = retention
        .clean_terminal_metadata(MetadataCleanupLimit::new(1).expect("single-root limit"))
        .await
        .expect("clean first bounded root");
    assert_eq!(first.removed_objects(), 1);
    assert_eq!(first.removed_segments(), 1);
    assert_eq!(first.removed_compaction_runs(), 0);
    assert_eq!(first.removed_roots(), 1);
    assert_eq!(first.removed_rows(), 2);
    assert!(
        !row_exists(
            &pool,
            MetadataRow::Object(fixtures.objects.removable_expired)
        )
        .await
    );
    assert!(
        !row_exists(
            &pool,
            MetadataRow::Segment(fixtures.segments.removable_expired)
        )
        .await
    );
    assert!(
        row_exists(
            &pool,
            MetadataRow::Object(fixtures.objects.removable_dead_letter)
        )
        .await
    );
    assert!(
        row_exists(
            &pool,
            MetadataRow::CompactionRun(fixtures.runs.unreferenced_terminal)
        )
        .await
    );

    let second = retention
        .clean_terminal_metadata(MetadataCleanupLimit::new(2).expect("two-root limit"))
        .await
        .expect("clean second bounded batch");
    assert_eq!(second.removed_objects(), 2);
    assert_eq!(second.removed_segments(), 1);
    assert_eq!(second.removed_compaction_runs(), 0);
    assert_eq!(second.removed_roots(), 2);
    assert_eq!(second.removed_rows(), 3);
    assert!(
        !row_exists(
            &pool,
            MetadataRow::Object(fixtures.objects.removable_dead_letter)
        )
        .await
    );
    assert!(
        !row_exists(
            &pool,
            MetadataRow::Object(fixtures.objects.removable_superseded)
        )
        .await
    );
    assert!(
        !row_exists(
            &pool,
            MetadataRow::Segment(fixtures.segments.removable_superseded)
        )
        .await
    );
    assert!(
        row_exists(
            &pool,
            MetadataRow::CompactionRun(fixtures.runs.removable_segment)
        )
        .await
    );

    let third = retention
        .clean_terminal_metadata(MetadataCleanupLimit::new(10).expect("cleanup limit"))
        .await
        .expect("clean newly unreferenced terminal runs");
    assert_eq!(third.removed_objects(), 0);
    assert_eq!(third.removed_segments(), 0);
    assert_eq!(third.removed_compaction_runs(), 2);
    assert_eq!(third.removed_roots(), 2);
    assert_eq!(third.removed_rows(), 2);
    assert!(
        !row_exists(
            &pool,
            MetadataRow::CompactionRun(fixtures.runs.unreferenced_terminal)
        )
        .await
    );
    assert!(
        !row_exists(
            &pool,
            MetadataRow::CompactionRun(fixtures.runs.removable_segment)
        )
        .await
    );

    let blocked = retention
        .clean_terminal_metadata(MetadataCleanupLimit::new(10).expect("cleanup limit"))
        .await
        .expect("retain live lifecycle relationships");
    assert_eq!(blocked.removed_rows(), 0);
    assert!(
        row_exists(
            &pool,
            MetadataRow::Object(fixtures.objects.blocked_terminal)
        )
        .await
    );
    assert!(
        row_exists(
            &pool,
            MetadataRow::Segment(fixtures.segments.blocked_terminal)
        )
        .await
    );
    assert!(row_exists(&pool, MetadataRow::CompactionRun(fixtures.runs.active)).await);
    assert!(row_exists(&pool, MetadataRow::Object(fixtures.objects.active_output)).await);
    assert!(row_exists(&pool, MetadataRow::Segment(fixtures.segments.active_output)).await);
    assert!(
        row_exists(
            &pool,
            MetadataRow::CompactionRun(fixtures.runs.referenced_terminal)
        )
        .await
    );
    assert!(
        row_exists(
            &pool,
            MetadataRow::Object(fixtures.objects.pending_deletion)
        )
        .await
    );
    assert!(
        row_exists(
            &pool,
            MetadataRow::Segment(fixtures.segments.pending_deletion)
        )
        .await
    );

    fail_compaction_run(&pool, fixtures.runs.active).await;
    let unblocked = retention
        .clean_terminal_metadata(MetadataCleanupLimit::new(10).expect("cleanup limit"))
        .await
        .expect("clean lifecycle after its run becomes terminal");
    assert_eq!(unblocked.removed_objects(), 1);
    assert_eq!(unblocked.removed_segments(), 1);
    assert_eq!(unblocked.removed_compaction_runs(), 1);
    assert_eq!(unblocked.removed_roots(), 2);
    assert_eq!(unblocked.removed_rows(), 3);
    assert!(
        !row_exists(
            &pool,
            MetadataRow::Object(fixtures.objects.blocked_terminal)
        )
        .await
    );
    assert!(
        !row_exists(
            &pool,
            MetadataRow::Segment(fixtures.segments.blocked_terminal)
        )
        .await
    );
    assert!(!row_exists(&pool, MetadataRow::CompactionRun(fixtures.runs.active)).await);

    let repeated = retention
        .clean_terminal_metadata(MetadataCleanupLimit::new(10).expect("cleanup limit"))
        .await
        .expect("repeat cleanup");
    assert_eq!(repeated.removed_rows(), 0);
}

struct FixtureIds {
    runs: CompactionRunFixtures,
    segments: SegmentFixtures,
    objects: ObjectFixtures,
}

struct CompactionRunFixtures {
    unreferenced_terminal: Uuid,
    active: Uuid,
    referenced_terminal: Uuid,
    removable_segment: Uuid,
}

struct SegmentFixtures {
    removable_expired: Uuid,
    blocked_terminal: Uuid,
    active_output: Uuid,
    removable_superseded: Uuid,
    pending_deletion: Uuid,
}

struct ObjectFixtures {
    removable_expired: Uuid,
    removable_dead_letter: Uuid,
    blocked_terminal: Uuid,
    active_output: Uuid,
    removable_superseded: Uuid,
    pending_deletion: Uuid,
}

fn fixture_ids() -> FixtureIds {
    FixtureIds {
        runs: CompactionRunFixtures {
            unreferenced_terminal: uuid(100),
            active: uuid(101),
            referenced_terminal: uuid(102),
            removable_segment: uuid(103),
        },
        segments: SegmentFixtures {
            removable_expired: uuid(201),
            blocked_terminal: uuid(202),
            active_output: uuid(203),
            removable_superseded: uuid(204),
            pending_deletion: uuid(205),
        },
        objects: ObjectFixtures {
            removable_expired: uuid(301),
            removable_dead_letter: uuid(302),
            blocked_terminal: uuid(303),
            active_output: uuid(304),
            removable_superseded: uuid(305),
            pending_deletion: uuid(306),
        },
    }
}

async fn insert_compaction_runs(
    pool: &PgPool,
    source_id: Uuid,
    schema_id: Uuid,
    runs: &CompactionRunFixtures,
) {
    sqlx::query(
        r#"
        INSERT INTO compaction_runs (
            compaction_run_id, source_id, schema_id, event_day, state,
            failure_code, created_at, updated_at, completed_at
        ) VALUES
            ($1, $5, $6, DATE '2026-08-20', 'FAILED', 'FIXTURE_FAILURE',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 01:00:00Z',
             TIMESTAMPTZ '2026-08-20 01:00:00Z'),
            ($2, $5, $6, DATE '2026-08-20', 'BUILDING', NULL,
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z', NULL),
            ($3, $5, $6, DATE '2026-08-20', 'COMMITTED', NULL,
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 02:00:00Z',
             TIMESTAMPTZ '2026-08-20 02:00:00Z'),
            ($4, $5, $6, DATE '2026-08-20', 'COMMITTED', NULL,
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 03:00:00Z',
             TIMESTAMPTZ '2026-08-20 03:00:00Z')
        "#,
    )
    .bind(runs.unreferenced_terminal)
    .bind(runs.active)
    .bind(runs.referenced_terminal)
    .bind(runs.removable_segment)
    .bind(source_id)
    .bind(schema_id)
    .execute(pool)
    .await
    .expect("insert compaction-run fixtures");
}

async fn insert_segments(
    pool: &PgPool,
    source_id: Uuid,
    schema_id: Uuid,
    runs: &CompactionRunFixtures,
    segments: &SegmentFixtures,
) {
    sqlx::query(
        r#"
        INSERT INTO segments (
            segment_id, source_id, schema_id, origin, produced_by_compaction_run_id,
            claimed_by_compaction_run_id, event_day, minimum_event_time, maximum_event_time,
            minimum_ingestion_time, maximum_ingestion_time, row_count, uncompressed_bytes,
            data_expires_at, state, published_at, retired_at, reclaim_after, created_at, updated_at
        ) VALUES
            ($6, $1, $2, 'INGESTION', NULL, NULL, DATE '2026-08-20',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             1, 64, TIMESTAMPTZ '2026-08-20 01:00:00Z', 'EXPIRED',
             TIMESTAMPTZ '2026-08-20 00:10:00Z', TIMESTAMPTZ '2026-08-20 02:00:00Z',
             TIMESTAMPTZ '2026-08-20 03:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             TIMESTAMPTZ '2026-08-20 04:00:00Z'),
            ($7, $1, $2, 'INGESTION', NULL, $3, DATE '2026-08-20',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             1, 64, TIMESTAMPTZ '2026-08-20 01:00:00Z', 'SUPERSEDED',
             TIMESTAMPTZ '2026-08-20 00:10:00Z', TIMESTAMPTZ '2026-08-20 02:00:00Z',
             TIMESTAMPTZ '2026-08-20 03:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             TIMESTAMPTZ '2026-08-20 04:00:00Z'),
            ($8, $1, $2, 'COMPACTION', $4, NULL, DATE '2026-08-20',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             1, 64, TIMESTAMPTZ '2026-09-20 00:00:00Z', 'ACTIVE',
             TIMESTAMPTZ '2026-08-20 00:10:00Z', NULL, NULL,
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 04:00:00Z'),
            ($9, $1, $2, 'INGESTION', NULL, $5, DATE '2026-08-20',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             1, 64, TIMESTAMPTZ '2026-08-20 01:00:00Z', 'SUPERSEDED',
             TIMESTAMPTZ '2026-08-20 00:10:00Z', TIMESTAMPTZ '2026-08-20 02:00:00Z',
             TIMESTAMPTZ '2026-08-20 03:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             TIMESTAMPTZ '2026-08-20 04:00:00Z'),
            ($10, $1, $2, 'INGESTION', NULL, NULL, DATE '2026-08-20',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             1, 64, TIMESTAMPTZ '2026-08-20 01:00:00Z', 'EXPIRED',
             TIMESTAMPTZ '2026-08-20 00:10:00Z', TIMESTAMPTZ '2026-08-20 02:00:00Z',
             TIMESTAMPTZ '2026-08-20 03:00:00Z', TIMESTAMPTZ '2026-08-20 00:00:00Z',
             TIMESTAMPTZ '2026-08-20 04:00:00Z')
        "#,
    )
    .bind(source_id)
    .bind(schema_id)
    .bind(runs.active)
    .bind(runs.referenced_terminal)
    .bind(runs.removable_segment)
    .bind(segments.removable_expired)
    .bind(segments.blocked_terminal)
    .bind(segments.active_output)
    .bind(segments.removable_superseded)
    .bind(segments.pending_deletion)
    .execute(pool)
    .await
    .expect("insert segment fixtures");
}

async fn insert_stored_objects(
    pool: &PgPool,
    input_id: Uuid,
    segments: &SegmentFixtures,
    objects: &ObjectFixtures,
) {
    sqlx::query(
        r#"
        INSERT INTO stored_objects (
            object_id, kind, segment_id, input_id, batch_id, object_key,
            expected_byte_size, blake3_digest, media_type, format_version, state,
            uploaded_at, published_at, retention_deadline, delete_requested_at,
            deleted_at, created_at, updated_at
        ) VALUES
            ($7, 'PARQUET_DATA', $2, NULL, NULL, 'cleanup/expired.parquet',
             64, decode(repeat('11', 32), 'hex'), 'application/vnd.apache.parquet', 1, 'DELETED',
             TIMESTAMPTZ '2026-08-20 00:05:00Z', TIMESTAMPTZ '2026-08-20 00:10:00Z', NULL,
             TIMESTAMPTZ '2026-08-20 04:00:00Z', TIMESTAMPTZ '2026-08-20 05:00:00Z',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-21 00:01:00Z'),
            ($8, 'DEAD_LETTER', NULL, $1, '019d0000-0000-7000-8000-000000000400',
             'cleanup/dead-letter.ndjson', 64, decode(repeat('22', 32), 'hex'),
             'application/x-ndjson', 1, 'DELETED', TIMESTAMPTZ '2026-08-20 00:05:00Z',
             TIMESTAMPTZ '2026-08-20 00:10:00Z', TIMESTAMPTZ '2026-08-20 03:00:00Z',
             TIMESTAMPTZ '2026-08-20 04:00:00Z', TIMESTAMPTZ '2026-08-20 05:00:00Z',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-21 00:02:00Z'),
            ($9, 'PARQUET_DATA', $3, NULL, NULL, 'cleanup/blocked.parquet',
             64, decode(repeat('33', 32), 'hex'), 'application/vnd.apache.parquet', 1, 'DELETED',
             TIMESTAMPTZ '2026-08-20 00:05:00Z', TIMESTAMPTZ '2026-08-20 00:10:00Z', NULL,
             TIMESTAMPTZ '2026-08-20 04:00:00Z', TIMESTAMPTZ '2026-08-20 05:00:00Z',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-21 00:04:00Z'),
            ($10, 'PARQUET_DATA', $4, NULL, NULL, 'cleanup/active.parquet',
             64, decode(repeat('44', 32), 'hex'), 'application/vnd.apache.parquet', 1, 'PUBLISHED',
             TIMESTAMPTZ '2026-08-20 00:05:00Z', TIMESTAMPTZ '2026-08-20 00:10:00Z', NULL,
             NULL, NULL, TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-21 00:05:00Z'),
            ($11, 'PARQUET_DATA', $5, NULL, NULL, 'cleanup/superseded.parquet',
             64, decode(repeat('55', 32), 'hex'), 'application/vnd.apache.parquet', 1, 'DELETED',
             TIMESTAMPTZ '2026-08-20 00:05:00Z', TIMESTAMPTZ '2026-08-20 00:10:00Z', NULL,
             TIMESTAMPTZ '2026-08-20 04:00:00Z', TIMESTAMPTZ '2026-08-20 05:00:00Z',
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-21 00:03:00Z'),
            ($12, 'PARQUET_DATA', $6, NULL, NULL, 'cleanup/pending.parquet',
             64, decode(repeat('66', 32), 'hex'), 'application/vnd.apache.parquet', 1,
             'DELETE_PENDING', TIMESTAMPTZ '2026-08-20 00:05:00Z',
             TIMESTAMPTZ '2026-08-20 00:10:00Z', NULL,
             TIMESTAMPTZ '2026-08-20 04:00:00Z', NULL,
             TIMESTAMPTZ '2026-08-20 00:00:00Z', TIMESTAMPTZ '2026-08-21 00:06:00Z')
        "#,
    )
    .bind(input_id)
    .bind(segments.removable_expired)
    .bind(segments.blocked_terminal)
    .bind(segments.active_output)
    .bind(segments.removable_superseded)
    .bind(segments.pending_deletion)
    .bind(objects.removable_expired)
    .bind(objects.removable_dead_letter)
    .bind(objects.blocked_terminal)
    .bind(objects.active_output)
    .bind(objects.removable_superseded)
    .bind(objects.pending_deletion)
    .execute(pool)
    .await
    .expect("insert stored-object fixtures");
}

async fn fail_compaction_run(pool: &PgPool, run_id: Uuid) {
    sqlx::query(
        r#"
        UPDATE compaction_runs
        SET
            state = 'FAILED',
            failure_code = 'FIXTURE_FAILURE',
            completed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE compaction_run_id = $1 AND state = 'BUILDING'
        "#,
    )
    .bind(run_id)
    .execute(pool)
    .await
    .expect("fail active compaction run");
}

#[derive(Clone, Copy)]
enum MetadataRow {
    Object(Uuid),
    Segment(Uuid),
    CompactionRun(Uuid),
}

async fn row_exists(pool: &PgPool, row: MetadataRow) -> bool {
    let (query, id) = match row {
        MetadataRow::Object(id) => (
            "SELECT EXISTS(SELECT 1 FROM stored_objects WHERE object_id = $1)",
            id,
        ),
        MetadataRow::Segment(id) => (
            "SELECT EXISTS(SELECT 1 FROM segments WHERE segment_id = $1)",
            id,
        ),
        MetadataRow::CompactionRun(id) => (
            "SELECT EXISTS(SELECT 1 FROM compaction_runs WHERE compaction_run_id = $1)",
            id,
        ),
    };
    sqlx::query_scalar(query)
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("check metadata row existence")
}

fn uuid(sequence: u64) -> Uuid {
    Uuid::from_u128(0x019d_0000_0000_7000_8000_0000_0000_0000 | u128::from(sequence))
}
