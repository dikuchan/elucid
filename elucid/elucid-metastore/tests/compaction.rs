use std::num::NonZeroU64;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, TimeDelta, TimeZone as _, Utc};
use elucid_catalog::{CatalogManifest, SchemaId, SourceId};
use elucid_metastore::{
    CatalogApplyOutcome, CatalogStore, CompactionClaimLimitConfiguration, CompactionClaimLimits,
    CompactionFailureCode, CompactionFailureOutcome, CompactionMetadataErrorKind,
    CompactionOutputRegistration, CompactionOutputRegistrationConfiguration,
    CompactionOutputRegistrationOutcome, CompactionPublicationOutcome, CompactionRecoveryLimit,
    CompactionRunClaim, CompactionStore, IngestionSegmentRegistration, IngestionSegmentTimes,
    MaintenanceOwnership, OrphanGracePeriod, PublicationStore, ReclamationGracePeriod,
    RetentionPeriod, install,
};
use elucid_storage::{
    ManagedObjectKey, ManagedRoot, ObjectByteSize, ObjectDescriptor, ObjectDigest,
    ObjectFormatVersion, ObjectMediaType, SegmentId, StoredObjectId,
};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Executor, Postgres as SqlxPostgres};
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
async fn compaction_replacement_is_atomic_idempotent_and_recoverable() {
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

    for fixture in [
        SegmentFixture::eligible_on_day(11, 111, 0, 2),
        SegmentFixture::eligible_on_day(12, 112, 2, 2),
        SegmentFixture::eligible_on_day(13, 113, 4, 2),
        SegmentFixture::eligible_on_day(21, 121, 0, 3),
        SegmentFixture::eligible_on_day(22, 122, 2, 3),
        SegmentFixture::eligible_on_day(23, 123, 4, 3),
    ] {
        publish_segment(&publication, &root, source_id, schema_id, fixture).await;
    }
    let build_only_claim = owner
        .claim(&limits)
        .await
        .expect("claim build-only run")
        .expect("eligible build-only run");
    assert_eq!(
        input_segment_ids(&build_only_claim),
        [segment_id(11), segment_id(12), segment_id(13)]
    );
    let incomplete_upload_claim = owner
        .claim(&limits)
        .await
        .expect("claim incomplete-upload run")
        .expect("eligible incomplete-upload run");
    assert_eq!(
        input_segment_ids(&incomplete_upload_claim),
        [segment_id(21), segment_id(22), segment_id(23)]
    );
    let incomplete_outputs = replacement_outputs(&root, &incomplete_upload_claim, 221, 321);
    compaction
        .register_outputs(incomplete_upload_claim.run_id(), &incomplete_outputs)
        .await
        .expect("register incomplete-upload outputs");
    publication
        .record_verified_upload(incomplete_outputs[0].object())
        .await
        .expect("record one incomplete-upload object");
    let incomplete_publication = owner
        .publish_replacement(
            incomplete_upload_claim.run_id(),
            ReclamationGracePeriod::new(60, 1).expect("reclamation grace"),
        )
        .await
        .expect_err("planned output must prevent publication");
    assert_eq!(
        incomplete_publication.kind(),
        CompactionMetadataErrorKind::Conflict
    );
    assert_eq!(
        run_state(&pool, incomplete_upload_claim.run_id().as_uuid()).await,
        "UPLOADING"
    );
    assert_eq!(
        active_claimed_input_count(&pool, incomplete_upload_claim.run_id().as_uuid()).await,
        3
    );
    assert_eq!(
        prepared_output_count(&pool, incomplete_upload_claim.run_id().as_uuid()).await,
        2
    );

    for output in &outputs {
        publication
            .record_verified_upload(output.object())
            .await
            .expect("record compaction output upload");
    }
    let mut old_snapshot = pool.begin().await.expect("begin old query snapshot");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *old_snapshot)
        .await
        .expect("set repeatable-read isolation");
    let active_before =
        active_segments_for_day(&mut *old_snapshot, source_id, claim.event_day()).await;
    assert_eq!(
        active_before,
        [
            segment_id(1),
            segment_id(2),
            segment_id(3),
            segment_id(5),
            segment_id(6),
        ]
    );
    assert_eq!(
        owner
            .publish_replacement(
                claim.run_id(),
                ReclamationGracePeriod::new(60, 1).expect("reclamation grace"),
            )
            .await
            .expect("publish compaction replacement"),
        CompactionPublicationOutcome::Published
    );
    assert_eq!(
        active_segments_for_day(&mut *old_snapshot, source_id, claim.event_day()).await,
        active_before
    );
    old_snapshot
        .rollback()
        .await
        .expect("close old query snapshot");
    assert_eq!(
        active_segments_for_day(&pool, source_id, claim.event_day()).await,
        [
            segment_id(5),
            segment_id(6),
            segment_id(201),
            segment_id(202)
        ]
    );
    assert_eq!(
        owner
            .publish_replacement(
                claim.run_id(),
                ReclamationGracePeriod::new(60, 1).expect("reclamation grace"),
            )
            .await
            .expect("resolve repeated publication"),
        CompactionPublicationOutcome::AlreadyPublished
    );
    assert_eq!(
        owner
            .fail_run(
                claim.run_id(),
                CompactionFailureCode::PublicationFailed,
                OrphanGracePeriod::new(300).expect("orphan grace"),
            )
            .await
            .expect("resolve committed run during failure handling"),
        CompactionFailureOutcome::AlreadyCommitted
    );
    assert_committed_replacement(&pool, claim.run_id().as_uuid(), 3, 2, 61).await;

    assert_eq!(
        owner
            .fail_run(
                build_only_claim.run_id(),
                CompactionFailureCode::BuildFailed,
                OrphanGracePeriod::new(300).expect("orphan grace"),
            )
            .await
            .expect("fail build-only run"),
        CompactionFailureOutcome::Failed
    );
    assert_eq!(
        owner
            .fail_run(
                build_only_claim.run_id(),
                CompactionFailureCode::InputInvalid,
                OrphanGracePeriod::new(600).expect("different orphan grace"),
            )
            .await
            .expect("repeat build-only failure"),
        CompactionFailureOutcome::AlreadyFailed
    );
    assert_failed_run(
        &pool,
        build_only_claim.run_id().as_uuid(),
        "COMPACTION_BUILD_FAILED",
        &input_segment_ids(&build_only_claim),
        0,
    )
    .await;

    owner
        .release()
        .await
        .expect("release maintenance ownership");
    let mut recovered_owner = match compaction
        .try_acquire_maintenance()
        .await
        .expect("reacquire maintenance ownership")
    {
        MaintenanceOwnership::Acquired(owner) => owner,
        MaintenanceOwnership::HeldElsewhere => panic!("released maintenance lock stayed held"),
        _ => panic!("unknown maintenance ownership outcome"),
    };
    let recovery = recovered_owner
        .recover_unfinished(
            OrphanGracePeriod::new(300).expect("recovery orphan grace"),
            CompactionRecoveryLimit::new(10).expect("recovery limit"),
        )
        .await
        .expect("recover unfinished runs");
    assert_eq!(recovery.failed_runs(), [incomplete_upload_claim.run_id()]);
    assert_failed_run(
        &pool,
        incomplete_upload_claim.run_id().as_uuid(),
        "COMPACTION_RECOVERY_FAILED",
        &input_segment_ids(&incomplete_upload_claim),
        2,
    )
    .await;
    assert!(
        recovered_owner
            .recover_unfinished(
                OrphanGracePeriod::new(300).expect("recovery orphan grace"),
                CompactionRecoveryLimit::new(10).expect("recovery limit"),
            )
            .await
            .expect("repeat unfinished recovery")
            .failed_runs()
            .is_empty()
    );
    assert_eq!(
        run_state(&pool, claim.run_id().as_uuid()).await,
        "COMMITTED"
    );
    recovered_owner
        .release()
        .await
        .expect("release recovered maintenance ownership");
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

    const fn eligible_on_day(
        segment_sequence: u64,
        object_sequence: u64,
        first_second: i64,
        day_offset: i64,
    ) -> Self {
        Self {
            segment_sequence,
            object_sequence,
            first_second,
            rows: 3,
            uncompressed_bytes: 30,
            day_offset: day_offset * 86_400,
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

fn replacement_outputs(
    root: &ManagedRoot,
    claim: &CompactionRunClaim,
    first_segment_sequence: u64,
    first_object_sequence: u64,
) -> Vec<CompactionOutputRegistration> {
    let minimum_event_time = claim
        .inputs()
        .iter()
        .map(|input| input.times().minimum_event_time())
        .min()
        .expect("claimed inputs");
    let maximum_event_time = claim
        .inputs()
        .iter()
        .map(|input| input.times().maximum_event_time())
        .max()
        .expect("claimed inputs");
    let minimum_ingestion_time = claim
        .inputs()
        .iter()
        .map(|input| input.times().minimum_ingestion_time())
        .min()
        .expect("claimed inputs");
    let maximum_ingestion_time = claim
        .inputs()
        .iter()
        .map(|input| input.times().maximum_ingestion_time())
        .max()
        .expect("claimed inputs");
    [
        OutputFixture {
            segment_sequence: first_segment_sequence,
            object_sequence: first_object_sequence,
            times: IngestionSegmentTimes::new(
                claim.event_day(),
                minimum_event_time,
                minimum_event_time + TimeDelta::seconds(3),
                minimum_ingestion_time,
                minimum_ingestion_time + TimeDelta::seconds(3),
            )
            .expect("first replacement bounds"),
            rows: 5,
            uncompressed_bytes: 50,
        },
        OutputFixture {
            segment_sequence: first_segment_sequence + 1,
            object_sequence: first_object_sequence + 1,
            times: IngestionSegmentTimes::new(
                claim.event_day(),
                minimum_event_time + TimeDelta::seconds(4),
                maximum_event_time,
                minimum_ingestion_time + TimeDelta::seconds(4),
                maximum_ingestion_time,
            )
            .expect("second replacement bounds"),
            rows: 4,
            uncompressed_bytes: 40,
        },
    ]
    .into_iter()
    .map(|fixture| {
        output_registration(
            root,
            claim.run_id(),
            claim.source_id(),
            claim.schema().id(),
            fixture,
            claim.data_expires_at(),
        )
    })
    .collect()
}

fn input_segment_ids(claim: &CompactionRunClaim) -> Vec<SegmentId> {
    claim
        .inputs()
        .iter()
        .map(|input| input.segment_id())
        .collect()
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

async fn active_segments_for_day<'executor, ExecutorType>(
    executor: ExecutorType,
    source_id: SourceId,
    event_day: NaiveDate,
) -> Vec<SegmentId>
where
    ExecutorType: Executor<'executor, Database = SqlxPostgres>,
{
    sqlx::query_scalar::<_, Uuid>(
        "SELECT segment_id FROM segments WHERE source_id = $1 AND event_day = $2 AND state = 'ACTIVE' ORDER BY segment_id",
    )
    .bind(source_id.as_uuid())
    .bind(event_day)
    .fetch_all(executor)
    .await
    .expect("list active segments for day")
    .into_iter()
    .map(SegmentId::from)
    .collect()
}

async fn assert_committed_replacement(
    pool: &PgPool,
    run_id: Uuid,
    input_count: i64,
    output_count: i64,
    grace_seconds: i64,
) {
    let run: (String, Option<String>, bool) = sqlx::query_as(
        "SELECT state, failure_code, completed_at IS NOT NULL FROM compaction_runs WHERE compaction_run_id = $1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .expect("load committed run");
    assert_eq!(run, ("COMMITTED".to_owned(), None, true));
    let superseded_inputs: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM segments AS segment
        JOIN compaction_runs AS run ON run.compaction_run_id = segment.claimed_by_compaction_run_id
        WHERE run.compaction_run_id = $1
          AND segment.state = 'SUPERSEDED'
          AND segment.retired_at = run.completed_at
          AND EXTRACT(EPOCH FROM (segment.reclaim_after - segment.retired_at))::BIGINT = $2
        "#,
    )
    .bind(run_id)
    .bind(grace_seconds)
    .fetch_one(pool)
    .await
    .expect("count superseded compaction inputs");
    assert_eq!(superseded_inputs, input_count);
    let published_outputs: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM segments AS segment
        JOIN stored_objects AS object USING (segment_id)
        JOIN compaction_runs AS run ON run.compaction_run_id = segment.produced_by_compaction_run_id
        WHERE run.compaction_run_id = $1
          AND segment.state = 'ACTIVE'
          AND object.state = 'PUBLISHED'
          AND segment.published_at = object.published_at
          AND segment.published_at = run.completed_at
        "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .expect("count published compaction outputs");
    assert_eq!(published_outputs, output_count);
}

async fn assert_failed_run(
    pool: &PgPool,
    run_id: Uuid,
    failure_code: &str,
    input_ids: &[SegmentId],
    output_count: i64,
) {
    let run: (String, Option<String>, bool) = sqlx::query_as(
        "SELECT state, failure_code, completed_at IS NOT NULL FROM compaction_runs WHERE compaction_run_id = $1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .expect("load failed run");
    assert_eq!(
        run,
        ("FAILED".to_owned(), Some(failure_code.to_owned()), true)
    );
    let input_uuids = input_ids
        .iter()
        .map(|segment_id| segment_id.as_uuid())
        .collect::<Vec<_>>();
    let released_inputs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM segments WHERE segment_id = ANY($1::uuid[]) AND state = 'ACTIVE' AND claimed_by_compaction_run_id IS NULL",
    )
    .bind(&input_uuids)
    .fetch_one(pool)
    .await
    .expect("count released compaction inputs");
    assert_eq!(released_inputs, input_ids.len() as i64);
    let abandoned_outputs: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM segments AS segment
        JOIN stored_objects AS object USING (segment_id)
        WHERE segment.produced_by_compaction_run_id = $1
          AND segment.state = 'ABANDONED'
          AND segment.retired_at IS NOT NULL
          AND EXTRACT(EPOCH FROM (segment.reclaim_after - segment.retired_at))::BIGINT = 300
          AND object.state IN ('PLANNED', 'UPLOADED')
        "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .expect("count abandoned compaction outputs");
    assert_eq!(abandoned_outputs, output_count);
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
