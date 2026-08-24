use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, TimeZone as _, Utc};
use elucid_catalog::{CatalogManifest, InputId, SchemaId, SourceId};
use elucid_metastore::{
    CatalogApplyOutcome, CatalogStore, ObjectDeletionAttempt, ObjectDeletionCompletion,
    ObjectDeletionFailure, ObjectDeletionFailureRecording, ObjectDeletionRetryDelay,
    ObjectReclamationLimit, ObjectReclamationStore, StoredObjectState, install,
};
use elucid_storage::{
    BatchId, ImmutableObjectStore, ManagedObjectKey, ManagedRoot, ObjectDeleteOutcome,
    ObjectDescriptor, ObjectFormatVersion, ObjectMediaType, ObjectUploadOutcome,
    ObjectVerificationOutcome, SegmentId, StoredObjectId, TransferLimit,
};
use object_store::aws::AmazonS3Builder;
use sqlx::postgres::{PgPool, PgPoolOptions};
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::GenericImage;
use testcontainers_modules::testcontainers::core::{ImageExt as _, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner as _;
use uuid::Uuid;

const BUCKET: &str = "elucid-reclamation-test";
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
async fn reclamation_deletes_only_due_exact_objects_and_recovers_ambiguous_attempts() {
    let test_identity = Uuid::now_v7().simple().to_string();
    let network = format!("elucid-reclamation-{test_identity}");
    let server_name = format!("elucid-reclamation-minio-{test_identity}");
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
    let object_store = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://{minio_host}:{minio_port}"))
            .with_bucket_name(BUCKET)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .expect("build S3 client"),
    );
    let objects = ImmutableObjectStore::new(object_store);

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
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&format!(
            "postgresql://postgres:postgres@{postgres_host}:{postgres_port}/postgres"
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
    let root = ManagedRoot::parse("reclamation-test").expect("managed root");
    let transfer_limit = TransferLimit::new(1_024).expect("transfer limit");
    let now = postgres_now(&pool).await;
    let fixtures = FixtureEnvironment {
        pool: &pool,
        objects: &objects,
        root: &root,
        transfer_limit,
        source_id,
        schema_id,
        input_id,
        now,
    };

    let expired = fixtures
        .insert_segment(1, 101, SegmentFixtureState::ExpiredDue)
        .await;
    let superseded = fixtures
        .insert_segment(2, 102, SegmentFixtureState::SupersededDue)
        .await;
    let abandoned = fixtures
        .insert_segment(3, 103, SegmentFixtureState::AbandonedDue)
        .await;
    let due_dead_letter = fixtures
        .insert_dead_letter(201, 301, RetentionDeadline::Due)
        .await;
    let integrity_dead_letter = fixtures
        .insert_dead_letter(203, 303, RetentionDeadline::Due)
        .await;
    let active = fixtures
        .insert_segment(4, 104, SegmentFixtureState::Active)
        .await;
    let future_expired = fixtures
        .insert_segment(5, 105, SegmentFixtureState::ExpiredFuture)
        .await;
    let future_dead_letter = fixtures
        .insert_dead_letter(202, 302, RetentionDeadline::Future)
        .await;

    let reclamation = ObjectReclamationStore::new(pool.clone());
    let retry_delay = ObjectDeletionRetryDelay::new(60).expect("retry delay");
    let first = reclamation
        .claim(
            retry_delay,
            ObjectReclamationLimit::new(3).expect("claim limit"),
        )
        .await
        .expect("claim first bounded batch");
    assert_eq!(first.len(), 3);
    assert!(
        first
            .iter()
            .all(|claim| claim.attempt() == ObjectDeletionAttempt::Initial)
    );
    let second = reclamation
        .claim(
            retry_delay,
            ObjectReclamationLimit::new(3).expect("claim limit"),
        )
        .await
        .expect("claim remaining eligible objects");
    assert_eq!(second.len(), 2);
    assert!(
        second
            .iter()
            .all(|claim| claim.attempt() == ObjectDeletionAttempt::Initial)
    );

    let claims = first
        .into_iter()
        .chain(second)
        .map(|claim| (claim.descriptor().key().object_id(), claim))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        claims.keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([
            expired.object_id,
            superseded.object_id,
            abandoned.object_id,
            due_dead_letter.object_id,
            integrity_dead_letter.object_id,
        ])
    );

    let abandoned_claim = claims.get(&abandoned.object_id).expect("abandoned claim");
    assert_eq!(
        objects
            .delete_exact(abandoned_claim.descriptor())
            .await
            .expect("delete absent planned object"),
        ObjectDeleteOutcome::AlreadyAbsent
    );
    assert_eq!(
        reclamation
            .record_deleted(abandoned_claim)
            .await
            .expect("record absent object deletion"),
        ObjectDeletionCompletion::Deleted
    );

    let expired_claim = claims.get(&expired.object_id).expect("expired claim");
    assert_eq!(
        objects
            .delete_exact(expired_claim.descriptor())
            .await
            .expect("delete expired object before simulated crash"),
        ObjectDeleteOutcome::Deleted
    );

    let superseded_claim = claims.get(&superseded.object_id).expect("superseded claim");
    assert_eq!(
        reclamation
            .record_failure(superseded_claim, ObjectDeletionFailure::Retryable)
            .await
            .expect("record transient deletion failure"),
        ObjectDeletionFailureRecording::Recorded
    );
    assert_eq!(
        object_last_error(&pool, superseded.object_id)
            .await
            .as_deref(),
        Some("OBJECT_DELETE_FAILED")
    );

    let dead_letter_claim = claims
        .get(&due_dead_letter.object_id)
        .expect("dead-letter claim");
    assert_eq!(
        objects
            .delete_exact(dead_letter_claim.descriptor())
            .await
            .expect("delete due dead letter"),
        ObjectDeleteOutcome::Deleted
    );
    assert_eq!(
        reclamation
            .record_deleted(dead_letter_claim)
            .await
            .expect("record dead-letter deletion"),
        ObjectDeletionCompletion::Deleted
    );

    let integrity_claim = claims
        .get(&integrity_dead_letter.object_id)
        .expect("integrity-failure claim");
    assert_eq!(
        reclamation
            .record_failure(integrity_claim, ObjectDeletionFailure::Integrity)
            .await
            .expect("record permanent integrity failure"),
        ObjectDeletionFailureRecording::Recorded
    );
    assert_eq!(
        object_last_error(&pool, integrity_dead_letter.object_id)
            .await
            .as_deref(),
        Some("OBJECT_INTEGRITY_ERROR")
    );

    assert!(
        reclamation
            .claim(
                retry_delay,
                ObjectReclamationLimit::new(10).expect("claim limit")
            )
            .await
            .expect("respect retry delay")
            .is_empty()
    );
    make_retry_due(
        &pool,
        &[
            expired.object_id,
            superseded.object_id,
            integrity_dead_letter.object_id,
        ],
    )
    .await;
    let retries = reclamation
        .claim(
            retry_delay,
            ObjectReclamationLimit::new(10).expect("claim limit"),
        )
        .await
        .expect("retry interrupted and failed deletion");
    assert_eq!(retries.len(), 2);
    assert!(
        retries
            .iter()
            .all(|claim| claim.attempt() == ObjectDeletionAttempt::Retry)
    );
    let retries = retries
        .into_iter()
        .map(|claim| (claim.descriptor().key().object_id(), claim))
        .collect::<BTreeMap<_, _>>();

    let expired_retry = retries.get(&expired.object_id).expect("expired retry");
    assert_eq!(
        objects
            .delete_exact(expired_retry.descriptor())
            .await
            .expect("confirm object absent after ambiguous deletion"),
        ObjectDeleteOutcome::AlreadyAbsent
    );
    assert_eq!(
        reclamation
            .record_deleted(expired_retry)
            .await
            .expect("finish ambiguous deletion"),
        ObjectDeletionCompletion::Deleted
    );
    assert_eq!(
        reclamation
            .record_deleted(expired_retry)
            .await
            .expect("repeat deletion completion"),
        ObjectDeletionCompletion::AlreadyDeleted
    );

    let superseded_retry = retries
        .get(&superseded.object_id)
        .expect("superseded retry");
    assert_eq!(
        objects
            .delete_exact(superseded_retry.descriptor())
            .await
            .expect("delete superseded object on retry"),
        ObjectDeleteOutcome::Deleted
    );
    assert_eq!(
        reclamation
            .record_deleted(superseded_retry)
            .await
            .expect("record retried deletion"),
        ObjectDeletionCompletion::Deleted
    );

    for object_id in [
        expired.object_id,
        superseded.object_id,
        abandoned.object_id,
        due_dead_letter.object_id,
    ] {
        assert_eq!(
            object_state(&pool, object_id).await,
            StoredObjectState::Deleted
        );
    }
    for retained in [&active, &future_expired, &future_dead_letter] {
        assert_eq!(
            object_state(&pool, retained.object_id).await,
            StoredObjectState::Published
        );
        assert_eq!(
            objects
                .verify(&retained.descriptor)
                .await
                .expect("verify retained exact object"),
            ObjectVerificationOutcome::Verified
        );
    }
    assert_eq!(
        object_state(&pool, integrity_dead_letter.object_id).await,
        StoredObjectState::DeletePending
    );
    assert_eq!(
        objects
            .verify(&integrity_dead_letter.descriptor)
            .await
            .expect("verify object retained after integrity failure"),
        ObjectVerificationOutcome::Verified
    );
}

#[derive(Clone, Debug)]
struct FixtureObject {
    object_id: StoredObjectId,
    descriptor: ObjectDescriptor,
}

#[derive(Clone, Copy, Debug)]
enum SegmentFixtureState {
    Active,
    ExpiredDue,
    ExpiredFuture,
    SupersededDue,
    AbandonedDue,
}

#[derive(Clone, Copy, Debug)]
enum RetentionDeadline {
    Due,
    Future,
}

struct FixtureEnvironment<'a> {
    pool: &'a PgPool,
    objects: &'a ImmutableObjectStore,
    root: &'a ManagedRoot,
    transfer_limit: TransferLimit,
    source_id: SourceId,
    schema_id: SchemaId,
    input_id: InputId,
    now: DateTime<Utc>,
}

impl FixtureEnvironment<'_> {
    async fn insert_segment(
        &self,
        segment_sequence: u64,
        object_sequence: u64,
        state: SegmentFixtureState,
    ) -> FixtureObject {
        let segment_id = SegmentId::from(uuid(segment_sequence));
        let object_id = StoredObjectId::from(uuid(object_sequence));
        let bytes = Bytes::from(format!("parquet fixture {segment_sequence}"));
        let descriptor = ObjectDescriptor::for_bytes(
            ManagedObjectKey::parquet(self.root, segment_id, object_id),
            &bytes,
            ObjectMediaType::ParquetData,
            ObjectFormatVersion::new(1).expect("format version"),
        )
        .expect("segment descriptor");
        let created_at = self.now - ChronoDuration::minutes(30);
        let published_at = self.now - ChronoDuration::minutes(20);
        let retired_at = self.now - ChronoDuration::minutes(10);
        let (state_name, data_expires_at, published_at, retired_at, reclaim_after, claim_id) =
            match state {
                SegmentFixtureState::Active => (
                    "ACTIVE",
                    Some(self.now + ChronoDuration::hours(1)),
                    Some(published_at),
                    None,
                    None,
                    None,
                ),
                SegmentFixtureState::ExpiredDue => (
                    "EXPIRED",
                    Some(retired_at),
                    Some(published_at),
                    Some(retired_at),
                    Some(self.now - ChronoDuration::minutes(5)),
                    None,
                ),
                SegmentFixtureState::ExpiredFuture => (
                    "EXPIRED",
                    Some(retired_at),
                    Some(published_at),
                    Some(retired_at),
                    Some(self.now + ChronoDuration::hours(1)),
                    None,
                ),
                SegmentFixtureState::SupersededDue => {
                    let claim_id = uuid(900 + segment_sequence);
                    insert_committed_compaction(
                        self.pool,
                        self.source_id,
                        self.schema_id,
                        claim_id,
                        created_at,
                        self.now,
                    )
                    .await;
                    (
                        "SUPERSEDED",
                        Some(retired_at),
                        Some(published_at),
                        Some(retired_at),
                        Some(self.now - ChronoDuration::minutes(4)),
                        Some(claim_id),
                    )
                }
                SegmentFixtureState::AbandonedDue => (
                    "ABANDONED",
                    None,
                    None,
                    Some(retired_at),
                    Some(self.now - ChronoDuration::minutes(6)),
                    None,
                ),
            };
        let event_day = NaiveDate::from_ymd_opt(2026, 8, 20).expect("event day");
        let event_time = Utc
            .with_ymd_and_hms(2026, 8, 20, 10, 0, 0)
            .single()
            .expect("event time");
        sqlx::query(
            r#"
            INSERT INTO segments (
                segment_id, source_id, schema_id, origin, claimed_by_compaction_run_id,
                event_day, minimum_event_time, maximum_event_time, minimum_ingestion_time,
                maximum_ingestion_time, row_count, uncompressed_bytes, data_expires_at, state,
                published_at, retired_at, reclaim_after, created_at, updated_at
            ) VALUES (
                $1, $2, $3, 'INGESTION', $4, $5, $6, $6, $6, $6, 1, 128, $7, $8,
                $9, $10, $11, $12, $13
            )
            "#,
        )
        .bind(segment_id.as_uuid())
        .bind(self.source_id.as_uuid())
        .bind(self.schema_id.as_uuid())
        .bind(claim_id)
        .bind(event_day)
        .bind(event_time)
        .bind(data_expires_at)
        .bind(state_name)
        .bind(published_at)
        .bind(retired_at)
        .bind(reclaim_after)
        .bind(created_at)
        .bind(self.now)
        .execute(self.pool)
        .await
        .expect("insert segment fixture");

        let object_state = match published_at {
            Some(published_at) => StoredObjectFixtureState::Published {
                published_at,
                retention_deadline: None,
            },
            None => StoredObjectFixtureState::Planned,
        };
        self.insert_stored_object(
            &descriptor,
            StoredObjectFixtureOwner::Segment(segment_id),
            object_state,
            created_at,
        )
        .await;
        if published_at.is_some() {
            assert_eq!(
                self.objects
                    .upload(&descriptor, bytes, self.transfer_limit)
                    .await
                    .expect("upload segment fixture"),
                ObjectUploadOutcome::Uploaded
            );
        }
        FixtureObject {
            object_id,
            descriptor,
        }
    }

    async fn insert_dead_letter(
        &self,
        batch_sequence: u64,
        object_sequence: u64,
        deadline: RetentionDeadline,
    ) -> FixtureObject {
        let batch_id = BatchId::try_from(uuid(batch_sequence)).expect("batch identity");
        let object_id = StoredObjectId::from(uuid(object_sequence));
        let bytes = Bytes::from_static(b"{\"error\":\"invalid\"}\n");
        let descriptor = ObjectDescriptor::for_bytes(
            ManagedObjectKey::dead_letter(self.root, batch_id, object_id),
            &bytes,
            ObjectMediaType::DeadLetter,
            ObjectFormatVersion::new(1).expect("format version"),
        )
        .expect("dead-letter descriptor");
        let created_at = self.now - ChronoDuration::minutes(30);
        let published_at = self.now - ChronoDuration::minutes(20);
        let retention_deadline = match deadline {
            RetentionDeadline::Due => self.now - ChronoDuration::minutes(5),
            RetentionDeadline::Future => self.now + ChronoDuration::hours(1),
        };
        self.insert_stored_object(
            &descriptor,
            StoredObjectFixtureOwner::DeadLetter {
                input_id: self.input_id,
                batch_id,
            },
            StoredObjectFixtureState::Published {
                published_at,
                retention_deadline: Some(retention_deadline),
            },
            created_at,
        )
        .await;
        assert_eq!(
            self.objects
                .upload(&descriptor, bytes, self.transfer_limit)
                .await
                .expect("upload dead-letter fixture"),
            ObjectUploadOutcome::Uploaded
        );
        FixtureObject {
            object_id,
            descriptor,
        }
    }

    async fn insert_stored_object(
        &self,
        descriptor: &ObjectDescriptor,
        owner: StoredObjectFixtureOwner,
        state: StoredObjectFixtureState,
        created_at: DateTime<Utc>,
    ) {
        let (kind, segment_id, input_id, batch_id) = match owner {
            StoredObjectFixtureOwner::Segment(segment_id) => {
                ("PARQUET_DATA", Some(segment_id.as_uuid()), None, None)
            }
            StoredObjectFixtureOwner::DeadLetter { input_id, batch_id } => (
                "DEAD_LETTER",
                None,
                Some(input_id.as_uuid()),
                Some(batch_id.as_uuid()),
            ),
        };
        let (state_name, uploaded_at, published_at, retention_deadline) = match state {
            StoredObjectFixtureState::Planned => ("PLANNED", None, None, None),
            StoredObjectFixtureState::Published {
                published_at,
                retention_deadline,
            } => (
                "PUBLISHED",
                Some(published_at - ChronoDuration::minutes(1)),
                Some(published_at),
                retention_deadline,
            ),
        };
        sqlx::query(
            r#"
            INSERT INTO stored_objects (
                object_id, kind, segment_id, input_id, batch_id, object_key,
                expected_byte_size, blake3_digest, media_type, format_version, state,
                uploaded_at, published_at, retention_deadline, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
        )
        .bind(descriptor.key().object_id().as_uuid())
        .bind(kind)
        .bind(segment_id)
        .bind(input_id)
        .bind(batch_id)
        .bind(descriptor.key().as_str())
        .bind(i64::try_from(descriptor.expected_byte_size().get()).expect("fixture size fits i64"))
        .bind(descriptor.digest().as_bytes().as_slice())
        .bind(descriptor.media_type().as_str())
        .bind(i64::try_from(descriptor.format_version().get()).expect("format version fits i64"))
        .bind(state_name)
        .bind(uploaded_at)
        .bind(published_at)
        .bind(retention_deadline)
        .bind(created_at)
        .bind(self.now)
        .execute(self.pool)
        .await
        .expect("insert stored-object fixture");
    }
}

#[derive(Clone, Copy)]
enum StoredObjectFixtureOwner {
    Segment(SegmentId),
    DeadLetter {
        input_id: InputId,
        batch_id: BatchId,
    },
}

#[derive(Clone, Copy)]
enum StoredObjectFixtureState {
    Planned,
    Published {
        published_at: DateTime<Utc>,
        retention_deadline: Option<DateTime<Utc>>,
    },
}

async fn insert_committed_compaction(
    pool: &PgPool,
    source_id: SourceId,
    schema_id: SchemaId,
    run_id: Uuid,
    created_at: DateTime<Utc>,
    now: DateTime<Utc>,
) {
    sqlx::query(
        r#"
        INSERT INTO compaction_runs (
            compaction_run_id, source_id, schema_id, event_day, state,
            created_at, updated_at, completed_at
        ) VALUES ($1, $2, $3, $4, 'COMMITTED', $5, $6, $6)
        "#,
    )
    .bind(run_id)
    .bind(source_id.as_uuid())
    .bind(schema_id.as_uuid())
    .bind(NaiveDate::from_ymd_opt(2026, 8, 20).expect("event day"))
    .bind(created_at)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert committed compaction fixture");
}

async fn postgres_now(pool: &PgPool) -> DateTime<Utc> {
    sqlx::query_scalar("SELECT CURRENT_TIMESTAMP")
        .fetch_one(pool)
        .await
        .expect("read PostgreSQL time")
}

async fn make_retry_due(pool: &PgPool, object_ids: &[StoredObjectId]) {
    let object_ids = object_ids
        .iter()
        .map(|object_id| object_id.as_uuid())
        .collect::<Vec<_>>();
    sqlx::query(
        "UPDATE stored_objects SET updated_at = CURRENT_TIMESTAMP - INTERVAL '2 minutes' WHERE object_id = ANY($1::uuid[])",
    )
    .bind(&object_ids)
    .execute(pool)
    .await
    .expect("make deletion retries due");
}

async fn object_last_error(pool: &PgPool, object_id: StoredObjectId) -> Option<String> {
    sqlx::query_scalar("SELECT last_error_code FROM stored_objects WHERE object_id = $1")
        .bind(object_id.as_uuid())
        .fetch_one(pool)
        .await
        .expect("load object error code")
}

async fn object_state(pool: &PgPool, object_id: StoredObjectId) -> StoredObjectState {
    let state: String = sqlx::query_scalar("SELECT state FROM stored_objects WHERE object_id = $1")
        .bind(object_id.as_uuid())
        .fetch_one(pool)
        .await
        .expect("load object state");
    match state.as_str() {
        "PUBLISHED" => StoredObjectState::Published,
        "DELETE_PENDING" => StoredObjectState::DeletePending,
        "DELETED" => StoredObjectState::Deleted,
        other => panic!("unexpected object state {other}"),
    }
}

fn uuid(sequence: u64) -> Uuid {
    Uuid::from_u128(0x019d_0000_0000_7000_8000_0000_0000_0000 | u128::from(sequence))
}
