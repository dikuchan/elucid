use std::num::NonZeroU64;
use std::time::Duration;

use chrono::{NaiveDate, TimeZone as _, Utc};
use elucid_catalog::CatalogManifest;
use elucid_metastore::{
    AbandonmentOutcome, CatalogApplyOutcome, CatalogStore, DeadLetterRegistration,
    IngestionSegmentRegistration, IngestionSegmentTimes, ObjectPublicationState,
    ObjectUploadRecordOutcome, OrphanGracePeriod, PublicationErrorKind, PublicationOutcome,
    PublicationStore, ReconciliationLimit, RegistrationOutcome, RetentionPeriod, StoredObjectState,
    install,
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
async fn ingestion_and_dead_letter_publication_are_atomic_retryable_and_postgres_timed() {
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

    let publication = PublicationStore::new(pool.clone());
    let root = ManagedRoot::parse("publication-test").expect("managed root");
    let segment_id = SegmentId::from(uuid(1));
    let segment_object_id = StoredObjectId::from(uuid(2));
    let segment_descriptor = descriptor(
        ManagedObjectKey::parquet(&root, segment_id, segment_object_id),
        b"parquet bytes",
        ObjectMediaType::ParquetData,
    );
    let event_day = NaiveDate::from_ymd_opt(2026, 8, 20).expect("event day");
    let times = IngestionSegmentTimes::new(
        event_day,
        Utc.with_ymd_and_hms(2026, 8, 20, 10, 0, 0)
            .single()
            .expect("minimum event time"),
        Utc.with_ymd_and_hms(2026, 8, 20, 10, 1, 0)
            .single()
            .expect("maximum event time"),
        Utc.with_ymd_and_hms(2026, 8, 20, 10, 2, 0)
            .single()
            .expect("minimum ingestion time"),
        Utc.with_ymd_and_hms(2026, 8, 20, 10, 2, 1)
            .single()
            .expect("maximum ingestion time"),
    )
    .expect("valid segment times");
    let segment = IngestionSegmentRegistration::new(
        segment_id,
        source.id(),
        source.active_schema().id(),
        times,
        NonZeroU64::new(2).expect("positive rows"),
        NonZeroU64::new(256).expect("positive bytes"),
        segment_descriptor.clone(),
    )
    .expect("valid segment registration");

    assert_eq!(
        publication
            .ingestion_output_state(&segment)
            .await
            .expect("resolve unregistered segment"),
        ObjectPublicationState::Unregistered
    );

    assert_eq!(
        publication
            .register_ingestion_segment(&segment)
            .await
            .expect("register segment"),
        RegistrationOutcome::Registered
    );
    assert_eq!(
        publication
            .ingestion_output_state(&segment)
            .await
            .expect("resolve planned segment"),
        ObjectPublicationState::Planned
    );
    assert_eq!(visible_segment_count(&pool, segment_id).await, 0);
    assert_eq!(
        publication
            .stored_object_state(segment_object_id)
            .await
            .expect("load planned object"),
        Some(StoredObjectState::Planned)
    );
    assert_eq!(
        publication
            .register_ingestion_segment(&segment)
            .await
            .expect("retry segment registration"),
        RegistrationOutcome::AlreadyRegistered
    );

    let conflicting_segment = IngestionSegmentRegistration::new(
        segment_id,
        source.id(),
        source.active_schema().id(),
        times,
        NonZeroU64::new(3).expect("positive rows"),
        NonZeroU64::new(256).expect("positive bytes"),
        segment_descriptor.clone(),
    )
    .expect("locally valid conflicting segment");
    let error = publication
        .register_ingestion_segment(&conflicting_segment)
        .await
        .expect_err("changed immutable segment metadata must conflict");
    assert_eq!(error.kind(), PublicationErrorKind::Conflict);
    assert_eq!(visible_segment_count(&pool, segment_id).await, 0);

    let mismatched_segment_descriptor = descriptor(
        ManagedObjectKey::parquet(&root, segment_id, segment_object_id),
        b"different bytes",
        ObjectMediaType::ParquetData,
    );
    let error = publication
        .record_verified_upload(&mismatched_segment_descriptor)
        .await
        .expect_err("verified bytes must match registered immutable metadata");
    assert_eq!(error.kind(), PublicationErrorKind::Conflict);
    assert_eq!(
        publication
            .stored_object_state(segment_object_id)
            .await
            .expect("reload planned object"),
        Some(StoredObjectState::Planned)
    );

    assert_eq!(
        publication
            .record_verified_upload(&segment_descriptor)
            .await
            .expect("record verified segment upload"),
        ObjectUploadRecordOutcome::Recorded
    );
    assert_eq!(
        publication
            .ingestion_output_state(&segment)
            .await
            .expect("resolve uploaded segment"),
        ObjectPublicationState::Uploaded
    );
    assert_eq!(
        publication
            .record_verified_upload(&segment_descriptor)
            .await
            .expect("retry verified segment upload"),
        ObjectUploadRecordOutcome::AlreadyRecorded
    );
    assert_eq!(
        publication
            .publish_ingestion_segment(segment_id, RetentionPeriod::new(3_600).expect("retention"))
            .await
            .expect("publish segment"),
        PublicationOutcome::Published
    );
    assert_eq!(
        publication
            .ingestion_output_state(&segment)
            .await
            .expect("resolve published segment"),
        ObjectPublicationState::Published
    );
    assert_eq!(visible_segment_count(&pool, segment_id).await, 1);
    assert_eq!(segment_retention_seconds(&pool, segment_id).await, 3_600);
    assert_eq!(
        publication
            .record_verified_upload(&segment_descriptor)
            .await
            .expect("resolve upload recording after publication"),
        ObjectUploadRecordOutcome::AlreadyRecorded
    );
    assert_eq!(
        publication
            .publish_ingestion_segment(segment_id, RetentionPeriod::new(7_200).expect("retention"))
            .await
            .expect("retry segment publication"),
        PublicationOutcome::AlreadyPublished
    );
    assert_eq!(segment_retention_seconds(&pool, segment_id).await, 3_600);

    let input = &source.inputs()[0];
    let batch_id = BatchId::try_from(uuid(3)).expect("batch identity");
    let dead_letter_object_id = StoredObjectId::from(uuid(4));
    let dead_letter_descriptor = descriptor(
        ManagedObjectKey::dead_letter(&root, batch_id, dead_letter_object_id),
        b"{\"error\":\"invalid\"}\n",
        ObjectMediaType::DeadLetter,
    );
    let dead_letter =
        DeadLetterRegistration::new(input.id(), batch_id, dead_letter_descriptor.clone())
            .expect("valid dead-letter registration");
    assert_eq!(
        publication
            .dead_letter_output_state(&dead_letter)
            .await
            .expect("resolve unregistered dead letter"),
        ObjectPublicationState::Unregistered
    );
    assert_eq!(
        publication
            .register_dead_letter(&dead_letter)
            .await
            .expect("register dead letter"),
        RegistrationOutcome::Registered
    );
    assert_eq!(
        publication
            .dead_letter_output_state(&dead_letter)
            .await
            .expect("resolve planned dead letter"),
        ObjectPublicationState::Planned
    );
    assert_eq!(
        publication
            .register_dead_letter(&dead_letter)
            .await
            .expect("retry dead-letter registration"),
        RegistrationOutcome::AlreadyRegistered
    );
    let error = publication
        .publish_dead_letter(
            dead_letter_object_id,
            RetentionPeriod::new(600).expect("retention"),
        )
        .await
        .expect_err("planned object must not publish");
    assert_eq!(error.kind(), PublicationErrorKind::Conflict);
    assert_eq!(
        publication
            .record_verified_upload(&dead_letter_descriptor)
            .await
            .expect("record verified dead-letter upload"),
        ObjectUploadRecordOutcome::Recorded
    );
    assert_eq!(
        publication
            .dead_letter_output_state(&dead_letter)
            .await
            .expect("resolve uploaded dead letter"),
        ObjectPublicationState::Uploaded
    );
    assert_eq!(
        publication
            .publish_dead_letter(
                dead_letter_object_id,
                RetentionPeriod::new(600).expect("retention"),
            )
            .await
            .expect("publish dead letter"),
        PublicationOutcome::Published
    );
    assert_eq!(
        publication
            .dead_letter_output_state(&dead_letter)
            .await
            .expect("resolve published dead letter"),
        ObjectPublicationState::Published
    );
    assert_eq!(
        dead_letter_retention_seconds(&pool, dead_letter_object_id).await,
        600
    );
    assert_eq!(
        publication
            .publish_dead_letter(
                dead_letter_object_id,
                RetentionPeriod::new(1_200).expect("retention"),
            )
            .await
            .expect("retry dead-letter publication"),
        PublicationOutcome::AlreadyPublished
    );
    assert_eq!(
        dead_letter_retention_seconds(&pool, dead_letter_object_id).await,
        600
    );

    let rebuild_segment_id = SegmentId::from(uuid(35));
    let rebuild_segment = IngestionSegmentRegistration::new(
        rebuild_segment_id,
        source.id(),
        source.active_schema().id(),
        times,
        NonZeroU64::new(1).expect("positive rows"),
        NonZeroU64::new(128).expect("positive bytes"),
        descriptor(
            ManagedObjectKey::parquet(&root, rebuild_segment_id, StoredObjectId::from(uuid(36))),
            b"missing staged parquet",
            ObjectMediaType::ParquetData,
        ),
    )
    .expect("rebuild segment registration");
    publication
        .register_ingestion_segment(&rebuild_segment)
        .await
        .expect("register rebuild segment");
    assert_eq!(
        publication
            .abandon_ingestion_output(
                &rebuild_segment,
                OrphanGracePeriod::new(300).expect("orphan grace"),
            )
            .await
            .expect("abandon missing rebuild output"),
        AbandonmentOutcome::Abandoned
    );
    assert_eq!(
        publication
            .abandon_ingestion_output(
                &rebuild_segment,
                OrphanGracePeriod::new(600).expect("orphan grace"),
            )
            .await
            .expect("retry output abandonment"),
        AbandonmentOutcome::AlreadyAbandoned
    );

    let orphan_segment_id = SegmentId::from(uuid(40));
    let orphan_segment_object_id = StoredObjectId::from(uuid(41));
    let orphan_segment = IngestionSegmentRegistration::new(
        orphan_segment_id,
        source.id(),
        source.active_schema().id(),
        times,
        NonZeroU64::new(1).expect("positive rows"),
        NonZeroU64::new(128).expect("positive bytes"),
        descriptor(
            ManagedObjectKey::parquet(&root, orphan_segment_id, orphan_segment_object_id),
            b"orphan parquet",
            ObjectMediaType::ParquetData,
        ),
    )
    .expect("orphan segment registration");
    publication
        .register_ingestion_segment(&orphan_segment)
        .await
        .expect("register orphan segment");
    let orphan_batch_id = BatchId::try_from(uuid(42)).expect("orphan batch identity");
    let orphan_dead_letter_object_id = StoredObjectId::from(uuid(43));
    let orphan_dead_letter = DeadLetterRegistration::new(
        input.id(),
        orphan_batch_id,
        descriptor(
            ManagedObjectKey::dead_letter(&root, orphan_batch_id, orphan_dead_letter_object_id),
            b"{\"error\":\"orphan\"}\n",
            ObjectMediaType::DeadLetter,
        ),
    )
    .expect("orphan dead-letter registration");
    publication
        .register_dead_letter(&orphan_dead_letter)
        .await
        .expect("register orphan dead letter");

    let reconciled = publication
        .reconcile_unreferenced_outputs(
            &[segment_id],
            &[dead_letter_object_id],
            OrphanGracePeriod::new(300).expect("orphan grace"),
            ReconciliationLimit::new(10).expect("reconciliation limit"),
        )
        .await
        .expect("reconcile unreferenced outputs");
    assert_eq!(reconciled.abandoned_segments(), &[orphan_segment_id]);
    assert_eq!(
        reconciled.scheduled_dead_letters(),
        &[orphan_dead_letter_object_id]
    );
    assert_eq!(
        publication
            .ingestion_output_state(&orphan_segment)
            .await
            .expect("resolve abandoned segment"),
        ObjectPublicationState::Abandoned
    );
    assert_eq!(
        publication
            .dead_letter_output_state(&orphan_dead_letter)
            .await
            .expect("resolve scheduled dead letter"),
        ObjectPublicationState::Abandoned
    );
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

fn uuid(sequence: u64) -> Uuid {
    Uuid::from_u128(0x019d_0000_0000_7000_8000_0000_0000_0000 | u128::from(sequence))
}

async fn visible_segment_count(pool: &PgPool, segment_id: SegmentId) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM segments AS segment
        JOIN stored_objects AS object USING (segment_id)
        WHERE segment.segment_id = $1
          AND segment.state = 'ACTIVE'
          AND object.state = 'PUBLISHED'
        "#,
    )
    .bind(segment_id.as_uuid())
    .fetch_one(pool)
    .await
    .expect("count visible segments")
}

async fn segment_retention_seconds(pool: &PgPool, segment_id: SegmentId) -> i64 {
    sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (data_expires_at - published_at))::BIGINT FROM segments WHERE segment_id = $1",
    )
    .bind(segment_id.as_uuid())
    .fetch_one(pool)
    .await
    .expect("load segment retention")
}

async fn dead_letter_retention_seconds(pool: &PgPool, object_id: StoredObjectId) -> i64 {
    sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (retention_deadline - published_at))::BIGINT FROM stored_objects WHERE object_id = $1 AND state = 'PUBLISHED'",
    )
    .bind(object_id.as_uuid())
    .fetch_one(pool)
    .await
    .expect("load dead-letter retention")
}
