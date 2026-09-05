use std::sync::Arc;
use std::time::Duration;

use elucid_catalog::{CatalogManifest, Source, SourceName};
use elucid_core::{CodedError, ErrorCode};
use elucid_metastore::{CatalogApplyOutcome, CatalogPersistenceErrorKind, CatalogStore, install};
use sqlx::postgres::{PgPool, PgPoolOptions};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ImageExt as _, runners::AsyncRunner as _};

const BASE_MANIFEST: &str = r#"
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
          description: Rendered message
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

const EXTENDED_MANIFEST: &str = r#"
format_version: 1
source:
  name: logs
  display_name: Security logs
  active_schema_version: 2
  schemas:
    - version: 1
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
          description: Rendered message
    - version: 2
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
          description: Rendered message
        - name: status
          logical_type: int32
          nullability: NULLABLE
          historical_remainder_pointer: /status
  inputs:
    - name: vector
      active_ingestion_profile_revision: 2
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
        - revision: 2
          target_schema_version: 2
          maximum_record_bytes: 1048576
          event_time:
            json_pointer: /timestamp
            format: RFC3339
          mappings:
            - target_field: message
              json_pointer: /message
            - target_field: status
              json_pointer: /status
"#;

#[tokio::test]
#[ignore = "requires Docker"]
async fn catalog_apply_load_and_snapshot_failures_follow_the_durable_contract() {
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
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&url)
        .await
        .expect("connect to PostgreSQL");
    install(&pool).await.expect("install metastore");

    let store = CatalogStore::load(pool.clone())
        .await
        .expect("load empty catalog");
    assert!(store.snapshot().is_empty());

    let base = decode(BASE_MANIFEST);
    let created = expect_applied(store.apply(&base).await.expect("apply new catalog"));
    let identities = CatalogIdentities::from_source(&created);
    let row_counts = catalog_row_counts(&pool).await;
    assert_eq!(row_counts, [1, 1, 1, 1]);

    let unchanged = store.apply(&base).await.expect("reapply unchanged catalog");
    let unchanged = expect_unchanged(unchanged);
    identities.assert_preserved(&unchanged);
    assert_eq!(catalog_row_counts(&pool).await, row_counts);

    let old_snapshot = store.snapshot();
    let extended = decode(EXTENDED_MANIFEST);
    let updated = expect_applied(store.apply(&extended).await.expect("extend catalog"));
    identities.assert_preserved(&updated);
    assert_eq!(updated.active_schema().version().get(), 2);
    assert_eq!(
        updated.inputs()[0]
            .active_profile_revision()
            .revision()
            .get(),
        2
    );
    assert_eq!(updated.display_name(), "Security logs");
    assert_eq!(source(&old_snapshot).active_schema().version().get(), 1);
    assert_eq!(source(&store.snapshot()).active_schema().version().get(), 2);

    let restarted = CatalogStore::load(pool.clone())
        .await
        .expect("reload catalog after restart");
    let restarted_snapshot = restarted.snapshot();
    let restarted_source = source(&restarted_snapshot);
    identities.assert_preserved(restarted_source);
    assert_eq!(restarted_source.schemas().len(), 2);
    assert_eq!(restarted_source.inputs()[0].profile_revisions().len(), 2);

    let conflict = decode(&EXTENDED_MANIFEST.replace(
        "description: Rendered message",
        "description: Rewritten message",
    ));
    let error = store
        .apply(&conflict)
        .await
        .expect_err("immutable history rewrite must conflict");
    assert_eq!(error.kind(), CatalogPersistenceErrorKind::Conflict);
    assert_eq!(error.error_code(), ErrorCode::MetastoreConflict);
    assert_eq!(catalog_row_counts(&pool).await, [1, 2, 1, 2]);
    assert_eq!(source(&store.snapshot()).active_schema().version().get(), 2);

    sqlx::query(
        "UPDATE schema_versions SET definition = jsonb_set(definition, '{version}', '999') WHERE version = 1",
    )
    .execute(&pool)
    .await
    .expect("corrupt stored definition");
    let error = store
        .refresh()
        .await
        .expect_err("corrupt durable catalog must not replace the snapshot");
    assert_eq!(error.kind(), CatalogPersistenceErrorKind::Corrupt);
    assert_eq!(error.error_code(), ErrorCode::MetastoreCorrupt);
    assert_eq!(source(&store.snapshot()).active_schema().version().get(), 2);

    container
        .stop_with_timeout(Some(1))
        .await
        .expect("stop PostgreSQL container");
    let error = store
        .refresh()
        .await
        .expect_err("dependency outage must be reported");
    assert_eq!(error.kind(), CatalogPersistenceErrorKind::Unavailable);
    assert_eq!(error.error_code(), ErrorCode::MetastoreUnavailable);
}

fn decode(document: &str) -> CatalogManifest {
    CatalogManifest::decode(document.as_bytes()).expect("catalog manifest is valid")
}

fn expect_applied(outcome: CatalogApplyOutcome) -> Arc<Source> {
    match outcome {
        CatalogApplyOutcome::Applied { source } => source,
        CatalogApplyOutcome::Unchanged { .. } => panic!("catalog unexpectedly remained unchanged"),
        _ => panic!("unknown catalog application outcome"),
    }
}

fn expect_unchanged(outcome: CatalogApplyOutcome) -> Arc<Source> {
    match outcome {
        CatalogApplyOutcome::Unchanged { source } => source,
        CatalogApplyOutcome::Applied { .. } => panic!("catalog unexpectedly changed"),
        _ => panic!("unknown catalog application outcome"),
    }
}

fn source(snapshot: &elucid_metastore::CatalogSnapshot) -> &Source {
    let name = SourceName::try_from("logs").expect("source name");
    snapshot.source_by_name(&name).expect("logs source")
}

async fn catalog_row_counts(pool: &PgPool) -> [i64; 4] {
    let sources = sqlx::query_scalar("SELECT count(*) FROM sources")
        .fetch_one(pool)
        .await
        .expect("count sources");
    let schemas = sqlx::query_scalar("SELECT count(*) FROM schema_versions")
        .fetch_one(pool)
        .await
        .expect("count schemas");
    let inputs = sqlx::query_scalar("SELECT count(*) FROM inputs")
        .fetch_one(pool)
        .await
        .expect("count inputs");
    let profiles = sqlx::query_scalar("SELECT count(*) FROM ingestion_profile_revisions")
        .fetch_one(pool)
        .await
        .expect("count profiles");
    [sources, schemas, inputs, profiles]
}

#[derive(Debug)]
struct CatalogIdentities {
    source: elucid_catalog::SourceId,
    first_schema: elucid_catalog::SchemaId,
    first_user_field: elucid_catalog::FieldId,
    input: elucid_catalog::InputId,
    first_profile: elucid_catalog::IngestionProfileRevisionId,
}

impl CatalogIdentities {
    fn from_source(source: &Source) -> Self {
        Self {
            source: source.id(),
            first_schema: source.schemas()[0].id(),
            first_user_field: source.schemas()[0].fields()[3].id(),
            input: source.inputs()[0].id(),
            first_profile: source.inputs()[0].profile_revisions()[0].id(),
        }
    }

    fn assert_preserved(&self, source: &Source) {
        assert_eq!(source.id(), self.source);
        assert_eq!(source.schemas()[0].id(), self.first_schema);
        assert_eq!(source.schemas()[0].fields()[3].id(), self.first_user_field);
        assert_eq!(source.inputs()[0].id(), self.input);
        assert_eq!(
            source.inputs()[0].profile_revisions()[0].id(),
            self.first_profile
        );
    }
}
