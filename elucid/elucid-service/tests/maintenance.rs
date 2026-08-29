use std::fmt::Write as _;
use std::time::{Duration, Instant};

use object_store::ObjectStore as _;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use reqwest::{Client, StatusCode};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tempfile::TempDir;
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::GenericImage;
use testcontainers_modules::testcontainers::core::{ImageExt as _, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner as _;
use uuid::Uuid;

use elucid_service::{Environment, RuntimeConfiguration, start};

const BUCKET: &str = "elucid-maintenance-test";
const COMPACTION_INPUT_ROWS: u64 = 50_000;
const MINIO_CLIENT_TAG: &str = "RELEASE.2025-02-21T16-00-46Z";
const CATALOG: &str = r#"
format_version: 1
source:
  name: maintenance_logs
  display_name: Maintenance logs
  active_schema_version: 1
  schemas:
    - version: 1
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
        - name: status
          logical_type: int32
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
            - target_field: status
              json_pointer: /status
"#;

#[tokio::test]
#[ignore = "requires Docker"]
async fn automatic_owner_compacts_expires_reclaims_and_cleans_metadata() {
    let postgres = Postgres::default()
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("start PostgreSQL");
    let postgres_host = postgres.get_host().await.expect("PostgreSQL host");
    let postgres_port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("PostgreSQL port");
    let postgresql_url =
        format!("postgresql://postgres:postgres@{postgres_host}:{postgres_port}/postgres");

    let test_identity = Uuid::now_v7().simple().to_string();
    let network = format!("elucid-maintenance-{test_identity}");
    let server_name = format!("elucid-maintenance-minio-{test_identity}");
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
    let minio_port = minio.get_host_port_ipv4(9000).await.expect("MinIO port");
    let minio_endpoint = format!("http://{minio_host}:{minio_port}");

    let local = TempDir::new().expect("create local storage root");
    let document = runtime_configuration(
        &minio_endpoint,
        local.path().join("spool").to_str().expect("spool path"),
        local.path().join("scratch").to_str().expect("scratch path"),
    );
    let environment = Environment::from_pairs([
        ("ELUCID_METASTORE__POSTGRESQL_URL", postgresql_url.clone()),
        ("ELUCID_OBJECT_STORE__ACCESS_KEY_ID", "minioadmin".into()),
        (
            "ELUCID_OBJECT_STORE__SECRET_ACCESS_KEY",
            "minioadmin".into(),
        ),
    ]);
    let configuration = RuntimeConfiguration::from_toml(&document, &environment)
        .expect("decode runtime configuration");
    let server = start(configuration).await.expect("start server");
    let endpoint = format!("http://{}", server.local_address());
    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("build HTTP client");
    wait_until_ready(&client, &endpoint).await;

    let catalog = client
        .post(format!("{endpoint}/api/v1/catalog-applications"))
        .header("Content-Type", "application/yaml")
        .body(CATALOG)
        .send()
        .await
        .expect("apply catalog");
    assert_eq!(catalog.status(), StatusCode::OK);

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&postgresql_url)
        .await
        .expect("connect test observer");
    ingest(&client, &endpoint, event_batch(0)).await;
    wait_for_active_ingestion_segments(&pool, 1).await;
    ingest(&client, &endpoint, event_batch(COMPACTION_INPUT_ROWS)).await;

    let compacted = wait_for_compacted_segment(&pool).await;
    assert_eq!(
        compacted.row_count,
        i64::try_from(COMPACTION_INPUT_ROWS * 2).expect("expected compacted rows fit i64")
    );
    assert_eq!(active_ingestion_segments(&pool).await, 0);

    sqlx::query("UPDATE segments SET data_expires_at = published_at WHERE segment_id = $1")
        .bind(compacted.segment_id)
        .execute(&pool)
        .await
        .expect("make compacted segment due for expiration");
    wait_for_segment_state(&pool, compacted.segment_id, "EXPIRED").await;
    sqlx::query("UPDATE segments SET reclaim_after = retired_at WHERE segment_id = $1")
        .bind(compacted.segment_id)
        .execute(&pool)
        .await
        .expect("make expired object reclaimable");
    wait_for_metadata_removal(&pool, compacted.segment_id, compacted.object_id).await;

    let objects = AmazonS3Builder::new()
        .with_endpoint(minio_endpoint)
        .with_bucket_name(BUCKET)
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_region("us-east-1")
        .with_allow_http(true)
        .build()
        .expect("build MinIO observer");
    assert!(matches!(
        objects.head(&ObjectPath::from(compacted.object_key)).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let ready = client
        .get(format!("{endpoint}/health/ready"))
        .send()
        .await
        .expect("read final readiness");
    assert_eq!(ready.status(), StatusCode::OK);
    let ready: serde_json::Value = ready.json().await.expect("decode final readiness");
    assert_eq!(ready["components"]["maintenance"], "UP");
    server.shutdown().await.expect("shutdown server");
}

struct CompactedSegment {
    segment_id: Uuid,
    object_id: Uuid,
    object_key: String,
    row_count: i64,
}

async fn ingest(client: &Client, endpoint: &str, body: String) {
    let response = client
        .post(format!(
            "{endpoint}/api/v1/sources/maintenance_logs/inputs/vector/events"
        ))
        .header("Content-Type", "application/x-ndjson")
        .body(body)
        .send()
        .await
        .expect("ingest batch");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
}

fn event_batch(first_sequence: u64) -> String {
    let mut body = String::with_capacity(4 * 1024 * 1024);
    for sequence in first_sequence..first_sequence + COMPACTION_INPUT_ROWS {
        writeln!(
            body,
            r#"{{"timestamp":"2026-08-20T12:00:00.000Z","message":"event-{sequence}","status":200}}"#,
        )
        .expect("write in-memory event");
    }
    body
}

async fn wait_until_ready(client: &Client, endpoint: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = client.get(format!("{endpoint}/health/ready")).send().await
            && response.status() == StatusCode::OK
        {
            let body: serde_json::Value = response.json().await.expect("decode readiness");
            assert_eq!(body["components"]["maintenance"], "UP");
            return;
        }
        assert!(Instant::now() < deadline, "server did not become ready");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_active_ingestion_segments(pool: &PgPool, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if active_ingestion_segments(pool).await == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "active ingestion segment count did not reach {expected}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn active_ingestion_segments(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM segments WHERE state = 'ACTIVE' AND origin = 'INGESTION'",
    )
    .fetch_one(pool)
    .await
    .expect("count active ingestion segments")
}

async fn wait_for_compacted_segment(pool: &PgPool) -> CompactedSegment {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let row = sqlx::query_as::<_, (Uuid, Uuid, String, i64)>(
            r#"
            SELECT segment.segment_id, object.object_id, object.object_key, segment.row_count
            FROM segments AS segment
            JOIN stored_objects AS object ON object.segment_id = segment.segment_id
            WHERE segment.state = 'ACTIVE'
              AND segment.origin = 'COMPACTION'
              AND object.state = 'PUBLISHED'
            "#,
        )
        .fetch_optional(pool)
        .await
        .expect("inspect compacted segment");
        if let Some((segment_id, object_id, object_key, row_count)) = row {
            return CompactedSegment {
                segment_id,
                object_id,
                object_key,
                row_count,
            };
        }
        assert!(
            Instant::now() < deadline,
            "automatic maintenance did not publish a compacted segment"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_segment_state(pool: &PgPool, segment_id: Uuid, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let state =
            sqlx::query_scalar::<_, String>("SELECT state FROM segments WHERE segment_id = $1")
                .bind(segment_id)
                .fetch_optional(pool)
                .await
                .expect("inspect segment state");
        if state.as_deref() == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "segment state did not reach {expected}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_metadata_removal(pool: &PgPool, segment_id: Uuid, object_id: Uuid) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let rows = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT
                (SELECT COUNT(*) FROM segments WHERE segment_id = $1)
                + (SELECT COUNT(*) FROM stored_objects WHERE object_id = $2)
            "#,
        )
        .bind(segment_id)
        .bind(object_id)
        .fetch_one(pool)
        .await
        .expect("inspect terminal metadata");
        if rows == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "terminal segment and object metadata were not removed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn runtime_configuration(endpoint: &str, spool_path: &str, scratch_path: &str) -> String {
    format!(
        r#"
[server]
listen_address = "127.0.0.1:0"
request_timeout_seconds = 15
shutdown_timeout_seconds = 10

[metastore]
maximum_connections = 4

[object_store]
endpoint = "{endpoint}"
bucket = "{BUCKET}"
root_prefix = "maintenance-test"
request_timeout_seconds = 2

[local_storage]
spool_path = "{spool_path}"
spool_capacity_bytes = 16777216
scratch_path = "{scratch_path}"
scratch_capacity_bytes = 268435456

[ingestion]
maximum_http_batch_bytes = 8388608
maximum_concurrent_requests = 1

[query]
maximum_concurrent_queries = 1
timeout_seconds = 1
maximum_scan_bytes = 1073741824
memory_bytes = 67108864
maximum_result_rows = 1000
maximum_result_bytes = 1048576

[maintenance]
mode = "AUTOMATIC"
event_retention_seconds = 2592000
dead_letter_retention_seconds = 604800
"#,
    )
}
