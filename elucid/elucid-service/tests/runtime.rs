use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use elucid_ingestion::{AppendBodyLimit, Spool, SpoolCapacity};
use elucid_service::{Environment, RuntimeConfiguration, start};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use tempfile::TempDir;
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::GenericImage;
use testcontainers_modules::testcontainers::core::{ImageExt as _, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

const BUCKET: &str = "elucid-service-test";
const MINIO_CLIENT_TAG: &str = "RELEASE.2025-02-21T16-00-46Z";
const CATALOG: &str = r#"
format_version: 1
source:
  name: demo_logs
  display_name: Demo logs
  active_schema_version: 1
  schemas:
    - version: 1
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
        - name: status
          logical_type: int32
          nullability: NULLABLE
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
async fn dropping_server_releases_its_bound_listener() {
    let dependency = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalled dependency");
    let dependency_address = dependency.local_addr().expect("dependency address");
    let stalled_dependency = tokio::spawn(async move {
        let (_connection, _) = dependency.accept().await.expect("accept PostgreSQL probe");
        std::future::pending::<()>().await;
    });
    let local = TempDir::new().expect("create local storage root");
    let document = runtime_configuration(
        "http://127.0.0.1:9",
        local.path().join("spool").to_str().expect("spool path"),
        local.path().join("scratch").to_str().expect("scratch path"),
    );
    let environment = Environment::from_pairs([
        (
            "ELUCID_METASTORE__POSTGRESQL_URL",
            format!("postgresql://postgres:postgres@{dependency_address}/postgres"),
        ),
        ("ELUCID_OBJECT_STORE__ACCESS_KEY_ID", "unused".into()),
        ("ELUCID_OBJECT_STORE__SECRET_ACCESS_KEY", "unused".into()),
    ]);
    let configuration = RuntimeConfiguration::from_toml(&document, &environment)
        .expect("decode runtime configuration");
    let server = start(configuration).await.expect("bind server");
    let server_address = server.local_address();
    let client = Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .expect("build HTTP client");
    let live = client
        .get(format!("http://{server_address}/health/live"))
        .send()
        .await
        .expect("request liveness");
    assert_eq!(live.status(), StatusCode::OK);

    drop(server);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if TcpStream::connect(server_address).await.is_err() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "dropping the server left its listener running"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stalled_dependency.abort();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn server_bootstraps_dependencies_and_keeps_diagnostics_live_during_an_outage() {
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
    let network = format!("elucid-service-{test_identity}");
    let server_name = format!("elucid-service-minio-{test_identity}");
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

    let local = TempDir::new().expect("create local storage root");
    let document = runtime_configuration(
        &format!("http://{minio_host}:{minio_port}"),
        local.path().join("spool").to_str().expect("spool path"),
        local.path().join("scratch").to_str().expect("scratch path"),
    );
    let environment = Environment::from_pairs([
        ("ELUCID_METASTORE__POSTGRESQL_URL", postgresql_url),
        ("ELUCID_OBJECT_STORE__ACCESS_KEY_ID", "minioadmin".into()),
        (
            "ELUCID_OBJECT_STORE__SECRET_ACCESS_KEY",
            "minioadmin".into(),
        ),
    ]);
    let configuration = RuntimeConfiguration::from_toml(&document, &environment)
        .expect("decode runtime configuration");
    let server = start(configuration).await.expect("bind server");
    let endpoint = format!("http://{}", server.local_address());
    let client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("build HTTP client");

    let request_id = Uuid::now_v7();
    let request_id_header = request_id.to_string().to_uppercase();
    let live = client
        .get(format!("{endpoint}/health/live"))
        .header("X-Request-Id", &request_id_header)
        .send()
        .await
        .expect("request liveness");
    assert_eq!(live.status(), StatusCode::OK);
    assert_eq!(
        live.headers()
            .get("X-Request-Id")
            .expect("response request identity"),
        request_id_header.as_str()
    );
    assert_eq!(json(live).await["status"], "UP");

    let unsupported_method = client
        .post(format!("{endpoint}/health/live"))
        .send()
        .await
        .expect("request unsupported method");
    assert_eq!(unsupported_method.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert!(unsupported_method.headers().contains_key("X-Request-Id"));
    assert_eq!(
        json(unsupported_method).await["error"]["code"],
        "INVALID_REQUEST"
    );

    let ready = wait_for_status(&client, &format!("{endpoint}/health/ready"), StatusCode::OK).await;
    assert_eq!(ready["status"], "READY");
    assert_eq!(ready["components"]["postgresql"], "UP");
    assert_eq!(ready["components"]["object_store"], "UP");
    assert_eq!(ready["components"]["spool"], "UP");
    assert_eq!(ready["components"]["maintenance"], "DEGRADED");

    let applied = apply_catalog(&client, &endpoint).await;
    assert_eq!(applied["outcome"], "APPLIED");
    let source_id = applied["source_id"]
        .as_str()
        .expect("source identity")
        .to_owned();
    let unchanged = apply_catalog(&client, &endpoint).await;
    assert_eq!(unchanged["outcome"], "UNCHANGED");
    assert_eq!(unchanged["source_id"], source_id);
    assert_eq!(
        unchanged["active_input_profile_revisions"],
        applied["active_input_profile_revisions"]
    );

    let sources = get_json(
        &client,
        &format!("{endpoint}/api/v1/sources"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(sources["completion"], "COMPLETE");
    assert_eq!(sources["sources"].as_array().expect("sources").len(), 1);
    assert_eq!(sources["sources"][0]["name"], "demo_logs");
    assert_eq!(sources["sources"][0]["source_id"], source_id);

    let source = get_json(
        &client,
        &format!("{endpoint}/api/v1/sources/{source_id}"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(source["active_schema"]["version"], 1);
    assert_eq!(
        source["schema_versions"].as_array().expect("schemas").len(),
        1
    );
    assert_eq!(source["inputs"][0]["name"], "vector");
    assert_eq!(source["inputs"][0]["active_profile"]["revision"], 1);
    let input_id = applied["active_input_profile_revisions"][0]["input_id"]
        .as_str()
        .expect("input identity")
        .to_owned();
    let profile_revision_id = applied["active_input_profile_revisions"][0]["profile_revision_id"]
        .as_str()
        .expect("profile revision identity")
        .to_owned();
    let target_schema_id = source["active_schema"]["schema_id"]
        .as_str()
        .expect("schema identity")
        .to_owned();

    let status = get_json(
        &client,
        &format!("{endpoint}/api/v1/status"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(status["phase"], "READY");
    assert_eq!(status["admission"], "OPEN");
    assert_eq!(status["limits"]["maximum_http_batch_bytes"], 1_048_576);
    assert_eq!(status["limits"]["maximum_http_batch_records"], 100_000);
    assert_eq!(status["spool"]["used_bytes"], 0);
    assert_eq!(status["maintenance"]["ownership"], "OWNED");

    assert_ingestion_error(
        client
            .post(format!(
                "{endpoint}/api/v1/sources/demo_logs/inputs/vector/events"
            ))
            .header("Content-Type", "application/json")
            .body("{}\n")
            .send()
            .await
            .expect("reject JSON media type"),
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "INVALID_REQUEST",
    )
    .await;
    assert_ingestion_error(
        client
            .post(format!(
                "{endpoint}/api/v1/sources/demo_logs/inputs/vector/events"
            ))
            .header("Content-Type", "application/x-ndjson")
            .header("Content-Encoding", "gzip")
            .body("{}\n")
            .send()
            .await
            .expect("reject compressed body"),
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "INVALID_REQUEST",
    )
    .await;
    assert_ingestion_error(
        client
            .post(format!(
                "{endpoint}/api/v1/sources/unknown/inputs/vector/events"
            ))
            .header("Content-Type", "application/x-ndjson")
            .body("{}\n")
            .send()
            .await
            .expect("reject unknown source"),
        StatusCode::NOT_FOUND,
        "NOT_FOUND",
    )
    .await;
    assert_ingestion_error(
        client
            .post(format!(
                "{endpoint}/api/v1/sources/demo_logs/inputs/unknown/events"
            ))
            .header("Content-Type", "application/x-ndjson")
            .body("{}\n")
            .send()
            .await
            .expect("reject unknown input"),
        StatusCode::NOT_FOUND,
        "NOT_FOUND",
    )
    .await;
    assert_ingestion_error(
        client
            .post(format!(
                "{endpoint}/api/v1/sources/demo_logs/inputs/vector/events"
            ))
            .header("Content-Type", "application/x-ndjson")
            .body(vec![b'x'; 1_048_577])
            .send()
            .await
            .expect("reject oversized body"),
        StatusCode::PAYLOAD_TOO_LARGE,
        "INGESTION_BATCH_LIMIT_EXCEEDED",
    )
    .await;
    assert_ingestion_error(
        client
            .post(format!(
                "{endpoint}/api/v1/sources/demo_logs/inputs/vector/events"
            ))
            .header("Content-Type", "application/x-ndjson")
            .body("\n".repeat(100_001))
            .send()
            .await
            .expect("reject too many framed records"),
        StatusCode::PAYLOAD_TOO_LARGE,
        "INGESTION_BATCH_LIMIT_EXCEEDED",
    )
    .await;

    let body = concat!(
        "{\"timestamp\":\"2026-08-20T12:00:00.000Z\",\"message\":\"first\"}\n",
        "{\"timestamp\":\"2026-08-20T12:00:01.000Z\",\"message\":\"second\"}\n",
    );
    let mut admitted_request = TcpStream::connect(server.local_address())
        .await
        .expect("connect admitted ingestion request");
    let admitted_headers = format!(
        "POST /api/v1/sources/demo_logs/inputs/vector/events HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nContent-Encoding: identity\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    admitted_request
        .write_all(admitted_headers.as_bytes())
        .await
        .expect("write admitted request headers");
    admitted_request
        .write_all(&body.as_bytes()[..body.len() - 1])
        .await
        .expect("write partial admitted body");
    admitted_request
        .flush()
        .await
        .expect("flush partial admitted request");

    let status_url = format!("{endpoint}/api/v1/status");
    wait_for_admission_state(&client, &status_url, "CLOSED").await;
    let capacity_exhausted = client
        .post(format!(
            "{endpoint}/api/v1/sources/demo_logs/inputs/vector/events"
        ))
        .header("Content-Type", "application/x-ndjson")
        .body("{}\n")
        .send()
        .await
        .expect("reject ingestion beyond spool capacity");
    assert_eq!(capacity_exhausted.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        capacity_exhausted
            .headers()
            .get("Retry-After")
            .expect("capacity retry delay"),
        "1"
    );
    assert_eq!(
        json(capacity_exhausted).await["error"]["code"],
        "CAPACITY_EXHAUSTED"
    );

    minio.stop_with_timeout(Some(1)).await.expect("stop MinIO");
    admitted_request
        .write_all(&body.as_bytes()[body.len() - 1..])
        .await
        .expect("complete admitted body after dependency outage");
    admitted_request
        .flush()
        .await
        .expect("flush complete admitted request");
    let mut raw_response = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(3),
        admitted_request.read_to_end(&mut raw_response),
    )
    .await
    .expect("admitted response timed out")
    .expect("read admitted response");
    let raw_response = String::from_utf8(raw_response).expect("UTF-8 admitted response");
    let (response_headers, response_body) = raw_response
        .split_once("\r\n\r\n")
        .expect("split admitted response");
    assert!(response_headers.starts_with("HTTP/1.1 202 Accepted"));
    let accepted: Value = serde_json::from_str(response_body).expect("decode admitted response");
    assert_eq!(accepted["state"], "DURABLY_QUEUED");
    assert_eq!(accepted["body_bytes"], body.len());
    let batch_id = accepted["batch_id"]
        .as_str()
        .expect("batch identity")
        .to_owned();
    let parsed_batch_id = Uuid::parse_str(&batch_id).expect("UUID batch identity");
    assert_eq!(parsed_batch_id.get_version_num(), 7);
    assert_eq!(parsed_batch_id.to_string(), batch_id);
    let ingestion_time = accepted["ingestion_time"].as_str().expect("ingestion time");
    assert_eq!(ingestion_time.len(), 24);
    assert!(ingestion_time.ends_with('Z'));

    let status = get_json(&client, &status_url, StatusCode::OK).await;
    assert!(status["spool"]["used_bytes"].as_u64().expect("used bytes") > body.len() as u64);
    assert_eq!(status["publication"]["pending_batches"], 1);
    assert_eq!(status["admission"], "CLOSED");

    let unavailable =
        wait_for_unready_component(&client, &format!("{endpoint}/health/ready"), "object_store")
            .await;
    assert_eq!(unavailable["error"]["code"], "SERVER_NOT_READY");
    assert_eq!(
        unavailable["error"]["details"]["components"]["object_store"],
        "DOWN"
    );

    let live = get_json(&client, &format!("{endpoint}/health/live"), StatusCode::OK).await;
    assert_eq!(live["status"], "UP");
    let status = get_json(
        &client,
        &format!("{endpoint}/api/v1/status"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(status["phase"], "DEGRADED");
    assert_eq!(status["components"]["object_store"], "DOWN");
    let unavailable_ingestion = client
        .post(format!(
            "{endpoint}/api/v1/sources/demo_logs/inputs/vector/events"
        ))
        .header("Content-Type", "application/x-ndjson")
        .body("{}\n")
        .send()
        .await
        .expect("reject ingestion while unavailable");
    assert_eq!(
        unavailable_ingestion.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        unavailable_ingestion
            .headers()
            .get("Retry-After")
            .expect("readiness retry delay"),
        "1"
    );
    assert_eq!(
        json(unavailable_ingestion).await["error"]["code"],
        "SERVER_NOT_READY"
    );
    let cached_sources = get_json(
        &client,
        &format!("{endpoint}/api/v1/sources"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cached_sources["sources"][0]["source_id"], source_id);

    server.shutdown().await.expect("shutdown server");

    let recovery = Spool::open(
        local.path().join("spool"),
        SpoolCapacity::new(1_049_000).expect("spool capacity"),
        AppendBodyLimit::new(1_048_576).expect("maximum batch"),
    )
    .await
    .expect("recover service spool");
    assert_eq!(recovery.report().pending_batches(), 1);
    let (_, mut batches, _) = recovery.into_parts();
    let recovered = batches
        .next_batch()
        .await
        .expect("read accepted batch")
        .expect("accepted batch");
    assert_eq!(recovered.body().as_ref(), body.as_bytes());
    assert_eq!(recovered.metadata().batch_id().to_string(), batch_id);
    assert_eq!(
        recovered.metadata().catalog().source_id().to_string(),
        source_id
    );
    assert_eq!(
        recovered.metadata().catalog().input_id().to_string(),
        input_id
    );
    assert_eq!(
        recovered
            .metadata()
            .catalog()
            .profile_revision_id()
            .to_string(),
        profile_revision_id
    );
    assert_eq!(
        recovered
            .metadata()
            .catalog()
            .target_schema_id()
            .to_string(),
        target_schema_id
    );
    let recovered_time = DateTime::<Utc>::from_timestamp_millis(
        recovered.metadata().ingestion_time().unix_milliseconds(),
    )
    .expect("recovered ingestion time")
    .to_rfc3339_opts(SecondsFormat::Millis, true);
    assert_eq!(recovered_time, ingestion_time);
    assert!(
        batches
            .next_batch()
            .await
            .expect("finish recovered batches")
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn disconnect_is_ambiguous_and_shutdown_recovers_an_incomplete_append() {
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
    let network = format!("elucid-service-shutdown-{test_identity}");
    let server_name = format!("elucid-service-shutdown-minio-{test_identity}");
    let minio = MinIO::default()
        .with_network(network.clone())
        .with_container_name(server_name.clone())
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("start MinIO");
    let minio_host = minio.get_host().await.expect("MinIO host");
    let minio_port = minio.get_host_port_ipv4(9000).await.expect("MinIO port");
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

    let local = TempDir::new().expect("create local storage root");
    let spool_path = local.path().join("spool");
    let document = runtime_configuration_with_timeouts(
        &format!("http://{minio_host}:{minio_port}"),
        spool_path.to_str().expect("spool path"),
        local.path().join("scratch").to_str().expect("scratch path"),
        30,
        1,
    );
    let environment = Environment::from_pairs([
        ("ELUCID_METASTORE__POSTGRESQL_URL", postgresql_url),
        ("ELUCID_OBJECT_STORE__ACCESS_KEY_ID", "minioadmin".into()),
        (
            "ELUCID_OBJECT_STORE__SECRET_ACCESS_KEY",
            "minioadmin".into(),
        ),
    ]);
    let configuration = RuntimeConfiguration::from_toml(&document, &environment)
        .expect("decode runtime configuration");
    let server = start(configuration).await.expect("bind server");
    let endpoint = format!("http://{}", server.local_address());
    let client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("build HTTP client");
    wait_for_status(&client, &format!("{endpoint}/health/ready"), StatusCode::OK).await;
    apply_catalog(&client, &endpoint).await;

    let disconnected_body = b"{}\n";
    let mut disconnected = TcpStream::connect(server.local_address())
        .await
        .expect("connect ingestion request with a lost response");
    let disconnected_headers = format!(
        "POST /api/v1/sources/demo_logs/inputs/vector/events HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-ndjson\r\nContent-Length: {}\r\n\r\n",
        disconnected_body.len()
    );
    disconnected
        .write_all(disconnected_headers.as_bytes())
        .await
        .expect("write ingestion headers");
    disconnected
        .write_all(&disconnected_body[..disconnected_body.len() - 1])
        .await
        .expect("write partial ingestion body");
    disconnected.flush().await.expect("flush ingestion request");

    let status_url = format!("{endpoint}/api/v1/status");
    wait_for_admission_state(&client, &status_url, "CLOSED").await;
    disconnected
        .write_all(&disconnected_body[disconnected_body.len() - 1..])
        .await
        .expect("complete ingestion body");
    disconnected.flush().await.expect("flush complete request");
    drop(disconnected);

    let status = wait_for_pending_batches(&client, &status_url, 1).await;
    assert_eq!(status["admission"], "OPEN");

    let mut connection = TcpStream::connect(server.local_address())
        .await
        .expect("connect incomplete ingestion request");
    connection
        .write_all(
            b"POST /api/v1/sources/demo_logs/inputs/vector/events HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-ndjson\r\nContent-Length: 1024\r\n\r\n{",
        )
        .await
        .expect("write incomplete ingestion request");
    connection.flush().await.expect("flush incomplete request");

    wait_for_admission_state(&client, &status_url, "CLOSED").await;

    server
        .shutdown()
        .await
        .expect("shutdown with incomplete admitted request");
    drop(connection);

    let recovery = Spool::open(
        spool_path,
        SpoolCapacity::new(1_049_000).expect("spool capacity"),
        AppendBodyLimit::new(1_048_576).expect("maximum batch"),
    )
    .await
    .expect("recover service spool");
    assert_eq!(recovery.report().pending_batches(), 1);
    let (_, mut batches, _) = recovery.into_parts();
    let recovered = batches
        .next_batch()
        .await
        .expect("read disconnected batch")
        .expect("disconnected batch");
    assert_eq!(recovered.body().as_ref(), disconnected_body);
    assert!(
        batches
            .next_batch()
            .await
            .expect("finish recovered batches")
            .is_none()
    );
}

fn runtime_configuration(endpoint: &str, spool_path: &str, scratch_path: &str) -> String {
    runtime_configuration_with_timeouts(endpoint, spool_path, scratch_path, 5, 5)
}

fn runtime_configuration_with_timeouts(
    endpoint: &str,
    spool_path: &str,
    scratch_path: &str,
    request_timeout_seconds: u64,
    shutdown_timeout_seconds: u64,
) -> String {
    format!(
        r#"
[server]
listen_address = "127.0.0.1:0"
request_timeout_seconds = {request_timeout_seconds}
shutdown_timeout_seconds = {shutdown_timeout_seconds}

[metastore]
maximum_connections = 4

[object_store]
endpoint = "{endpoint}"
bucket = "{BUCKET}"
root_prefix = "showcase"
request_timeout_seconds = 1

[local_storage]
spool_path = "{spool_path}"
spool_capacity_bytes = 1049000
scratch_path = "{scratch_path}"
scratch_capacity_bytes = 16777216

[ingestion]
maximum_http_batch_bytes = 1048576
maximum_concurrent_requests = 2

[maintenance]
mode = "AUTOMATIC"
"#
    )
}

async fn apply_catalog(client: &Client, endpoint: &str) -> Value {
    let response = client
        .post(format!("{endpoint}/api/v1/catalog-applications"))
        .header("Content-Type", "application/yaml")
        .body(CATALOG)
        .send()
        .await
        .expect("apply catalog");
    assert_eq!(response.status(), StatusCode::OK);
    json(response).await
}

async fn get_json(client: &Client, url: &str, expected_status: StatusCode) -> Value {
    let response = client.get(url).send().await.expect("request endpoint");
    assert_eq!(response.status(), expected_status);
    assert!(response.headers().contains_key("X-Request-Id"));
    json(response).await
}

async fn assert_ingestion_error(
    response: reqwest::Response,
    expected_status: StatusCode,
    expected_code: &str,
) {
    assert_eq!(response.status(), expected_status);
    assert!(response.headers().contains_key("X-Request-Id"));
    assert_eq!(json(response).await["error"]["code"], expected_code);
}

async fn wait_for_status(client: &Client, url: &str, expected_status: StatusCode) -> Value {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = client.get(url).send().await
            && response.status() == expected_status
        {
            if expected_status == StatusCode::SERVICE_UNAVAILABLE {
                assert!(response.headers().contains_key("Retry-After"));
            }
            return json(response).await;
        }
        assert!(
            Instant::now() < deadline,
            "endpoint did not reach {expected_status}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_unready_component(client: &Client, url: &str, component: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = client.get(url).send().await
            && response.status() == StatusCode::SERVICE_UNAVAILABLE
            && response.headers().contains_key("Retry-After")
        {
            let body = json(response).await;
            if body["error"]["details"]["components"][component] == "DOWN" {
                return body;
            }
        }
        assert!(
            Instant::now() < deadline,
            "component {component} did not become unavailable"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_pending_batches(client: &Client, url: &str, expected: u64) -> Value {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let status = get_json(client, url, StatusCode::OK).await;
        if status["publication"]["pending_batches"] == expected {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "pending batch count did not reach {expected}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_admission_state(client: &Client, url: &str, expected: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let status = get_json(client, url, StatusCode::OK).await;
        if status["admission"] == expected {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "admission state did not reach {expected}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn json(response: reqwest::Response) -> Value {
    response.json().await.expect("decode JSON response")
}
