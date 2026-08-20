use std::time::{Duration, Instant};

use elucid_service::{Environment, RuntimeConfiguration, start};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use tempfile::TempDir;
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::GenericImage;
use testcontainers_modules::testcontainers::core::{ImageExt as _, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner as _;
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

    let status = get_json(
        &client,
        &format!("{endpoint}/api/v1/status"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(status["phase"], "READY");
    assert_eq!(status["admission"], "OPEN");
    assert_eq!(status["limits"]["maximum_http_batch_bytes"], 1_048_576);
    assert_eq!(status["spool"]["used_bytes"], 0);
    assert_eq!(status["maintenance"]["ownership"], "OWNED");

    minio.stop_with_timeout(Some(1)).await.expect("stop MinIO");
    let unavailable = wait_for_status(
        &client,
        &format!("{endpoint}/health/ready"),
        StatusCode::SERVICE_UNAVAILABLE,
    )
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
    let cached_sources = get_json(
        &client,
        &format!("{endpoint}/api/v1/sources"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cached_sources["sources"][0]["source_id"], source_id);

    server.shutdown().await.expect("shutdown server");
}

fn runtime_configuration(endpoint: &str, spool_path: &str, scratch_path: &str) -> String {
    format!(
        r#"
[server]
listen_address = "127.0.0.1:0"
request_timeout_seconds = 5
shutdown_timeout_seconds = 5

[metastore]
maximum_connections = 4

[object_store]
endpoint = "{endpoint}"
bucket = "{BUCKET}"
root_prefix = "showcase"
request_timeout_seconds = 1

[local_storage]
spool_path = "{spool_path}"
spool_capacity_bytes = 16777216
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

async fn json(response: reqwest::Response) -> Value {
    response.json().await.expect("decode JSON response")
}
