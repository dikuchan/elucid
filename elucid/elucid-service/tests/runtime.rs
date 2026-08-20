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
    assert_eq!(status["limits"]["maximum_batch_event_days"], 32);
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
    assert_raw_ingestion_error(
        server.local_address(),
        1_048_577,
        "413 Payload Too Large",
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

    let mixed_body = concat!(
        "{\"timestamp\":\"2026-08-20T11:59:59.000Z\",\"message\":\"valid before\",\"status\":200}\n",
        "{\"timestamp\":\"not-a-timestamp\",\"message\":\"invalid neighbor\"}\n",
        "{\"timestamp\":\"2026-08-20T12:00:00.000Z\",\"message\":\"valid after\",\"status\":500}\n",
    );
    let accepted = client
        .post(format!(
            "{endpoint}/api/v1/sources/demo_logs/inputs/vector/events"
        ))
        .header("Content-Type", "application/x-ndjson")
        .body(mixed_body)
        .send()
        .await
        .expect("admit mixed ingestion batch");
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    assert_eq!(json(accepted).await["state"], "DURABLY_QUEUED");

    let segments_url = format!("{endpoint}/api/v1/segments?source_id={source_id}&state=ACTIVE");
    let segments = wait_for_list_items(&client, &segments_url, "segments", 1).await;
    assert_eq!(segments["completion"], "COMPLETE");
    assert_eq!(segments["limit"], 100);
    assert_eq!(segments["segments"][0]["source_id"], source_id);
    assert_eq!(segments["segments"][0]["schema_id"], target_schema_id);
    assert_eq!(segments["segments"][0]["state"], "ACTIVE");
    assert_eq!(segments["segments"][0]["origin"], "INGESTION");
    assert_eq!(segments["segments"][0]["event_day"], "2026-08-20");
    assert_eq!(segments["segments"][0]["row_count"], 2);
    assert!(
        segments["segments"][0]["parquet_bytes"]
            .as_u64()
            .expect("Parquet byte count")
            > 0
    );

    let dead_letters_url = format!("{endpoint}/api/v1/dead-letters?source_id={source_id}");
    let dead_letters = wait_for_list_items(&client, &dead_letters_url, "dead_letters", 1).await;
    assert_eq!(dead_letters["completion"], "COMPLETE");
    assert_eq!(dead_letters["limit"], 100);
    assert_eq!(dead_letters["dead_letters"][0]["source_id"], source_id);
    assert_eq!(dead_letters["dead_letters"][0]["input_id"], input_id);
    let dead_letter_object_id = dead_letters["dead_letters"][0]["object_id"]
        .as_str()
        .expect("dead-letter object identity");
    let dead_letter = get_json(
        &client,
        &format!("{endpoint}/api/v1/dead-letters/{dead_letter_object_id}"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(dead_letter["completion"], "COMPLETE");
    assert_eq!(dead_letter["limit_bytes"], 1_048_576);
    assert_eq!(dead_letter["entries"].as_array().expect("entries").len(), 1);
    assert_eq!(dead_letter["entries"][0]["line_number"], 2);
    assert_eq!(
        dead_letter["entries"][0]["code"],
        "RECORD_EVENT_TIME_INVALID"
    );
    assert_eq!(dead_letter["entries"][0]["payload"]["encoding"], "UTF8");
    assert_eq!(dead_letter["entries"][0]["payload"]["extent"], "COMPLETE");

    let drained = wait_for_pending_batches(&client, &format!("{endpoint}/api/v1/status"), 0).await;
    assert_eq!(drained["publication"]["status"], "UP");
    assert_eq!(drained["publication"]["prepared_segments"], 0);
    assert_eq!(drained["publication"]["planned_objects"], 0);
    assert_eq!(drained["publication"]["uploaded_objects"], 0);
    assert_eq!(regular_files_under(&local.path().join("scratch")), 0);

    let metrics = client
        .get(format!("{endpoint}/metrics"))
        .send()
        .await
        .expect("request metrics");
    assert_eq!(metrics.status(), StatusCode::OK);
    assert!(
        metrics
            .headers()
            .get("Content-Type")
            .expect("metrics content type")
            .to_str()
            .expect("metrics content type text")
            .starts_with("text/plain")
    );
    let metrics = metrics.text().await.expect("read metrics");
    for metric in [
        "elucid_ingestion_http_batches_accepted_total 1",
        "elucid_ingestion_http_batches_rejected_total 6",
        "elucid_ingestion_records_accepted_total 2",
        "elucid_ingestion_records_rejected_total 1",
        "elucid_ingestion_segments_published_total 1",
        "elucid_ingestion_dead_letter_objects_published_total 1",
        "elucid_spool_pending_batches 0",
        "elucid_publication_prepared_segments 0",
    ] {
        assert!(metrics.contains(metric), "missing metric {metric}");
    }

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
    wait_for_publication_backlog(&client, &status_url, 1).await;

    server.shutdown().await.expect("shutdown server");

    let recovery = Spool::open(
        local.path().join("spool"),
        SpoolCapacity::new(1_049_000).expect("spool capacity"),
        AppendBodyLimit::new(1_048_576).expect("maximum batch"),
    )
    .await
    .expect("recover service spool");
    assert_eq!(recovery.report().pending_batches(), 1);
    let (recovered_spool, mut batches, _) = recovery.into_parts();
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
    drop(batches);
    drop(recovered_spool);

    let scratch_path = local.path().join("scratch");
    std::fs::remove_dir_all(&scratch_path).expect("remove staged output before restart");
    std::fs::create_dir(&scratch_path).expect("recreate empty scratch directory");
    let restart_network = format!("elucid-service-restart-{test_identity}");
    let restart_server_name = format!("elucid-service-restart-minio-{test_identity}");
    let restarted_minio = MinIO::default()
        .with_network(restart_network.clone())
        .with_container_name(restart_server_name.clone())
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("start replacement MinIO");
    let restart_minio_alias = format!("http://minioadmin:minioadmin@{restart_server_name}:9000");
    let _restart_bucket = GenericImage::new("minio/mc", MINIO_CLIENT_TAG)
        .with_wait_for(WaitFor::message_on_stdout("Bucket created successfully"))
        .with_network(restart_network)
        .with_env_var("MC_HOST_local", restart_minio_alias)
        .with_cmd(["mb", bucket_path.as_str()])
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("create replacement MinIO bucket");
    let restart_minio_host = restarted_minio
        .get_host()
        .await
        .expect("replacement MinIO host");
    let restart_minio_port = restarted_minio
        .get_host_port_ipv4(9000)
        .await
        .expect("replacement MinIO port");
    let restart_document = runtime_configuration(
        &format!("http://{restart_minio_host}:{restart_minio_port}"),
        local.path().join("spool").to_str().expect("spool path"),
        scratch_path.to_str().expect("scratch path"),
    );
    let configuration = RuntimeConfiguration::from_toml(&restart_document, &environment)
        .expect("decode restart configuration");
    let restarted = start(configuration).await.expect("restart server");
    let restarted_endpoint = format!("http://{}", restarted.local_address());
    let restart_ready = wait_for_status_until(
        &client,
        &format!("{restarted_endpoint}/health/ready"),
        StatusCode::OK,
        Duration::from_secs(20),
    )
    .await;
    if !restart_ready {
        let error = restarted
            .shutdown()
            .await
            .expect_err("restarted server should expose its startup failure");
        panic!("restarted server did not become ready: {error:?}");
    }
    let segments = wait_for_list_items(
        &client,
        &format!("{restarted_endpoint}/api/v1/segments?source_id={source_id}&state=ACTIVE"),
        "segments",
        2,
    )
    .await;
    assert_eq!(segments["segments"][0]["row_count"], 2);
    let drained =
        wait_for_pending_batches(&client, &format!("{restarted_endpoint}/api/v1/status"), 0).await;
    assert_eq!(drained["publication"]["prepared_segments"], 0);
    assert_eq!(drained["publication"]["planned_objects"], 0);
    assert_eq!(regular_files_under(&scratch_path), 0);
    restarted
        .shutdown()
        .await
        .expect("shutdown restarted server");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn disconnect_is_ambiguous_and_shutdown_discards_an_incomplete_request() {
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
    let applied = apply_catalog(&client, &endpoint).await;
    let source_id = applied["source_id"].as_str().expect("source identity");

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

    let dead_letters = wait_for_list_items(
        &client,
        &format!("{endpoint}/api/v1/dead-letters?source_id={source_id}"),
        "dead_letters",
        1,
    )
    .await;
    let disconnected_batch_id = dead_letters["dead_letters"][0]["batch_id"]
        .as_str()
        .expect("disconnected batch identity");
    assert_eq!(
        Uuid::parse_str(disconnected_batch_id)
            .expect("UUID disconnected batch identity")
            .get_version_num(),
        7
    );
    let status = wait_for_pending_batches(&client, &status_url, 0).await;
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
    assert_eq!(recovery.report().pending_batches(), 0);
    let (_, mut batches, _) = recovery.into_parts();
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

async fn assert_raw_ingestion_error(
    address: std::net::SocketAddr,
    content_length: u64,
    expected_status: &str,
    expected_code: &str,
) {
    let mut connection = TcpStream::connect(address)
        .await
        .expect("connect raw ingestion request");
    let request = format!(
        "POST /api/v1/sources/demo_logs/inputs/vector/events HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-ndjson\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
    );
    connection
        .write_all(request.as_bytes())
        .await
        .expect("write raw ingestion request");
    connection
        .flush()
        .await
        .expect("flush raw ingestion request");
    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(3),
        connection.read_to_end(&mut response),
    )
    .await
    .expect("raw ingestion response timed out")
    .expect("read raw ingestion response");
    let response = String::from_utf8(response).expect("UTF-8 raw ingestion response");
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("split raw ingestion response");
    assert!(headers.starts_with(&format!("HTTP/1.1 {expected_status}")));
    assert!(headers.to_ascii_lowercase().contains("x-request-id:"));
    let body: Value = serde_json::from_str(body).expect("decode raw ingestion error");
    assert_eq!(body["error"]["code"], expected_code);
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

async fn wait_for_status_until(
    client: &Client,
    url: &str,
    expected_status: StatusCode,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(response) = client.get(url).send().await
            && response.status() == expected_status
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
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

async fn wait_for_list_items(client: &Client, url: &str, field: &str, expected: usize) -> Value {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = client.get(url).send().await
            && response.status() == StatusCode::OK
        {
            let body = json(response).await;
            if body[field]
                .as_array()
                .is_some_and(|items| items.len() == expected)
            {
                return body;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{field} did not reach {expected} items"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_publication_backlog(client: &Client, url: &str, expected_planned: u64) -> Value {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = client.get(url).send().await
            && response.status() == StatusCode::OK
        {
            let body = json(response).await;
            if body["publication"]["planned_objects"] == expected_planned {
                return body;
            }
        }
        assert!(
            Instant::now() < deadline,
            "publication backlog did not reach {expected_planned} planned objects"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn regular_files_under(root: &std::path::Path) -> usize {
    let mut pending = vec![root.to_owned()];
    let mut files = 0_usize;
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).expect("read scratch directory") {
            let entry = entry.expect("read scratch entry");
            let file_type = entry.file_type().expect("read scratch entry type");
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                files = files.checked_add(1).expect("scratch file count");
            }
        }
    }
    files
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
