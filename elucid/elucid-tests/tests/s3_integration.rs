//! S3 integration tests using LocalStack via testcontainers.
//!
//! These tests require Docker. They are ignored by default — run with:
//!   cargo test --test s3_integration -- --ignored

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use elucid_engine::{Context, StorageConfig};
use elucid_ingest::{
    DeadLetterWriter, LineSource, NoopWal, ObjectStoreSink, SchemaConfig, TableName, ingest,
};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use testcontainers_modules::localstack::LocalStack;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
use url::Url;

const TEST_DATA: &str = r#"{"timestamp":"2025-01-15T10:00:00Z","source":"nginx","status":200,"path":"/home"}
{"timestamp":"2025-01-15T10:01:00Z","source":"nginx","status":404,"path":"/missing"}
{"timestamp":"2025-01-15T10:02:00Z","source":"app","status":500,"path":"/api/error"}
{"timestamp":"2025-01-15T10:03:00Z","source":"nginx","status":200,"path":"/about"}
{"timestamp":"2025-01-15T10:04:00Z","source":"app","status":403,"path":"/admin"}
"#;

const SCHEMA_YAML: &str = r#"
table: test_logs
columns:
  - name: timestamp
    type: timestamp
    time: true
  - name: source
    type: utf8
  - name: status
    type: int64
  - name: path
    type: utf8
"#;

struct VecWriter(Vec<u8>);

impl tokio::io::AsyncWrite for VecWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().0.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn count_rows(df: datafusion::dataframe::DataFrame) -> usize {
    df.collect()
        .await
        .expect("query execution failed")
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

/// Build an `object_store` S3 client pointing at a LocalStack container.
fn build_s3_store(endpoint: &str, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .expect("failed to build S3 object store"),
    )
}

/// Create the bucket in LocalStack via a raw S3 CreateBucket PUT request.
async fn create_bucket(endpoint: &str, bucket: &str) {
    let client = reqwest::Client::new();
    let url = format!("{endpoint}/{bucket}");
    let resp = client
        .put(&url)
        .send()
        .await
        .expect("bucket create request failed");
    assert!(
        resp.status().is_success(),
        "failed to create bucket: {}",
        resp.text().await.unwrap_or_default()
    );
}

/// Ingest test data into S3 via ObjectStoreSink.
async fn ingest_to_s3(store: Arc<dyn ObjectStore>, prefix: &str, table: &str) {
    let schema = SchemaConfig::from_yaml(SCHEMA_YAML).expect("schema parse failed");
    let arrow_schema = schema.compile();
    let table_name = TableName::new(table).expect("table name");

    let reader = tokio::io::BufReader::new(TEST_DATA.as_bytes());
    let source = LineSource::new(reader, 10 * 1024 * 1024);

    let object_prefix = ObjectPath::from(prefix);
    let mut sink = ObjectStoreSink::new(store, object_prefix, table_name.clone());
    let mut wal = NoopWal::new();
    let mut dead_letter = DeadLetterWriter::new(VecWriter(Vec::new()));

    let summary = ingest(
        source,
        arrow_schema,
        &mut sink,
        10_000,
        &mut wal,
        &mut dead_letter,
    )
    .await
    .expect("ingestion failed");

    assert_eq!(summary.read_line_count, 5, "should read 5 lines");
    assert_eq!(summary.ingested_row_count, 5, "should ingest 5 rows");
    assert_eq!(summary.dead_letter_count, 0, "no dead-letter entries");
}

#[tokio::test]
#[ignore]
async fn s3_ingest_then_query_all_rows() {
    let container = LocalStack::default()
        .with_env_var("SERVICES", "s3")
        .start()
        .await
        .expect("failed to start LocalStack");

    let host_ip = container.get_host().await.expect("host ip");
    let host_port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("host port");
    let endpoint = format!("http://{host_ip}:{host_port}");
    let bucket = "elucid-test";

    let store = build_s3_store(&endpoint, bucket);
    create_bucket(&endpoint, bucket).await;

    let prefix = "data";
    let table = "test_logs";
    ingest_to_s3(store.clone(), prefix, table).await;

    // Query via engine
    let url = Url::parse(&format!("s3://{bucket}/{prefix}")).expect("url parse");
    let config = StorageConfig::ObjectStore {
        store,
        url,
        prefix: prefix.to_owned(),
    };
    let ctx = Context::with_storage_config(config);

    let df = ctx
        .execute(&format!("dataset {table}"))
        .await
        .expect("query failed");
    let rows = count_rows(df).await;
    assert_eq!(rows, 5, "unfiltered query should return all 5 rows");
}

#[tokio::test]
#[ignore]
async fn s3_ingest_then_query_filter() {
    let container = LocalStack::default()
        .with_env_var("SERVICES", "s3")
        .start()
        .await
        .expect("failed to start LocalStack");

    let host_ip = container.get_host().await.expect("host ip");
    let host_port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("host port");
    let endpoint = format!("http://{host_ip}:{host_port}");
    let bucket = "elucid-test";

    let store = build_s3_store(&endpoint, bucket);
    create_bucket(&endpoint, bucket).await;

    let prefix = "data";
    let table = "test_logs";
    ingest_to_s3(store.clone(), prefix, table).await;

    let url = Url::parse(&format!("s3://{bucket}/{prefix}")).expect("url parse");
    let config = StorageConfig::ObjectStore {
        store,
        url,
        prefix: prefix.to_owned(),
    };
    let ctx = Context::with_storage_config(config);

    let df = ctx
        .execute(&format!("dataset {table} | where status >= 400"))
        .await
        .expect("query failed");
    let rows = count_rows(df).await;
    assert_eq!(rows, 3, "status >= 400 should match 3 rows");
}

#[tokio::test]
#[ignore]
async fn s3_ingest_then_query_count() {
    let container = LocalStack::default()
        .with_env_var("SERVICES", "s3")
        .start()
        .await
        .expect("failed to start LocalStack");

    let host_ip = container.get_host().await.expect("host ip");
    let host_port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("host port");
    let endpoint = format!("http://{host_ip}:{host_port}");
    let bucket = "elucid-test";

    let store = build_s3_store(&endpoint, bucket);
    create_bucket(&endpoint, bucket).await;

    let prefix = "data";
    let table = "test_logs";
    ingest_to_s3(store.clone(), prefix, table).await;

    let url = Url::parse(&format!("s3://{bucket}/{prefix}")).expect("url parse");
    let config = StorageConfig::ObjectStore {
        store,
        url,
        prefix: prefix.to_owned(),
    };
    let ctx = Context::with_storage_config(config);

    let df = ctx
        .execute(&format!("dataset {table} | stats count()"))
        .await
        .expect("query failed");

    let batches = df.collect().await.expect("collect failed");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "stats count should return 1 row");

    let batch = &batches[0];
    let col = batch.column(0);
    let count_val = col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .map(|c| c.value(0))
        .or_else(|| {
            col.as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .map(|c| c.value(0) as i64)
        })
        .expect("count column should be Int64 or UInt64");
    assert_eq!(count_val, 5, "count(*) of 5 rows should be 5");
}

#[tokio::test]
#[ignore]
async fn s3_ingest_multiple_batches_then_query() {
    let container = LocalStack::default()
        .with_env_var("SERVICES", "s3")
        .start()
        .await
        .expect("failed to start LocalStack");

    let host_ip = container.get_host().await.expect("host ip");
    let host_port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("host port");
    let endpoint = format!("http://{host_ip}:{host_port}");
    let bucket = "elucid-test";

    let store = build_s3_store(&endpoint, bucket);
    create_bucket(&endpoint, bucket).await;

    let schema = SchemaConfig::from_yaml(SCHEMA_YAML).expect("schema parse failed");
    let arrow_schema = schema.compile();
    let table = "test_logs";
    let table_name = TableName::new(table).expect("table name");

    // Use batch_size=2 to force multiple Parquet files (5 rows / 2 = 3 files)
    let reader = tokio::io::BufReader::new(TEST_DATA.as_bytes());
    let source = LineSource::new(reader, 10 * 1024 * 1024);

    let object_prefix = ObjectPath::from("data");
    let mut sink = ObjectStoreSink::new(store.clone(), object_prefix, table_name.clone());
    let mut wal = NoopWal::new();
    let mut dead_letter = DeadLetterWriter::new(VecWriter(Vec::new()));

    let summary = ingest(
        source,
        arrow_schema,
        &mut sink,
        2, // batch_size=2 → 3 Parquet files
        &mut wal,
        &mut dead_letter,
    )
    .await
    .expect("ingestion failed");

    assert_eq!(summary.read_line_count, 5);
    assert_eq!(summary.ingested_row_count, 5);

    let url = Url::parse(&format!("s3://{bucket}/data")).expect("url parse");
    let config = StorageConfig::ObjectStore {
        store,
        url,
        prefix: "data".to_owned(),
    };
    let ctx = Context::with_storage_config(config);

    let df = ctx
        .execute(&format!("dataset {table}"))
        .await
        .expect("query failed");
    let rows = count_rows(df).await;
    assert_eq!(rows, 5, "should return all 5 rows across multiple Parquet files");
}
