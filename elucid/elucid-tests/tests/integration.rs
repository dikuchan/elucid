use std::io;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use elucid_engine::Context;
use elucid_ingestion::{
    DeadLetterWriter, LineSource, NoopWal, ParquetSink, SchemaConfig, TableName, run_ingestion,
};
use elucid_language::CatalogSnapshot;

mod support;

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

/// Count rows across all RecordBatches in a DataFrame result.
async fn count_rows(df: datafusion::dataframe::DataFrame) -> usize {
    df.collect()
        .await
        .expect("query execution failed")
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

/// Register schema and ingestion test data.
async fn setup_and_ingestion(data_dir: &std::path::Path) {
    let schema = SchemaConfig::from_yaml(SCHEMA_YAML).expect("schema parse failed");
    schema
        .register(data_dir)
        .expect("schema registration failed");

    let arrow_schema = schema.compile();
    let table = TableName::new("test_logs").expect("table name");

    let reader = tokio::io::BufReader::new(TEST_DATA.as_bytes());
    let source = LineSource::new(reader, 10 * 1024 * 1024);
    let mut sink = ParquetSink::new(data_dir.to_path_buf(), table.clone());
    let mut wal = NoopWal::new();
    let mut dead_letter = DeadLetterWriter::new(VecWriter(Vec::new()));

    let summary = run_ingestion(
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
    assert_eq!(summary.accepted_row_count, 5, "should accept 5 rows");
    assert_eq!(summary.dead_letter_count, 0, "no dead-letter entries");
}

#[tokio::test]
async fn ingestion_then_query_all_rows() {
    let dir = tempfile::tempdir().expect("temp dir creation failed");
    setup_and_ingestion(dir.path()).await;

    let ctx = Context::new(dir.path());
    let source = support::test_logs_source();
    let df = ctx
        .execute("source test_logs", &CatalogSnapshot::new(&source))
        .await
        .expect("query failed");
    let rows = count_rows(df).await;

    assert_eq!(rows, 5, "unfiltered query should return all 5 rows");
}

#[tokio::test]
async fn ingestion_then_query_filter_status_gte_400() {
    let dir = tempfile::tempdir().expect("temp dir creation failed");
    setup_and_ingestion(dir.path()).await;

    let ctx = Context::new(dir.path());
    let source = support::test_logs_source();
    let df = ctx
        .execute(
            "source test_logs | filter status >= 400",
            &CatalogSnapshot::new(&source),
        )
        .await
        .expect("query failed");
    let rows = count_rows(df).await;

    assert_eq!(rows, 3, "status >= 400 should match 3 rows (404, 500, 403)");
}

#[tokio::test]
async fn ingestion_then_query_filter_source_nginx() {
    let dir = tempfile::tempdir().expect("temp dir creation failed");
    setup_and_ingestion(dir.path()).await;

    let ctx = Context::new(dir.path());
    let source = support::test_logs_source();
    let df = ctx
        .execute(
            "source test_logs | filter source == \"nginx\"",
            &CatalogSnapshot::new(&source),
        )
        .await
        .expect("query failed");
    let rows = count_rows(df).await;

    assert_eq!(rows, 3, "source=nginx should match 3 rows");
}

#[tokio::test]
async fn ingestion_then_query_count() {
    let dir = tempfile::tempdir().expect("temp dir creation failed");
    setup_and_ingestion(dir.path()).await;

    let ctx = Context::new(dir.path());
    let source = support::test_logs_source();
    let df = ctx
        .execute(
            "source test_logs | summarize event_count = count()",
            &CatalogSnapshot::new(&source),
        )
        .await
        .expect("query failed");

    let batches = df.collect().await.expect("collect failed");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "summarize count should return 1 row");

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
