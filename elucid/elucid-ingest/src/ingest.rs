use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::{SinkExt, StreamExt};

use crate::batcher::Batcher;
use crate::dead_letter_writer::DeadLetterWriter;
use crate::event::{EventContext, RawEvent};
use crate::normalizer::Normalizer;
use crate::stage_error::StageError;
use crate::wal;
use crate::wal::Wal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestSummary {
    pub read_line_count: u64,
    pub ingested_row_count: u64,
    pub dead_letter_count: u64,
    pub written_file_count: u64,
}

pub async fn ingest<I, O, W, DL, C>(
    source: I,
    schema: Schema,
    sink: &mut O,
    batch_size: usize,
    wal: &mut W,
    dead_letter_writer: &mut DeadLetterWriter<DL>,
) -> Result<IngestSummary, StageError>
where
    I: futures::Stream<Item = Result<RawEvent<C>, StageError>> + Unpin,
    O: futures::Sink<RecordBatch, Error = StageError> + Unpin,
    W: Wal,
    DL: tokio::io::AsyncWrite + Unpin,
    C: EventContext,
{
    let normalizer = Normalizer::new(&schema)?;
    let mut batcher = Batcher::new(schema.clone(), batch_size);

    let mut read_line_count: u64 = 0;
    let mut ingested_row_count: u64 = 0;
    let mut dead_letter_count: u64 = 0;

    let mut last_wal_offset = wal::Offset(0);

    let mut source = std::pin::pin!(source);

    while let Some(result) = source.next().await {
        let raw_event = match result {
            Ok(event) => event,
            Err(StageError::LineTooLarge { .. }) => {
                read_line_count += 1;
                continue;
            }
            Err(e) => return Err(e),
        };

        read_line_count += 1;

        let raw_for_dead_letter = raw_event.raw.clone();
        let context_for_dead_letter = raw_event.context.clone();

        let wal_offset = wal.append(&raw_event.raw).await.map_err(StageError::Wal)?;
        last_wal_offset = wal_offset;

        match normalizer.normalize(&raw_event.raw, raw_event.context) {
            Ok(event) => {
                if let Some(batch) = batcher.push(event)? {
                    let row_count = batch.num_rows() as u64;
                    sink.send(batch).await?;
                    ingested_row_count += row_count;
                    wal.checkpoint(last_wal_offset)
                        .await
                        .map_err(StageError::Wal)?;
                }
            }
            Err(e) => {
                dead_letter_writer
                    .write(&raw_for_dead_letter, &e, &context_for_dead_letter)
                    .await
                    .map_err(StageError::Wal)?;
                dead_letter_count += 1;
            }
        }
    }

    if let Some(batch) = batcher.flush()? {
        let row_count = batch.num_rows() as u64;
        sink.send(batch).await?;
        ingested_row_count += row_count;
        wal.checkpoint(last_wal_offset)
            .await
            .map_err(StageError::Wal)?;
    }

    sink.close().await?;

    Ok(IngestSummary {
        read_line_count,
        ingested_row_count,
        dead_letter_count,
        written_file_count: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::line_source::LineSource;
    use crate::parquet_sink::ParquetSink;
    use crate::schema::{SchemaConfig, TableName};
    use crate::wal::NoopWal;

    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[derive(Debug)]
    struct VecWriter(Vec<u8>);

    impl VecWriter {
        fn new() -> Self {
            Self(Vec::new())
        }
    }

    impl tokio::io::AsyncWrite for VecWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().0.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn test_schema() -> Schema {
        let yaml = r#"
            table: test
            columns:
              - name: _ts
                type: timestamp
                time: true
              - name: msg
                type: utf8
        "#;
        SchemaConfig::from_yaml(yaml).expect("schema").compile()
    }

    fn count_parquet_files(dir: &std::path::Path) -> usize {
        let table_dir = dir.join("test");
        if !table_dir.exists() {
            return 0;
        }
        std::fs::read_dir(table_dir)
            .expect("read dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count()
    }

    #[tokio::test]
    async fn five_valid_events_batch_size_three_two_files() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let schema = test_schema();
        let table = TableName::new("test").expect("table name");

        let input = r#"{"_ts":"2025-01-01T00:00:00Z","msg":"a"}
{"_ts":"2025-01-01T00:00:01Z","msg":"b"}
{"_ts":"2025-01-01T00:00:02Z","msg":"c"}
{"_ts":"2025-01-01T00:00:03Z","msg":"d"}
{"_ts":"2025-01-01T00:00:04Z","msg":"e"}
"#;

        let reader = tokio::io::BufReader::new(input.as_bytes());
        let source = LineSource::new(reader, 1024 * 1024);
        let mut sink = ParquetSink::new(tmp.path().to_path_buf(), table.clone());
        let mut wal = NoopWal::new();
        let mut dead_letter = DeadLetterWriter::new(VecWriter::new());

        let summary = ingest(source, schema, &mut sink, 3, &mut wal, &mut dead_letter)
            .await
            .expect("pipeline");

        assert_eq!(summary.read_line_count, 5);
        assert_eq!(summary.ingested_row_count, 5);
        assert_eq!(summary.dead_letter_count, 0);
        assert_eq!(summary.written_file_count, 0); // TODO: sink doesn't expose count through trait.
        assert_eq!(count_parquet_files(tmp.path()), 2);
    }

    #[tokio::test]
    async fn three_valid_one_bad_json_three_rows_one_dead() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let schema = test_schema();
        let table = TableName::new("test").expect("table name");

        let input = r#"{"_ts":"2025-01-01T00:00:00Z","msg":"a"}
NOT_JSON
{"_ts":"2025-01-01T00:00:01Z","msg":"b"}
{"_ts":"2025-01-01T00:00:02Z","msg":"c"}
"#;

        let reader = tokio::io::BufReader::new(input.as_bytes());
        let source = LineSource::new(reader, 1024 * 1024);
        let mut sink = ParquetSink::new(tmp.path().to_path_buf(), table.clone());
        let mut wal = NoopWal::new();
        let mut dead_letter = DeadLetterWriter::new(VecWriter::new());

        let summary = ingest(source, schema, &mut sink, 10, &mut wal, &mut dead_letter)
            .await
            .expect("pipeline");

        assert_eq!(summary.read_line_count, 4);
        assert_eq!(summary.ingested_row_count, 3);
        assert_eq!(summary.dead_letter_count, 1);
        assert_eq!(count_parquet_files(tmp.path()), 1);
    }

    #[tokio::test]
    async fn empty_input_zero_files_zero_rows() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let schema = test_schema();
        let table = TableName::new("test").expect("table name");

        let reader = tokio::io::BufReader::new(&b""[..]);
        let source = LineSource::new(reader, 1024 * 1024);
        let mut sink = ParquetSink::new(tmp.path().to_path_buf(), table.clone());
        let mut wal = NoopWal::new();
        let mut dead_letter = DeadLetterWriter::new(VecWriter::new());

        let summary = ingest(source, schema, &mut sink, 10, &mut wal, &mut dead_letter)
            .await
            .expect("pipeline");

        assert_eq!(summary.read_line_count, 0);
        assert_eq!(summary.ingested_row_count, 0);
        assert_eq!(summary.dead_letter_count, 0);
        assert_eq!(count_parquet_files(tmp.path()), 0);
    }
}
