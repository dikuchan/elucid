//! Parquet file sink.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::schema::TableName;
use crate::stage_error::StageError;

pub struct ParquetSink {
    data_root: PathBuf,
    table: TableName,
    pending: Option<RecordBatch>,
    in_flight: Option<Pin<Box<dyn Future<Output = Result<(), StageError>> + Send>>>,
    written_file_count: u64,
}

impl ParquetSink {
    /// Create a new sink that writes Parquet files under `data_root/<table>/`.
    pub fn new(data_root: PathBuf, table: TableName) -> Self {
        Self {
            data_root,
            table,
            pending: None,
            in_flight: None,
            written_file_count: 0,
        }
    }

    /// Number of Parquet files written so far.
    pub fn written_file_count(&self) -> u64 {
        self.written_file_count
    }
}

impl futures::Sink<RecordBatch> for ParquetSink {
    type Error = StageError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: RecordBatch) -> Result<(), Self::Error> {
        self.get_mut().pending = Some(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        // Drive any in-flight write to completion before starting a new one.
        if let Some(fut) = this.in_flight.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    this.in_flight = None;
                    this.written_file_count += 1;
                }
                Poll::Ready(Err(e)) => {
                    this.in_flight = None;
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // No in-flight write; check for a pending batch.
        let batch = match this.pending.take() {
            Some(b) => b,
            None => return Poll::Ready(Ok(())),
        };

        let data_root = this.data_root.clone();
        let table = this.table.clone();
        let mut fut = Box::pin(flush_batch(data_root, table, batch));

        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => {
                this.written_file_count += 1;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                this.in_flight = Some(fut);
                Poll::Pending
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

/// Write a single batch to a Parquet file on disk.
async fn flush_batch(
    data_root: std::path::PathBuf,
    table: TableName,
    batch: RecordBatch,
) -> Result<(), StageError> {
    let table_dir = data_root.join(table.as_str());
    tokio::fs::create_dir_all(&table_dir)
        .await
        .map_err(|e| StageError::Write(format!("failed to create directory: {e}")))?;

    let id = uuid::Uuid::now_v7();
    let path = table_dir.join(format!("{id}.parquet"));

    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await
        .map_err(|e| StageError::Write(format!("failed to create file: {e}")))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = AsyncArrowWriter::try_new(file, batch.schema().clone(), Some(props))
        .map_err(|e| StageError::Write(format!("Parquet writer init: {e}")))?;
    writer
        .write(&batch)
        .await
        .map_err(|e| StageError::Write(format!("Parquet write: {e}")))?;
    writer
        .finish()
        .await
        .map_err(|e| StageError::Write(format!("Parquet finish: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::{self, File};
    use std::path::Path;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::SinkExt;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .expect("valid batch")
    }

    fn count_parquet_files(dir: &Path) -> usize {
        fs::read_dir(dir)
            .expect("read dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count()
    }

    fn read_parquet_batches(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).expect("open Parquet file");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("builder")
            .build()
            .expect("reader");
        reader.map(|b| b.expect("read batch")).collect()
    }

    #[tokio::test]
    async fn write_one_batch_creates_one_file_with_correct_data() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let mut sink = ParquetSink::new(
            tmp.path().to_path_buf(),
            TableName::new("events").expect("table name"),
        );

        let original = test_batch();
        sink.send(original.clone())
            .await
            .expect("send should succeed");
        sink.close().await.expect("close should succeed");

        let table_dir = tmp.path().join("events");
        assert!(table_dir.is_dir(), "table directory should exist");
        assert_eq!(count_parquet_files(&table_dir), 1);
        assert_eq!(sink.written_file_count(), 1);

        let parquet_path = std::fs::read_dir(&table_dir)
            .expect("read dir")
            .filter_map(|e| e.ok())
            .find(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .expect("should find a Parquet file")
            .path();

        let batches = read_parquet_batches(&parquet_path);
        assert_eq!(batches.len(), 1);

        let read_batch = &batches[0];
        assert_eq!(original.num_rows(), read_batch.num_rows());
        assert_eq!(original.schema(), read_batch.schema());

        let original_ids = original
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        let read_ids = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(original_ids, read_ids);

        let original_names = original
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string");
        let read_names = read_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string");
        assert_eq!(original_names, read_names);
    }

    #[tokio::test]
    async fn write_three_batches_creates_three_files() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let mut sink = ParquetSink::new(
            tmp.path().to_path_buf(),
            TableName::new("events").expect("table name"),
        );

        for _ in 0..3 {
            sink.send(test_batch()).await.expect("send");
        }
        sink.close().await.expect("close");

        let table_dir = tmp.path().join("events");
        assert_eq!(count_parquet_files(&table_dir), 3);
        assert_eq!(sink.written_file_count(), 3);
    }

    #[tokio::test]
    async fn flush_without_pending_is_noop() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let mut sink = ParquetSink::new(
            tmp.path().to_path_buf(),
            TableName::new("events").expect("table name"),
        );

        // Flush without ever sending anything.
        futures::SinkExt::flush(&mut sink)
            .await
            .expect("flush should succeed");
        sink.close().await.expect("close");

        let table_dir = tmp.path().join("events");
        assert!(
            !table_dir.exists(),
            "table directory should not exist when no batches written"
        );
        assert_eq!(sink.written_file_count(), 0);
    }

    #[tokio::test]
    async fn written_parquet_schema_matches_input() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let mut sink = ParquetSink::new(
            tmp.path().to_path_buf(),
            TableName::new("metrics").expect("table name"),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(arrow::array::Float64Array::from(vec![Some(42.5)])),
            ],
        )
        .expect("valid batch");

        sink.send(batch).await.expect("send");
        sink.close().await.expect("close");

        let table_dir = tmp.path().join("metrics");
        let parquet_path = std::fs::read_dir(&table_dir)
            .expect("read dir")
            .filter_map(|e| e.ok())
            .find(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .expect("parquet file")
            .path();

        let batches = read_parquet_batches(&parquet_path);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().as_ref(), schema.as_ref());
    }
}
