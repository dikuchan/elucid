//! Parquet sink backed by an `object_store::ObjectStore` via multipart uploads.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::schema::TableName;
use crate::stage_error::StageError;

/// Sink that writes [`RecordBatch`] values as individual Parquet objects to an
/// [`ObjectStore`] using `put_multipart` (via [`BufWriter`]).
///
/// Each flushed batch becomes one `<prefix>/<table>/<uuid>.parquet` object.
pub struct ObjectStoreSink {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    table: TableName,
    pending: Option<RecordBatch>,
    in_flight: Option<BoxFuture<'static, Result<(), StageError>>>,
    written_file_count: u64,
}

impl ObjectStoreSink {
    /// Create a new sink that writes Parquet objects under `<prefix>/<table>/`.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: ObjectPath, table: TableName) -> Self {
        Self {
            store,
            prefix,
            table,
            pending: None,
            in_flight: None,
            written_file_count: 0,
        }
    }

    /// Number of Parquet objects written so far.
    pub fn written_file_count(&self) -> u64 {
        self.written_file_count
    }
}

impl futures::Sink<RecordBatch> for ObjectStoreSink {
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

        let store = this.store.clone();
        let prefix = this.prefix.clone();
        let table = this.table.clone();
        let mut fut = Box::pin(flush_batch_object_store(store, prefix, table, batch));

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

/// Write a single batch to a Parquet object in the store.
///
/// The object path is `<prefix>/<table>/<uuid>.parquet`.
async fn flush_batch_object_store(
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    table: TableName,
    batch: RecordBatch,
) -> Result<(), StageError> {
    let id = uuid::Uuid::now_v7();
    let path = ObjectPath::from(format!("{}/{}/{}.parquet", prefix, table, id));

    let buf_writer = BufWriter::new(store, path);
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = AsyncArrowWriter::try_new(buf_writer, batch.schema().clone(), Some(props))
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

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::{SinkExt, StreamExt};
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

    fn read_parquet_batches_from_bytes(bytes: &bytes::Bytes) -> Vec<RecordBatch> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .expect("builder")
            .build()
            .expect("reader");
        reader.map(|b| b.expect("read batch")).collect()
    }

    #[tokio::test]
    async fn write_one_batch_creates_one_object_with_correct_data() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let prefix = ObjectPath::from("data");
        let table = TableName::new("events").expect("table name");

        let mut sink = ObjectStoreSink::new(store.clone(), prefix.clone(), table);
        let original = test_batch();
        sink.send(original.clone())
            .await
            .expect("send should succeed");
        sink.close().await.expect("close should succeed");

        let table_prefix = ObjectPath::from("data/events");
        let objects: Vec<_> = store
            .list(Some(&table_prefix))
            .filter_map(|r| async { r.ok() })
            .collect()
            .await;

        assert_eq!(objects.len(), 1, "expected exactly one object");
        assert_eq!(sink.written_file_count(), 1);

        let result = store.get(&objects[0].location).await.expect("get object");
        let bytes = result.bytes().await.expect("read bytes");
        let batches = read_parquet_batches_from_bytes(&bytes);
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
    async fn write_three_batches_creates_three_objects() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let prefix = ObjectPath::from("data");
        let table = TableName::new("events").expect("table name");

        let mut sink = ObjectStoreSink::new(store.clone(), prefix.clone(), table);
        for _ in 0..3 {
            sink.send(test_batch()).await.expect("send");
        }
        sink.close().await.expect("close");

        let table_prefix = ObjectPath::from("data/events");
        let objects: Vec<_> = store
            .list(Some(&table_prefix))
            .filter_map(|r| async { r.ok() })
            .collect()
            .await;

        assert_eq!(objects.len(), 3);
        assert_eq!(sink.written_file_count(), 3);
    }

    #[tokio::test]
    async fn flush_without_pending_is_noop() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let prefix = ObjectPath::from("data");
        let table = TableName::new("events").expect("table name");

        let mut sink = ObjectStoreSink::new(store.clone(), prefix.clone(), table);
        futures::SinkExt::flush(&mut sink)
            .await
            .expect("flush should succeed");
        sink.close().await.expect("close");

        let table_prefix = ObjectPath::from("data/events");
        let objects: Vec<_> = store
            .list(Some(&table_prefix))
            .filter_map(|r| async { r.ok() })
            .collect()
            .await;

        assert!(objects.is_empty(), "no objects should exist");
        assert_eq!(sink.written_file_count(), 0);
    }
}
