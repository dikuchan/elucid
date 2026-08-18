use std::path::Path;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext};

use crate::planner::QueryPlanner;
use crate::storage::StorageConfig;

pub struct Context {
    context: SessionContext,
    storage: StorageConfig,
}

impl Context {
    /// Creates a new context backed by a local filesystem directory.
    ///
    /// Equivalent to `with_storage_config(StorageConfig::local(data_dir.into()))`.
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Self {
        Self::with_storage_config(StorageConfig::local(data_dir.as_ref().to_owned()))
    }

    /// Creates a new context with the given storage configuration.
    pub fn with_storage_config(storage: StorageConfig) -> Self {
        let config = SessionConfig::new().with_information_schema(true).set_bool(
            "datafusion.execution.parquet.schema_force_view_types",
            false,
        );
        let context = SessionContext::new_with_config(config);
        Self { context, storage }
    }

    pub async fn execute(&self, source: &str) -> Result<DataFrame> {
        let pipeline = elucid_language::analyze(source)
            .map_err(|e| DataFusionError::Plan(format!("Query analysis error: {e}")))?;

        if !self.context.table_exist(pipeline.source().dataset())? {
            self.register_table(pipeline.source().dataset()).await?;
        }

        let planner = QueryPlanner::new(&self.context);
        let plan = planner.create_logical_plan(pipeline).await?;

        self.context.execute_logical_plan(plan).await
    }

    async fn register_table(&self, table_name: &str) -> Result<()> {
        match &self.storage {
            StorageConfig::Local { data_dir } => {
                let table_path = data_dir.join(table_name);
                if !table_path.exists() {
                    return Err(DataFusionError::Execution(format!(
                        "Table '{}' does not exist (directory not found: {:?})",
                        table_name, table_path,
                    )));
                }
                let table_path_str = table_path
                    .to_str()
                    .ok_or(DataFusionError::Execution("Invalid table path".to_owned()))?;

                let options = ParquetReadOptions::new().parquet_pruning(true);
                self.context
                    .register_parquet(table_name, table_path_str, options)
                    .await?;
            }
            StorageConfig::ObjectStore {
                store,
                url,
                prefix: _,
            } => {
                self.context.register_object_store(url, store.clone());

                // DataFusion resolves object-store paths by URL scheme.
                // Build the full URL (with trailing slash) so `register_parquet`
                // treats it as a directory listing.
                let mut full_url = url.to_string();
                if !full_url.ends_with('/') {
                    full_url.push('/');
                }
                full_url.push_str(table_name);
                full_url.push('/');

                let options = ParquetReadOptions::new().parquet_pruning(true);
                self.context
                    .register_parquet(table_name, &full_url, options)
                    .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStore, PutPayload};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use tempfile::TempDir;
    use url::Url;

    use crate::{Context, StorageConfig};

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["alice", "bob", "carol"]);
        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
            .expect("failed to create test batch")
    }

    fn write_parquet_to_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buf, batch.schema(), None).expect("arrow writer create");
        writer.write(batch).expect("arrow writer write");
        writer.finish().expect("arrow writer finish");
        drop(writer);
        buf
    }

    #[tokio::test]
    async fn query_parquet_from_memory_object_store() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = make_test_batch();
        let parquet_bytes = write_parquet_to_bytes(&batch);

        let path = ObjectPath::from("tables/my_table/part-001.parquet");
        let payload = PutPayload::from_bytes(Bytes::from(parquet_bytes));
        store.put(&path, payload).await.expect("store put");

        let url = Url::parse("memory:///tables").expect("url parse");
        let config = StorageConfig::ObjectStore {
            store,
            url,
            prefix: "tables".to_owned(),
        };

        let ctx = Context::with_storage_config(config);
        let df = ctx.execute("source my_table").await.expect("execute");

        let results = df.collect().await.expect("collect");
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn local_context_preserves_behavior() {
        let tmp = TempDir::new().expect("tempdir");

        let table_dir = tmp.path().join("test_table");
        std::fs::create_dir(&table_dir).expect("create table dir");

        let batch = make_test_batch();
        let parquet_bytes = write_parquet_to_bytes(&batch);
        let parquet_path = table_dir.join("part-001.parquet");
        std::fs::write(&parquet_path, &parquet_bytes).expect("write parquet");

        let ctx = Context::new(tmp.path());
        let df = ctx.execute("source test_table").await.expect("execute");

        let results = df.collect().await.expect("collect");
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }
}
