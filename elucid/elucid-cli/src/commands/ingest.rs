use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{Context as _, anyhow};
use clap::Parser;

use crate::command::Command;
use crate::utils::{DataDirConfig, resolve_data_dir};

/// Ingest NDJSON events from stdin into a table.
#[derive(Parser)]
pub struct IngestCommand {
    /// Table name to ingest into.
    table: String,

    /// Data directory: a local path or object-store URL (e.g. `s3://bucket/prefix`).
    /// Defaults to `$HOME/.elucid/data`.
    #[arg(long, value_name = "DATA_DIR")]
    data_dir: Option<String>,

    /// Path to a schema YAML file. Required when `--data-dir` is an object-store URL.
    /// When using a local data directory, the schema is loaded from
    /// `<data_dir>/<table>/_schema.yaml` unless this flag is specified.
    #[arg(long, value_name = "SCHEMA_FILE")]
    schema: Option<PathBuf>,

    /// Maximum rows per Parquet file.
    #[arg(long, default_value = "10000")]
    batch_size: usize,
}

struct VecAsyncWriter(Vec<u8>);

impl tokio::io::AsyncWrite for VecAsyncWriter {
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

impl Command for IngestCommand {
    async fn execute(&self) -> anyhow::Result<()> {
        let config = resolve_data_dir(self.data_dir.clone())?;
        let table_name = elucid_ingest::TableName::new(&self.table)?;

        let schema_config = match (&config, &self.schema) {
            (DataDirConfig::Local(data_dir), None) => {
                elucid_ingest::SchemaConfig::load(data_dir, &table_name)?
            }
            (_, Some(schema_path)) => {
                let yaml =
                    std::fs::read_to_string(schema_path).context("Failed to read schema file")?;
                elucid_ingest::SchemaConfig::from_yaml(&yaml)?
            }
            (DataDirConfig::ObjectStore { .. }, None) => {
                return Err(anyhow!(
                    "The --schema flag is required when using an object-store URL for --data-dir"
                ));
            }
        };
        let arrow_schema = schema_config.compile();

        let reader = tokio::io::BufReader::new(tokio::io::stdin());
        let source = elucid_ingest::LineSource::new(reader, 10 * 1024 * 1024);

        let summary = match &config {
            DataDirConfig::Local(data_dir) => {
                let mut sink =
                    elucid_ingest::ParquetSink::new(data_dir.clone(), table_name.clone());
                let mut wal = elucid_ingest::NoopWal::new();
                let mut dead_letter_writer =
                    elucid_ingest::DeadLetterWriter::new(VecAsyncWriter(Vec::new()));

                let summary = elucid_ingest::ingest(
                    source,
                    arrow_schema,
                    &mut sink,
                    self.batch_size,
                    &mut wal,
                    &mut dead_letter_writer,
                )
                .await?;

                let dead_data = dead_letter_writer.into_inner().0;
                if !dead_data.is_empty() {
                    let dead_dir = data_dir.join(table_name.as_str()).join("_dead");
                    std::fs::create_dir_all(&dead_dir)?;
                    let id = uuid::Uuid::now_v7();
                    let dead_path = dead_dir.join(format!("{id}.jsonl"));
                    std::fs::write(&dead_path, &dead_data)?;
                    eprintln!("dead-letter written to {}", dead_path.display());
                }

                summary
            }
            DataDirConfig::ObjectStore { store, prefix, .. } => {
                let object_prefix = object_store::path::Path::from(prefix.as_str());
                let mut sink = elucid_ingest::ObjectStoreSink::new(
                    store.clone(),
                    object_prefix,
                    table_name.clone(),
                );
                let mut wal = elucid_ingest::NoopWal::new();
                let mut dead_letter_writer =
                    elucid_ingest::DeadLetterWriter::new(VecAsyncWriter(Vec::new()));

                let summary = elucid_ingest::ingest(
                    source,
                    arrow_schema,
                    &mut sink,
                    self.batch_size,
                    &mut wal,
                    &mut dead_letter_writer,
                )
                .await?;

                // Object-store path: dead-letter goes to stderr for now.
                let dead_data = dead_letter_writer.into_inner().0;
                if !dead_data.is_empty() {
                    eprintln!(
                        "dead-letter: {} bytes (not written to object store)",
                        dead_data.len()
                    );
                }

                summary
            }
        };

        eprintln!(
            "lines_read={} rows_ingested={} dead={} files={}",
            summary.read_line_count,
            summary.ingested_row_count,
            summary.dead_letter_count,
            summary.written_file_count,
        );

        Ok(())
    }
}
