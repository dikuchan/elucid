use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use clap::Parser;

use crate::command::Command;
use crate::utils::get_data_dir;

/// Ingest NDJSON events from stdin into a table.
#[derive(Parser)]
pub struct IngestCommand {
    /// Table name to ingest into.
    table: String,

    /// Path to the data directory. Defaults to `$HOME/.elucid/data`.
    #[arg(long)]
    data_dir: Option<PathBuf>,

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
        let data_dir = get_data_dir(self.data_dir.clone())?;
        let table_name = elucid_ingest::TableName::new(&self.table)?;

        let schema_config = elucid_ingest::SchemaConfig::load(&data_dir, &table_name)?;
        let arrow_schema = schema_config.compile();

        let reader = tokio::io::BufReader::new(tokio::io::stdin());
        let source = elucid_ingest::LineSource::new(reader, 10 * 1024 * 1024);
        let mut sink = elucid_ingest::ParquetSink::new(data_dir.clone(), table_name.clone());
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

        eprintln!(
            "lines_read={} rows_ingested={} dead={} files={}",
            summary.read_line_count,
            summary.ingested_row_count,
            summary.dead_letter_count,
            summary.written_file_count,
        );

        let dead_data = dead_letter_writer.into_inner().0;
        if !dead_data.is_empty() {
            // For now, write dead-letter to <table>/_dead/<id>.jsonl
            let dead_dir = data_dir.join(table_name.as_str()).join("_dead");
            std::fs::create_dir_all(&dead_dir)?;
            let id = uuid::Uuid::now_v7();
            let dead_path = dead_dir.join(format!("{id}.jsonl"));
            std::fs::write(&dead_path, &dead_data)?;
            eprintln!("dead-letter written to {}", dead_path.display());
        }

        Ok(())
    }
}
