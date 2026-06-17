use std::fs::File;
use std::io::{Read, stdin};
use std::path::PathBuf;

use clap::Args;

use crate::command::Command;
use crate::utils::{build_engine_context, resolve_data_dir};

#[derive(Args)]
pub struct ExecuteCommand {
    /// Query text.
    #[arg(value_name = "QUERY")]
    pub source: Option<String>,

    /// Path to a query file.
    #[arg(long = "file", short = 'f', value_name = "FILE")]
    pub file_path: Option<PathBuf>,

    /// Data directory: a local path or object-store URL (e.g. `s3://bucket/prefix`).
    /// Defaults to `$HOME/.elucid/data`.
    #[arg(long = "data-dir", short = 'd', value_name = "DATA_DIR")]
    pub data_dir: Option<String>,
}

impl ExecuteCommand {
    async fn execute_query_input<R: Read>(&self, mut input: R) -> anyhow::Result<()> {
        let mut buffer = Vec::new();
        let _ = input.read_to_end(&mut buffer)?;
        let source = String::from_utf8(buffer)?;

        let config = resolve_data_dir(self.data_dir.clone())?;
        let context = build_engine_context(&config)?;

        let df = context.execute(&source).await?;
        df.show().await?;

        Ok(())
    }
}

impl Command for ExecuteCommand {
    async fn execute(&self) -> anyhow::Result<()> {
        match &self.source {
            Some(source) => self.execute_query_input(source.as_bytes()).await,
            None => match &self.file_path {
                Some(file_path) => {
                    let file = File::open(file_path)?;
                    self.execute_query_input(&file).await
                }
                None => self.execute_query_input(stdin()).await,
            },
        }
    }
}
