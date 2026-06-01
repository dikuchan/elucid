use std::fs::File;
use std::io::{Read, stdin};
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use clap::Args;
use elucid_engine::Context;

use crate::command::Command;
use crate::utils::get_data_dir_path;

#[derive(Args)]
pub struct ExecuteCommand {
    /// Query text.
    #[arg(value_name = "QUERY")]
    pub source: Option<String>,

    /// Path to a query file.
    #[arg(long = "file", short = 'f', value_name = "FILE")]
    pub file_path: Option<PathBuf>,

    /// Path to the data directory. Defaults to `$HOME/.lantern/data`.
    #[arg(long = "data-dir", short = 'd', value_name = "DATA_DIR")]
    pub data_dir_path: Option<PathBuf>,
}

impl ExecuteCommand {
    async fn execute_query_input<R: Read>(&self, mut input: R) -> anyhow::Result<()> {
        let mut buffer = Vec::new();
        let _ = input.read_to_end(&mut buffer)?;
        let source = String::from_utf8(buffer)?;

        let data_dir_path = get_data_dir_path(self.data_dir_path.clone())?;
        if !data_dir_path.exists() {
            return Err(anyhow!("Data directory doesn't exist"));
        }

        self.execute_query(&source, data_dir_path).await
    }

    async fn execute_query<P: AsRef<Path>>(
        &self,
        source: &str,
        data_dir_path: P,
    ) -> anyhow::Result<()> {
        let context = Context::new(data_dir_path);
        let data = context.execute(&source).await?;
        data.show().await?;

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
