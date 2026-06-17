use clap::Args;

use crate::command::Command;
use crate::repl;
use crate::utils::{build_engine_context, resolve_data_dir};

#[derive(Args)]
pub struct ReplCommand {
    /// Data directory: a local path or object-store URL (e.g. `s3://bucket/prefix`).
    /// Defaults to `$HOME/.elucid/data`.
    #[arg(long = "data-dir", short = 'd', value_name = "DATA_DIR")]
    pub data_dir: Option<String>,
}

impl Command for ReplCommand {
    async fn execute(&self) -> anyhow::Result<()> {
        let config = resolve_data_dir(self.data_dir.clone())?;
        let context = build_engine_context(&config)?;
        repl::start(&context).await?;

        Ok(())
    }
}
