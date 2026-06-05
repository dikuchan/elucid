use std::path::PathBuf;

use anyhow::anyhow;
use clap::Args;
use elucid_engine::Context;

use crate::command::Command;
use crate::repl;
use crate::utils::get_data_dir;

#[derive(Args)]
pub struct ReplCommand {
    /// Path to the data directory. Defaults to `$HOME/.elucid/data`.
    #[arg(long = "data-dir", short = 'd', value_name = "DATA_DIR")]
    pub data_dir: Option<PathBuf>,
}

impl Command for ReplCommand {
    async fn execute(&self) -> anyhow::Result<()> {
        let data_dir = get_data_dir(self.data_dir.clone())?;
        if !data_dir.exists() {
            return Err(anyhow!("Data dir doesn't exist"));
        }

        let context = Context::new(data_dir);
        repl::start(&context).await?;

        Ok(())
    }
}
