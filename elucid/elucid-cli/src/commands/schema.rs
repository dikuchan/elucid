use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::command::Command;
use crate::utils::get_data_dir;

/// Schema management commands.
#[derive(Parser)]
pub struct SchemaCommand {
    /// Path to the data directory. Defaults to `$HOME/.elucid/data`.
    #[arg(long, global = true)]
    pub data_dir: Option<PathBuf>,

    #[command(subcommand)]
    subcommand: SchemaSubcommand,
}

#[derive(Subcommand)]
enum SchemaSubcommand {
    /// Register a new table schema from a YAML file.
    Register(RegisterArgs),
}

#[derive(Parser)]
struct RegisterArgs {
    /// Path to the schema YAML file.
    path: PathBuf,
}

impl Command for SchemaCommand {
    async fn execute(&self) -> anyhow::Result<()> {
        let data_dir = get_data_dir(self.data_dir.clone())?;
        match &self.subcommand {
            SchemaSubcommand::Register(args) => {
                let config_str = std::fs::read_to_string(&args.path)?;
                let config = elucid_ingest::SchemaConfig::from_yaml(&config_str)?;
                let table = config.table.clone();
                config.register(&data_dir)?;
                println!("registered table '{table}'");
                Ok(())
            }
        }
    }
}
