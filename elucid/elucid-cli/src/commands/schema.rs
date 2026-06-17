use std::path::PathBuf;

use anyhow::anyhow;
use clap::{Parser, Subcommand};

use crate::command::Command;
use crate::utils::{DataDirConfig, resolve_data_dir};

/// Schema management commands.
#[derive(Parser)]
pub struct SchemaCommand {
    /// Data directory: a local path or object-store URL (e.g. `s3://bucket/prefix`).
    /// Defaults to `$HOME/.elucid/data`. Schema registration requires a local path.
    #[arg(long, global = true, value_name = "DATA_DIR")]
    pub data_dir: Option<String>,

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
        let config = resolve_data_dir(self.data_dir.clone())?;
        let data_dir = match &config {
            DataDirConfig::Local(path) => path,
            DataDirConfig::ObjectStore { .. } => {
                return Err(anyhow!(
                    "Schema registration requires a local data directory"
                ));
            }
        };

        match &self.subcommand {
            SchemaSubcommand::Register(args) => {
                let config_str = std::fs::read_to_string(&args.path)?;
                let schema_config = elucid_ingest::SchemaConfig::from_yaml(&config_str)?;
                let table = schema_config.table.clone();
                schema_config.register(data_dir)?;
                println!("registered table '{table}'");
                Ok(())
            }
        }
    }
}
