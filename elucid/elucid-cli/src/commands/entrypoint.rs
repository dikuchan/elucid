use clap::{Parser, Subcommand};

use crate::command::Command;
use crate::commands::{ExecuteCommand, IngestCommand, ReplCommand, SchemaCommand, ValidateCommand};

#[derive(Parser)]
#[command(version, about)]
pub struct Entrypoint {
    #[command(subcommand)]
    subcommand: Option<Subcommands>,
}

impl Command for Entrypoint {
    async fn execute(&self) -> anyhow::Result<()> {
        match &self.subcommand {
            Some(Subcommands::Execute(v)) => v.execute().await,
            Some(Subcommands::Repl(v)) => v.execute().await,
            Some(Subcommands::Validate(v)) => v.execute().await,
            Some(Subcommands::Schema(v)) => v.execute().await,
            Some(Subcommands::Ingest(v)) => v.execute().await,
            None => Ok(()),
        }
    }
}

#[derive(Subcommand)]
pub enum Subcommands {
    Execute(ExecuteCommand),
    Repl(ReplCommand),
    Validate(ValidateCommand),
    Schema(SchemaCommand),
    Ingest(IngestCommand),
}
