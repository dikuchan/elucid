use std::fs::File;
use std::io::{Read, stdin};
use std::path::PathBuf;

use clap::{Args, Subcommand};
use elucid_language::parser;

use crate::command::Command;

#[derive(Args)]
pub struct ValidateCommand {
    #[command(subcommand)]
    subcommand: ValidateSubcommands,
}

impl Command for ValidateCommand {
    async fn execute(&self) -> anyhow::Result<()> {
        match &self.subcommand {
            ValidateSubcommands::Query(command) => command.execute().await,
        }
    }
}

#[derive(Subcommand)]
pub enum ValidateSubcommands {
    /// Validate a query.
    Query(ValidateQueryCommand),
}

#[derive(Args)]
pub struct ValidateQueryCommand {
    /// Path to a query file.
    #[arg(value_name = "FILE")]
    pub file_path: Option<PathBuf>,
}

impl Command for ValidateQueryCommand {
    async fn execute(&self) -> anyhow::Result<()> {
        validate_query(self.file_path.clone())
    }
}

pub fn validate_query(file_path: Option<PathBuf>) -> anyhow::Result<()> {
    match file_path {
        Some(file_path) => {
            let file = File::open(file_path)?;
            validate_query_input(file)
        }
        None => validate_query_input(stdin()),
    }
}

fn validate_query_input<R: Read>(mut input: R) -> anyhow::Result<()> {
    let mut buffer = Vec::new();
    let _ = input.read_to_end(&mut buffer)?;
    let source = String::from_utf8(buffer)?;

    match parser::check(&source) {
        Ok(_) => Ok(()),
        Err(error) => Ok(error.eprint(&source)?),
    }
}
