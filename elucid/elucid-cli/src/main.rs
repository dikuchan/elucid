mod application;
mod arguments;
mod client;
mod error;
mod input;
mod version;

use std::process::ExitCode;

use clap::Parser;

use crate::arguments::Arguments;
use crate::error::ProcessExit;

#[tokio::main]
async fn main() -> ExitCode {
    match Arguments::try_parse() {
        Ok(arguments) => application::run(arguments).await.into(),
        Err(error) => {
            let process_exit = if error.use_stderr() {
                ProcessExit::CommandOrDocumentValidationFailure
            } else {
                ProcessExit::Success
            };
            match error.print() {
                Ok(()) => process_exit.into(),
                Err(_) => ProcessExit::UncategorizedInternalFailure.into(),
            }
        }
    }
}
