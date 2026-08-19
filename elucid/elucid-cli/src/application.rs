use std::path::Path;

use anyhow::Context as _;
use elucid_service::RuntimeConfiguration;
use tokio::io::AsyncWriteExt as _;

use crate::arguments::{
    Action, Arguments, CatalogSubcommand, IngestionSubcommand, RootCommand, ServerSubcommand,
};
use crate::client::{HttpResponse, ProductClient};
use crate::error::{CliErrorCode, Failure, ProcessExit, RemoteOperation};
use crate::input::RequestInput;

pub(crate) async fn run(arguments: Arguments) -> ProcessExit {
    match execute(arguments).await {
        Ok(output) => match write_standard_output(&output).await {
            Ok(()) => ProcessExit::Success,
            Err(failure) => report_failure(failure).await,
        },
        Err(failure) => report_failure(failure).await,
    }
}

async fn execute(arguments: Arguments) -> Result<Vec<u8>, Failure> {
    let action = arguments.into_action().map_err(|message| {
        Failure::command(CliErrorCode::CommandInvalid, anyhow::anyhow!(message))
    })?;
    match action {
        Action::Version(output) => crate::version::render(output),
        Action::Command(command) => execute_command(*command).await,
    }
}

async fn execute_command(command: RootCommand) -> Result<Vec<u8>, Failure> {
    match command {
        RootCommand::Server(command) => match command.into_command() {
            ServerSubcommand::Run(command) => execute_server(&command.into_config_path()),
        },
        RootCommand::Catalog(command) => match command.into_command() {
            CatalogSubcommand::Apply(command) => {
                let (endpoint, file, token_environment_variable, timeout_seconds) =
                    command.into_parts();
                let client =
                    ProductClient::new(timeout_seconds, token_environment_variable.as_ref())?;
                let response = client
                    .apply_catalog(&endpoint, RequestInput::from_path_or_dash(file))
                    .await?;
                require_success(RemoteOperation::CatalogApplication, response)
            }
        },
        RootCommand::Ingestion(command) => match command.into_command() {
            IngestionSubcommand::Submit(command) => {
                let (
                    endpoint,
                    source,
                    input_name,
                    file,
                    idempotency_key,
                    token_environment_variable,
                    timeout_seconds,
                ) = command.into_parts();
                let client =
                    ProductClient::new(timeout_seconds, token_environment_variable.as_ref())?;
                let response = client
                    .submit_ingestion(
                        &endpoint,
                        &source,
                        &input_name,
                        &idempotency_key,
                        RequestInput::from_path_or_dash(file),
                    )
                    .await?;
                require_success(RemoteOperation::Ingestion, response)
            }
        },
    }
}

fn execute_server(configuration_path: &Path) -> Result<Vec<u8>, Failure> {
    let _configuration =
        RuntimeConfiguration::load(Some(configuration_path)).map_err(Failure::configuration)?;
    Err(Failure::internal(
        CliErrorCode::ServerRuntimeUnavailable,
        anyhow::anyhow!("server runtime is not implemented in this build"),
    ))
}

fn require_success(operation: RemoteOperation, response: HttpResponse) -> Result<Vec<u8>, Failure> {
    let status = response.status();
    let body = response.into_body();
    if status.is_success() {
        Ok(body)
    } else {
        Err(Failure::remote_response(operation, status, body))
    }
}

async fn report_failure(failure: Failure) -> ProcessExit {
    let process_exit = failure.process_exit();
    let output = failure.into_stderr();
    let mut stderr = tokio::io::stderr();
    if stderr.write_all(&output).await.is_err() || stderr.flush().await.is_err() {
        return ProcessExit::UncategorizedInternalFailure;
    }
    process_exit
}

async fn write_standard_output(output: &[u8]) -> Result<(), Failure> {
    let mut stdout = tokio::io::stdout();
    stdout
        .write_all(output)
        .await
        .context("failed to write command output")
        .map_err(|error| Failure::internal(CliErrorCode::StandardOutputWriteFailed, error))?;
    stdout
        .flush()
        .await
        .context("failed to flush command output")
        .map_err(|error| Failure::internal(CliErrorCode::StandardOutputWriteFailed, error))
}
