use std::fmt::{Display, Formatter};
use std::process::ExitCode;

use anyhow::Error;
use elucid_service::{ConfigurationError, ConfigurationErrorCode};
use reqwest::StatusCode;
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ProcessExit {
    Success = 0,
    UncategorizedInternalFailure = 1,
    CommandOrDocumentValidationFailure = 2,
    ConfigurationFailure = 3,
    RemoteServiceUnavailable = 4,
    CatalogConflict = 5,
    TerminalIngestionFailure = 6,
    LocalClientTimeout = 7,
}

impl From<ProcessExit> for ExitCode {
    fn from(value: ProcessExit) -> Self {
        Self::from(value as u8)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CliErrorCode {
    ClientTimeout,
    CommandInvalid,
    EndpointUrlConstructionFailed,
    HttpClientInitializationFailed,
    InputFileUnreadable,
    InputReadFailed,
    OperatorBearerTokenInvalid,
    OperatorBearerTokenMissing,
    RemoteResponseFailed,
    RemoteResponseInvalid,
    RemoteResponseTooLarge,
    RemoteServiceUnavailable,
    ServerRuntimeUnavailable,
    StandardOutputWriteFailed,
    VersionEncodingFailed,
}

impl CliErrorCode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ClientTimeout => "CLIENT_TIMEOUT",
            Self::CommandInvalid => "COMMAND_INVALID",
            Self::EndpointUrlConstructionFailed => "ENDPOINT_URL_CONSTRUCTION_FAILED",
            Self::HttpClientInitializationFailed => "HTTP_CLIENT_INITIALIZATION_FAILED",
            Self::InputFileUnreadable => "INPUT_FILE_UNREADABLE",
            Self::InputReadFailed => "INPUT_READ_FAILED",
            Self::OperatorBearerTokenInvalid => "OPERATOR_BEARER_TOKEN_INVALID",
            Self::OperatorBearerTokenMissing => "OPERATOR_BEARER_TOKEN_MISSING",
            Self::RemoteResponseFailed => "REMOTE_RESPONSE_FAILED",
            Self::RemoteResponseInvalid => "REMOTE_RESPONSE_INVALID",
            Self::RemoteResponseTooLarge => "REMOTE_RESPONSE_TOO_LARGE",
            Self::RemoteServiceUnavailable => "REMOTE_SERVICE_UNAVAILABLE",
            Self::ServerRuntimeUnavailable => "SERVER_RUNTIME_UNAVAILABLE",
            Self::StandardOutputWriteFailed => "STANDARD_OUTPUT_WRITE_FAILED",
            Self::VersionEncodingFailed => "VERSION_ENCODING_FAILED",
        }
    }
}

impl Display for CliErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RemoteOperation {
    CatalogApplication,
    Ingestion,
}

#[derive(Debug)]
pub(crate) struct Failure {
    process_exit: ProcessExit,
    presentation: FailurePresentation,
}

#[derive(Debug)]
enum FailurePresentation {
    Message { code: FailureCode, source: Error },
    ExactRemoteBody(Vec<u8>),
}

#[derive(Clone, Copy, Debug)]
enum FailureCode {
    Cli(CliErrorCode),
    Configuration(ConfigurationErrorCode),
}

impl Display for FailureCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cli(code) => Display::fmt(code, formatter),
            Self::Configuration(code) => Display::fmt(code, formatter),
        }
    }
}

impl Failure {
    pub(crate) fn command(code: CliErrorCode, source: impl Into<Error>) -> Self {
        Self::message(
            ProcessExit::CommandOrDocumentValidationFailure,
            FailureCode::Cli(code),
            source,
        )
    }

    pub(crate) fn configuration(error: ConfigurationError) -> Self {
        let code = error.code();
        Self::message(
            ProcessExit::ConfigurationFailure,
            FailureCode::Configuration(code),
            error,
        )
    }

    pub(crate) fn internal(code: CliErrorCode, source: impl Into<Error>) -> Self {
        Self::message(
            ProcessExit::UncategorizedInternalFailure,
            FailureCode::Cli(code),
            source,
        )
    }

    pub(crate) fn remote_unavailable(code: CliErrorCode, source: impl Into<Error>) -> Self {
        Self::message(
            ProcessExit::RemoteServiceUnavailable,
            FailureCode::Cli(code),
            source,
        )
    }

    pub(crate) fn client_timeout(source: impl Into<Error>) -> Self {
        Self::message(
            ProcessExit::LocalClientTimeout,
            FailureCode::Cli(CliErrorCode::ClientTimeout),
            source,
        )
    }

    pub(crate) fn from_request_error(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::client_timeout(error)
        } else if error.is_body() {
            Self::command(CliErrorCode::InputReadFailed, error)
        } else {
            Self::remote_unavailable(CliErrorCode::RemoteServiceUnavailable, error)
        }
    }

    pub(crate) fn remote_response(
        operation: RemoteOperation,
        status: StatusCode,
        body: Vec<u8>,
    ) -> Self {
        let error_code = serde_json::from_slice::<RemoteErrorEnvelope>(&body)
            .ok()
            .map(|envelope| envelope.error.code);
        let process_exit = classify_remote_response(operation, status, error_code);
        let presentation = if body.is_empty() {
            FailurePresentation::Message {
                code: FailureCode::Cli(CliErrorCode::RemoteResponseFailed),
                source: anyhow::anyhow!("remote service returned HTTP {status}"),
            }
        } else {
            FailurePresentation::ExactRemoteBody(body)
        };
        Self {
            process_exit,
            presentation,
        }
    }

    pub(crate) const fn process_exit(&self) -> ProcessExit {
        self.process_exit
    }

    pub(crate) fn into_stderr(self) -> Vec<u8> {
        match self.presentation {
            FailurePresentation::Message { code, source } => {
                format!("{code}: {source:#}\n").into_bytes()
            }
            FailurePresentation::ExactRemoteBody(body) => body,
        }
    }

    fn message(process_exit: ProcessExit, code: FailureCode, source: impl Into<Error>) -> Self {
        Self {
            process_exit,
            presentation: FailurePresentation::Message {
                code,
                source: source.into(),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
struct RemoteErrorEnvelope {
    error: RemoteError,
}

#[derive(Debug, Deserialize)]
struct RemoteError {
    code: RemoteErrorCode,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum RemoteErrorCode {
    CatalogCorrupt,
    CatalogDefinitionConflict,
    CatalogManifestInvalid,
    CatalogProfileInvalid,
    CatalogSchemaIncompatible,
    IdempotencyKeyReused,
    IngestionRequestFailed,
    IngestionRequestInProgress,
    MetastoreUnavailable,
    ObjectStoreUnavailable,
    RequestTimeout,
    ServerDraining,
    #[serde(other)]
    Other,
}

fn classify_remote_response(
    operation: RemoteOperation,
    status: StatusCode,
    error_code: Option<RemoteErrorCode>,
) -> ProcessExit {
    if status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        return ProcessExit::RemoteServiceUnavailable;
    }
    match error_code {
        Some(
            RemoteErrorCode::MetastoreUnavailable
            | RemoteErrorCode::ObjectStoreUnavailable
            | RemoteErrorCode::ServerDraining
            | RemoteErrorCode::IngestionRequestInProgress
            | RemoteErrorCode::RequestTimeout,
        ) => return ProcessExit::RemoteServiceUnavailable,
        Some(
            RemoteErrorCode::CatalogDefinitionConflict
            | RemoteErrorCode::CatalogProfileInvalid
            | RemoteErrorCode::CatalogSchemaIncompatible,
        ) if matches!(operation, RemoteOperation::CatalogApplication) => {
            return ProcessExit::CatalogConflict;
        }
        Some(RemoteErrorCode::IngestionRequestFailed | RemoteErrorCode::IdempotencyKeyReused)
            if matches!(operation, RemoteOperation::Ingestion) =>
        {
            return ProcessExit::TerminalIngestionFailure;
        }
        Some(
            RemoteErrorCode::CatalogCorrupt
            | RemoteErrorCode::CatalogDefinitionConflict
            | RemoteErrorCode::CatalogManifestInvalid
            | RemoteErrorCode::CatalogProfileInvalid
            | RemoteErrorCode::CatalogSchemaIncompatible
            | RemoteErrorCode::IdempotencyKeyReused
            | RemoteErrorCode::IngestionRequestFailed
            | RemoteErrorCode::Other,
        )
        | None => {}
    }
    match (operation, status) {
        (
            RemoteOperation::CatalogApplication,
            StatusCode::CONFLICT | StatusCode::UNPROCESSABLE_ENTITY,
        ) => ProcessExit::CatalogConflict,
        (RemoteOperation::Ingestion, StatusCode::UNPROCESSABLE_ENTITY) => {
            ProcessExit::TerminalIngestionFailure
        }
        (RemoteOperation::Ingestion, StatusCode::CONFLICT) => ProcessExit::RemoteServiceUnavailable,
        (_, status) if status.is_client_error() => ProcessExit::CommandOrDocumentValidationFailure,
        _ => ProcessExit::UncategorizedInternalFailure,
    }
}
