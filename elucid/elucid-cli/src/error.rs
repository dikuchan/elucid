use std::fmt::{self, Display, Formatter};
use std::process::ExitCode;

use anyhow::Error;
use elucid_core::{CodedError, ErrorCode};
use elucid_service::{ConfigurationError, ServiceError};
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
    LocalClientTimeout = 7,
}

impl From<ProcessExit> for ExitCode {
    fn from(value: ProcessExit) -> Self {
        Self::from(value as u8)
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
    Message(LocalFailure),
    ExactRemoteBody(Vec<u8>),
}

#[derive(Clone, Copy, Debug)]
enum LocalPresentation {
    Summary,
    WithContext,
}

#[derive(Debug)]
struct LocalFailure {
    code: ErrorCode,
    source: Error,
    presentation: LocalPresentation,
}

impl Display for LocalFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.presentation {
            LocalPresentation::Summary => write!(formatter, "{}", self.source),
            LocalPresentation::WithContext => write!(formatter, "{:#}", self.source),
        }
    }
}

impl std::error::Error for LocalFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl CodedError for LocalFailure {
    fn error_code(&self) -> ErrorCode {
        self.code
    }
}

impl Failure {
    pub(crate) fn command(code: ErrorCode, source: impl Into<Error>) -> Self {
        Self::message(
            ProcessExit::CommandOrDocumentValidationFailure,
            code,
            source,
            LocalPresentation::WithContext,
        )
    }

    pub(crate) fn configuration(error: ConfigurationError) -> Self {
        Self::coded(
            ProcessExit::ConfigurationFailure,
            error,
            LocalPresentation::WithContext,
        )
    }

    pub(crate) fn server(error: ServiceError) -> Self {
        Self::coded(
            ProcessExit::UncategorizedInternalFailure,
            error,
            LocalPresentation::Summary,
        )
    }

    pub(crate) fn internal(code: ErrorCode, source: impl Into<Error>) -> Self {
        Self::message(
            ProcessExit::UncategorizedInternalFailure,
            code,
            source,
            LocalPresentation::WithContext,
        )
    }

    pub(crate) fn remote_unavailable(code: ErrorCode, source: impl Into<Error>) -> Self {
        Self::message(
            ProcessExit::RemoteServiceUnavailable,
            code,
            source,
            LocalPresentation::WithContext,
        )
    }

    pub(crate) fn client_timeout(source: impl Into<Error>) -> Self {
        Self::message(
            ProcessExit::LocalClientTimeout,
            ErrorCode::ClientTimeout,
            source,
            LocalPresentation::WithContext,
        )
    }

    pub(crate) fn from_request_error(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::client_timeout(error)
        } else if error.is_body() {
            Self::command(ErrorCode::InputReadFailed, error)
        } else {
            Self::remote_unavailable(ErrorCode::RemoteServiceUnavailable, error)
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
        let process_exit = classify_remote_response(operation, status, error_code.as_ref());
        if body.is_empty() {
            Self::message(
                process_exit,
                ErrorCode::RemoteResponseFailed,
                anyhow::anyhow!("remote service returned HTTP {status}"),
                LocalPresentation::WithContext,
            )
        } else {
            Self {
                process_exit,
                presentation: FailurePresentation::ExactRemoteBody(body),
            }
        }
    }

    pub(crate) const fn process_exit(&self) -> ProcessExit {
        self.process_exit
    }

    pub(crate) fn into_stderr(self) -> Vec<u8> {
        match self.presentation {
            FailurePresentation::Message(error) => {
                format!("{}: {error}\n", error.error_code()).into_bytes()
            }
            FailurePresentation::ExactRemoteBody(body) => body,
        }
    }

    fn coded<E: CodedError>(
        process_exit: ProcessExit,
        error: E,
        presentation: LocalPresentation,
    ) -> Self {
        Self::message(process_exit, error.error_code(), error, presentation)
    }

    fn message(
        process_exit: ProcessExit,
        code: ErrorCode,
        source: impl Into<Error>,
        presentation: LocalPresentation,
    ) -> Self {
        Self {
            process_exit,
            presentation: FailurePresentation::Message(LocalFailure {
                code,
                source: source.into(),
                presentation,
            }),
        }
    }
}

impl Display for Failure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match &self.presentation {
            FailurePresentation::Message(error) => {
                write!(formatter, "{}: {error}", error.error_code())
            }
            FailurePresentation::ExactRemoteBody(body) => {
                formatter.write_str(&String::from_utf8_lossy(body))
            }
        }
    }
}

impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.presentation {
            FailurePresentation::Message(error) => error.source(),
            FailurePresentation::ExactRemoteBody(_) => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RemoteErrorEnvelope {
    error: RemoteError,
}

#[derive(Debug, Deserialize)]
struct RemoteError {
    code: ReceivedErrorCode,
}

#[derive(Debug, Deserialize)]
#[serde(from = "String")]
enum ReceivedErrorCode {
    Known(ErrorCode),
    Unknown(String),
}

impl From<String> for ReceivedErrorCode {
    fn from(value: String) -> Self {
        match value.parse() {
            Ok(code) => Self::Known(code),
            Err(_) => Self::Unknown(value),
        }
    }
}

impl Display for ReceivedErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Known(code) => Display::fmt(code, formatter),
            Self::Unknown(value) => formatter.write_str(value),
        }
    }
}

fn classify_remote_response(
    operation: RemoteOperation,
    status: StatusCode,
    error_code: Option<&ReceivedErrorCode>,
) -> ProcessExit {
    if status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        return ProcessExit::RemoteServiceUnavailable;
    }
    if let Some(ReceivedErrorCode::Known(code)) = error_code {
        if matches!(
            code,
            ErrorCode::MetastoreUnavailable
                | ErrorCode::ObjectStoreUnavailable
                | ErrorCode::CapacityExhausted
                | ErrorCode::ServerDraining
                | ErrorCode::ServerNotReady
        ) {
            return ProcessExit::RemoteServiceUnavailable;
        }
        if matches!(operation, RemoteOperation::CatalogApplication)
            && matches!(
                code,
                ErrorCode::CatalogDefinitionConflict
                    | ErrorCode::CatalogProfileInvalid
                    | ErrorCode::CatalogSchemaIncompatible
            )
        {
            return ProcessExit::CatalogConflict;
        }
    }
    match (operation, status) {
        (
            RemoteOperation::CatalogApplication,
            StatusCode::CONFLICT | StatusCode::UNPROCESSABLE_ENTITY,
        ) => ProcessExit::CatalogConflict,
        (_, status) if status.is_client_error() => ProcessExit::CommandOrDocumentValidationFailure,
        _ => ProcessExit::UncategorizedInternalFailure,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::io;

    use super::{Failure, ProcessExit};
    use elucid_service::ServiceError;

    #[test]
    fn server_failure_retains_its_cause_without_exposing_it_on_stderr() {
        let failure = Failure::server(ServiceError::HttpRuntime {
            source: io::Error::other("private runtime detail"),
        });
        let source = failure
            .source()
            .expect("the original service error is retained");
        assert!(source.downcast_ref::<ServiceError>().is_some());
        assert_eq!(
            source.source().expect("runtime cause").to_string(),
            "private runtime detail"
        );
        assert_eq!(
            failure.process_exit(),
            ProcessExit::UncategorizedInternalFailure
        );
        assert_eq!(
            failure.into_stderr(),
            b"SERVER_RUNTIME_FAILED: HTTP runtime failed\n"
        );
    }
}
