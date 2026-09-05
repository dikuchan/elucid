use std::error::Error;
use std::fmt::{Display, Formatter};

use elucid_core::{CodedError, ErrorCode};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SpoolErrorKind {
    CapacityExhausted,
    BatchLimitExceeded,
    Corrupt,
    Unavailable,
}

impl From<SpoolErrorKind> for ErrorCode {
    fn from(value: SpoolErrorKind) -> Self {
        match value {
            SpoolErrorKind::CapacityExhausted => Self::CapacityExhausted,
            SpoolErrorKind::BatchLimitExceeded => Self::IngestionBatchLimitExceeded,
            SpoolErrorKind::Corrupt => Self::SpoolCorrupt,
            SpoolErrorKind::Unavailable => Self::SpoolUnavailable,
        }
    }
}

impl SpoolErrorKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        ErrorCode::from(self).as_str()
    }
}

impl Display for SpoolErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct SpoolError {
    kind: SpoolErrorKind,
    source: SpoolErrorSource,
}

impl SpoolError {
    #[must_use]
    pub const fn kind(&self) -> SpoolErrorKind {
        self.kind
    }

    pub(crate) fn capacity(required_bytes: u64, available_bytes: u64) -> Self {
        Self {
            kind: SpoolErrorKind::CapacityExhausted,
            source: SpoolErrorSource::Capacity {
                required_bytes,
                available_bytes,
            },
        }
    }

    pub(crate) fn batch_limit(actual_bytes: u64, maximum_bytes: u64) -> Self {
        Self {
            kind: SpoolErrorKind::BatchLimitExceeded,
            source: SpoolErrorSource::BatchLimit {
                actual_bytes,
                maximum_bytes,
            },
        }
    }

    pub(crate) fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self {
            kind: SpoolErrorKind::Unavailable,
            source: SpoolErrorSource::Io { operation, source },
        }
    }

    pub(crate) fn corrupt(message: &'static str) -> Self {
        Self {
            kind: SpoolErrorKind::Corrupt,
            source: SpoolErrorSource::Corrupt(message),
        }
    }

    pub(crate) fn invariant(message: &'static str) -> Self {
        Self {
            kind: SpoolErrorKind::Unavailable,
            source: SpoolErrorSource::Invariant(message),
        }
    }

    pub(crate) fn task(source: tokio::task::JoinError) -> Self {
        Self {
            kind: SpoolErrorKind::Unavailable,
            source: SpoolErrorSource::Task(source),
        }
    }
}

impl Display for SpoolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for SpoolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl CodedError for SpoolError {
    fn error_code(&self) -> ErrorCode {
        self.kind().into()
    }
}

#[derive(Debug, thiserror::Error)]
enum SpoolErrorSource {
    #[error("spool requires {required_bytes} bytes but only {available_bytes} bytes are available")]
    Capacity {
        required_bytes: u64,
        available_bytes: u64,
    },

    #[error("batch body contains {actual_bytes} bytes but the reservation allows {maximum_bytes}")]
    BatchLimit {
        actual_bytes: u64,
        maximum_bytes: u64,
    },

    #[error("{0}")]
    Corrupt(&'static str),

    #[error("cannot {operation} the spool")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("{0}")]
    Invariant(&'static str),

    #[error("blocking spool operation failed")]
    Task(#[source] tokio::task::JoinError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum SpoolModelError {
    #[error("spool capacity must be positive")]
    CapacityMustBePositive,

    #[error("append body limit must be positive")]
    AppendBodyLimitMustBePositive,

    #[error("ingestion time is outside the representable UTC range")]
    IngestionTimeOutOfRange,
}
