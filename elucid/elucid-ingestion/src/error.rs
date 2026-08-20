use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SpoolErrorCode {
    CapacityExhausted,
    BatchLimitExceeded,
    Corrupt,
    Unavailable,
}

impl SpoolErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CapacityExhausted => "CAPACITY_EXHAUSTED",
            Self::BatchLimitExceeded => "INGESTION_BATCH_LIMIT_EXCEEDED",
            Self::Corrupt => "SPOOL_CORRUPT",
            Self::Unavailable => "SPOOL_UNAVAILABLE",
        }
    }
}

impl Display for SpoolErrorCode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct SpoolError {
    code: SpoolErrorCode,
    source: SpoolErrorSource,
}

impl SpoolError {
    #[must_use]
    pub const fn code(&self) -> SpoolErrorCode {
        self.code
    }

    pub(crate) fn capacity(required_bytes: u64, available_bytes: u64) -> Self {
        Self {
            code: SpoolErrorCode::CapacityExhausted,
            source: SpoolErrorSource::Capacity {
                required_bytes,
                available_bytes,
            },
        }
    }

    pub(crate) fn batch_limit(actual_bytes: u64, maximum_bytes: u64) -> Self {
        Self {
            code: SpoolErrorCode::BatchLimitExceeded,
            source: SpoolErrorSource::BatchLimit {
                actual_bytes,
                maximum_bytes,
            },
        }
    }

    pub(crate) fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self {
            code: SpoolErrorCode::Unavailable,
            source: SpoolErrorSource::Io { operation, source },
        }
    }

    pub(crate) fn corrupt(message: &'static str) -> Self {
        Self {
            code: SpoolErrorCode::Corrupt,
            source: SpoolErrorSource::Corrupt(message),
        }
    }

    pub(crate) fn invariant(message: &'static str) -> Self {
        Self {
            code: SpoolErrorCode::Unavailable,
            source: SpoolErrorSource::Invariant(message),
        }
    }

    pub(crate) fn task(source: tokio::task::JoinError) -> Self {
        Self {
            code: SpoolErrorCode::Unavailable,
            source: SpoolErrorSource::Task(source),
        }
    }
}

impl Display for SpoolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.code, formatter)
    }
}

impl Error for SpoolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
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
