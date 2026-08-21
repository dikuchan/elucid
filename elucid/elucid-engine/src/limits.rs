use std::fmt::{Display, Formatter};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::MAXIMUM_ENCODED_QUERY_ROW_BYTES;

const EMPTY_ENCODED_ROWS_BYTES: u64 = 2;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum QueryExecutionLimit {
    ScanBytes,
    MemoryBytes,
    ScratchCapacityBytes,
    ResultRows,
    ResultBytes,
}

impl QueryExecutionLimit {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScanBytes => "maximum_scan_bytes",
            Self::MemoryBytes => "memory_bytes",
            Self::ScratchCapacityBytes => "scratch_capacity_bytes",
            Self::ResultRows => "maximum_result_rows",
            Self::ResultBytes => "maximum_result_bytes",
        }
    }
}

impl Display for QueryExecutionLimit {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryExecutionLimitConfiguration {
    pub timeout: Duration,
    pub maximum_scan_bytes: u64,
    pub memory_bytes: u64,
    pub scratch_path: PathBuf,
    pub scratch_capacity_bytes: u64,
    pub maximum_result_rows: u64,
    pub maximum_result_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum QueryExecutionLimitsError {
    #[error("query timeout must be positive")]
    TimeoutMustBePositive,

    #[error("query timeout is not representable by the monotonic clock")]
    TimeoutUnsupported,

    #[error("query limit {limit} must be positive")]
    LimitMustBePositive { limit: QueryExecutionLimit },

    #[error("query memory limit is not representable on this platform")]
    MemorySizeUnsupported,

    #[error("query scratch path must not be empty")]
    ScratchPathMustNotBeEmpty,

    #[error("query result byte limit must encode at least an empty row array")]
    ResultBytesTooSmall,

    #[error("query result byte limit exceeds the query memory limit")]
    ResultBytesExceedMemory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum QueryOutputRowLimitError {
    #[error("query output row limit must be positive")]
    MustBePositive,

    #[error("query output row limit exceeds the configured maximum of {maximum}")]
    ExceedsConfiguredMaximum { maximum: u64 },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryOutputRowLimit(NonZeroU64);

impl QueryOutputRowLimit {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryExecutionLimits {
    timeout: Duration,
    maximum_scan_bytes: u64,
    memory_bytes: usize,
    scratch_path: PathBuf,
    scratch_capacity_bytes: u64,
    maximum_result_rows: NonZeroU64,
    maximum_result_bytes: u64,
}

impl QueryExecutionLimits {
    pub fn new(
        configuration: QueryExecutionLimitConfiguration,
    ) -> Result<Self, QueryExecutionLimitsError> {
        if configuration.timeout.is_zero() {
            return Err(QueryExecutionLimitsError::TimeoutMustBePositive);
        }
        if Instant::now().checked_add(configuration.timeout).is_none() {
            return Err(QueryExecutionLimitsError::TimeoutUnsupported);
        }
        for (limit, value) in [
            (
                QueryExecutionLimit::ScanBytes,
                configuration.maximum_scan_bytes,
            ),
            (QueryExecutionLimit::MemoryBytes, configuration.memory_bytes),
            (
                QueryExecutionLimit::ScratchCapacityBytes,
                configuration.scratch_capacity_bytes,
            ),
            (
                QueryExecutionLimit::ResultRows,
                configuration.maximum_result_rows,
            ),
            (
                QueryExecutionLimit::ResultBytes,
                configuration.maximum_result_bytes,
            ),
        ] {
            if value == 0 {
                return Err(QueryExecutionLimitsError::LimitMustBePositive { limit });
            }
        }
        if configuration.scratch_path.as_os_str().is_empty() {
            return Err(QueryExecutionLimitsError::ScratchPathMustNotBeEmpty);
        }
        if configuration.maximum_result_bytes < EMPTY_ENCODED_ROWS_BYTES {
            return Err(QueryExecutionLimitsError::ResultBytesTooSmall);
        }
        if configuration.maximum_result_bytes > configuration.memory_bytes {
            return Err(QueryExecutionLimitsError::ResultBytesExceedMemory);
        }
        let memory_bytes = usize::try_from(configuration.memory_bytes)
            .map_err(|_| QueryExecutionLimitsError::MemorySizeUnsupported)?;
        let Some(maximum_result_rows) = NonZeroU64::new(configuration.maximum_result_rows) else {
            return Err(QueryExecutionLimitsError::LimitMustBePositive {
                limit: QueryExecutionLimit::ResultRows,
            });
        };
        Ok(Self {
            timeout: configuration.timeout,
            maximum_scan_bytes: configuration.maximum_scan_bytes,
            memory_bytes,
            scratch_path: configuration.scratch_path,
            scratch_capacity_bytes: configuration.scratch_capacity_bytes,
            maximum_result_rows,
            maximum_result_bytes: configuration.maximum_result_bytes,
        })
    }

    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    #[must_use]
    pub const fn maximum_scan_bytes(&self) -> u64 {
        self.maximum_scan_bytes
    }

    #[must_use]
    pub const fn memory_bytes(&self) -> usize {
        self.memory_bytes
    }

    #[must_use]
    pub fn scratch_path(&self) -> &Path {
        &self.scratch_path
    }

    #[must_use]
    pub const fn scratch_capacity_bytes(&self) -> u64 {
        self.scratch_capacity_bytes
    }

    #[must_use]
    pub const fn maximum_result_rows(&self) -> u64 {
        self.maximum_result_rows.get()
    }

    #[must_use]
    pub const fn maximum_output_row_limit(&self) -> QueryOutputRowLimit {
        QueryOutputRowLimit(self.maximum_result_rows)
    }

    pub fn output_row_limit(
        &self,
        requested: u64,
    ) -> Result<QueryOutputRowLimit, QueryOutputRowLimitError> {
        let requested =
            NonZeroU64::new(requested).ok_or(QueryOutputRowLimitError::MustBePositive)?;
        if requested > self.maximum_result_rows {
            return Err(QueryOutputRowLimitError::ExceedsConfiguredMaximum {
                maximum: self.maximum_result_rows.get(),
            });
        }
        Ok(QueryOutputRowLimit(requested))
    }

    #[must_use]
    pub const fn maximum_result_bytes(&self) -> u64 {
        self.maximum_result_bytes
    }

    #[must_use]
    pub const fn maximum_encoded_row_bytes(&self) -> u64 {
        MAXIMUM_ENCODED_QUERY_ROW_BYTES
    }
}
