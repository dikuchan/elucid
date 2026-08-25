use std::time::{Duration, Instant};

use elucid_storage::PARQUET_MAX_ROW_GROUP_ROWS;

const MAXIMUM_BUILD_INPUT_SEGMENTS: u64 = 1_000;

#[derive(Clone, Copy, Debug)]
pub struct CompactionBuildLimitConfiguration {
    pub maximum_input_segments: u64,
    pub maximum_input_rows: u64,
    pub maximum_input_parquet_bytes: u64,
    pub maximum_input_uncompressed_bytes: u64,
    pub reader_batch_rows: u64,
    pub target_output_rows: u64,
    pub target_output_uncompressed_bytes: u64,
    pub maximum_output_parquet_bytes: u64,
    pub maximum_staging_bytes: u64,
    pub maximum_duration: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionBuildLimits {
    maximum_input_segments: usize,
    maximum_input_rows: u64,
    maximum_input_parquet_bytes: u64,
    maximum_input_uncompressed_bytes: u64,
    reader_batch_rows: usize,
    target_output_rows: usize,
    target_output_uncompressed_bytes: u64,
    maximum_output_parquet_bytes: u64,
    maximum_staging_bytes: u64,
    maximum_duration: Duration,
}

impl CompactionBuildLimits {
    pub fn new(
        configuration: CompactionBuildLimitConfiguration,
    ) -> Result<Self, CompactionBuildModelError> {
        if configuration.maximum_input_segments < 2
            || configuration.maximum_input_segments > MAXIMUM_BUILD_INPUT_SEGMENTS
        {
            return Err(CompactionBuildModelError::InputSegmentLimitOutOfRange {
                maximum: MAXIMUM_BUILD_INPUT_SEGMENTS,
            });
        }
        if configuration.maximum_input_rows == 0
            || configuration.maximum_input_parquet_bytes == 0
            || configuration.maximum_input_uncompressed_bytes == 0
            || configuration.target_output_rows == 0
            || configuration.target_output_uncompressed_bytes == 0
            || configuration.maximum_output_parquet_bytes == 0
            || configuration.maximum_staging_bytes == 0
        {
            return Err(CompactionBuildModelError::ByteAndRowLimitsMustBePositive);
        }
        let maximum_reader_batch_rows =
            u64::try_from(PARQUET_MAX_ROW_GROUP_ROWS).map_err(|_| {
                CompactionBuildModelError::ReaderBatchRowsOutOfRange { maximum: u64::MAX }
            })?;
        if configuration.reader_batch_rows == 0
            || configuration.reader_batch_rows > maximum_reader_batch_rows
        {
            return Err(CompactionBuildModelError::ReaderBatchRowsOutOfRange {
                maximum: maximum_reader_batch_rows,
            });
        }
        if configuration.target_output_rows > configuration.maximum_input_rows
            || configuration.target_output_uncompressed_bytes
                > configuration.maximum_input_uncompressed_bytes
        {
            return Err(CompactionBuildModelError::OutputTargetExceedsInputLimit);
        }
        if configuration.maximum_output_parquet_bytes > configuration.maximum_staging_bytes {
            return Err(CompactionBuildModelError::OutputObjectExceedsStagingLimit);
        }
        if configuration.maximum_duration.is_zero()
            || Instant::now()
                .checked_add(configuration.maximum_duration)
                .is_none()
        {
            return Err(CompactionBuildModelError::MaximumDurationInvalid);
        }
        let maximum_input_segments = usize::try_from(configuration.maximum_input_segments)
            .map_err(|_| CompactionBuildModelError::InputSegmentLimitOutOfRange {
                maximum: MAXIMUM_BUILD_INPUT_SEGMENTS,
            })?;
        let reader_batch_rows = usize::try_from(configuration.reader_batch_rows).map_err(|_| {
            CompactionBuildModelError::ReaderBatchRowsOutOfRange {
                maximum: maximum_reader_batch_rows,
            }
        })?;
        let target_output_rows = usize::try_from(configuration.target_output_rows)
            .map_err(|_| CompactionBuildModelError::OutputRowTargetOutOfRange)?;
        Ok(Self {
            maximum_input_segments,
            maximum_input_rows: configuration.maximum_input_rows,
            maximum_input_parquet_bytes: configuration.maximum_input_parquet_bytes,
            maximum_input_uncompressed_bytes: configuration.maximum_input_uncompressed_bytes,
            reader_batch_rows,
            target_output_rows,
            target_output_uncompressed_bytes: configuration.target_output_uncompressed_bytes,
            maximum_output_parquet_bytes: configuration.maximum_output_parquet_bytes,
            maximum_staging_bytes: configuration.maximum_staging_bytes,
            maximum_duration: configuration.maximum_duration,
        })
    }

    pub(crate) const fn maximum_input_segments(self) -> usize {
        self.maximum_input_segments
    }

    pub(crate) const fn maximum_input_rows(self) -> u64 {
        self.maximum_input_rows
    }

    pub(crate) const fn maximum_input_parquet_bytes(self) -> u64 {
        self.maximum_input_parquet_bytes
    }

    pub(crate) const fn maximum_input_uncompressed_bytes(self) -> u64 {
        self.maximum_input_uncompressed_bytes
    }

    pub(crate) const fn reader_batch_rows(self) -> usize {
        self.reader_batch_rows
    }

    pub(crate) const fn target_output_rows(self) -> usize {
        self.target_output_rows
    }

    pub(crate) const fn target_output_uncompressed_bytes(self) -> u64 {
        self.target_output_uncompressed_bytes
    }

    pub(crate) const fn maximum_output_parquet_bytes(self) -> u64 {
        self.maximum_output_parquet_bytes
    }

    pub(crate) const fn maximum_staging_bytes(self) -> u64 {
        self.maximum_staging_bytes
    }

    pub(crate) const fn maximum_duration(self) -> Duration {
        self.maximum_duration
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum CompactionBuildModelError {
    #[error("compaction build input limit must be between 2 and {maximum} segments")]
    InputSegmentLimitOutOfRange { maximum: u64 },

    #[error("compaction build row and byte limits must be positive")]
    ByteAndRowLimitsMustBePositive,

    #[error("compaction reader batch size must be between 1 and {maximum} rows")]
    ReaderBatchRowsOutOfRange { maximum: u64 },

    #[error("compaction output target must not exceed its corresponding input limit")]
    OutputTargetExceedsInputLimit,

    #[error("one compaction output object must fit in the total staging limit")]
    OutputObjectExceedsStagingLimit,

    #[error("compaction output row target does not fit this platform")]
    OutputRowTargetOutOfRange,

    #[error("compaction maximum duration must be positive and representable")]
    MaximumDurationInvalid,
}
