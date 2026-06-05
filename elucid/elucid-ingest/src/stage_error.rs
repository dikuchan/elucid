/// Errors that can occur at a stage of the pipeline.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StageError {
    /// Input line exceeded the configured size limit.
    #[error("line too large: {size} bytes (max {max})")]
    LineTooLarge {
        /// Actual size of the line in bytes.
        size: usize,
        /// Configured maximum.
        max: usize,
    },
    /// JSON parse failure.
    #[error("JSON parse error: {0}")]
    Parse(String),
    /// Field coercion or schema mismatch during normalization.
    #[error("normalization error: {0}")]
    Normalization(String),
    /// WAL I/O failure.
    #[error("WAL error: {0}")]
    Wal(#[from] std::io::Error),
    /// Parquet or Arrow write failure.
    #[error("write error: {0}")]
    Write(String),
}
