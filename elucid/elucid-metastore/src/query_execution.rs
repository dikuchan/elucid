use std::error::Error;
use std::fmt::{Display, Formatter};

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::error::{is_database_conflict, is_row_decode_error};
use crate::{MetastoreErrorCode, QueryRequestTimeRange};

const MAXIMUM_QUERY_TEXT_BYTES: usize = 1_048_576;
const QUERY_EXECUTION_RETENTION: i64 = 100;

pub const MAXIMUM_RETAINED_QUERY_EXECUTIONS: u64 = QUERY_EXECUTION_RETENTION as u64;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryExecutionId(Uuid);

impl QueryExecutionId {
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for QueryExecutionId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl Display for QueryExecutionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum QueryExecutionModelError {
    #[error("query text exceeds the limit of {maximum_bytes} bytes")]
    QueryTextTooLarge { maximum_bytes: usize },
    #[error("query output row limit must be positive")]
    OutputRowsMustBePositive,
    #[error("query execution list limit must be between 1 and {maximum}")]
    ListLimitOutOfRange { maximum: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct NewQueryExecution {
    query_id: QueryExecutionId,
    query: String,
    time_range: QueryRequestTimeRange,
    output_rows: u64,
}

impl NewQueryExecution {
    pub fn new(
        query_id: QueryExecutionId,
        query: String,
        time_range: QueryRequestTimeRange,
        output_rows: u64,
    ) -> Result<Self, QueryExecutionModelError> {
        if query.len() > MAXIMUM_QUERY_TEXT_BYTES {
            return Err(QueryExecutionModelError::QueryTextTooLarge {
                maximum_bytes: MAXIMUM_QUERY_TEXT_BYTES,
            });
        }
        if output_rows == 0 {
            return Err(QueryExecutionModelError::OutputRowsMustBePositive);
        }
        Ok(Self {
            query_id,
            query,
            time_range,
            output_rows,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct QueryExecutionListLimit(i64);

impl QueryExecutionListLimit {
    pub fn new(items: u64) -> Result<Self, QueryExecutionModelError> {
        if items == 0 || items > MAXIMUM_RETAINED_QUERY_EXECUTIONS {
            return Err(QueryExecutionModelError::ListLimitOutOfRange {
                maximum: MAXIMUM_RETAINED_QUERY_EXECUTIONS,
            });
        }
        i64::try_from(items)
            .map(Self)
            .map_err(|_| QueryExecutionModelError::ListLimitOutOfRange {
                maximum: MAXIMUM_RETAINED_QUERY_EXECUTIONS,
            })
    }

    const fn query_limit(self) -> i64 {
        self.0 + 1
    }

    const fn item_limit(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct QueryExecutionRecord {
    query_id: QueryExecutionId,
    query: String,
    time_range: QueryRequestTimeRange,
    output_rows: u64,
    submitted_at: DateTime<Utc>,
}

impl QueryExecutionRecord {
    #[must_use]
    pub const fn query_id(&self) -> QueryExecutionId {
        self.query_id
    }

    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }

    #[must_use]
    pub const fn time_range(&self) -> QueryRequestTimeRange {
        self.time_range
    }

    #[must_use]
    pub const fn output_rows(&self) -> u64 {
        self.output_rows
    }

    #[must_use]
    pub const fn submitted_at(&self) -> DateTime<Utc> {
        self.submitted_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BoundedQueryExecutions {
    items: Vec<QueryExecutionRecord>,
    truncated: bool,
    limit: usize,
}

impl BoundedQueryExecutions {
    #[must_use]
    pub fn items(&self) -> &[QueryExecutionRecord] {
        &self.items
    }

    #[must_use]
    pub const fn is_truncated(&self) -> bool {
        self.truncated
    }

    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueryExecutionPersistenceErrorKind {
    Conflict,
    Unavailable,
    Corrupt,
}

impl Display for QueryExecutionPersistenceErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Conflict => "query execution persistence conflict",
            Self::Unavailable => "query execution persistence unavailable",
            Self::Corrupt => "query execution persistence corrupt",
        })
    }
}

#[derive(Debug)]
pub struct QueryExecutionPersistenceError {
    kind: QueryExecutionPersistenceErrorKind,
    source: QueryExecutionPersistenceErrorSource,
}

impl QueryExecutionPersistenceError {
    #[must_use]
    pub const fn kind(&self) -> QueryExecutionPersistenceErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn code(&self) -> MetastoreErrorCode {
        match self.kind {
            QueryExecutionPersistenceErrorKind::Conflict => MetastoreErrorCode::Conflict,
            QueryExecutionPersistenceErrorKind::Unavailable => MetastoreErrorCode::Unavailable,
            QueryExecutionPersistenceErrorKind::Corrupt => MetastoreErrorCode::Corrupt,
        }
    }

    fn read(source: sqlx::Error) -> Self {
        let kind = if is_row_decode_error(&source) {
            QueryExecutionPersistenceErrorKind::Corrupt
        } else {
            QueryExecutionPersistenceErrorKind::Unavailable
        };
        Self {
            kind,
            source: QueryExecutionPersistenceErrorSource::Database(source),
        }
    }

    fn write(source: sqlx::Error) -> Self {
        let kind = if is_row_decode_error(&source) {
            QueryExecutionPersistenceErrorKind::Corrupt
        } else if is_database_conflict(&source) {
            QueryExecutionPersistenceErrorKind::Conflict
        } else {
            QueryExecutionPersistenceErrorKind::Unavailable
        };
        Self {
            kind,
            source: QueryExecutionPersistenceErrorSource::Database(source),
        }
    }

    fn corrupt(message: &'static str) -> Self {
        Self {
            kind: QueryExecutionPersistenceErrorKind::Corrupt,
            source: QueryExecutionPersistenceErrorSource::Invariant(message),
        }
    }
}

impl Display for QueryExecutionPersistenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, formatter)
    }
}

impl Error for QueryExecutionPersistenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug, thiserror::Error)]
enum QueryExecutionPersistenceErrorSource {
    #[error("PostgreSQL operation failed")]
    Database(#[source] sqlx::Error),
    #[error("{0}")]
    Invariant(&'static str),
}

#[derive(Clone, Debug)]
pub struct QueryExecutionStore {
    pool: PgPool,
}

impl QueryExecutionStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Records an admitted query execution and prunes the oldest records in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns a conflict, unavailable, or corrupt error when PostgreSQL cannot preserve the
    /// bounded execution log.
    pub async fn record(
        &self,
        execution: NewQueryExecution,
    ) -> Result<QueryExecutionRecord, QueryExecutionPersistenceError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(QueryExecutionPersistenceError::write)?;
        lock_writers(&mut transaction).await?;
        let output_rows = execution.output_rows.to_string();
        let submitted_at = sqlx::query_scalar::<_, DateTime<Utc>>(
            r#"
            INSERT INTO query_executions (
                query_id, query_text, start_inclusive, end_exclusive, output_rows
            ) VALUES ($1, $2, $3, $4, $5::NUMERIC)
            RETURNING submitted_at
            "#,
        )
        .bind(execution.query_id.as_uuid())
        .bind(&execution.query)
        .bind(execution.time_range.start_inclusive())
        .bind(execution.time_range.end_exclusive())
        .bind(output_rows)
        .fetch_one(&mut *transaction)
        .await
        .map_err(QueryExecutionPersistenceError::write)?;
        sqlx::query(
            r#"
            DELETE FROM query_executions
            WHERE recorded_sequence IN (
                SELECT recorded_sequence
                FROM query_executions
                ORDER BY recorded_sequence DESC
                OFFSET $1
            )
            "#,
        )
        .bind(QUERY_EXECUTION_RETENTION)
        .execute(&mut *transaction)
        .await
        .map_err(QueryExecutionPersistenceError::write)?;
        transaction
            .commit()
            .await
            .map_err(QueryExecutionPersistenceError::write)?;
        execution.into_record(submitted_at)
    }

    /// Lists the most recently recorded query executions in submission order.
    ///
    /// # Errors
    ///
    /// Returns unavailable or corrupt when PostgreSQL cannot return a valid execution log.
    pub async fn recent(
        &self,
        limit: QueryExecutionListLimit,
    ) -> Result<BoundedQueryExecutions, QueryExecutionPersistenceError> {
        let mut rows = sqlx::query_as::<_, QueryExecutionRow>(
            r#"
            SELECT
                query_id,
                query_text,
                start_inclusive,
                end_exclusive,
                output_rows::TEXT AS output_rows,
                submitted_at
            FROM query_executions
            ORDER BY recorded_sequence DESC
            LIMIT $1
            "#,
        )
        .bind(limit.query_limit())
        .fetch_all(&self.pool)
        .await
        .map_err(QueryExecutionPersistenceError::read)?;
        let truncated = rows.len() > limit.item_limit();
        rows.truncate(limit.item_limit());
        let items = rows
            .into_iter()
            .map(QueryExecutionRecord::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BoundedQueryExecutions {
            items,
            truncated,
            limit: limit.item_limit(),
        })
    }
}

impl NewQueryExecution {
    fn into_record(
        self,
        submitted_at: DateTime<Utc>,
    ) -> Result<QueryExecutionRecord, QueryExecutionPersistenceError> {
        if !submitted_at
            .timestamp_subsec_nanos()
            .is_multiple_of(1_000_000)
        {
            return Err(QueryExecutionPersistenceError::corrupt(
                "stored query submission time exceeds millisecond precision",
            ));
        }
        Ok(QueryExecutionRecord {
            query_id: self.query_id,
            query: self.query,
            time_range: self.time_range,
            output_rows: self.output_rows,
            submitted_at,
        })
    }
}

async fn lock_writers(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), QueryExecutionPersistenceError> {
    sqlx::query("LOCK TABLE query_executions IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut **transaction)
        .await
        .map_err(QueryExecutionPersistenceError::write)?;
    Ok(())
}

#[derive(Debug, FromRow)]
struct QueryExecutionRow {
    query_id: Uuid,
    query_text: String,
    start_inclusive: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
    output_rows: String,
    submitted_at: DateTime<Utc>,
}

impl TryFrom<QueryExecutionRow> for QueryExecutionRecord {
    type Error = QueryExecutionPersistenceError;

    fn try_from(row: QueryExecutionRow) -> Result<Self, Self::Error> {
        if row.query_text.len() > MAXIMUM_QUERY_TEXT_BYTES {
            return Err(QueryExecutionPersistenceError::corrupt(
                "stored query text exceeds the supported byte limit",
            ));
        }
        let time_range = QueryRequestTimeRange::new(row.start_inclusive, row.end_exclusive)
            .map_err(|_| {
                QueryExecutionPersistenceError::corrupt("stored query range is invalid")
            })?;
        let output_rows = row.output_rows.parse::<u64>().map_err(|_| {
            QueryExecutionPersistenceError::corrupt("stored query output row limit is invalid")
        })?;
        if output_rows == 0 {
            return Err(QueryExecutionPersistenceError::corrupt(
                "stored query output row limit is zero",
            ));
        }
        if !row
            .submitted_at
            .timestamp_subsec_nanos()
            .is_multiple_of(1_000_000)
        {
            return Err(QueryExecutionPersistenceError::corrupt(
                "stored query submission time exceeds millisecond precision",
            ));
        }
        Ok(Self {
            query_id: QueryExecutionId::from(row.query_id),
            query: row.query_text,
            time_range,
            output_rows,
            submitted_at: row.submitted_at,
        })
    }
}
