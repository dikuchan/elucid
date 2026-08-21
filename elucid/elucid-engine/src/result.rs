use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array as _, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use elucid_catalog::{LogicalType, Nullability};
use elucid_language::Diagnostic;
use elucid_language::ir;
use elucid_metastore::QuerySnapshot;
use futures::{Stream, StreamExt as _};
use serde_json::{Number, Value};

use crate::execution::QueryExecutionGuard;
use crate::{EngineError, QueryExecutionLimits, QueryOutputRowLimit, QueryResourceLimitExceeded};

pub const MAXIMUM_ENCODED_QUERY_ROW_BYTES: u64 = 1_048_576;
const EMPTY_ENCODED_ROWS_BYTES: u64 = 2;

pub(crate) type QueryBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, EngineError>> + Send + 'static>>;
pub type QueryRow = Vec<Value>;

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct QueryColumn {
    name: String,
    logical_type: LogicalType,
    nullability: Nullability,
}

impl QueryColumn {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn logical_type(&self) -> LogicalType {
        self.logical_type
    }

    #[must_use]
    pub const fn nullability(&self) -> Nullability {
        self.nullability
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueryTruncationReason {
    OutputRows,
    OutputBytes,
}

impl QueryTruncationReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OutputRows => "OUTPUT_ROWS",
            Self::OutputBytes => "OUTPUT_BYTES",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueryCompletion {
    Complete,
    Truncated { reason: QueryTruncationReason },
}

impl QueryCompletion {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "COMPLETE",
            Self::Truncated { .. } => "TRUNCATED",
        }
    }

    #[must_use]
    pub const fn truncation_reason(self) -> Option<QueryTruncationReason> {
        match self {
            Self::Complete => None,
            Self::Truncated { reason } => Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct QueryExecutionStatistics {
    selected_segments: u64,
    selected_parquet_bytes: u64,
    output_rows: u64,
    output_bytes: u64,
    elapsed_milliseconds: u64,
}

impl QueryExecutionStatistics {
    #[must_use]
    pub const fn selected_segments(self) -> u64 {
        self.selected_segments
    }

    #[must_use]
    pub const fn selected_parquet_bytes(self) -> u64 {
        self.selected_parquet_bytes
    }

    #[must_use]
    pub const fn output_rows(self) -> u64 {
        self.output_rows
    }

    #[must_use]
    pub const fn output_bytes(self) -> u64 {
        self.output_bytes
    }

    #[must_use]
    pub const fn elapsed_milliseconds(self) -> u64 {
        self.elapsed_milliseconds
    }
}

pub struct QueryResult {
    columns: Vec<QueryColumn>,
    rows: Vec<QueryRow>,
    completion: QueryCompletion,
    diagnostics: Vec<Diagnostic>,
    statistics: QueryExecutionStatistics,
    _memory_reservation: MemoryReservation,
}

impl Debug for QueryResult {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryResult")
            .field("columns", &self.columns)
            .field("row_count", &self.rows.len())
            .field("completion", &self.completion)
            .field("diagnostics", &self.diagnostics)
            .field("statistics", &self.statistics)
            .finish_non_exhaustive()
    }
}

impl QueryResult {
    #[must_use]
    pub fn columns(&self) -> &[QueryColumn] {
        &self.columns
    }

    #[must_use]
    pub fn rows(&self) -> &[QueryRow] {
        &self.rows
    }

    #[must_use]
    pub const fn completion(&self) -> QueryCompletion {
        self.completion
    }

    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    #[must_use]
    pub const fn statistics(&self) -> QueryExecutionStatistics {
        self.statistics
    }
}

pub(crate) async fn encode_query_result(
    snapshot: &QuerySnapshot,
    limits: &QueryExecutionLimits,
    output_row_limit: QueryOutputRowLimit,
    memory_pool: &Arc<dyn MemoryPool>,
    mut batches: QueryBatchStream,
    started_at: Instant,
    guard: QueryExecutionGuard<'_>,
) -> Result<QueryResult, EngineError> {
    guard.ensure_active()?;
    let relation = snapshot.analysis().pipeline().output_relation();
    let columns = relation
        .fields()
        .iter()
        .map(|field| QueryColumn {
            name: field.name().to_owned(),
            logical_type: field.logical_type(),
            nullability: field.nullability(),
        })
        .collect();
    let mut builder = QueryResultBuilder::new(limits, output_row_limit, memory_pool)?;
    while let Some(batch) = batches.next().await {
        guard.ensure_active()?;
        let batch = batch?;
        validate_batch(&batch, relation)?;
        for row_index in 0..batch.num_rows() {
            guard.ensure_active()?;
            match builder.push(&batch, relation, row_index)? {
                PushOutcome::Accepted => {}
                PushOutcome::Truncated(reason) => {
                    return finish_result(
                        snapshot,
                        columns,
                        builder,
                        QueryCompletion::Truncated { reason },
                        started_at,
                    );
                }
            }
        }
        tokio::task::yield_now().await;
        guard.ensure_active()?;
    }
    finish_result(
        snapshot,
        columns,
        builder,
        QueryCompletion::Complete,
        started_at,
    )
}

fn finish_result(
    snapshot: &QuerySnapshot,
    columns: Vec<QueryColumn>,
    builder: QueryResultBuilder,
    completion: QueryCompletion,
    started_at: Instant,
) -> Result<QueryResult, EngineError> {
    let output_rows = u64::try_from(builder.rows.len())
        .map_err(|_| EngineError::execution_invariant("query result row count exceeds u64"))?;
    let selected_segments = u64::try_from(snapshot.segments().len())
        .map_err(|_| EngineError::execution_invariant("selected segment count exceeds u64"))?;
    let elapsed_milliseconds = u64::try_from(started_at.elapsed().as_millis()).map_err(|_| {
        EngineError::execution_invariant("query elapsed time exceeds millisecond statistics")
    })?;
    Ok(QueryResult {
        columns,
        rows: builder.rows,
        completion,
        diagnostics: snapshot.analysis().diagnostics().to_vec(),
        statistics: QueryExecutionStatistics {
            selected_segments,
            selected_parquet_bytes: snapshot.selected_parquet_bytes(),
            output_rows,
            output_bytes: builder.output_bytes,
            elapsed_milliseconds,
        },
        _memory_reservation: builder.memory_reservation,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PushOutcome {
    Accepted,
    Truncated(QueryTruncationReason),
}

#[derive(Debug)]
struct QueryResultBuilder {
    rows: Vec<QueryRow>,
    output_bytes: u64,
    maximum_rows: u64,
    maximum_bytes: u64,
    maximum_row_bytes: u64,
    memory_reservation: MemoryReservation,
}

impl QueryResultBuilder {
    fn new(
        limits: &QueryExecutionLimits,
        output_row_limit: QueryOutputRowLimit,
        memory_pool: &Arc<dyn MemoryPool>,
    ) -> Result<Self, EngineError> {
        let mut memory_reservation =
            MemoryConsumer::new("Elucid encoded query result").register(memory_pool);
        memory_reservation
            .try_grow(usize::try_from(EMPTY_ENCODED_ROWS_BYTES).map_err(|_| {
                EngineError::execution_invariant("empty encoded query result size exceeds usize")
            })?)
            .map_err(EngineError::resources_exhausted)?;
        Ok(Self {
            rows: Vec::new(),
            output_bytes: EMPTY_ENCODED_ROWS_BYTES,
            maximum_rows: output_row_limit.get(),
            maximum_bytes: limits.maximum_result_bytes(),
            maximum_row_bytes: limits.maximum_encoded_row_bytes(),
            memory_reservation,
        })
    }

    fn push(
        &mut self,
        batch: &RecordBatch,
        relation: &ir::Relation,
        row_index: usize,
    ) -> Result<PushOutcome, EngineError> {
        let output_rows = u64::try_from(self.rows.len())
            .map_err(|_| EngineError::execution_invariant("query result row count exceeds u64"))?;
        if output_rows >= self.maximum_rows {
            return Ok(PushOutcome::Truncated(QueryTruncationReason::OutputRows));
        }
        let row = encode_row(batch, relation, row_index)?;
        let encoded_row_bytes = u64::try_from(
            serde_json::to_vec(&row)
                .map_err(EngineError::execution)?
                .len(),
        )
        .map_err(|_| EngineError::execution_invariant("encoded query row size exceeds u64"))?;
        if encoded_row_bytes > self.maximum_row_bytes {
            return Err(EngineError::resource_limit(
                QueryResourceLimitExceeded::EncodedRowBytes {
                    maximum: self.maximum_row_bytes,
                },
            ));
        }
        let separator_bytes = u64::from(!self.rows.is_empty());
        let additional_bytes = encoded_row_bytes
            .checked_add(separator_bytes)
            .ok_or_else(|| {
                EngineError::execution_invariant("query result byte count overflowed")
            })?;
        let candidate_bytes = self
            .output_bytes
            .checked_add(additional_bytes)
            .ok_or_else(|| {
                EngineError::execution_invariant("query result byte count overflowed")
            })?;
        if candidate_bytes > self.maximum_bytes {
            return Ok(PushOutcome::Truncated(QueryTruncationReason::OutputBytes));
        }
        self.memory_reservation
            .try_grow(usize::try_from(additional_bytes).map_err(|_| {
                EngineError::execution_invariant("encoded query result size exceeds usize")
            })?)
            .map_err(EngineError::resources_exhausted)?;
        self.rows.push(row);
        self.output_bytes = candidate_bytes;
        Ok(PushOutcome::Accepted)
    }
}

fn validate_batch(batch: &RecordBatch, relation: &ir::Relation) -> Result<(), EngineError> {
    if batch.num_columns() != relation.fields().len() {
        return Err(EngineError::execution_invariant(
            "query result width contradicts its typed relation",
        ));
    }
    for (physical, logical) in batch.schema().fields().iter().zip(relation.fields()) {
        if physical.name() != logical.name()
            || physical.data_type() != &logical.logical_type().arrow_data_type()
        {
            return Err(EngineError::execution_invariant(
                "query result field contradicts its typed relation",
            ));
        }
    }
    Ok(())
}

fn encode_row(
    batch: &RecordBatch,
    relation: &ir::Relation,
    row_index: usize,
) -> Result<QueryRow, EngineError> {
    batch
        .columns()
        .iter()
        .zip(relation.fields())
        .map(|(array, field)| encode_value(array, field, row_index))
        .collect()
}

fn encode_value(
    array: &ArrayRef,
    field: &ir::Field,
    row_index: usize,
) -> Result<Value, EngineError> {
    if array.is_null(row_index) {
        if field.nullability() == Nullability::NonNull {
            return Err(EngineError::execution_invariant(
                "query produced null for a non-null typed field",
            ));
        }
        return Ok(Value::Null);
    }
    match field.logical_type() {
        LogicalType::Bool => Ok(Value::Bool(
            downcast::<BooleanArray>(array)?.value(row_index),
        )),
        LogicalType::Int32 => Ok(Value::Number(Number::from(
            downcast::<Int32Array>(array)?.value(row_index),
        ))),
        LogicalType::Int64 => Ok(Value::String(
            downcast::<Int64Array>(array)?.value(row_index).to_string(),
        )),
        LogicalType::UInt32 => Ok(Value::Number(Number::from(
            downcast::<UInt32Array>(array)?.value(row_index),
        ))),
        LogicalType::UInt64 => Ok(Value::String(
            downcast::<UInt64Array>(array)?.value(row_index).to_string(),
        )),
        LogicalType::Float32 => {
            encode_float(f64::from(downcast::<Float32Array>(array)?.value(row_index)))
        }
        LogicalType::Float64 => encode_float(downcast::<Float64Array>(array)?.value(row_index)),
        LogicalType::Utf8 => Ok(Value::String(
            downcast::<StringArray>(array)?.value(row_index).to_owned(),
        )),
        LogicalType::Datetime => {
            let milliseconds = downcast::<TimestampMillisecondArray>(array)?.value(row_index);
            let instant =
                DateTime::<Utc>::from_timestamp_millis(milliseconds).ok_or_else(|| {
                    EngineError::corrupt_object_invariant(
                        "published datetime is outside the supported RFC 3339 range",
                    )
                })?;
            Ok(Value::String(
                instant.to_rfc3339_opts(SecondsFormat::Millis, true),
            ))
        }
        LogicalType::Eid => Ok(Value::String(encode_eid(
            downcast::<FixedSizeBinaryArray>(array)?.value(row_index),
        )?)),
        LogicalType::Json => serde_json::from_str(downcast::<StringArray>(array)?.value(row_index))
            .map(canonicalize_json)
            .map_err(EngineError::corrupt_object),
        _ => Err(EngineError::execution_invariant(
            "query result uses an unsupported logical type",
        )),
    }
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize_json).collect()),
        Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize_json(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

fn encode_float(value: f64) -> Result<Value, EngineError> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| EngineError::evaluation_invariant("query produced a non-finite float"))
}

fn encode_eid(bytes: &[u8]) -> Result<String, EngineError> {
    if bytes.len() != 16 {
        return Err(EngineError::execution_invariant(
            "query produced an event identity with invalid width",
        ));
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(32);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

fn downcast<T: 'static>(array: &ArrayRef) -> Result<&T, EngineError> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        EngineError::execution_invariant("query result array has an unexpected physical type")
    })
}
