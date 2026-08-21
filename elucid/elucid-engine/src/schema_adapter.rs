use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::{
    Array as _, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampMillisecondArray, UInt32Array, UInt64Array, new_null_array,
};
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use chrono::{DateTime, Timelike as _, Utc};
use datafusion::common::{ColumnStatistics, DataFusionError, Result};
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};
use elucid_catalog::{JsonPointer, LogicalType, Nullability, Schema};
use serde_json::Value;

use crate::{EngineError, HistoricalConversionMetrics};

#[derive(Clone, Debug)]
pub(crate) struct StoredSchemaPlan {
    stored_arrow_schema: SchemaRef,
    columns: Vec<ColumnPlan>,
}

impl StoredSchemaPlan {
    pub(crate) fn build(active: &Schema, stored: &Schema) -> Result<Self, EngineError> {
        if active.source_id() != stored.source_id() {
            return Err(EngineError::catalog_corrupt(
                "stored and active schemas belong to different sources",
            ));
        }
        let remainder_index = stored
            .fields()
            .iter()
            .position(|field| field.id() == elucid_catalog::FieldId::REMAINDER)
            .ok_or_else(|| EngineError::catalog_corrupt("stored schema has no remainder field"))?;
        let mut columns = Vec::with_capacity(active.fields().len());
        for active_field in active.fields() {
            let plan = if let Some((stored_index, stored_field)) = stored
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| field.id() == active_field.id())
            {
                if stored_field.logical_type() != active_field.logical_type()
                    || stored_field.nullability() != active_field.nullability()
                {
                    return Err(EngineError::catalog_corrupt(
                        "one field identity has contradictory schema definitions",
                    ));
                }
                ColumnPlan::Direct { stored_index }
            } else {
                if active_field.nullability() != Nullability::Nullable {
                    return Err(EngineError::catalog_corrupt(
                        "a non-null active field is absent from a stored schema",
                    ));
                }
                match active_field.historical_remainder_pointer() {
                    Some(pointer) => {
                        if !supports_historical_conversion(active_field.logical_type()) {
                            return Err(EngineError::catalog_corrupt(
                                "historical remainder adapter targets an unsupported type",
                            ));
                        }
                        ColumnPlan::Historical {
                            remainder_index,
                            pointer: pointer.clone(),
                            logical_type: active_field.logical_type(),
                        }
                    }
                    None => ColumnPlan::Null,
                }
            };
            columns.push(plan);
        }
        Ok(Self {
            stored_arrow_schema: Arc::new(stored.arrow_schema().clone()),
            columns,
        })
    }

    fn matches_file_schema(&self, file_schema: &ArrowSchema) -> bool {
        self.stored_arrow_schema.fields() == file_schema.fields()
    }
}

#[derive(Clone, Debug)]
enum ColumnPlan {
    Direct {
        stored_index: usize,
    },
    Historical {
        remainder_index: usize,
        pointer: JsonPointer,
        logical_type: LogicalType,
    },
    Null,
}

impl ColumnPlan {
    const fn stored_index(&self) -> Option<usize> {
        match self {
            Self::Direct { stored_index } => Some(*stored_index),
            Self::Historical {
                remainder_index, ..
            } => Some(*remainder_index),
            Self::Null => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ElucidSchemaAdapterFactory {
    active_arrow_schema: SchemaRef,
    plans: Arc<[StoredSchemaPlan]>,
    metrics: Arc<HistoricalConversionMetrics>,
}

impl ElucidSchemaAdapterFactory {
    pub(crate) fn new(
        active_arrow_schema: SchemaRef,
        plans: Arc<[StoredSchemaPlan]>,
        metrics: Arc<HistoricalConversionMetrics>,
    ) -> Self {
        Self {
            active_arrow_schema,
            plans,
            metrics,
        }
    }
}

impl SchemaAdapterFactory for ElucidSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(ElucidSchemaAdapter {
            active_arrow_schema: Arc::clone(&self.active_arrow_schema),
            projected_table_schema,
            table_schema,
            plans: Arc::clone(&self.plans),
            metrics: Arc::clone(&self.metrics),
        })
    }
}

#[derive(Clone, Debug)]
struct ElucidSchemaAdapter {
    active_arrow_schema: SchemaRef,
    projected_table_schema: SchemaRef,
    table_schema: SchemaRef,
    plans: Arc<[StoredSchemaPlan]>,
    metrics: Arc<HistoricalConversionMetrics>,
}

impl ElucidSchemaAdapter {
    fn plan<'a>(&'a self, file_schema: &ArrowSchema) -> Result<&'a StoredSchemaPlan> {
        if self.table_schema.fields() != self.active_arrow_schema.fields() {
            return Err(DataFusionError::Plan(
                "snapshot table schema changed after validation".to_owned(),
            ));
        }
        self.plans
            .iter()
            .find(|plan| plan.matches_file_schema(file_schema))
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "Parquet schema is absent from the validated query snapshot".to_owned(),
                )
            })
    }

    fn active_index(&self, field: &arrow::datatypes::Field) -> Result<usize> {
        self.active_arrow_schema
            .fields()
            .iter()
            .position(|active| active.as_ref() == field)
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "projected field is absent from the active snapshot schema".to_owned(),
                )
            })
    }
}

impl SchemaAdapter for ElucidSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &ArrowSchema) -> Option<usize> {
        let field = self.table_schema.fields().get(index)?;
        let active_index = self
            .active_arrow_schema
            .fields()
            .iter()
            .position(|active| active == field)?;
        self.plan(file_schema)
            .ok()?
            .columns
            .get(active_index)?
            .stored_index()
    }

    fn map_schema(&self, file_schema: &ArrowSchema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let plan = self.plan(file_schema)?;
        let mut projection = Vec::new();
        let mut mappings = Vec::with_capacity(self.projected_table_schema.fields().len());
        for field in self.projected_table_schema.fields() {
            let active_index = self.active_index(field)?;
            let mapping = match &plan.columns[active_index] {
                ColumnPlan::Direct { stored_index } => BatchColumn::Direct {
                    input_index: projection_index(&mut projection, *stored_index),
                },
                ColumnPlan::Historical {
                    remainder_index,
                    pointer,
                    logical_type,
                } => BatchColumn::Historical {
                    input_index: projection_index(&mut projection, *remainder_index),
                    pointer: pointer.clone(),
                    logical_type: *logical_type,
                },
                ColumnPlan::Null => BatchColumn::Null,
            };
            mappings.push(mapping);
        }
        Ok((
            Arc::new(ElucidSchemaMapper {
                projected_table_schema: Arc::clone(&self.projected_table_schema),
                mappings,
                metrics: Arc::clone(&self.metrics),
            }),
            projection,
        ))
    }
}

fn projection_index(projection: &mut Vec<usize>, stored_index: usize) -> usize {
    projection
        .iter()
        .position(|candidate| *candidate == stored_index)
        .unwrap_or_else(|| {
            let input_index = projection.len();
            projection.push(stored_index);
            input_index
        })
}

#[derive(Clone, Debug)]
enum BatchColumn {
    Direct {
        input_index: usize,
    },
    Historical {
        input_index: usize,
        pointer: JsonPointer,
        logical_type: LogicalType,
    },
    Null,
}

struct ElucidSchemaMapper {
    projected_table_schema: SchemaRef,
    mappings: Vec<BatchColumn>,
    metrics: Arc<HistoricalConversionMetrics>,
}

impl Debug for ElucidSchemaMapper {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ElucidSchemaMapper")
            .field("projected_table_schema", &self.projected_table_schema)
            .field("mappings", &self.mappings)
            .finish_non_exhaustive()
    }
}

impl SchemaMapper for ElucidSchemaMapper {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let row_count = batch.num_rows();
        let mut columns = Vec::with_capacity(self.mappings.len());
        for (field, mapping) in self
            .projected_table_schema
            .fields()
            .iter()
            .zip(&self.mappings)
        {
            let column = match mapping {
                BatchColumn::Direct { input_index } => Arc::clone(batch.column(*input_index)),
                BatchColumn::Historical {
                    input_index,
                    pointer,
                    logical_type,
                } => {
                    let remainder = batch
                        .column(*input_index)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "validated remainder column is not UTF-8".to_owned(),
                            )
                        })?;
                    historical_array(remainder, pointer, *logical_type, &self.metrics)
                }
                BatchColumn::Null => new_null_array(field.data_type(), row_count),
            };
            columns.push(column);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        RecordBatch::try_new_with_options(
            Arc::clone(&self.projected_table_schema),
            columns,
            &options,
        )
        .map_err(DataFusionError::from)
    }

    fn map_column_statistics(
        &self,
        file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        Ok(self
            .mappings
            .iter()
            .map(|mapping| match mapping {
                BatchColumn::Direct { input_index } => file_col_statistics
                    .get(*input_index)
                    .cloned()
                    .unwrap_or_else(ColumnStatistics::new_unknown),
                BatchColumn::Historical { .. } | BatchColumn::Null => {
                    ColumnStatistics::new_unknown()
                }
            })
            .collect())
    }
}

fn historical_array(
    remainder: &StringArray,
    pointer: &JsonPointer,
    logical_type: LogicalType,
    metrics: &HistoricalConversionMetrics,
) -> ArrayRef {
    match logical_type {
        LogicalType::Bool => Arc::new(BooleanArray::from(convert_rows(
            remainder,
            pointer,
            logical_type,
            metrics,
            Value::as_bool,
        ))),
        LogicalType::Int32 => Arc::new(Int32Array::from(convert_rows(
            remainder,
            pointer,
            logical_type,
            metrics,
            |value| integer_i64(value).and_then(|value| i32::try_from(value).ok()),
        ))),
        LogicalType::Int64 => Arc::new(Int64Array::from(convert_rows(
            remainder,
            pointer,
            logical_type,
            metrics,
            integer_i64,
        ))),
        LogicalType::UInt32 => Arc::new(UInt32Array::from(convert_rows(
            remainder,
            pointer,
            logical_type,
            metrics,
            |value| integer_u64(value).and_then(|value| u32::try_from(value).ok()),
        ))),
        LogicalType::UInt64 => Arc::new(UInt64Array::from(convert_rows(
            remainder,
            pointer,
            logical_type,
            metrics,
            integer_u64,
        ))),
        LogicalType::Float32 => Arc::new(Float32Array::from(convert_rows(
            remainder,
            pointer,
            logical_type,
            metrics,
            |value| {
                value
                    .as_number()?
                    .to_string()
                    .parse::<f32>()
                    .ok()
                    .filter(|value| value.is_finite())
            },
        ))),
        LogicalType::Float64 => Arc::new(Float64Array::from(convert_rows(
            remainder,
            pointer,
            logical_type,
            metrics,
            |value| {
                value
                    .as_number()?
                    .to_string()
                    .parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite())
            },
        ))),
        LogicalType::Utf8 => Arc::new(StringArray::from(convert_rows(
            remainder,
            pointer,
            logical_type,
            metrics,
            |value| value.as_str().map(ToOwned::to_owned),
        ))),
        LogicalType::Datetime => Arc::new(
            TimestampMillisecondArray::from(convert_rows(
                remainder,
                pointer,
                logical_type,
                metrics,
                parse_datetime,
            ))
            .with_timezone("UTC"),
        ),
        LogicalType::Eid | LogicalType::Json => {
            new_null_array(&logical_type.arrow_data_type(), remainder.len())
        }
        _ => new_null_array(&logical_type.arrow_data_type(), remainder.len()),
    }
}

fn convert_rows<T>(
    remainder: &StringArray,
    pointer: &JsonPointer,
    logical_type: LogicalType,
    metrics: &HistoricalConversionMetrics,
    convert: impl Fn(&Value) -> Option<T>,
) -> Vec<Option<T>> {
    (0..remainder.len())
        .map(|index| {
            if remainder.is_null(index) {
                return None;
            }
            let document = match serde_json::from_str::<Value>(remainder.value(index)) {
                Ok(document) => document,
                Err(_) => {
                    metrics.increment(logical_type);
                    return None;
                }
            };
            let value = resolve_pointer(&document, pointer)?;
            if value.is_null() {
                return None;
            }
            let converted = convert(value);
            if converted.is_none() {
                metrics.increment(logical_type);
            }
            converted
        })
        .collect()
}

fn resolve_pointer<'a>(document: &'a Value, pointer: &JsonPointer) -> Option<&'a Value> {
    let mut current = document;
    for token in pointer.tokens() {
        current = match current {
            Value::Object(object) => object.get(token.as_str())?,
            Value::Array(array) => array.get(parse_array_index(token.as_str())?)?,
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => return None,
        };
    }
    Some(current)
}

fn parse_array_index(token: &str) -> Option<usize> {
    if token.starts_with('+') || (token.len() > 1 && token.starts_with('0')) {
        return None;
    }
    token.parse().ok()
}

fn integer_i64(value: &Value) -> Option<i64> {
    let number = value.as_number()?;
    number
        .as_i64()
        .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn integer_u64(value: &Value) -> Option<u64> {
    let number = value.as_number()?;
    number
        .as_u64()
        .or_else(|| number.as_i64().and_then(|value| u64::try_from(value).ok()))
}

fn parse_datetime(value: &Value) -> Option<i64> {
    let parsed = DateTime::parse_from_rfc3339(value.as_str()?).ok()?;
    if !parsed.nanosecond().is_multiple_of(1_000_000) {
        return None;
    }
    let milliseconds = parsed.timestamp_millis();
    DateTime::<Utc>::from_timestamp_millis(milliseconds).map(|_| milliseconds)
}

const fn supports_historical_conversion(logical_type: LogicalType) -> bool {
    match logical_type {
        LogicalType::Bool
        | LogicalType::Int32
        | LogicalType::Int64
        | LogicalType::UInt32
        | LogicalType::UInt64
        | LogicalType::Float32
        | LogicalType::Float64
        | LogicalType::Utf8
        | LogicalType::Datetime => true,
        LogicalType::Eid | LogicalType::Json => false,
        _ => false,
    }
}
