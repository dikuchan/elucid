use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMillisecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow::record_batch::RecordBatch;
use elucid_catalog::{
    Field, FieldId, FieldRole, LogicalType, Nullability, Schema, SchemaId, SourceId,
};

use crate::{NormalizedValue, SealedSegment};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SegmentMaterializationError {
    #[error(
        "sealed segment source {segment_source_id} does not match stored schema source {schema_source_id}"
    )]
    StoredSchemaSourceMismatch {
        segment_source_id: SourceId,
        schema_source_id: SourceId,
    },

    #[error(
        "sealed segment schema {segment_schema_id} does not match stored schema {stored_schema_id}"
    )]
    StoredSchemaIdentityMismatch {
        segment_schema_id: SchemaId,
        stored_schema_id: SchemaId,
    },

    #[error("stored data field {field_id} has unsupported logical type {logical_type}")]
    UnsupportedDataFieldType {
        field_id: FieldId,
        logical_type: LogicalType,
    },

    #[error(
        "normalized row has {actual_fields} fields but stored schema requires {expected_fields} data fields"
    )]
    NormalizedFieldCountMismatch {
        expected_fields: usize,
        actual_fields: usize,
    },

    #[error("normalized field identity {actual_field_id} does not match {expected_field_id}")]
    NormalizedFieldIdentityMismatch {
        expected_field_id: FieldId,
        actual_field_id: FieldId,
    },

    #[error("normalized field {field_id} is null but the stored field is non-null")]
    NonNullFieldContainsNull { field_id: FieldId },

    #[error("normalized field {field_id} does not contain its declared {logical_type} value")]
    NormalizedFieldTypeMismatch {
        field_id: FieldId,
        logical_type: LogicalType,
    },

    #[error("normalized floating-point field {field_id} is not finite")]
    NormalizedFloatNotFinite { field_id: FieldId },

    #[error("event identity cannot be materialized as fixed 16-byte Arrow binary")]
    EventIdentityInvalid {
        #[source]
        source: arrow::error::ArrowError,
    },

    #[error("remainder JSON cannot be serialized")]
    RemainderSerializationFailed {
        #[source]
        source: serde_json::Error,
    },

    #[error("materialized columns do not form the stored Arrow schema")]
    RecordBatchInvalid {
        #[source]
        source: arrow::error::ArrowError,
    },
}

/// Materializes one sealed ingestion segment into its exact stored-schema Arrow representation.
///
/// # Errors
///
/// Returns an internal invariant error when the pinned schema and normalized rows disagree, or an
/// Arrow/JSON error when bounded physical materialization fails.
pub fn materialize_segment_record_batch(
    segment: &SealedSegment,
    stored_schema: &Schema,
) -> Result<RecordBatch, SegmentMaterializationError> {
    validate_stored_schema(segment, stored_schema)?;
    let row_count = segment.rows().len();
    let data_fields = stored_schema
        .fields()
        .iter()
        .filter(|field| field.role() == FieldRole::Data)
        .collect::<Vec<_>>();
    let mut data_builders = data_fields
        .iter()
        .map(|field| DataColumnBuilder::new(field, row_count))
        .collect::<Result<Vec<_>, _>>()?;
    let mut event_times =
        TimestampMillisecondBuilder::with_capacity(row_count).with_timezone("UTC");
    let mut ingestion_times =
        TimestampMillisecondBuilder::with_capacity(row_count).with_timezone("UTC");
    let mut event_ids = FixedSizeBinaryBuilder::with_capacity(row_count, 16);
    let mut remainders = StringBuilder::with_capacity(row_count, 0);

    for segment_row in segment.rows() {
        let row = segment_row.row();
        event_times.append_value(row.event_time().unix_milliseconds());
        ingestion_times.append_value(row.ingestion_time().unix_milliseconds());
        event_ids
            .append_value(row.event_id().as_bytes())
            .map_err(|source| SegmentMaterializationError::EventIdentityInvalid { source })?;
        if row.fields().len() != data_fields.len() {
            return Err(SegmentMaterializationError::NormalizedFieldCountMismatch {
                expected_fields: data_fields.len(),
                actual_fields: row.fields().len(),
            });
        }
        for ((builder, normalized), field) in
            data_builders.iter_mut().zip(row.fields()).zip(&data_fields)
        {
            if normalized.field_id() != field.id() {
                return Err(
                    SegmentMaterializationError::NormalizedFieldIdentityMismatch {
                        expected_field_id: field.id(),
                        actual_field_id: normalized.field_id(),
                    },
                );
            }
            builder.append(field, normalized.value())?;
        }
        if let Some(remainder) = row.remainder() {
            let json = serde_json::to_string(remainder.as_map()).map_err(|source| {
                SegmentMaterializationError::RemainderSerializationFailed { source }
            })?;
            remainders.append_value(json);
        } else {
            remainders.append_null();
        }
    }

    let mut columns = Vec::with_capacity(data_builders.len() + 4);
    columns.push(Arc::new(event_times.finish()) as ArrayRef);
    columns.push(Arc::new(ingestion_times.finish()) as ArrayRef);
    columns.push(Arc::new(event_ids.finish()) as ArrayRef);
    columns.extend(data_builders.into_iter().map(DataColumnBuilder::finish));
    columns.push(Arc::new(remainders.finish()) as ArrayRef);
    RecordBatch::try_new(Arc::new(stored_schema.arrow_schema().clone()), columns)
        .map_err(|source| SegmentMaterializationError::RecordBatchInvalid { source })
}

fn validate_stored_schema(
    segment: &SealedSegment,
    stored_schema: &Schema,
) -> Result<(), SegmentMaterializationError> {
    if segment.source_id() != stored_schema.source_id() {
        return Err(SegmentMaterializationError::StoredSchemaSourceMismatch {
            segment_source_id: segment.source_id(),
            schema_source_id: stored_schema.source_id(),
        });
    }
    if segment.schema_id() != stored_schema.id() {
        return Err(SegmentMaterializationError::StoredSchemaIdentityMismatch {
            segment_schema_id: segment.schema_id(),
            stored_schema_id: stored_schema.id(),
        });
    }
    Ok(())
}

#[derive(Debug)]
enum DataColumnBuilder {
    Bool(BooleanBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Datetime(TimestampMillisecondBuilder),
}

impl DataColumnBuilder {
    fn new(field: &Field, row_count: usize) -> Result<Self, SegmentMaterializationError> {
        match field.logical_type() {
            LogicalType::Bool => Ok(Self::Bool(BooleanBuilder::with_capacity(row_count))),
            LogicalType::Int32 => Ok(Self::Int32(Int32Builder::with_capacity(row_count))),
            LogicalType::Int64 => Ok(Self::Int64(Int64Builder::with_capacity(row_count))),
            LogicalType::UInt32 => Ok(Self::UInt32(UInt32Builder::with_capacity(row_count))),
            LogicalType::UInt64 => Ok(Self::UInt64(UInt64Builder::with_capacity(row_count))),
            LogicalType::Float32 => Ok(Self::Float32(Float32Builder::with_capacity(row_count))),
            LogicalType::Float64 => Ok(Self::Float64(Float64Builder::with_capacity(row_count))),
            LogicalType::Utf8 => Ok(Self::Utf8(StringBuilder::with_capacity(row_count, 0))),
            LogicalType::Datetime => Ok(Self::Datetime(
                TimestampMillisecondBuilder::with_capacity(row_count).with_timezone("UTC"),
            )),
            logical_type => Err(SegmentMaterializationError::UnsupportedDataFieldType {
                field_id: field.id(),
                logical_type,
            }),
        }
    }

    fn append(
        &mut self,
        field: &Field,
        value: &NormalizedValue,
    ) -> Result<(), SegmentMaterializationError> {
        if matches!(value, NormalizedValue::Null) {
            if field.nullability() == Nullability::NonNull {
                return Err(SegmentMaterializationError::NonNullFieldContainsNull {
                    field_id: field.id(),
                });
            }
            self.append_null();
            return Ok(());
        }
        match (self, value) {
            (Self::Bool(builder), NormalizedValue::Bool(value)) => builder.append_value(*value),
            (Self::Int32(builder), NormalizedValue::Int32(value)) => builder.append_value(*value),
            (Self::Int64(builder), NormalizedValue::Int64(value)) => builder.append_value(*value),
            (Self::UInt32(builder), NormalizedValue::UInt32(value)) => builder.append_value(*value),
            (Self::UInt64(builder), NormalizedValue::UInt64(value)) => builder.append_value(*value),
            (Self::Float32(builder), NormalizedValue::Float32(value)) if value.is_finite() => {
                builder.append_value(*value);
            }
            (Self::Float64(builder), NormalizedValue::Float64(value)) if value.is_finite() => {
                builder.append_value(*value);
            }
            (Self::Utf8(builder), NormalizedValue::Utf8(value)) => builder.append_value(value),
            (Self::Datetime(builder), NormalizedValue::Datetime(value)) => {
                builder.append_value(*value);
            }
            (Self::Float32(_), NormalizedValue::Float32(_))
            | (Self::Float64(_), NormalizedValue::Float64(_)) => {
                return Err(SegmentMaterializationError::NormalizedFloatNotFinite {
                    field_id: field.id(),
                });
            }
            _ => {
                return Err(SegmentMaterializationError::NormalizedFieldTypeMismatch {
                    field_id: field.id(),
                    logical_type: field.logical_type(),
                });
            }
        }
        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            Self::Bool(builder) => builder.append_null(),
            Self::Int32(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::UInt32(builder) => builder.append_null(),
            Self::UInt64(builder) => builder.append_null(),
            Self::Float32(builder) => builder.append_null(),
            Self::Float64(builder) => builder.append_null(),
            Self::Utf8(builder) => builder.append_null(),
            Self::Datetime(builder) => builder.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Bool(mut builder) => Arc::new(builder.finish()),
            Self::Int32(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::UInt32(mut builder) => Arc::new(builder.finish()),
            Self::UInt64(mut builder) => Arc::new(builder.finish()),
            Self::Float32(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::Datetime(mut builder) => Arc::new(builder.finish()),
        }
    }
}
