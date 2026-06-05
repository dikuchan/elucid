use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::event::{Event, EventContext, EventValue};
use crate::schema::REST_COLUMN_NAME;
use crate::stage_error::StageError;

pub struct Batcher<C: EventContext> {
    buffer: Vec<Event<C>>,
    schema: Schema,
    capacity: usize,
}

impl<C: EventContext> Batcher<C> {
    pub fn new(schema: Schema, capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            schema,
            capacity,
        }
    }

    pub fn push(&mut self, event: Event<C>) -> Result<Option<RecordBatch>, StageError> {
        self.buffer.push(event);
        if self.buffer.len() >= self.capacity {
            let events = std::mem::take(&mut self.buffer);
            let batch = build_batch(events, &self.schema)?;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    pub fn flush(&mut self) -> Result<Option<RecordBatch>, StageError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let events = std::mem::take(&mut self.buffer);
        let batch = build_batch(events, &self.schema)?;
        Ok(Some(batch))
    }
}

pub fn build_batch<C: EventContext>(
    events: Vec<Event<C>>,
    schema: &Schema,
) -> Result<RecordBatch, StageError> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (field_index, field) in schema.fields().iter().enumerate() {
        let field_name = field.name();
        let field_type = field.data_type();

        if field_name == REST_COLUMN_NAME {
            arrays.push(build_rest_column(&events, field_index));
            continue;
        }

        arrays.push(build_typed_column(
            &events,
            field_index,
            field_name,
            field_type,
        )?);
    }

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| StageError::Write(format!("failed to build RecordBatch: {e}")))?;

    Ok(batch)
}

fn build_typed_column<C: EventContext>(
    events: &[Event<C>],
    field_index: usize,
    field_name: &str,
    field_type: &DataType,
) -> Result<ArrayRef, StageError> {
    match field_type {
        DataType::Utf8 => build_utf8(events, field_index),
        DataType::Int64 => build_int64(events, field_index),
        DataType::Int32 => build_int32(events, field_index, field_name),
        DataType::UInt64 => build_uint64(events, field_index),
        DataType::UInt32 => build_uint32(events, field_index, field_name),
        DataType::Float64 => build_float64(events, field_index),
        DataType::Float32 => build_float32(events, field_index),
        DataType::Boolean => build_boolean(events, field_index),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            build_timestamp(events, field_index, field_name)
        }
        _ => Err(StageError::Write(format!(
            "unsupported Arrow data type for field '{field_name}': {field_type:?}"
        ))),
    }
}

fn build_rest_column<C: EventContext>(events: &[Event<C>], rest_index: usize) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(events.len(), events.len() * 64);
    for event in events {
        match event.row.values.get(rest_index) {
            Some(EventValue::String(s)) => builder.append_value(s),
            _ => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

macro_rules! typed_column {
    ($name:ident, $builder:ty, $variant:ident($inner:ty)) => {
        fn $name<C: EventContext>(
            events: &[Event<C>],
            field_index: usize,
        ) -> Result<ArrayRef, StageError> {
            let mut builder = <$builder>::with_capacity(events.len());
            for event in events {
                match event_value(event, field_index) {
                    Some(EventValue::$variant(v)) => builder.append_value(*v),
                    Some(EventValue::Null) => builder.append_null(),
                    Some(other) => {
                        return Err(StageError::Normalization(format!(
                            "type mismatch for column index {field_index}: expected {}, got {:?}",
                            stringify!($variant),
                            other,
                        )));
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    };
}

typed_column!(build_int64, Int64Builder, Int64(i64));
typed_column!(build_uint64, UInt64Builder, UInt64(u64));
typed_column!(build_float64, Float64Builder, Float64(f64));
typed_column!(build_boolean, BooleanBuilder, Bool(bool));

fn build_utf8<C: EventContext>(
    events: &[Event<C>],
    field_index: usize,
) -> Result<ArrayRef, StageError> {
    let mut builder = StringBuilder::with_capacity(events.len(), events.len() * 32);
    for event in events {
        match event_value(event, field_index) {
            Some(EventValue::String(v)) => builder.append_value(v),
            Some(EventValue::Null) => builder.append_null(),
            Some(other) => {
                return Err(StageError::Normalization(format!(
                    "type mismatch for column index {field_index}: expected String, got {other:?}",
                )));
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn build_int32<C: EventContext>(
    events: &[Event<C>],
    field_index: usize,
    field_name: &str,
) -> Result<ArrayRef, StageError> {
    let mut builder = Int32Builder::with_capacity(events.len());
    for event in events {
        match event_value(event, field_index) {
            Some(EventValue::Int64(v)) => {
                let narrowed = i32::try_from(*v).map_err(|_| {
                    StageError::Normalization(format!(
                        "value {v} overflows Int32 range for field '{field_name}'"
                    ))
                })?;
                builder.append_value(narrowed);
            }
            Some(EventValue::Null) => builder.append_null(),
            Some(other) => {
                return Err(StageError::Normalization(format!(
                    "type mismatch for field '{field_name}': expected Int64, got {other:?}",
                )));
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn build_uint32<C: EventContext>(
    events: &[Event<C>],
    field_index: usize,
    field_name: &str,
) -> Result<ArrayRef, StageError> {
    let mut builder = UInt32Builder::with_capacity(events.len());
    for event in events {
        match event_value(event, field_index) {
            Some(EventValue::UInt64(v)) => {
                let narrowed = u32::try_from(*v).map_err(|_| {
                    StageError::Normalization(format!(
                        "value {v} overflows Uint32 range for field '{field_name}'"
                    ))
                })?;
                builder.append_value(narrowed);
            }
            Some(EventValue::Null) => builder.append_null(),
            Some(other) => {
                return Err(StageError::Normalization(format!(
                    "type mismatch for field '{field_name}': expected UInt64, got {other:?}",
                )));
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn build_float32<C: EventContext>(
    events: &[Event<C>],
    field_index: usize,
) -> Result<ArrayRef, StageError> {
    let mut builder = Float32Builder::with_capacity(events.len());
    for event in events {
        match event_value(event, field_index) {
            Some(EventValue::Float64(v)) => builder.append_value(*v as f32),
            Some(EventValue::Null) => builder.append_null(),
            Some(other) => {
                return Err(StageError::Normalization(format!(
                    "type mismatch for column index {field_index}: expected Float64, got {other:?}",
                )));
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn build_timestamp<C: EventContext>(
    events: &[Event<C>],
    field_index: usize,
    field_name: &str,
) -> Result<ArrayRef, StageError> {
    let mut builder = TimestampMillisecondBuilder::with_capacity(events.len());
    for event in events {
        match event_value(event, field_index) {
            Some(EventValue::Timestamp(v)) => builder.append_value(*v),
            Some(EventValue::Null) => builder.append_null(),
            Some(other) => {
                return Err(StageError::Normalization(format!(
                    "type mismatch for field '{field_name}': expected Timestamp, got {other:?}",
                )));
            }
            None => builder.append_null(),
        }
    }
    let array = builder.finish().with_timezone("UTC");
    Ok(Arc::new(array) as ArrayRef)
}

fn event_value<C: EventContext>(event: &Event<C>, field_index: usize) -> Option<&EventValue> {
    event.row.values.get(field_index)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::event::EventRow;
    use crate::schema::{SchemaConfig, TIMESTAMP_COLUMN_NAME};

    #[derive(Debug, Clone)]
    struct TestContext;

    impl EventContext for TestContext {
        fn to_json(&self) -> serde_json::Value {
            serde_json::json!({"test": true})
        }
    }

    fn all_types_schema() -> Schema {
        let yaml = r#"
            table: all_types
            columns:
              - name: ts
                type: timestamp
                time: true
              - name: s
                type: utf8
              - name: i64
                type: int64
              - name: i32
                type: int32
              - name: u64
                type: uint64
              - name: u32
                type: uint32
              - name: f64
                type: float64
              - name: f32
                type: float32
              - name: b
                type: bool
        "#;
        SchemaConfig::from_yaml(yaml).expect("schema").compile()
    }

    fn minimal_schema() -> Schema {
        let yaml = r#"
            table: minimal
            columns:
              - name: _ts
                type: timestamp
                time: true
        "#;
        SchemaConfig::from_yaml(yaml).expect("schema").compile()
    }

    fn make_event(values: Vec<EventValue>) -> Event<TestContext> {
        Event {
            row: EventRow { values },
            context: TestContext,
        }
    }

    #[test]
    fn single_row_all_types() {
        let schema = all_types_schema();
        let events = vec![make_event(vec![
            EventValue::Timestamp(1_735_689_600_000),
            EventValue::String("hello".to_owned()),
            EventValue::Int64(42),
            EventValue::Int64(7),
            EventValue::UInt64(99),
            EventValue::UInt64(5),
            EventValue::Float64(3.14),
            EventValue::Float64(2.5),
            EventValue::Bool(true),
            EventValue::Null, // @rest.
        ])];

        let batch = build_batch(events, &schema).expect("build_batch");
        assert_eq!(batch.num_rows(), 1);

        // @timestamp (index 0).
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("ts");
        assert_eq!(ts.value(0), 1_735_689_600_000);

        // s (index 1).
        let s = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("s");
        assert_eq!(s.value(0), "hello");

        // i64 (index 2).
        let i64v = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        assert_eq!(i64v.value(0), 42);

        // i32 (index 3).
        let i32v = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("i32");
        assert_eq!(i32v.value(0), 7);

        // u64 (index 4).
        let u64v = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("u64");
        assert_eq!(u64v.value(0), 99);

        // u32 (index 5).
        let u32v = batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("u32");
        assert_eq!(u32v.value(0), 5);

        // f64 (index 6).
        let f64v = batch
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64");
        assert!((f64v.value(0) - 3.14).abs() < f64::EPSILON);

        // f32 (index 7).
        let f32v = batch
            .column(7)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("f32");
        assert!((f32v.value(0) - 2.5f32).abs() < f32::EPSILON);

        // b (index 8).
        let bv = batch
            .column(8)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("b");
        assert!(bv.value(0));

        // @rest (index 9).
        let rest = batch
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("@rest");
        assert!(rest.is_null(0));
    }

    #[test]
    fn multiple_rows_correct_arrays() {
        let schema = minimal_schema();
        let events = vec![
            make_event(vec![EventValue::Timestamp(100), EventValue::Null]),
            make_event(vec![EventValue::Timestamp(200), EventValue::Null]),
            make_event(vec![EventValue::Timestamp(300), EventValue::Null]),
        ];

        let batch = build_batch(events, &schema).expect("build_batch");
        assert_eq!(batch.num_rows(), 3);

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("ts");
        assert_eq!(ts.value(0), 100);
        assert_eq!(ts.value(1), 200);
        assert_eq!(ts.value(2), 300);
    }

    #[test]
    fn null_in_nullable_column() {
        // Minimal schema has `@timestamp` (non-nullable) and `@rest` (nullable).
        // We test nulls via the `all_types_schema` where user columns are nullable.
        let schema = all_types_schema();
        let events = vec![make_event(vec![
            EventValue::Timestamp(1000),
            EventValue::Null, // s.
            EventValue::Null, // i64.
            EventValue::Null, // i32.
            EventValue::Null, // u64.
            EventValue::Null, // u32.
            EventValue::Null, // f64.
            EventValue::Null, // f32.
            EventValue::Null, // b.
            EventValue::Null, // @rest.
        ])];

        let batch = build_batch(events, &schema).expect("build_batch");
        assert_eq!(batch.num_rows(), 1);

        // s is null.
        let s = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("s");
        assert!(s.is_null(0));

        // i64 is null.
        let i64v = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        assert!(i64v.is_null(0));

        // b is null.
        let bv = batch
            .column(8)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("b");
        assert!(bv.is_null(0));
    }

    #[test]
    fn int32_overflow_returns_error() {
        let schema = all_types_schema();
        let overflow_val = (i32::MAX as i64) + 1;
        let events = vec![make_event(vec![
            EventValue::Timestamp(1000),
            EventValue::String("x".to_owned()),
            EventValue::Int64(0),
            EventValue::Int64(overflow_val), // i32 overflow.
            EventValue::UInt64(0),
            EventValue::UInt64(0),
            EventValue::Float64(0.0),
            EventValue::Float64(0.0),
            EventValue::Bool(false),
            EventValue::Null, // @rest.
        ])];

        let err = build_batch(events, &schema).unwrap_err();
        assert!(
            matches!(err, StageError::Normalization(ref msg) if msg.contains("overflows Int32")),
            "expected Int32 overflow, got: {err:?}"
        );
    }

    #[test]
    fn uint32_overflow_returns_error() {
        let schema = all_types_schema();
        let overflow_val = (u32::MAX as u64) + 1;
        let events = vec![make_event(vec![
            EventValue::Timestamp(1000),
            EventValue::String("x".to_owned()),
            EventValue::Int64(0),
            EventValue::Int64(0),
            EventValue::UInt64(0),
            EventValue::UInt64(overflow_val), // u32 overflow.
            EventValue::Float64(0.0),
            EventValue::Float64(0.0),
            EventValue::Bool(false),
            EventValue::Null, // @rest.
        ])];

        let err = build_batch(events, &schema).unwrap_err();
        assert!(
            matches!(err, StageError::Normalization(ref msg) if msg.contains("overflows Uint32")),
            "expected Uint32 overflow, got: {err:?}"
        );
    }

    #[test]
    fn empty_input_returns_zero_row_batch() {
        let schema = minimal_schema();
        let events: Vec<Event<TestContext>> = vec![];

        let batch = build_batch(events, &schema).expect("build_batch");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), schema.fields().len());
    }

    #[test]
    fn rest_column_carries_values() {
        let schema = minimal_schema();
        let events = vec![
            make_event(vec![
                EventValue::Timestamp(100),
                EventValue::String(r#"{"extra":42}"#.to_owned()),
            ]),
            make_event(vec![EventValue::Timestamp(200), EventValue::Null]),
            make_event(vec![
                EventValue::Timestamp(300),
                EventValue::String(r#"{"foo":"bar"}"#.to_owned()),
            ]),
        ];

        let batch = build_batch(events, &schema).expect("build_batch");

        let rest = batch
            .column(schema.index_of("@rest").expect("rest"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("@rest");
        assert_eq!(rest.value(0), r#"{"extra":42}"#);
        assert!(rest.is_null(1));
        assert_eq!(rest.value(2), r#"{"foo":"bar"}"#);
    }

    #[test]
    fn timestamp_arrays_have_utc_timezone() {
        let schema = minimal_schema();
        let events = vec![make_event(vec![
            EventValue::Timestamp(1_735_689_600_000),
            EventValue::Null,
        ])];

        let batch = build_batch(events, &schema).expect("build_batch");

        let ts_field = schema.field_with_name(TIMESTAMP_COLUMN_NAME).expect("ts");
        assert_eq!(
            ts_field.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("ts");
        assert_eq!(
            ts.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert_eq!(ts.value(0), 1_735_689_600_000);
    }

    #[test]
    fn mixed_nulls_and_values_in_same_column() {
        let schema = all_types_schema();
        let events = vec![
            // Row 0: all values present.
            make_event(vec![
                EventValue::Timestamp(100),
                EventValue::String("a".to_owned()),
                EventValue::Int64(1),
                EventValue::Int64(10),
                EventValue::UInt64(100),
                EventValue::UInt64(1000),
                EventValue::Float64(1.0),
                EventValue::Float64(10.0),
                EventValue::Bool(true),
                EventValue::Null, // @rest.
            ]),
            // Row 1: all nullable columns are null.
            make_event(vec![
                EventValue::Timestamp(200),
                EventValue::Null,
                EventValue::Null,
                EventValue::Null,
                EventValue::Null,
                EventValue::Null,
                EventValue::Null,
                EventValue::Null,
                EventValue::Null,
                EventValue::Null, // @rest.
            ]),
            // Row 2: mix.
            make_event(vec![
                EventValue::Timestamp(300),
                EventValue::String("b".to_owned()),
                EventValue::Null,
                EventValue::Int64(30),
                EventValue::Null,
                EventValue::UInt64(3000),
                EventValue::Null,
                EventValue::Float64(30.0),
                EventValue::Bool(false),
                EventValue::Null, // @rest
            ]),
        ];

        let batch = build_batch(events, &schema).expect("build_batch");
        assert_eq!(batch.num_rows(), 3);

        // String column (s, index 1): "a", null, "b".
        let s = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("s");
        assert_eq!(s.value(0), "a");
        assert!(s.is_null(1));
        assert_eq!(s.value(2), "b");

        // Int64 column (index 2): 1, null, null.
        let i64v = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        assert_eq!(i64v.value(0), 1);
        assert!(i64v.is_null(1));
        assert!(i64v.is_null(2));

        // Int32 column (index 3): 10, null, 30.
        let i32v = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("i32");
        assert_eq!(i32v.value(0), 10);
        assert!(i32v.is_null(1));
        assert_eq!(i32v.value(2), 30);

        // Boolean column (index 8): true, null, false.
        let bv = batch
            .column(8)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("b");
        assert!(bv.value(0));
        assert!(bv.is_null(1));
        assert!(!bv.value(2));
    }

    #[test]
    fn type_mismatch_returns_error() {
        let schema = all_types_schema();
        let events = vec![make_event(vec![
            EventValue::Timestamp(1000),
            EventValue::String("x".to_owned()),
            EventValue::String("not_an_int".to_owned()), // wrong type.
            EventValue::Int64(0),
            EventValue::UInt64(0),
            EventValue::UInt64(0),
            EventValue::Float64(0.0),
            EventValue::Float64(0.0),
            EventValue::Bool(false),
            EventValue::Null, // @rest.
        ])];

        let err = build_batch(events, &schema).unwrap_err();
        assert!(
            matches!(err, StageError::Normalization(ref msg) if msg.contains("type mismatch")),
            "expected type mismatch, got: {err:?}"
        );
    }

    fn batcher_schema() -> Schema {
        minimal_schema()
    }

    fn batcher_event(ts: i64) -> Event<TestContext> {
        make_event(vec![EventValue::Timestamp(ts), EventValue::Null])
    }

    #[test]
    fn push_returns_none_until_full() {
        let schema = batcher_schema();
        let capacity = 5;
        let mut batcher = Batcher::new(schema, capacity);

        for i in 0..capacity - 1 {
            let result = batcher.push(batcher_event(i as i64)).expect("push");
            assert!(
                result.is_none(),
                "push {i} should return None (not yet at capacity)"
            );
        }
    }

    #[test]
    fn push_returns_batch_at_capacity() {
        let schema = batcher_schema();
        let capacity = 3;
        let mut batcher = Batcher::new(schema, capacity);

        let r0 = batcher.push(batcher_event(100)).expect("push 0");
        assert!(r0.is_none());
        let r1 = batcher.push(batcher_event(200)).expect("push 1");
        assert!(r1.is_none());

        let r2 = batcher.push(batcher_event(300)).expect("push 2");
        let batch = r2.expect("should return Some at capacity");
        assert_eq!(batch.num_rows(), capacity);
    }

    #[test]
    fn flush_returns_remaining() {
        let schema = batcher_schema();
        let capacity = 10;
        let mut batcher = Batcher::new(schema, capacity);

        batcher.push(batcher_event(1)).expect("push");
        batcher.push(batcher_event(2)).expect("push");
        batcher.push(batcher_event(3)).expect("push");

        let batch = batcher.flush().expect("flush").expect("should have batch");
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn flush_returns_none_when_empty() {
        let schema = batcher_schema();
        let mut batcher: Batcher<TestContext> = Batcher::new(schema, 5);
        assert!(batcher.flush().expect("flush").is_none());
    }

    #[test]
    fn push_then_flush_works() {
        let schema = batcher_schema();
        let capacity = 2;
        let mut batcher = Batcher::new(schema, capacity);

        // First push returns None.
        let r0 = batcher.push(batcher_event(10)).expect("push 0");
        assert!(r0.is_none());

        // Second push triggers a full batch.
        let r1 = batcher.push(batcher_event(20)).expect("push 1");
        let batch = r1.expect("should be Some");
        assert_eq!(batch.num_rows(), 2);

        // Flush after draining should return None.
        let flush_result = batcher.flush().expect("flush");
        assert!(
            flush_result.is_none(),
            "buffer should be empty after batch was taken"
        );
    }
}
