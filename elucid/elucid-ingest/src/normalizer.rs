use std::collections::HashSet;

use arrow::datatypes::{DataType, Schema, TimeUnit};

use crate::event::{Event, EventContext, EventRow, EventValue};
use crate::schema::{REST_COLUMN_NAME, TIME_SOURCE_KEY, TIMESTAMP_COLUMN_NAME};
use crate::stage_error::StageError;

#[derive(Debug, Clone)]
pub struct Normalizer {
    timestamp_index: usize,
    rest_index: usize,
    time_source: String,
    field_count: usize,
    indexed_fields: Vec<IndexedField>,
    /// Pre-computed set of JSON keys that map to known schema columns.
    known_fields: HashSet<String>,
}

#[derive(Debug, Clone)]
struct IndexedField {
    name: String,
    arrow_type: DataType,
    arrow_index: usize,
    nullable: bool,
}

impl Normalizer {
    /// Build a normalizer from a compiled Arrow [`Schema`].
    pub fn new(schema: &Schema) -> Result<Self, StageError> {
        let timestamp_index = schema.index_of(TIMESTAMP_COLUMN_NAME).map_err(|_| {
            StageError::Normalization(format!("{TIMESTAMP_COLUMN_NAME} column missing"))
        })?;
        let rest_index = schema
            .index_of(REST_COLUMN_NAME)
            .map_err(|_| StageError::Normalization(format!("{REST_COLUMN_NAME} column missing")))?;

        let time_source = schema
            .metadata()
            .get(TIME_SOURCE_KEY)
            .cloned()
            .unwrap_or_default();

        let indexed_fields: Vec<IndexedField> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() != REST_COLUMN_NAME)
            .map(|(index, field)| IndexedField {
                name: field.name().clone(),
                arrow_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                arrow_index: index,
            })
            .collect();

        let mut known_fields: HashSet<String> = HashSet::with_capacity(indexed_fields.len() + 1);
        for field in &indexed_fields {
            known_fields.insert(field.name.clone());
        }
        if !time_source.is_empty() {
            known_fields.insert(time_source.clone());
        }

        Ok(Self {
            timestamp_index,
            rest_index,
            time_source,
            field_count: schema.fields().len(),
            indexed_fields,
            known_fields,
        })
    }

    /// Parse a raw NDJSON line and produce an [`Event`].
    ///
    /// Each JSON field is coerced to the appropriate [`EventValue`] variant.
    /// Fields not present in the schema are collected into `@rest` as a serialized JSON object.
    pub fn normalize<C: EventContext>(
        &self,
        raw: &str,
        context: C,
    ) -> Result<Event<C>, StageError> {
        let value: serde_json::Value =
            serde_json::from_str(raw).map_err(|e| StageError::Parse(e.to_string()))?;
        let object = match value {
            serde_json::Value::Object(m) => m,
            _ => {
                return Err(StageError::Parse("expected a JSON object".to_owned()));
            }
        };

        let mut values = vec![EventValue::Null; self.field_count];

        for field in &self.indexed_fields {
            if field.name == TIMESTAMP_COLUMN_NAME {
                let value = object.get(&self.time_source);
                let coerced = coerce_timestamp(value, field.nullable, &field.name)?;
                values[self.timestamp_index] = coerced;
            } else {
                let val = object.get(&field.name);
                let coerced = coerce_value(val, &field.arrow_type, field.nullable, &field.name)?;
                values[field.arrow_index] = coerced;
            }
        }

        // Collect unknown keys into @rest.
        let rest_object: serde_json::Map<String, serde_json::Value> = object
            .iter()
            .filter(|(k, _)| !self.known_fields.contains(k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        values[self.rest_index] = if rest_object.is_empty() {
            EventValue::Null
        } else {
            EventValue::String(serde_json::Value::Object(rest_object).to_string())
        };

        Ok(Event {
            row: EventRow { values },
            context,
        })
    }
}

pub fn check_line_size(raw: &str, max: usize) -> Result<(), StageError> {
    let size = raw.len();
    if size > max {
        return Err(StageError::LineTooLarge { size, max });
    }
    Ok(())
}

/// Coerce a JSON value to an [`EventValue`] for non-timestamp columns.
fn coerce_value(
    value: Option<&serde_json::Value>,
    arrow_type: &DataType,
    nullable: bool,
    field_name: &str,
) -> Result<EventValue, StageError> {
    match value {
        None | Some(serde_json::Value::Null) => {
            if nullable {
                Ok(EventValue::Null)
            } else {
                Err(StageError::Normalization(format!(
                    "missing non-nullable field '{field_name}'"
                )))
            }
        }
        Some(v) => match arrow_type {
            DataType::Utf8 => Ok(EventValue::String(match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })),
            DataType::Int64 | DataType::Int32 => v.as_i64().map_or_else(
                || type_mismatch(field_name, arrow_type, v),
                |n| Ok(EventValue::Int64(n)),
            ),
            DataType::UInt64 | DataType::UInt32 => v.as_u64().map_or_else(
                || type_mismatch(field_name, arrow_type, v),
                |n| Ok(EventValue::UInt64(n)),
            ),
            DataType::Float64 | DataType::Float32 => v.as_f64().map_or_else(
                || type_mismatch(field_name, arrow_type, v),
                |n| Ok(EventValue::Float64(n)),
            ),
            DataType::Boolean => v.as_bool().map_or_else(
                || type_mismatch(field_name, arrow_type, v),
                |b| Ok(EventValue::Bool(b)),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                coerce_timestamp(Some(v), nullable, field_name)
            }
            _ => Err(StageError::Normalization(format!(
                "unsupported Arrow type for field '{field_name}': {arrow_type:?}"
            ))),
        },
    }
}

/// Coerce a JSON value into an [`EventValue::Timestamp`] (epoch millis).
fn coerce_timestamp(
    value: Option<&serde_json::Value>,
    nullable: bool,
    field_name: &str,
) -> Result<EventValue, StageError> {
    match value {
        None | Some(serde_json::Value::Null) => {
            if nullable {
                Ok(EventValue::Null)
            } else {
                Err(StageError::Normalization(format!(
                    "missing non-nullable field '{field_name}'"
                )))
            }
        }
        Some(serde_json::Value::Number(n)) => n.as_i64().map_or_else(
            || {
                Err(StageError::Normalization(format!(
                    "non-integer number cannot be used as timestamp for field '{field_name}'"
                )))
            },
            |millis| Ok(EventValue::Timestamp(millis)),
        ),
        Some(serde_json::Value::String(s)) => match parse_timestamp_string(s) {
            Ok(millis) => Ok(EventValue::Timestamp(millis)),
            Err(_) if nullable => Ok(EventValue::Null),
            Err(message) => Err(StageError::Normalization(format!(
                "cannot parse timestamp for field '{field_name}': {message}"
            ))),
        },
        Some(_v) => {
            if nullable {
                Ok(EventValue::Null)
            } else {
                Err(StageError::Normalization(format!(
                    "value for field '{field_name}' is not a number or string"
                )))
            }
        }
    }
}

fn type_mismatch(
    field_name: &str,
    arrow_type: &DataType,
    value: &serde_json::Value,
) -> Result<EventValue, StageError> {
    Err(StageError::Normalization(format!(
        "cannot coerce JSON {:?} to {:?} for field '{field_name}'",
        value, arrow_type,
    )))
}

/// Parse a timestamp string into epoch millis.
fn parse_timestamp_string(s: &str) -> Result<i64, String> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_millis());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(dt.and_utc().timestamp_millis());
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        // SAFETY: midnight (0, 0, 0) is always a valid time.
        let dt = d.and_hms_opt(0, 0, 0).unwrap_or_else(|| unreachable!());
        return Ok(dt.and_utc().timestamp_millis());
    }
    Err(format!("cannot parse '{s}' as timestamp"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaConfig;

    fn web_access_schema() -> Schema {
        let yaml = r#"
            table: web_access
            columns:
              - name: _ts
                type: timestamp
                time: true
              - name: host
                type: utf8
              - name: status
                type: int64
              - name: bytes
                type: uint64
        "#;
        SchemaConfig::from_yaml(yaml).expect("schema").compile()
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

    /// Minimal schema: only `_ts` (timestamp) → `@timestamp` + `@rest`.
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

    #[derive(Debug, Clone)]
    struct TestContext;

    impl EventContext for TestContext {
        fn to_json(&self) -> serde_json::Value {
            serde_json::json!({"test": true})
        }
    }

    #[test]
    fn all_column_types_happy_path() {
        let schema = all_types_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = r#"{"ts":"2025-01-01T00:00:00Z","s":"hello","i64":42,"i32":7,"u64":99,"u32":5,"f64":3.125,"f32":2.5,"b":true}"#;
        let event = normalization
            .normalize(raw, TestContext)
            .expect("normalize");

        let v = &event.row.values;
        // @timestamp (index 0).
        assert_eq!(v[0], EventValue::Timestamp(1_735_689_600_000));
        // s (index 1).
        assert_eq!(v[1], EventValue::String("hello".to_owned()));
        // i64 (index 2).
        assert_eq!(v[2], EventValue::Int64(42));
        // i32 (index 3). Stored as Int64, narrowing happens in batch.
        assert_eq!(v[3], EventValue::Int64(7));
        // u64 (index 4).
        assert_eq!(v[4], EventValue::UInt64(99));
        // u32 (index 5). Stored as UInt64, narrowing happens in batch.
        assert_eq!(v[5], EventValue::UInt64(5));
        // f64 (index 6).
        assert!(matches!(v[6], EventValue::Float64(n) if (n - 3.125).abs() < f64::EPSILON));
        // f32 (index 7). Stored as Float64, narrowing happens in batch.
        assert!(matches!(v[7], EventValue::Float64(n) if (n - 2.5).abs() < f64::EPSILON));
        // b (index 8).
        assert_eq!(v[8], EventValue::Bool(true));
        // @rest (index 9). No unknown fields → Null.
        assert_eq!(v[9], EventValue::Null);
    }
    #[test]
    fn rest_with_unknown_fields() {
        let schema = minimal_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = r#"{"_ts":"2025-01-01T00:00:00Z","extra_a":"wow","extra_b":42}"#;
        let event = normalization
            .normalize(raw, TestContext)
            .expect("normalize");

        // @rest is at index 1 in the minimal schema.
        match &event.row.values[1] {
            EventValue::String(s) => {
                let rest: serde_json::Value = serde_json::from_str(s).expect("json");
                assert_eq!(rest["extra_a"], "wow");
                assert_eq!(rest["extra_b"], 42);
                assert!(rest.get("_ts").is_none());
            }
            other => panic!("expected String for @rest, got {other:?}"),
        }
    }

    #[test]
    fn rest_null_when_no_unknowns() {
        let schema = minimal_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = r#"{"_ts":"2025-01-01T00:00:00Z"}"#;
        let event = normalization
            .normalize(raw, TestContext)
            .expect("normalize");

        assert_eq!(event.row.values[1], EventValue::Null);
    }

    #[test]
    fn timestamp_rfc3339_string() {
        let schema = minimal_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = r#"{"_ts":"2025-01-01T00:00:00Z"}"#;
        let event = normalization
            .normalize(raw, TestContext)
            .expect("normalize");

        assert_eq!(
            event.row.values[0],
            EventValue::Timestamp(1_735_689_600_000)
        );
    }

    #[test]
    fn timestamp_epoch_millis() {
        let schema = minimal_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = r#"{"_ts":1735689600000}"#;
        let event = normalization
            .normalize(raw, TestContext)
            .expect("normalize");

        assert_eq!(
            event.row.values[0],
            EventValue::Timestamp(1_735_689_600_000)
        );
    }

    #[test]
    fn missing_non_nullable_field_returns_error() {
        let schema = minimal_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = r#"{"other":123}"#;
        let err = normalization.normalize(raw, TestContext).unwrap_err();

        assert!(
            matches!(err, StageError::Normalization(ref msg) if msg.contains("@timestamp")),
            "expected Normalization error for @timestamp, got: {err:?}"
        );
    }

    #[test]
    fn bad_timestamp_string_returns_error() {
        let schema = minimal_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = r#"{"_ts":"not-a-date"}"#;
        let err = normalization.normalize(raw, TestContext).unwrap_err();

        assert!(
            matches!(err, StageError::Normalization(ref msg) if msg.contains("@timestamp")),
            "expected Normalization error for @timestamp, got: {err:?}"
        );
    }

    #[test]
    fn oversized_line_returns_error() {
        let raw = "hello world";
        let err = check_line_size(raw, 5).unwrap_err();
        assert!(
            matches!(err, StageError::LineTooLarge { size: 11, max: 5 }),
            "expected LineTooLarge, got: {err:?}"
        );
    }

    #[test]
    fn line_within_limit_is_ok() {
        assert!(check_line_size("hello", 10).is_ok());
    }

    #[test]
    fn non_json_returns_parse_error() {
        let schema = minimal_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = "not json at all";
        let err = normalization.normalize(raw, TestContext).unwrap_err();

        assert!(
            matches!(err, StageError::Parse(_)),
            "expected Parse error, got: {err:?}"
        );
    }

    #[test]
    fn json_array_returns_parse_error() {
        let schema = minimal_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = "[1,2,3]";
        let err = normalization.normalize(raw, TestContext).unwrap_err();

        assert!(
            matches!(err, StageError::Parse(ref msg) if msg.contains("object")),
            "expected Parse error mentioning object, got: {err:?}"
        );
    }

    #[test]
    fn null_for_nullable_column_produces_null() {
        let schema = all_types_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        // Only send ts and b; all other user columns are nullable and absent → Null.
        let raw = r#"{"ts":"2025-01-01T00:00:00Z","b":false}"#;
        let event = normalization
            .normalize(raw, TestContext)
            .expect("normalize");

        // s (index 1) is nullable utf8. Absent → Null.
        assert_eq!(event.row.values[1], EventValue::Null);
        // i64 (index 2) is nullable int64. Absent → Null.
        assert_eq!(event.row.values[2], EventValue::Null);
        // b (index 8) should have the value.
        assert_eq!(event.row.values[8], EventValue::Bool(false));
    }

    #[test]
    fn multiple_events_independent() {
        let schema = minimal_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let e1 = normalization
            .normalize(r#"{"_ts":"2025-01-01T00:00:00Z"}"#, TestContext)
            .expect("e1");
        let e2 = normalization
            .normalize(r#"{"_ts":"2025-01-02T00:00:00Z"}"#, TestContext)
            .expect("e2");

        assert_eq!(e1.row.values[0], EventValue::Timestamp(1_735_689_600_000));
        assert_eq!(e2.row.values[0], EventValue::Timestamp(1_735_776_000_000));
    }

    #[test]
    fn int32_overflow_stored_as_int64() {
        let schema = all_types_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let overflow_value = (i32::MAX as i64) + 1;
        let raw = format!(
            r#"{{"ts":"2025-01-01T00:00:00Z","s":"x","i64":0,"i32":{overflow_value},"u64":0,"u32":0,"f64":0.0,"f32":0.0,"b":false}}"#
        );
        let event = normalization
            .normalize(&raw, TestContext)
            .expect("normalize");

        assert_eq!(event.row.values[3], EventValue::Int64(overflow_value));
    }

    #[test]
    fn type_mismatch_for_int64_returns_error() {
        let schema = web_access_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = r#"{"_ts":"2025-01-01T00:00:00Z","status":"not-a-number"}"#;
        let err = normalization.normalize(raw, TestContext).unwrap_err();

        assert!(
            matches!(err, StageError::Normalization(ref msg) if msg.contains("status")),
            "expected Normalization error for status, got: {err:?}"
        );
    }

    #[test]
    fn null_for_non_nullable_timestamp_returns_error() {
        let schema = minimal_schema();
        let normalization = Normalizer::new(&schema).expect("normalizer");

        let raw = r#"{"_ts":null}"#;
        let err = normalization.normalize(raw, TestContext).unwrap_err();

        assert!(
            matches!(err, StageError::Normalization(ref msg) if msg.contains("@timestamp")),
            "expected Normalization error for @timestamp, got: {err:?}"
        );
    }
}
