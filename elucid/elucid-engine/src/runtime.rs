use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::array::new_empty_array;
use arrow::datatypes::DataType;
use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::expr_fn::create_udf;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use elucid_catalog::LogicalType;
use elucid_language::ir::{BinaryOperator, CastKind, RemainderFunction, UnaryOperator};
use serde_json::{Number, Value};

use crate::EngineError;

const MAXIMUM_ERROR_CHAIN_DEPTH: usize = 32;
const UTC_TIMEZONE: &str = "UTC";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFailureKind {
    Cast,
    Evaluation,
    Execution,
}

#[derive(Debug)]
struct RuntimeFailure {
    kind: RuntimeFailureKind,
}

impl Display for RuntimeFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self.kind {
            RuntimeFailureKind::Cast => "query cast failed",
            RuntimeFailureKind::Evaluation => "query evaluation failed",
            RuntimeFailureKind::Execution => "query runtime invariant failed",
        })
    }
}

impl std::error::Error for RuntimeFailure {}

pub(crate) fn runtime_failure_kind(error: &DataFusionError) -> Option<RuntimeFailureKind> {
    let mut current: &(dyn std::error::Error + 'static) = error;
    for _ in 0..MAXIMUM_ERROR_CHAIN_DEPTH {
        if let Some(failure) = current.downcast_ref::<RuntimeFailure>() {
            return Some(failure.kind);
        }
        current = current.source()?;
    }
    None
}

pub(crate) fn runtime_error(kind: RuntimeFailureKind) -> DataFusionError {
    DataFusionError::External(Box::new(RuntimeFailure { kind }))
}

pub(crate) fn null_scalar(logical_type: LogicalType) -> Result<ScalarValue, EngineError> {
    match logical_type {
        LogicalType::Bool => Ok(ScalarValue::Boolean(None)),
        LogicalType::Int32 => Ok(ScalarValue::Int32(None)),
        LogicalType::Int64 => Ok(ScalarValue::Int64(None)),
        LogicalType::UInt32 => Ok(ScalarValue::UInt32(None)),
        LogicalType::UInt64 => Ok(ScalarValue::UInt64(None)),
        LogicalType::Float32 => Ok(ScalarValue::Float32(None)),
        LogicalType::Float64 => Ok(ScalarValue::Float64(None)),
        LogicalType::Utf8 | LogicalType::Json => Ok(ScalarValue::Utf8(None)),
        LogicalType::Datetime => Ok(ScalarValue::TimestampMillisecond(
            None,
            Some(Arc::from(UTC_TIMEZONE)),
        )),
        LogicalType::Eid => Ok(ScalarValue::FixedSizeBinary(16, None)),
        _ => Err(EngineError::catalog_corrupt(
            "typed query contains an unsupported logical type",
        )),
    }
}

pub(crate) fn checked_binary_udf(
    operator: BinaryOperator,
    logical_type: LogicalType,
) -> Result<ScalarUDF, EngineError> {
    if !is_numeric(logical_type) {
        return Err(EngineError::catalog_corrupt(
            "typed arithmetic expression is not numeric",
        ));
    }
    let name = match operator {
        BinaryOperator::Add => "elucid_checked_add",
        BinaryOperator::Subtract => "elucid_checked_subtract",
        BinaryOperator::Multiply => "elucid_checked_multiply",
        BinaryOperator::Divide => "elucid_checked_divide",
        _ => {
            return Err(EngineError::catalog_corrupt(
                "typed arithmetic expression has an invalid operator",
            ));
        }
    };
    let output_type = logical_type.arrow_data_type();
    let function_output_type = output_type.clone();
    Ok(create_udf(
        name,
        vec![output_type.clone(), output_type.clone()],
        output_type,
        Volatility::Immutable,
        Arc::new(move |arguments| {
            map_binary(arguments, &function_output_type, |left, right| {
                checked_binary(operator, logical_type, left, right).map_err(runtime_error)
            })
        }),
    ))
}

pub(crate) fn checked_negate_udf(
    operator: UnaryOperator,
    logical_type: LogicalType,
) -> Result<ScalarUDF, EngineError> {
    if !matches!(operator, UnaryOperator::Negate)
        || !matches!(
            logical_type,
            LogicalType::Int32 | LogicalType::Int64 | LogicalType::Float32 | LogicalType::Float64
        )
    {
        return Err(EngineError::catalog_corrupt(
            "typed negation expression has an invalid type or operator",
        ));
    }
    let output_type = logical_type.arrow_data_type();
    let function_output_type = output_type.clone();
    Ok(create_udf(
        "elucid_checked_negate",
        vec![output_type.clone()],
        output_type,
        Volatility::Immutable,
        Arc::new(move |arguments| {
            map_unary(arguments, &function_output_type, |value| {
                checked_negate(logical_type, value).map_err(runtime_error)
            })
        }),
    ))
}

pub(crate) fn cast_udf(
    kind: CastKind,
    source: LogicalType,
    target: LogicalType,
) -> Result<ScalarUDF, EngineError> {
    let failure_behavior = match kind {
        CastKind::Lossless | CastKind::Strict => CastFailureBehavior::Abort,
        CastKind::NullOnFailure => CastFailureBehavior::Null,
        _ => {
            return Err(EngineError::catalog_corrupt(
                "typed cast has an unsupported failure behavior",
            ));
        }
    };
    let output_type = target.arrow_data_type();
    let function_output_type = output_type.clone();
    Ok(create_udf(
        "elucid_cast",
        vec![source.arrow_data_type()],
        output_type,
        Volatility::Immutable,
        Arc::new(move |arguments| {
            map_unary(
                arguments,
                &function_output_type,
                |value| match convert_scalar(value, source, target) {
                    Ok(converted) => Ok(converted),
                    Err(InvalidCast) if failure_behavior == CastFailureBehavior::Null => {
                        null_scalar(target)
                            .map_err(|_| runtime_error(RuntimeFailureKind::Execution))
                    }
                    Err(InvalidCast) => Err(runtime_error(RuntimeFailureKind::Cast)),
                },
            )
        }),
    ))
}

pub(crate) fn remainder_udf(
    function: RemainderFunction,
    key: String,
) -> Result<ScalarUDF, EngineError> {
    let (name, output_type) = match function {
        RemainderFunction::Value => ("elucid_rest", DataType::Utf8),
        RemainderFunction::Exists => ("elucid_rest_exists", DataType::Boolean),
        _ => {
            return Err(EngineError::catalog_corrupt(
                "typed remainder expression has an unsupported function",
            ));
        }
    };
    let function_output_type = output_type.clone();
    Ok(create_udf(
        name,
        vec![DataType::Utf8],
        output_type,
        Volatility::Immutable,
        Arc::new(move |arguments| {
            map_unary(arguments, &function_output_type, |value| {
                evaluate_remainder(function, &key, value).map_err(runtime_error)
            })
        }),
    ))
}

fn map_unary(
    arguments: &[ColumnarValue],
    output_type: &DataType,
    operation: impl Fn(&ScalarValue) -> DataFusionResult<ScalarValue>,
) -> DataFusionResult<ColumnarValue> {
    let [argument] = arguments else {
        return Err(DataFusionError::Internal(
            "Elucid unary function received an invalid arity".to_owned(),
        ));
    };
    match argument {
        ColumnarValue::Scalar(value) => operation(value).map(ColumnarValue::Scalar),
        ColumnarValue::Array(array) if array.is_empty() => {
            Ok(ColumnarValue::Array(new_empty_array(output_type)))
        }
        ColumnarValue::Array(array) => {
            let values = (0..array.len())
                .map(|index| ScalarValue::try_from_array(array, index))
                .map(|value| value.and_then(|value| operation(&value)))
                .collect::<DataFusionResult<Vec<_>>>()?;
            ScalarValue::iter_to_array(values).map(ColumnarValue::Array)
        }
    }
}

fn map_binary(
    arguments: &[ColumnarValue],
    output_type: &DataType,
    operation: impl Fn(&ScalarValue, &ScalarValue) -> DataFusionResult<ScalarValue>,
) -> DataFusionResult<ColumnarValue> {
    let [left, right] = arguments else {
        return Err(DataFusionError::Internal(
            "Elucid binary function received an invalid arity".to_owned(),
        ));
    };
    if let (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) = (left, right) {
        return operation(left, right).map(ColumnarValue::Scalar);
    }
    let arrays = ColumnarValue::values_to_arrays(arguments)?;
    let [left, right] = arrays.as_slice() else {
        return Err(DataFusionError::Internal(
            "Elucid binary function expanded an invalid arity".to_owned(),
        ));
    };
    if left.is_empty() {
        return Ok(ColumnarValue::Array(new_empty_array(output_type)));
    }
    let values = (0..left.len())
        .map(|index| {
            let left = ScalarValue::try_from_array(left, index)?;
            let right = ScalarValue::try_from_array(right, index)?;
            operation(&left, &right)
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    ScalarValue::iter_to_array(values).map(ColumnarValue::Array)
}

fn checked_binary(
    operator: BinaryOperator,
    logical_type: LogicalType,
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<ScalarValue, RuntimeFailureKind> {
    if left.is_null() || right.is_null() {
        return null_scalar(logical_type).map_err(|_| RuntimeFailureKind::Execution);
    }
    macro_rules! checked_integer {
        ($variant:path, $left:expr, $right:expr) => {{
            let value = match operator {
                BinaryOperator::Add => $left.checked_add($right),
                BinaryOperator::Subtract => $left.checked_sub($right),
                BinaryOperator::Multiply => $left.checked_mul($right),
                BinaryOperator::Divide => $left.checked_div($right),
                _ => None,
            }
            .ok_or(RuntimeFailureKind::Evaluation)?;
            Ok($variant(Some(value)))
        }};
    }
    match (logical_type, left, right) {
        (LogicalType::Int32, ScalarValue::Int32(Some(left)), ScalarValue::Int32(Some(right))) => {
            checked_integer!(ScalarValue::Int32, *left, *right)
        }
        (LogicalType::Int64, ScalarValue::Int64(Some(left)), ScalarValue::Int64(Some(right))) => {
            checked_integer!(ScalarValue::Int64, *left, *right)
        }
        (
            LogicalType::UInt32,
            ScalarValue::UInt32(Some(left)),
            ScalarValue::UInt32(Some(right)),
        ) => {
            checked_integer!(ScalarValue::UInt32, *left, *right)
        }
        (
            LogicalType::UInt64,
            ScalarValue::UInt64(Some(left)),
            ScalarValue::UInt64(Some(right)),
        ) => {
            checked_integer!(ScalarValue::UInt64, *left, *right)
        }
        (
            LogicalType::Float32,
            ScalarValue::Float32(Some(left)),
            ScalarValue::Float32(Some(right)),
        ) => {
            let result = match operator {
                BinaryOperator::Add => *left + *right,
                BinaryOperator::Subtract => *left - *right,
                BinaryOperator::Multiply => *left * *right,
                BinaryOperator::Divide => *left / *right,
                _ => return Err(RuntimeFailureKind::Execution),
            };
            result
                .is_finite()
                .then_some(ScalarValue::Float32(Some(result)))
                .ok_or(RuntimeFailureKind::Evaluation)
        }
        (
            LogicalType::Float64,
            ScalarValue::Float64(Some(left)),
            ScalarValue::Float64(Some(right)),
        ) => {
            let result = match operator {
                BinaryOperator::Add => *left + *right,
                BinaryOperator::Subtract => *left - *right,
                BinaryOperator::Multiply => *left * *right,
                BinaryOperator::Divide => *left / *right,
                _ => return Err(RuntimeFailureKind::Execution),
            };
            result
                .is_finite()
                .then_some(ScalarValue::Float64(Some(result)))
                .ok_or(RuntimeFailureKind::Evaluation)
        }
        _ => Err(RuntimeFailureKind::Execution),
    }
}

fn checked_negate(
    logical_type: LogicalType,
    value: &ScalarValue,
) -> Result<ScalarValue, RuntimeFailureKind> {
    if value.is_null() {
        return null_scalar(logical_type).map_err(|_| RuntimeFailureKind::Execution);
    }
    match (logical_type, value) {
        (LogicalType::Int32, ScalarValue::Int32(Some(value))) => value
            .checked_neg()
            .map(|value| ScalarValue::Int32(Some(value)))
            .ok_or(RuntimeFailureKind::Evaluation),
        (LogicalType::Int64, ScalarValue::Int64(Some(value))) => value
            .checked_neg()
            .map(|value| ScalarValue::Int64(Some(value)))
            .ok_or(RuntimeFailureKind::Evaluation),
        (LogicalType::Float32, ScalarValue::Float32(Some(value))) => {
            let value = -*value;
            value
                .is_finite()
                .then_some(ScalarValue::Float32(Some(value)))
                .ok_or(RuntimeFailureKind::Evaluation)
        }
        (LogicalType::Float64, ScalarValue::Float64(Some(value))) => {
            let value = -*value;
            value
                .is_finite()
                .then_some(ScalarValue::Float64(Some(value)))
                .ok_or(RuntimeFailureKind::Evaluation)
        }
        _ => Err(RuntimeFailureKind::Execution),
    }
}

fn evaluate_remainder(
    function: RemainderFunction,
    key: &str,
    value: &ScalarValue,
) -> Result<ScalarValue, RuntimeFailureKind> {
    let ScalarValue::Utf8(remainder) = value else {
        return Err(RuntimeFailureKind::Execution);
    };
    let Some(remainder) = remainder else {
        return match function {
            RemainderFunction::Value => Ok(ScalarValue::Utf8(None)),
            RemainderFunction::Exists => Ok(ScalarValue::Boolean(Some(false))),
            _ => Err(RuntimeFailureKind::Execution),
        };
    };
    let Value::Object(remainder) =
        serde_json::from_str::<Value>(remainder).map_err(|_| RuntimeFailureKind::Execution)?
    else {
        return Err(RuntimeFailureKind::Execution);
    };
    match function {
        RemainderFunction::Value => remainder.get(key).map_or_else(
            || Ok(ScalarValue::Utf8(None)),
            |value| {
                serde_json::to_string(value)
                    .map(|value| ScalarValue::Utf8(Some(value)))
                    .map_err(|_| RuntimeFailureKind::Execution)
            },
        ),
        RemainderFunction::Exists => Ok(ScalarValue::Boolean(Some(remainder.contains_key(key)))),
        _ => Err(RuntimeFailureKind::Execution),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CastFailureBehavior {
    Abort,
    Null,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InvalidCast;

fn convert_scalar(
    value: &ScalarValue,
    source: LogicalType,
    target: LogicalType,
) -> Result<ScalarValue, InvalidCast> {
    if value.is_null() {
        return null_scalar(target).map_err(|_| InvalidCast);
    }
    if source == LogicalType::Json {
        return convert_json(value, target);
    }
    convert_non_json(value, source, target)
}

fn convert_json(value: &ScalarValue, target: LogicalType) -> Result<ScalarValue, InvalidCast> {
    let ScalarValue::Utf8(Some(encoded)) = value else {
        return Err(InvalidCast);
    };
    let json = serde_json::from_str::<Value>(encoded).map_err(|_| InvalidCast)?;
    if target == LogicalType::Json {
        return Ok(value.clone());
    }
    match json {
        Value::Null => null_scalar(target).map_err(|_| InvalidCast),
        Value::Bool(value) => convert_non_json(
            &ScalarValue::Boolean(Some(value)),
            LogicalType::Bool,
            target,
        ),
        Value::String(value) => {
            convert_non_json(&ScalarValue::Utf8(Some(value)), LogicalType::Utf8, target)
        }
        Value::Number(value) if is_numeric(target) => {
            numeric_to_scalar(json_number(value)?, target)
        }
        Value::Number(_) | Value::Array(_) | Value::Object(_) => Err(InvalidCast),
    }
}

fn convert_non_json(
    value: &ScalarValue,
    source: LogicalType,
    target: LogicalType,
) -> Result<ScalarValue, InvalidCast> {
    if source == target {
        return Ok(value.clone());
    }
    if target == LogicalType::Json {
        return scalar_to_json(value, source).map(|value| ScalarValue::Utf8(Some(value)));
    }
    if target == LogicalType::Utf8 {
        return scalar_to_utf8(value, source).map(|value| ScalarValue::Utf8(Some(value)));
    }
    if source == LogicalType::Utf8 {
        return utf8_to_scalar(value, target);
    }
    if is_numeric(source) && is_numeric(target) {
        return numeric_to_scalar(numeric_value(value, source)?, target);
    }
    Err(InvalidCast)
}

fn utf8_to_scalar(value: &ScalarValue, target: LogicalType) -> Result<ScalarValue, InvalidCast> {
    let ScalarValue::Utf8(Some(value)) = value else {
        return Err(InvalidCast);
    };
    match target {
        LogicalType::Bool => match value.as_str() {
            "true" => Ok(ScalarValue::Boolean(Some(true))),
            "false" => Ok(ScalarValue::Boolean(Some(false))),
            _ => Err(InvalidCast),
        },
        LogicalType::Int32 if valid_signed_decimal(value) => value
            .parse::<i32>()
            .map(|value| ScalarValue::Int32(Some(value)))
            .map_err(|_| InvalidCast),
        LogicalType::Int64 if valid_signed_decimal(value) => value
            .parse::<i64>()
            .map(|value| ScalarValue::Int64(Some(value)))
            .map_err(|_| InvalidCast),
        LogicalType::UInt32 if valid_unsigned_decimal(value) => value
            .parse::<u32>()
            .map(|value| ScalarValue::UInt32(Some(value)))
            .map_err(|_| InvalidCast),
        LogicalType::UInt64 if valid_unsigned_decimal(value) => value
            .parse::<u64>()
            .map(|value| ScalarValue::UInt64(Some(value)))
            .map_err(|_| InvalidCast),
        LogicalType::Float32 => {
            let value = parse_json_float(value)? as f32;
            value
                .is_finite()
                .then_some(ScalarValue::Float32(Some(value)))
                .ok_or(InvalidCast)
        }
        LogicalType::Float64 => {
            parse_json_float(value).map(|value| ScalarValue::Float64(Some(value)))
        }
        LogicalType::Datetime => {
            let value = DateTime::parse_from_rfc3339(value).map_err(|_| InvalidCast)?;
            if !value.timestamp_subsec_nanos().is_multiple_of(1_000_000) {
                return Err(InvalidCast);
            }
            Ok(ScalarValue::TimestampMillisecond(
                Some(value.timestamp_millis()),
                Some(Arc::from(UTC_TIMEZONE)),
            ))
        }
        LogicalType::Eid => {
            parse_eid(value).map(|value| ScalarValue::FixedSizeBinary(16, Some(value.to_vec())))
        }
        _ => Err(InvalidCast),
    }
}

fn scalar_to_utf8(value: &ScalarValue, source: LogicalType) -> Result<String, InvalidCast> {
    match (source, value) {
        (LogicalType::Bool, ScalarValue::Boolean(Some(value))) => Ok(value.to_string()),
        (LogicalType::Int32, ScalarValue::Int32(Some(value))) => Ok(value.to_string()),
        (LogicalType::Int64, ScalarValue::Int64(Some(value))) => Ok(value.to_string()),
        (LogicalType::UInt32, ScalarValue::UInt32(Some(value))) => Ok(value.to_string()),
        (LogicalType::UInt64, ScalarValue::UInt64(Some(value))) => Ok(value.to_string()),
        (LogicalType::Float32, ScalarValue::Float32(Some(value))) if value.is_finite() => {
            serde_json::to_string(value).map_err(|_| InvalidCast)
        }
        (LogicalType::Float64, ScalarValue::Float64(Some(value))) if value.is_finite() => {
            serde_json::to_string(value).map_err(|_| InvalidCast)
        }
        (LogicalType::Utf8, ScalarValue::Utf8(Some(value))) => Ok(value.clone()),
        (LogicalType::Datetime, ScalarValue::TimestampMillisecond(Some(value), Some(timezone)))
            if timezone.as_ref() == UTC_TIMEZONE =>
        {
            DateTime::<Utc>::from_timestamp_millis(*value)
                .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
                .ok_or(InvalidCast)
        }
        (LogicalType::Eid, ScalarValue::FixedSizeBinary(16, Some(value))) if value.len() == 16 => {
            Ok(encode_eid(value))
        }
        _ => Err(InvalidCast),
    }
}

fn scalar_to_json(value: &ScalarValue, source: LogicalType) -> Result<String, InvalidCast> {
    match source {
        LogicalType::Utf8 | LogicalType::Datetime | LogicalType::Eid => {
            serde_json::to_string(&scalar_to_utf8(value, source)?).map_err(|_| InvalidCast)
        }
        LogicalType::Bool
        | LogicalType::Int32
        | LogicalType::Int64
        | LogicalType::UInt32
        | LogicalType::UInt64
        | LogicalType::Float32
        | LogicalType::Float64 => scalar_to_utf8(value, source),
        _ => Err(InvalidCast),
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum NumericValue {
    Signed(i64),
    Unsigned(u64),
    Float32(f32),
    Float64(f64),
}

fn numeric_value(value: &ScalarValue, source: LogicalType) -> Result<NumericValue, InvalidCast> {
    match (source, value) {
        (LogicalType::Int32, ScalarValue::Int32(Some(value))) => {
            Ok(NumericValue::Signed(i64::from(*value)))
        }
        (LogicalType::Int64, ScalarValue::Int64(Some(value))) => Ok(NumericValue::Signed(*value)),
        (LogicalType::UInt32, ScalarValue::UInt32(Some(value))) => {
            Ok(NumericValue::Unsigned(u64::from(*value)))
        }
        (LogicalType::UInt64, ScalarValue::UInt64(Some(value))) => {
            Ok(NumericValue::Unsigned(*value))
        }
        (LogicalType::Float32, ScalarValue::Float32(Some(value))) if value.is_finite() => {
            Ok(NumericValue::Float32(*value))
        }
        (LogicalType::Float64, ScalarValue::Float64(Some(value))) if value.is_finite() => {
            Ok(NumericValue::Float64(*value))
        }
        _ => Err(InvalidCast),
    }
}

fn json_number(value: Number) -> Result<NumericValue, InvalidCast> {
    if let Some(value) = value.as_i64() {
        Ok(NumericValue::Signed(value))
    } else if let Some(value) = value.as_u64() {
        Ok(NumericValue::Unsigned(value))
    } else {
        value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(NumericValue::Float64)
            .ok_or(InvalidCast)
    }
}

fn numeric_to_scalar(value: NumericValue, target: LogicalType) -> Result<ScalarValue, InvalidCast> {
    match target {
        LogicalType::Int32 => to_i32(value).map(|value| ScalarValue::Int32(Some(value))),
        LogicalType::Int64 => to_i64(value).map(|value| ScalarValue::Int64(Some(value))),
        LogicalType::UInt32 => to_u32(value).map(|value| ScalarValue::UInt32(Some(value))),
        LogicalType::UInt64 => to_u64(value).map(|value| ScalarValue::UInt64(Some(value))),
        LogicalType::Float32 => to_f32(value).map(|value| ScalarValue::Float32(Some(value))),
        LogicalType::Float64 => to_f64(value).map(|value| ScalarValue::Float64(Some(value))),
        _ => Err(InvalidCast),
    }
}

fn to_i32(value: NumericValue) -> Result<i32, InvalidCast> {
    match value {
        NumericValue::Signed(value) => i32::try_from(value).map_err(|_| InvalidCast),
        NumericValue::Unsigned(value) => i32::try_from(value).map_err(|_| InvalidCast),
        NumericValue::Float32(value) => float_to_i32(f64::from(value)),
        NumericValue::Float64(value) => float_to_i32(value),
    }
}

fn to_i64(value: NumericValue) -> Result<i64, InvalidCast> {
    match value {
        NumericValue::Signed(value) => Ok(value),
        NumericValue::Unsigned(value) => i64::try_from(value).map_err(|_| InvalidCast),
        NumericValue::Float32(value) => float_to_i64(f64::from(value)),
        NumericValue::Float64(value) => float_to_i64(value),
    }
}

fn to_u32(value: NumericValue) -> Result<u32, InvalidCast> {
    match value {
        NumericValue::Signed(value) => u32::try_from(value).map_err(|_| InvalidCast),
        NumericValue::Unsigned(value) => u32::try_from(value).map_err(|_| InvalidCast),
        NumericValue::Float32(value) => float_to_u32(f64::from(value)),
        NumericValue::Float64(value) => float_to_u32(value),
    }
}

fn to_u64(value: NumericValue) -> Result<u64, InvalidCast> {
    match value {
        NumericValue::Signed(value) => u64::try_from(value).map_err(|_| InvalidCast),
        NumericValue::Unsigned(value) => Ok(value),
        NumericValue::Float32(value) => float_to_u64(f64::from(value)),
        NumericValue::Float64(value) => float_to_u64(value),
    }
}

fn to_f32(value: NumericValue) -> Result<f32, InvalidCast> {
    let converted = match value {
        NumericValue::Signed(value)
            if integer_is_exact(value.unsigned_abs(), f32::MANTISSA_DIGITS) =>
        {
            value as f32
        }
        NumericValue::Unsigned(value) if integer_is_exact(value, f32::MANTISSA_DIGITS) => {
            value as f32
        }
        NumericValue::Float32(value) => value,
        NumericValue::Float64(value) => {
            let converted = value as f32;
            if f64::from(converted) != value {
                return Err(InvalidCast);
            }
            converted
        }
        NumericValue::Signed(_) | NumericValue::Unsigned(_) => return Err(InvalidCast),
    };
    converted
        .is_finite()
        .then_some(converted)
        .ok_or(InvalidCast)
}

fn to_f64(value: NumericValue) -> Result<f64, InvalidCast> {
    let converted = match value {
        NumericValue::Signed(value)
            if integer_is_exact(value.unsigned_abs(), f64::MANTISSA_DIGITS) =>
        {
            value as f64
        }
        NumericValue::Unsigned(value) if integer_is_exact(value, f64::MANTISSA_DIGITS) => {
            value as f64
        }
        NumericValue::Float32(value) => f64::from(value),
        NumericValue::Float64(value) => value,
        NumericValue::Signed(_) | NumericValue::Unsigned(_) => return Err(InvalidCast),
    };
    converted
        .is_finite()
        .then_some(converted)
        .ok_or(InvalidCast)
}

fn float_to_i32(value: f64) -> Result<i32, InvalidCast> {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= f64::from(i32::MIN)
        && value <= f64::from(i32::MAX)
    {
        Ok(value as i32)
    } else {
        Err(InvalidCast)
    }
}

fn float_to_i64(value: f64) -> Result<i64, InvalidCast> {
    const LOWER: f64 = -9_223_372_036_854_775_808.0;
    const UPPER_EXCLUSIVE: f64 = 9_223_372_036_854_775_808.0;
    if value.is_finite() && value.fract() == 0.0 && (LOWER..UPPER_EXCLUSIVE).contains(&value) {
        Ok(value as i64)
    } else {
        Err(InvalidCast)
    }
}

fn float_to_u32(value: f64) -> Result<u32, InvalidCast> {
    if value.is_finite() && value.fract() == 0.0 && value >= 0.0 && value <= f64::from(u32::MAX) {
        Ok(value as u32)
    } else {
        Err(InvalidCast)
    }
}

fn float_to_u64(value: f64) -> Result<u64, InvalidCast> {
    const UPPER_EXCLUSIVE: f64 = 18_446_744_073_709_551_616.0;
    if value.is_finite() && value.fract() == 0.0 && (0.0..UPPER_EXCLUSIVE).contains(&value) {
        Ok(value as u64)
    } else {
        Err(InvalidCast)
    }
}

fn parse_json_float(value: &str) -> Result<f64, InvalidCast> {
    serde_json::from_str::<Number>(value)
        .map_err(|_| InvalidCast)?
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or(InvalidCast)
}

fn valid_signed_decimal(value: &str) -> bool {
    let digits = value.strip_prefix('-').unwrap_or(value);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn valid_unsigned_decimal(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn parse_eid(value: &str) -> Result<[u8; 16], InvalidCast> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(InvalidCast);
    }
    let mut result = [0_u8; 16];
    for (index, output) in result.iter_mut().enumerate() {
        let high = hex_value(value.as_bytes()[index * 2]);
        let low = hex_value(value.as_bytes()[index * 2 + 1]);
        *output = (high << 4) | low;
    }
    Ok(result)
}

fn encode_eid(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(32);
    for byte in value {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn hex_value(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => unreachable!("eid validation accepts only lowercase hexadecimal bytes"),
    }
}

fn integer_is_exact(value: u64, mantissa_digits: u32) -> bool {
    if value == 0 {
        return true;
    }
    let significant_bits = u64::BITS - value.leading_zeros();
    significant_bits <= mantissa_digits
        || value.trailing_zeros() >= significant_bits - mantissa_digits
}

fn is_numeric(logical_type: LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Int32
            | LogicalType::Int64
            | LogicalType::UInt32
            | LogicalType::UInt64
            | LogicalType::Float32
            | LogicalType::Float64
    )
}
