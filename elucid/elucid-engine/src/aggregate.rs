use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array as _, ArrayRef};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::functions_aggregate::expr_fn::{max, min};
use datafusion::logical_expr::expr_fn::create_udaf;
use datafusion::logical_expr::{Accumulator, Expr, Volatility};
use elucid_catalog::LogicalType;
use elucid_language::ir::AggregateFunction;

use crate::EngineError;
use crate::runtime::{RuntimeFailureKind, runtime_error};

pub(crate) fn aggregate_expression(
    function: AggregateFunction,
    argument_type: Option<LogicalType>,
    argument: Expr,
) -> Result<Expr, EngineError> {
    match function {
        AggregateFunction::Count => Ok(checked_count_udaf(
            argument_type.map_or(CountMode::AllRows, |_| CountMode::NonNull),
            argument_type
                .unwrap_or(LogicalType::Int64)
                .arrow_data_type(),
        )
        .call(vec![argument])),
        AggregateFunction::Sum => {
            let argument_type = argument_type.ok_or_else(|| {
                EngineError::catalog_corrupt("typed sum aggregate has no argument")
            })?;
            Ok(checked_sum_udaf(argument_type)?.call(vec![argument]))
        }
        AggregateFunction::Average => {
            let argument_type = argument_type.ok_or_else(|| {
                EngineError::catalog_corrupt("typed average aggregate has no argument")
            })?;
            Ok(checked_average_udaf(argument_type)?.call(vec![argument]))
        }
        AggregateFunction::Min => Ok(min(argument)),
        AggregateFunction::Max => Ok(max(argument)),
        _ => Err(EngineError::catalog_corrupt(
            "typed query contains an unsupported aggregate function",
        )),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CountMode {
    AllRows,
    NonNull,
}

fn checked_count_udaf(
    mode: CountMode,
    input_type: DataType,
) -> datafusion::logical_expr::AggregateUDF {
    create_udaf(
        "elucid_checked_count",
        vec![input_type],
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        Arc::new(move |_| Ok(Box::new(CheckedCountAccumulator { mode, count: 0 }))),
        Arc::new(vec![DataType::Int64]),
    )
}

#[derive(Debug)]
struct CheckedCountAccumulator {
    mode: CountMode,
    count: i64,
}

impl Accumulator for CheckedCountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        let [values] = values else {
            return Err(invalid_aggregate_state());
        };
        let increment = match self.mode {
            CountMode::AllRows => values.len(),
            CountMode::NonNull => values
                .len()
                .checked_sub(values.null_count())
                .ok_or_else(invalid_aggregate_state)?,
        };
        let increment =
            i64::try_from(increment).map_err(|_| runtime_error(RuntimeFailureKind::Evaluation))?;
        self.count = self
            .count
            .checked_add(increment)
            .ok_or_else(|| runtime_error(RuntimeFailureKind::Evaluation))?;
        Ok(())
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        let [counts] = states else {
            return Err(invalid_aggregate_state());
        };
        for index in 0..counts.len() {
            match ScalarValue::try_from_array(counts, index)? {
                ScalarValue::Int64(Some(count)) if count >= 0 => {
                    self.count = self
                        .count
                        .checked_add(count)
                        .ok_or_else(|| runtime_error(RuntimeFailureKind::Evaluation))?;
                }
                _ => return Err(invalid_aggregate_state()),
            }
        }
        Ok(())
    }
}

fn checked_sum_udaf(
    input_type: LogicalType,
) -> Result<datafusion::logical_expr::AggregateUDF, EngineError> {
    let kind = match input_type {
        LogicalType::Int32 | LogicalType::Int64 => SumKind::Signed,
        LogicalType::UInt32 | LogicalType::UInt64 => SumKind::Unsigned,
        LogicalType::Float32 | LogicalType::Float64 => SumKind::Float,
        _ => {
            return Err(EngineError::catalog_corrupt(
                "typed sum aggregate has a non-numeric argument",
            ));
        }
    };
    let output_type = kind.output_type();
    Ok(create_udaf(
        "elucid_checked_sum",
        vec![input_type.arrow_data_type()],
        Arc::new(output_type.clone()),
        Volatility::Immutable,
        Arc::new(move |_| Ok(Box::new(CheckedSumAccumulator::new(kind)))),
        Arc::new(vec![output_type]),
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SumKind {
    Signed,
    Unsigned,
    Float,
}

impl SumKind {
    const fn output_type(self) -> DataType {
        match self {
            Self::Signed => DataType::Int64,
            Self::Unsigned => DataType::UInt64,
            Self::Float => DataType::Float64,
        }
    }
}

#[derive(Debug)]
enum SumState {
    Signed(Option<i64>),
    Unsigned(Option<u64>),
    Float(Option<f64>),
}

#[derive(Debug)]
struct CheckedSumAccumulator {
    state: SumState,
}

impl CheckedSumAccumulator {
    const fn new(kind: SumKind) -> Self {
        let state = match kind {
            SumKind::Signed => SumState::Signed(None),
            SumKind::Unsigned => SumState::Unsigned(None),
            SumKind::Float => SumState::Float(None),
        };
        Self { state }
    }

    fn add_input(&mut self, value: ScalarValue) -> DataFusionResult<()> {
        if value.is_null() {
            return Ok(());
        }
        match (&mut self.state, value) {
            (SumState::Signed(sum), ScalarValue::Int32(Some(value))) => {
                add_signed(sum, i64::from(value))
            }
            (SumState::Signed(sum), ScalarValue::Int64(Some(value))) => add_signed(sum, value),
            (SumState::Unsigned(sum), ScalarValue::UInt32(Some(value))) => {
                add_unsigned(sum, u64::from(value))
            }
            (SumState::Unsigned(sum), ScalarValue::UInt64(Some(value))) => add_unsigned(sum, value),
            (SumState::Float(sum), ScalarValue::Float32(Some(value))) if value.is_finite() => {
                add_float(sum, f64::from(value))
            }
            (SumState::Float(sum), ScalarValue::Float64(Some(value))) if value.is_finite() => {
                add_float(sum, value)
            }
            _ => Err(invalid_aggregate_state()),
        }
    }

    fn scalar(&self) -> ScalarValue {
        match self.state {
            SumState::Signed(value) => ScalarValue::Int64(value),
            SumState::Unsigned(value) => ScalarValue::UInt64(value),
            SumState::Float(value) => ScalarValue::Float64(value),
        }
    }
}

impl Accumulator for CheckedSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        let [values] = values else {
            return Err(invalid_aggregate_state());
        };
        for index in 0..values.len() {
            self.add_input(ScalarValue::try_from_array(values, index)?)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        Ok(self.scalar())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok(vec![self.scalar()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        self.update_batch(states)
    }
}

fn add_signed(state: &mut Option<i64>, value: i64) -> DataFusionResult<()> {
    *state = Some(match *state {
        Some(current) => current
            .checked_add(value)
            .ok_or_else(|| runtime_error(RuntimeFailureKind::Evaluation))?,
        None => value,
    });
    Ok(())
}

fn add_unsigned(state: &mut Option<u64>, value: u64) -> DataFusionResult<()> {
    *state = Some(match *state {
        Some(current) => current
            .checked_add(value)
            .ok_or_else(|| runtime_error(RuntimeFailureKind::Evaluation))?,
        None => value,
    });
    Ok(())
}

fn add_float(state: &mut Option<f64>, value: f64) -> DataFusionResult<()> {
    let sum = state.map_or(value, |current| current + value);
    if !sum.is_finite() {
        return Err(runtime_error(RuntimeFailureKind::Evaluation));
    }
    *state = Some(sum);
    Ok(())
}

fn checked_average_udaf(
    input_type: LogicalType,
) -> Result<datafusion::logical_expr::AggregateUDF, EngineError> {
    if !matches!(
        input_type,
        LogicalType::Int32
            | LogicalType::Int64
            | LogicalType::UInt32
            | LogicalType::UInt64
            | LogicalType::Float32
            | LogicalType::Float64
    ) {
        return Err(EngineError::catalog_corrupt(
            "typed average aggregate has a non-numeric argument",
        ));
    }
    Ok(create_udaf(
        "elucid_checked_average",
        vec![input_type.arrow_data_type()],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(move |_| Ok(Box::new(CheckedAverageAccumulator::new(input_type)))),
        Arc::new(vec![DataType::Float64, DataType::UInt64]),
    ))
}

#[derive(Debug)]
struct CheckedAverageAccumulator {
    input_type: LogicalType,
    sum: Option<f64>,
    count: u64,
}

impl CheckedAverageAccumulator {
    const fn new(input_type: LogicalType) -> Self {
        Self {
            input_type,
            sum: None,
            count: 0,
        }
    }

    fn add_input(&mut self, value: ScalarValue) -> DataFusionResult<()> {
        if value.is_null() {
            return Ok(());
        }
        let value = match (self.input_type, value) {
            (LogicalType::Int32, ScalarValue::Int32(Some(value))) => f64::from(value),
            (LogicalType::Int64, ScalarValue::Int64(Some(value))) => value as f64,
            (LogicalType::UInt32, ScalarValue::UInt32(Some(value))) => f64::from(value),
            (LogicalType::UInt64, ScalarValue::UInt64(Some(value))) => value as f64,
            (LogicalType::Float32, ScalarValue::Float32(Some(value))) if value.is_finite() => {
                f64::from(value)
            }
            (LogicalType::Float64, ScalarValue::Float64(Some(value))) if value.is_finite() => value,
            _ => return Err(invalid_aggregate_state()),
        };
        add_float(&mut self.sum, value)?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| runtime_error(RuntimeFailureKind::Evaluation))?;
        Ok(())
    }
}

impl Accumulator for CheckedAverageAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        let [values] = values else {
            return Err(invalid_aggregate_state());
        };
        for index in 0..values.len() {
            self.add_input(ScalarValue::try_from_array(values, index)?)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        let Some(sum) = self.sum else {
            return Ok(ScalarValue::Float64(None));
        };
        if self.count == 0 {
            return Err(invalid_aggregate_state());
        }
        let average = sum / self.count as f64;
        average
            .is_finite()
            .then_some(ScalarValue::Float64(Some(average)))
            .ok_or_else(|| runtime_error(RuntimeFailureKind::Evaluation))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(self.sum),
            ScalarValue::UInt64(Some(self.count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        let [sums, counts] = states else {
            return Err(invalid_aggregate_state());
        };
        if sums.len() != counts.len() {
            return Err(invalid_aggregate_state());
        }
        for index in 0..sums.len() {
            let sum = ScalarValue::try_from_array(sums, index)?;
            let count = ScalarValue::try_from_array(counts, index)?;
            match (sum, count) {
                (ScalarValue::Float64(None), ScalarValue::UInt64(Some(0))) => {}
                (ScalarValue::Float64(Some(sum)), ScalarValue::UInt64(Some(count)))
                    if sum.is_finite() && count > 0 =>
                {
                    add_float(&mut self.sum, sum)?;
                    self.count = self
                        .count
                        .checked_add(count)
                        .ok_or_else(|| runtime_error(RuntimeFailureKind::Evaluation))?;
                }
                _ => return Err(invalid_aggregate_state()),
            }
        }
        Ok(())
    }
}

fn invalid_aggregate_state() -> DataFusionError {
    runtime_error(RuntimeFailureKind::Execution)
}
