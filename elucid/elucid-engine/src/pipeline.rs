use std::sync::Arc;

use datafusion::common::{Column, DFSchema, ScalarValue, TableReference};
use datafusion::datasource::{TableProvider, provider_as_source};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator};
use datafusion::prelude::{lit, not};
use elucid_language::ir;

use crate::EngineError;
use crate::aggregate::aggregate_expression;
use crate::runtime::{
    cast_udf, checked_binary_udf, checked_negate_udf, null_scalar, remainder_udf,
};

pub(crate) fn lower_pipeline(
    pipeline: &ir::Pipeline,
    provider: Arc<dyn TableProvider>,
) -> Result<LogicalPlan, EngineError> {
    let mut builder = LogicalPlanBuilder::scan(
        TableReference::bare(pipeline.source().name()),
        provider_as_source(provider),
        None,
    )
    .map_err(EngineError::execution)?;
    validate_relation(builder.schema(), pipeline.source_relation())?;

    for stage in pipeline.stages() {
        builder = lower_stage(builder, stage)?;
        validate_relation(builder.schema(), stage.output_relation())?;
    }
    builder.build().map_err(EngineError::execution)
}

fn lower_stage(
    builder: LogicalPlanBuilder,
    stage: &ir::Stage,
) -> Result<LogicalPlanBuilder, EngineError> {
    match stage.kind() {
        ir::StageKind::Filter(expression) => builder
            .filter(lower_expression(expression)?)
            .map_err(EngineError::execution),
        ir::StageKind::Project(projections) => {
            let expressions = projections
                .iter()
                .map(|projection| {
                    lower_expression(projection.expression())
                        .map(|expression| expression.alias(projection.output_field().name()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            builder.project(expressions).map_err(EngineError::execution)
        }
        ir::StageKind::Aggregate { measures, group_by } => {
            let groups = group_by.iter().map(field_expression).collect::<Vec<_>>();
            let measures = measures
                .iter()
                .map(|measure| {
                    let (argument_type, argument) = match measure.argument() {
                        Some(field) => (Some(field.logical_type()), field_expression(field)),
                        None => (None, lit(1_i64)),
                    };
                    aggregate_expression(measure.function(), argument_type, argument)
                        .map(|expression| expression.alias(measure.output_field().name()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            builder
                .aggregate(groups, measures)
                .map_err(EngineError::execution)
        }
        ir::StageKind::Sort(specifications) => {
            let expressions = specifications
                .iter()
                .map(|specification| {
                    let ascending = matches!(specification.order(), ir::SortOrder::Ascending);
                    field_expression(specification.field()).sort(ascending, false)
                })
                .collect::<Vec<_>>();
            builder.sort(expressions).map_err(EngineError::execution)
        }
        ir::StageKind::Take(limit) => {
            let limit = usize::try_from(*limit).map_err(|_| {
                EngineError::execution_invariant("typed take limit does not fit this platform")
            })?;
            builder
                .limit(0, Some(limit))
                .map_err(EngineError::execution)
        }
        _ => Err(EngineError::catalog_corrupt(
            "typed query contains an unsupported pipeline stage",
        )),
    }
}

fn lower_expression(expression: &ir::Expression) -> Result<Expr, EngineError> {
    match expression.kind() {
        ir::ExpressionKind::Literal(literal) => literal_expression(literal),
        ir::ExpressionKind::Field(field) => Ok(field_expression(field)),
        ir::ExpressionKind::Unary { operator, operand } => match operator {
            ir::UnaryOperator::Not => Ok(not(lower_expression(operand)?)),
            ir::UnaryOperator::Negate => {
                Ok(checked_negate_udf(*operator, expression.logical_type())?
                    .call(vec![lower_expression(operand)?]))
            }
            _ => Err(EngineError::catalog_corrupt(
                "typed query contains an unsupported unary operator",
            )),
        },
        ir::ExpressionKind::Binary {
            operator,
            left,
            right,
        } => lower_binary(*operator, left, right, expression.logical_type()),
        ir::ExpressionKind::Cast {
            kind,
            expression: input,
            target,
        } => Ok(
            cast_udf(*kind, input.logical_type(), *target)?.call(vec![lower_expression(input)?])
        ),
        ir::ExpressionKind::Remainder {
            function,
            remainder,
            key,
        } => Ok(remainder_udf(*function, key.clone())?.call(vec![field_expression(remainder)])),
        ir::ExpressionKind::NullPredicate {
            expression,
            predicate,
        } => {
            let expression = lower_expression(expression)?;
            match predicate {
                ir::NullPredicate::IsNull => Ok(expression.is_null()),
                ir::NullPredicate::IsNotNull => Ok(expression.is_not_null()),
                _ => Err(EngineError::catalog_corrupt(
                    "typed query contains an unsupported null predicate",
                )),
            }
        }
        _ => Err(EngineError::catalog_corrupt(
            "typed query contains an unsupported expression",
        )),
    }
}

fn lower_binary(
    operator: ir::BinaryOperator,
    left: &ir::Expression,
    right: &ir::Expression,
    logical_type: elucid_catalog::LogicalType,
) -> Result<Expr, EngineError> {
    let left = lower_expression(left)?;
    let right = lower_expression(right)?;
    match operator {
        ir::BinaryOperator::Add
        | ir::BinaryOperator::Subtract
        | ir::BinaryOperator::Multiply
        | ir::BinaryOperator::Divide => {
            Ok(checked_binary_udf(operator, logical_type)?.call(vec![left, right]))
        }
        ir::BinaryOperator::And => Ok(left.and(right)),
        ir::BinaryOperator::Or => Ok(left.or(right)),
        ir::BinaryOperator::Equal
        | ir::BinaryOperator::NotEqual
        | ir::BinaryOperator::GreaterThan
        | ir::BinaryOperator::GreaterThanOrEqual
        | ir::BinaryOperator::LessThan
        | ir::BinaryOperator::LessThanOrEqual => Ok(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: comparison_operator(operator)?,
            right: Box::new(right),
        })),
        _ => Err(EngineError::catalog_corrupt(
            "typed query contains an unsupported binary operator",
        )),
    }
}

fn comparison_operator(operator: ir::BinaryOperator) -> Result<Operator, EngineError> {
    match operator {
        ir::BinaryOperator::Equal => Ok(Operator::Eq),
        ir::BinaryOperator::NotEqual => Ok(Operator::NotEq),
        ir::BinaryOperator::GreaterThan => Ok(Operator::Gt),
        ir::BinaryOperator::GreaterThanOrEqual => Ok(Operator::GtEq),
        ir::BinaryOperator::LessThan => Ok(Operator::Lt),
        ir::BinaryOperator::LessThanOrEqual => Ok(Operator::LtEq),
        _ => Err(EngineError::catalog_corrupt(
            "typed comparison expression has an invalid operator",
        )),
    }
}

fn literal_expression(literal: &ir::Literal) -> Result<Expr, EngineError> {
    let value = match literal {
        ir::Literal::Null(logical_type) => null_scalar(*logical_type)?,
        ir::Literal::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        ir::Literal::Int32(value) => ScalarValue::Int32(Some(*value)),
        ir::Literal::Int64(value) => ScalarValue::Int64(Some(*value)),
        ir::Literal::UInt32(value) => ScalarValue::UInt32(Some(*value)),
        ir::Literal::UInt64(value) => ScalarValue::UInt64(Some(*value)),
        ir::Literal::Float32(value) => ScalarValue::Float32(Some(*value)),
        ir::Literal::Float64(value) => ScalarValue::Float64(Some(*value)),
        ir::Literal::Utf8(value) => ScalarValue::Utf8(Some(value.clone())),
        ir::Literal::Datetime(value) => ScalarValue::TimestampMillisecond(
            Some(value.unix_milliseconds()),
            Some(Arc::from("UTC")),
        ),
        ir::Literal::Eid(value) => ScalarValue::FixedSizeBinary(16, Some(value.to_vec())),
        _ => {
            return Err(EngineError::catalog_corrupt(
                "typed query contains an unsupported literal",
            ));
        }
    };
    Ok(lit(value))
}

fn field_expression(field: &ir::Field) -> Expr {
    Expr::Column(Column::from_name(field.name()))
}

fn validate_relation(schema: &DFSchema, relation: &ir::Relation) -> Result<(), EngineError> {
    if schema.fields().len() != relation.fields().len() {
        return Err(EngineError::catalog_corrupt(
            "typed relation width contradicts its DataFusion plan",
        ));
    }
    for (planned, typed) in schema.fields().iter().zip(relation.fields()) {
        if planned.name() != typed.name()
            || planned.data_type() != &typed.logical_type().arrow_data_type()
        {
            return Err(EngineError::catalog_corrupt(
                "typed relation field contradicts its DataFusion plan",
            ));
        }
    }
    Ok(())
}
