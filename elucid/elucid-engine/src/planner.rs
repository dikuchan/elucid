use std::sync::Arc;

use datafusion::common::ScalarValue::Null;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{BinaryExpr, LogicalPlan, LogicalPlanBuilder, Operator, SortExpr};
use datafusion::prelude::*;
use elucid_language::ir;

pub struct QueryPlanner<'a> {
    context: &'a SessionContext,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        Self { context: ctx }
    }

    pub async fn create_logical_plan(&self, pipeline: ir::Pipeline) -> Result<LogicalPlan> {
        let (source, _time_range, _source_relation, stages) = pipeline.into_parts();
        // TODO: apply time_range as a filter predicate for Parquet partition pruning.
        let table_name = source.name().to_owned();

        let table_provider = self
            .context
            .table_provider(&table_name)
            .await
            .map_err(|error| {
                DataFusionError::Plan(format!("Table '{}' not found: {}", table_name, error))
            })?;
        let table_source = DefaultTableSource::new(table_provider);

        let mut builder = LogicalPlanBuilder::scan(&table_name, Arc::new(table_source), None)?;
        for stage in stages {
            let (kind, _output_relation) = stage.into_parts();
            builder = self.apply_stage(builder, kind)?;
        }
        builder.build()
    }

    fn apply_stage(
        &self,
        builder: LogicalPlanBuilder,
        stage: ir::StageKind,
    ) -> Result<LogicalPlanBuilder> {
        match stage {
            ir::StageKind::Filter(expr) => {
                let expr = self.map_expression(expr)?;
                builder.filter(expr)
            }
            ir::StageKind::Sort(specs) => {
                let sort_exprs: Vec<SortExpr> = specs
                    .into_iter()
                    .map(|spec| {
                        let (field, order) = spec.into_parts();
                        let expr = col(field.name());
                        let ascending = matches!(order, ir::SortOrder::Ascending);
                        Ok(expr.sort(ascending, false))
                    })
                    .collect::<Result<_>>()?;
                builder.sort(sort_exprs)
            }
            ir::StageKind::Take(n) => {
                let limit = usize::try_from(n).map_err(|_| {
                    DataFusionError::Plan("take value exceeds the platform limit".to_owned())
                })?;
                builder.limit(0, Some(limit))
            }
            ir::StageKind::Project(projections) => {
                let exprs = projections
                    .into_iter()
                    .map(|projection| {
                        let (expression, output_field) = projection.into_parts();
                        self.map_expression(expression)
                            .map(|expression| expression.alias(output_field.name()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                builder.project(exprs)
            }
            ir::StageKind::Aggregate { measures, group_by } => {
                let group_exprs: Vec<Expr> = group_by
                    .into_iter()
                    .map(|field| col(field.name()))
                    .collect();

                let mut agg_exprs = Vec::new();
                for measure in measures {
                    let (function, argument, output_field) = measure.into_parts();
                    let arg_expr = match argument {
                        Some(field) => col(field.name()),
                        None => lit(1i64),
                    };

                    let agg_func = self.context.udaf(function.as_str()).map_err(|_| {
                        DataFusionError::Plan(format!(
                            "Aggregate function '{}' not found",
                            function
                        ))
                    })?;

                    let expr = Expr::AggregateFunction(AggregateFunction::new_udf(
                        agg_func,
                        vec![arg_expr],
                        false,
                        None,
                        Vec::new(),
                        None,
                    ))
                    .alias(output_field.name());
                    agg_exprs.push(expr);
                }

                builder.aggregate(group_exprs, agg_exprs)
            }
            _ => Err(DataFusionError::Plan(format!(
                "unsupported pipeline stage: {:?}",
                stage
            ))),
        }
    }

    fn map_expression(&self, expression: ir::Expression) -> Result<Expr> {
        let (kind, _logical_type, _nullability) = expression.into_parts();
        match kind {
            ir::ExpressionKind::Literal(value) => match value {
                ir::Literal::Null(_) => Ok(lit(Null)),
                ir::Literal::Boolean(v) => Ok(lit(v)),
                ir::Literal::Int32(v) => Ok(lit(v)),
                ir::Literal::Int64(v) => Ok(lit(v)),
                ir::Literal::UInt32(v) => Ok(lit(v)),
                ir::Literal::UInt64(v) => Ok(lit(v)),
                ir::Literal::Float32(v) => Ok(lit(v)),
                ir::Literal::Float64(v) => Ok(lit(v)),
                ir::Literal::Utf8(v) => Ok(lit(v)),
                _ => Err(DataFusionError::Plan(format!(
                    "unsupported literal: {:?}",
                    value
                ))),
            },
            ir::ExpressionKind::Field(field) => match field.origin() {
                ir::FieldOrigin::Remainder { .. } => Err(DataFusionError::Plan(
                    "remainder fields are lowered in the snapshot query milestone".to_owned(),
                )),
                _ => Ok(col(field.name())),
            },
            ir::ExpressionKind::Binary {
                operator: op,
                left,
                right,
            } => {
                let left = self.map_expression(*left)?;
                let right = self.map_expression(*right)?;
                match op {
                    ir::BinaryOperator::And => Ok(left.and(right)),
                    ir::BinaryOperator::Or => Ok(left.or(right)),
                    _ => Ok(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op: self.map_operator(op)?,
                        right: Box::new(right),
                    })),
                }
            }
            ir::ExpressionKind::Unary {
                operator: ir::UnaryOperator::Not,
                operand,
            } => Ok(Expr::Not(Box::new(self.map_expression(*operand)?))),
            ir::ExpressionKind::NullPredicate {
                expression,
                predicate,
            } => {
                let expression = self.map_expression(*expression)?;
                match predicate {
                    ir::NullPredicate::IsNull => Ok(expression.is_null()),
                    ir::NullPredicate::IsNotNull => Ok(expression.is_not_null()),
                    _ => Err(DataFusionError::Plan(
                        "unsupported null predicate".to_owned(),
                    )),
                }
            }
            _ => Err(DataFusionError::Plan(format!(
                "unsupported expression: {:?}",
                kind
            ))),
        }
    }

    fn map_operator(&self, operator: ir::BinaryOperator) -> Result<Operator> {
        match operator {
            ir::BinaryOperator::Add => Ok(Operator::Plus),
            ir::BinaryOperator::Subtract => Ok(Operator::Minus),
            ir::BinaryOperator::Multiply => Ok(Operator::Multiply),
            ir::BinaryOperator::Divide => Ok(Operator::Divide),
            ir::BinaryOperator::Equal => Ok(Operator::Eq),
            ir::BinaryOperator::NotEqual => Ok(Operator::NotEq),
            ir::BinaryOperator::GreaterThan => Ok(Operator::Gt),
            ir::BinaryOperator::GreaterThanOrEqual => Ok(Operator::GtEq),
            ir::BinaryOperator::LessThan => Ok(Operator::Lt),
            ir::BinaryOperator::LessThanOrEqual => Ok(Operator::LtEq),
            ir::BinaryOperator::And => Err(DataFusionError::Plan(
                "logical 'and' should be handled in expression mapper".to_owned(),
            )),
            ir::BinaryOperator::Or => Err(DataFusionError::Plan(
                "logical 'or' should be handled in expression mapper".to_owned(),
            )),
            _ => Err(DataFusionError::Plan(format!(
                "unsupported operator: {:?}",
                operator
            ))),
        }
    }
}
