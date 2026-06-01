use std::sync::Arc;

use datafusion::common::ScalarValue::Null;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction};
use datafusion::logical_expr::{BinaryExpr, LogicalPlan, LogicalPlanBuilder, Operator, SortExpr};
use datafusion::prelude::*;
use elucid_language::ir;

const COUNT_FN: &str = "count";

pub struct QueryPlanner<'a> {
    context: &'a SessionContext,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        Self { context: ctx }
    }

    pub async fn create_logical_plan(&self, pipeline: ir::Pipeline) -> Result<LogicalPlan> {
        let (source, _time_range, stages) = pipeline.into_parts();
        // TODO: apply time_range as a filter predicate for Parquet partition pruning.
        let table_name = source.into_dataset();

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
            builder = self.apply_stage(builder, stage)?;
        }
        builder.build()
    }

    fn apply_stage(
        &self,
        builder: LogicalPlanBuilder,
        stage: ir::PipelineStage,
    ) -> Result<LogicalPlanBuilder> {
        match stage {
            ir::PipelineStage::Filter(expr) => {
                let expr = self.map_expression(expr)?;
                builder.filter(expr)
            }
            ir::PipelineStage::Sort(specs) => {
                let sort_exprs: Vec<SortExpr> = specs
                    .into_iter()
                    .map(|spec| {
                        let (expr, order) = spec.into_parts();
                        let expr = self.map_expression(expr)?;
                        let ascending = matches!(order, ir::SortOrder::Ascending);
                        Ok(expr.sort(ascending, false))
                    })
                    .collect::<Result<_>>()?;
                builder.sort(sort_exprs)
            }
            ir::PipelineStage::Limit(n) => builder.limit(0, Some(n)),
            ir::PipelineStage::Project(field_refs) => {
                let exprs: Vec<Expr> = field_refs.into_iter().map(|fr| col(fr.as_str())).collect();
                builder.project(exprs)
            }
            ir::PipelineStage::Aggregate { measures, group_by } => {
                let group_exprs: Vec<Expr> =
                    group_by.into_iter().map(|fr| col(fr.as_str())).collect();

                let mut agg_exprs = Vec::new();
                for measure in measures {
                    let (function_name, argument, alias) = measure.into_parts();
                    let arg_expr = match argument {
                        Some(expr) => self.map_expression(expr)?,
                        None => lit(1i64),
                    };

                    let agg_func = self.context.udaf(&function_name).map_err(|_| {
                        DataFusionError::Plan(format!(
                            "Aggregate function '{}' not found",
                            function_name
                        ))
                    })?;

                    let mut expr = Expr::AggregateFunction(AggregateFunction::new_udf(
                        agg_func,
                        vec![arg_expr],
                        false,
                        None,
                        Vec::new(),
                        None,
                    ));

                    if let Some(alias) = alias {
                        expr = expr.alias(alias);
                    }
                    agg_exprs.push(expr);
                }

                builder.aggregate(group_exprs, agg_exprs)
            }
            ir::PipelineStage::Search => Err(DataFusionError::Plan(
                "unsupported stage: Search".to_owned(),
            )),
            _ => Err(DataFusionError::Plan(format!(
                "unsupported pipeline stage: {:?}",
                stage
            ))),
        }
    }

    fn map_expression(&self, expression: ir::Expression) -> Result<Expr> {
        match expression {
            ir::Expression::Literal(value) => match value {
                ir::Literal::Null => Ok(lit(Null)),
                ir::Literal::Boolean(v) => Ok(lit(v)),
                ir::Literal::Number(v) => Ok(lit(v)),
                ir::Literal::String(v) => Ok(lit(v)),
                _ => Err(DataFusionError::Plan(format!(
                    "unsupported literal: {:?}",
                    value
                ))),
            },
            ir::Expression::Field(field_ref) => Ok(col(field_ref.as_str())),
            ir::Expression::Binary(op, left, right) => {
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
            ir::Expression::Not(expr) => Ok(Expr::Not(Box::new(self.map_expression(*expr)?))),
            ir::Expression::Call(name, args) => {
                let mut arg_exprs: Vec<Expr> = args
                    .into_iter()
                    .map(|arg| self.map_expression(arg))
                    .collect::<Result<Vec<_>>>()?;

                // count() with no args is equivalent to count(*).
                if name == COUNT_FN && arg_exprs.is_empty() {
                    arg_exprs.push(lit(1i64));
                }

                if let Ok(agg_func) = self.context.udaf(&name) {
                    return Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                        agg_func,
                        arg_exprs,
                        false,
                        None,
                        Vec::new(),
                        None,
                    )));
                }

                if let Ok(scalar_func) = self.context.udf(&name) {
                    return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                        scalar_func,
                        arg_exprs,
                    )));
                }

                Err(DataFusionError::Plan(format!(
                    "Function '{}' not found",
                    name
                )))
            }
            _ => Err(DataFusionError::Plan(format!(
                "unsupported expression: {:?}",
                expression
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
