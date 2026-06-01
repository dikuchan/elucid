use std::sync::Arc;

use datafusion::common::ScalarValue::Null;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction};
use datafusion::logical_expr::{BinaryExpr, LogicalPlan, LogicalPlanBuilder, Operator, SortExpr};
use datafusion::prelude::*;
use elucid_language::ast;

pub struct QueryPlanner<'a> {
    context: &'a SessionContext,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        Self { context: ctx }
    }

    pub async fn create_logical_plan(&self, query: ast::Query) -> Result<LogicalPlan> {
        let (source, commands) = query.into_parts();
        let table_provider = self
            .context
            .table_provider(&source)
            .await
            .map_err(|error| {
                DataFusionError::Plan(format!("Table '{}' not found: {}", source, error))
            })?;
        let table_source = DefaultTableSource::new(table_provider);

        let mut builder = LogicalPlanBuilder::scan(&source, Arc::new(table_source), None)?;
        for command in commands {
            builder = self.apply_command(builder, command)?;
        }
        builder.build()
    }

    fn apply_command(
        &self,
        builder: LogicalPlanBuilder,
        command: ast::Command,
    ) -> Result<LogicalPlanBuilder> {
        match command {
            ast::Command::Where(expression) => {
                let expression = self.map_expression(expression)?;
                builder.filter(expression)
            }
            ast::Command::Sort(sort_expressions) => {
                let sort_expressions: Vec<SortExpr> = sort_expressions
                    .into_iter()
                    .map(|sort_expression| {
                        let (expression, order) = sort_expression.into_parts();
                        let expression = self.map_expression(expression)?;
                        let ascending = match order {
                            ast::SortOrder::Ascending => true,
                            ast::SortOrder::Descending => false,
                            _ => true,
                        };
                        Ok(expression.sort(ascending, false))
                    })
                    .collect::<Result<_>>()?;
                builder.sort(sort_expressions)
            }
            ast::Command::Head(n) => builder.limit(0, Some(n as usize)),
            ast::Command::Fields(expressions) => {
                let expressions: Vec<Expr> = expressions
                    .into_iter()
                    .map(|e| self.map_expression(e))
                    .collect::<Result<_>>()?;
                builder.project(expressions)
            }
            ast::Command::Stats { aggregates, by } => {
                let group_expressions: Vec<Expr> = by
                    .into_iter()
                    .map(|expression| self.map_expression(expression))
                    .collect::<Result<_>>()?;

                let mut aggregate_expressions = Vec::new();
                for (expression, alias_option) in aggregates {
                    let mut expression = self.map_expression(expression)?;
                    if let Some(alias) = alias_option {
                        expression = expression.alias(alias);
                    }
                    aggregate_expressions.push(expression);
                }

                builder.aggregate(group_expressions, aggregate_expressions)
            }
            _ => Err(DataFusionError::Plan(format!(
                "unsupported command: {:?}",
                command
            ))),
        }
    }

    fn map_expression(&self, expression: ast::Expression) -> Result<Expr> {
        match expression {
            ast::Expression::Null => Ok(lit(Null)),
            ast::Expression::Boolean(v) => Ok(lit(v)),
            ast::Expression::Number(v) => Ok(lit(v)),
            ast::Expression::String(v) => Ok(lit(v)),
            ast::Expression::Field(v) => Ok(col(v)),
            ast::Expression::Binary(operator, left, right) => {
                let left = Box::new(self.map_expression(*left)?);
                let right = Box::new(self.map_expression(*right)?);
                match operator {
                    ast::BinaryOperator::And => Ok(left.and(*right)),
                    ast::BinaryOperator::Or => Ok(left.or(*right)),
                    _ => Ok(Expr::BinaryExpr(BinaryExpr {
                        left,
                        op: self.map_operator(operator)?,
                        right,
                    })),
                }
            }
            ast::Expression::Not(expr) => Ok(Expr::Not(Box::new(self.map_expression(*expr)?))),
            ast::Expression::Call(function_name, arguments) => {
                let mut arguments: Vec<Expr> = arguments
                    .into_iter()
                    .map(|argument| self.map_expression(argument))
                    .collect::<Result<Vec<_>>>()?;

                // Hack: count(1) is equivalent to count(*).
                if function_name == "count" && arguments.is_empty() {
                    arguments.push(lit(1i64));
                }

                if let Ok(aggregation_function) = self.context.udaf(&function_name) {
                    return Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                        aggregation_function,
                        arguments,
                        false,      // Distinct.
                        None,       // Filter.
                        Vec::new(), // Order by.
                        None,
                    )));
                }

                if let Some(function) = self.context.udf(&function_name).ok() {
                    return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                        function, arguments,
                    )));
                }
                Err(DataFusionError::Plan(format!(
                    "Function '{}' not found. It is not a registered UDF or built-in function",
                    function_name,
                )))
            }
            _ => Err(DataFusionError::Plan(format!(
                "unsupported expression: {:?}",
                expression
            ))),
        }
    }

    fn map_operator(&self, operator: ast::BinaryOperator) -> Result<Operator> {
        match operator {
            ast::BinaryOperator::Add => Ok(Operator::Plus),
            ast::BinaryOperator::Subtract => Ok(Operator::Minus),
            ast::BinaryOperator::Multiply => Ok(Operator::Multiply),
            ast::BinaryOperator::Divide => Ok(Operator::Divide),
            ast::BinaryOperator::Equal => Ok(Operator::Eq),
            ast::BinaryOperator::NotEqual => Ok(Operator::NotEq),
            ast::BinaryOperator::GreaterThan => Ok(Operator::Gt),
            ast::BinaryOperator::GreaterThanOrEqual => Ok(Operator::GtEq),
            ast::BinaryOperator::LessThan => Ok(Operator::Lt),
            ast::BinaryOperator::LessThanOrEqual => Ok(Operator::LtEq),
            ast::BinaryOperator::And => {
                unreachable!("Logical 'and' should be handled in expression builder")
            }
            ast::BinaryOperator::Or => {
                unreachable!("Logical 'or' should be handled in expression builder")
            }
            _ => Err(DataFusionError::Plan(format!(
                "unsupported operator: {:?}",
                operator
            ))),
        }
    }
}
