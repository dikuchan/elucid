use std::sync::Arc;

use datafusion::common::ScalarValue::Null;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction};
use datafusion::logical_expr::{BinaryExpr, LogicalPlan, LogicalPlanBuilder, Operator, SortExpr};
use datafusion::prelude::*;
use elucid_language::{BinaryOperator, Command, Expression, Query, SortOrder};

pub struct QueryPlanner<'a> {
    context: &'a SessionContext,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        Self { context: ctx }
    }

    pub async fn create_logical_plan(&self, query: Query) -> Result<LogicalPlan> {
        let table_provider = self
            .context
            .table_provider(&query.source)
            .await
            .map_err(|error| {
                DataFusionError::Plan(format!("Table '{}' not found: {}", query.source, error))
            })?;
        let table_source = DefaultTableSource::new(table_provider);

        let mut builder = LogicalPlanBuilder::scan(&query.source, Arc::new(table_source), None)?;
        for command in query.commands {
            builder = self.apply_command(builder, command)?;
        }
        builder.build()
    }

    fn apply_command(
        &self,
        builder: LogicalPlanBuilder,
        command: Command,
    ) -> Result<LogicalPlanBuilder> {
        match command {
            Command::Where(expression) => {
                let expression = self.map_expression(expression)?;
                builder.filter(expression)
            }
            Command::Sort(sort_expressions) => {
                let sort_expressions: Vec<SortExpr> = sort_expressions
                    .into_iter()
                    .map(|sort_expression| {
                        let expression = self.map_expression(sort_expression.expression)?;
                        let ascending = match sort_expression.order {
                            SortOrder::Ascending => true,
                            SortOrder::Descending => false,
                        };
                        Ok(expression.sort(ascending, false))
                    })
                    .collect::<Result<_>>()?;
                builder.sort(sort_expressions)
            }
            Command::Head(n) => builder.limit(0, Some(n as usize)),
            Command::Aggregate { aggregates, by } => {
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
        }
    }

    fn map_expression(&self, expression: Expression) -> Result<Expr> {
        match expression {
            Expression::Null => Ok(lit(Null)),
            Expression::Boolean(v) => Ok(lit(v)),
            Expression::Number(v) => Ok(lit(v)),
            Expression::String(v) => Ok(lit(v)),
            Expression::Field(v) => Ok(col(v)),
            Expression::Binary(operator, left, right) => {
                let left = Box::new(self.map_expression(*left)?);
                let right = Box::new(self.map_expression(*right)?);
                match operator {
                    BinaryOperator::And => Ok(left.and(*right)),
                    BinaryOperator::Or => Ok(left.or(*right)),
                    _ => Ok(Expr::BinaryExpr(BinaryExpr {
                        left,
                        op: self.map_operator(operator)?,
                        right,
                    })),
                }
            }
            Expression::Call(function_name, arguments) => {
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
        }
    }

    fn map_operator(&self, operator: BinaryOperator) -> Result<Operator> {
        match operator {
            BinaryOperator::Add => Ok(Operator::Plus),
            BinaryOperator::Subtract => Ok(Operator::Minus),
            BinaryOperator::Multiply => Ok(Operator::Multiply),
            BinaryOperator::Divide => Ok(Operator::Divide),
            BinaryOperator::Equal => Ok(Operator::Eq),
            BinaryOperator::NotEqual => Ok(Operator::NotEq),
            BinaryOperator::GreaterThan => Ok(Operator::Gt),
            BinaryOperator::GreaterThanOrEqual => Ok(Operator::GtEq),
            BinaryOperator::LessThan => Ok(Operator::Lt),
            BinaryOperator::LessThanOrEqual => Ok(Operator::LtEq),
            BinaryOperator::And => {
                unreachable!("Logical 'and' should be handled in expression builder")
            }
            BinaryOperator::Or => {
                unreachable!("Logical 'or' should be handled in expression builder")
            }
        }
    }
}
