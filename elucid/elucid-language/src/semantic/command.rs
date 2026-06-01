use crate::ast::{Command, Expression, SortExpression, SortOrder};
use crate::ir::{AggregateExpr, IrFieldRef, PipelineStage, SortSpec};
use super::error::SemanticError;

use super::expression::convert_expression;

/// Converts an [`Command`] from the AST into a [`PipelineStage`].
///
/// Performs structural validation (e.g. non-empty field lists, positive limits)
/// and delegates expression conversion to [`convert_expression`].
///
/// # Errors
///
/// Returns a [`SemanticError`] when the command fails structural validation.
pub(crate) fn convert_command(cmd: Command) -> Result<PipelineStage, SemanticError> {
    match cmd {
        Command::Where(expr) => Ok(PipelineStage::Filter(convert_expression(expr))),
        Command::Sort(specs) => {
            if specs.is_empty() {
                return Err(SemanticError::EmptySortSpec);
            }
            let ir_specs = specs.into_iter().map(convert_sort_expr).collect();
            Ok(PipelineStage::Sort(ir_specs))
        }
        Command::Head(n) => {
            if n <= 0 {
                return Err(SemanticError::InvalidLimitValue { value: n });
            }
            Ok(PipelineStage::Limit(n as usize))
        }
        Command::Fields(exprs) => {
            if exprs.is_empty() {
                return Err(SemanticError::EmptyFieldList);
            }
            let fields = exprs
                .into_iter()
                .map(convert_field_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PipelineStage::Project(fields))
        }
        Command::Stats { aggregates, by } => {
            if aggregates.is_empty() {
                return Err(SemanticError::EmptyAggregateMeasures);
            }
            let measures = aggregates
                .into_iter()
                .map(|(expr, alias)| convert_aggregate_expr(expr, alias))
                .collect::<Result<Vec<_>, _>>()?;
            let group_by = by
                .into_iter()
                .map(convert_field_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PipelineStage::Aggregate { measures, group_by })
        }
    }
}

/// Converts a [`SortExpression`] from the AST into a [`SortSpec`].
fn convert_sort_expr(spec: SortExpression) -> SortSpec {
    let descending = matches!(spec.order, SortOrder::Descending);
    SortSpec::new(convert_expression(spec.expression), descending)
}

/// Extracts a field reference from a field-like [`Expression`].
///
/// Only [`Expression::Field`] is accepted; any other variant produces a
/// [`SemanticError::ConversionError`].
fn convert_field_expr(expr: Expression) -> Result<IrFieldRef, SemanticError> {
    match expr {
        Expression::Field(name) => Ok(IrFieldRef::new(name)),
        other => Err(SemanticError::ConversionError(format!(
            "expected field name, got {}",
            describe_expression_kind(&other)
        ))),
    }
}

/// Converts an aggregate expression pair into an [`AggregateExpr`].
///
/// The `expr` must be [`Expression::Call`]; otherwise a
/// [`SemanticError::ConversionError`] is returned.
fn convert_aggregate_expr(
    expr: Expression,
    alias: Option<String>,
) -> Result<AggregateExpr, SemanticError> {
    match expr {
        Expression::Call(name, args) => {
            if args.len() > 1 {
                return Err(SemanticError::ConversionError(format!(
                    "aggregate function '{name}' expects at most one argument, got {}",
                    args.len()
                )));
            }
            let argument = args.into_iter().next().map(convert_expression);
            Ok(AggregateExpr::new(name, argument, alias))
        }
        other => Err(SemanticError::ConversionError(format!(
            "expected aggregate function call, got {}",
            describe_expression_kind(&other)
        ))),
    }
}

/// Returns a human-readable label for an [`Expression`] variant, used in
/// error messages.
fn describe_expression_kind(expr: &Expression) -> &'static str {
    match expr {
        Expression::Null => "null literal",
        Expression::Boolean(_) => "boolean literal",
        Expression::Number(_) => "number literal",
        Expression::String(_) => "string literal",
        Expression::Field(_) => "field reference",
        Expression::Binary(_, _, _) => "binary expression",
        Expression::Not(_) => "negation expression",
        Expression::Call(_, _) => "function call",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BinaryOperator, SortOrder};
    use crate::ir::{IrBinaryOp, IrExpr, IrFieldRef, IrLiteral};

    // ── where command ──────────────────────────────────────────────────

    #[test]
    fn where_simple_comparison() {
        // where status == 200
        let cmd = Command::Where(Expression::Binary(
            BinaryOperator::Equal,
            Box::new(Expression::Field("status".to_owned())),
            Box::new(Expression::Number(200.0)),
        ));
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Filter(IrExpr::Binary(
                IrBinaryOp::Equal,
                Box::new(IrExpr::Field(IrFieldRef::new("status".to_owned()))),
                Box::new(IrExpr::Literal(IrLiteral::Number(200.0))),
            ))
        );
    }

    #[test]
    fn where_field_only() {
        // where active
        let cmd = Command::Where(Expression::Field("active".to_owned()));
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Filter(IrExpr::Field(IrFieldRef::new("active".to_owned())))
        );
    }

    #[test]
    fn where_negation() {
        // where not error
        let cmd = Command::Where(Expression::Not(Box::new(Expression::Field(
            "error".to_owned(),
        ))));
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Filter(IrExpr::Not(Box::new(IrExpr::Field(
                IrFieldRef::new("error".to_owned())
            ))))
        );
    }

    // ── sort command ───────────────────────────────────────────────────

    #[test]
    fn sort_ascending() {
        // sort by +time
        let cmd = Command::Sort(vec![SortExpression {
            expression: Expression::Field("time".to_owned()),
            order: SortOrder::Ascending,
        }]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Sort(vec![SortSpec::new(
                IrExpr::Field(IrFieldRef::new("time".to_owned())),
                false,
            )])
        );
    }

    #[test]
    fn sort_descending() {
        // sort by -count
        let cmd = Command::Sort(vec![SortExpression {
            expression: Expression::Field("count".to_owned()),
            order: SortOrder::Descending,
        }]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Sort(vec![SortSpec::new(
                IrExpr::Field(IrFieldRef::new("count".to_owned())),
                true,
            )])
        );
    }

    #[test]
    fn sort_multiple_specs() {
        // sort by -count, +status
        let cmd = Command::Sort(vec![
            SortExpression {
                expression: Expression::Field("count".to_owned()),
                order: SortOrder::Descending,
            },
            SortExpression {
                expression: Expression::Field("status".to_owned()),
                order: SortOrder::Ascending,
            },
        ]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Sort(vec![
                SortSpec::new(
                    IrExpr::Field(IrFieldRef::new("count".to_owned())),
                    true,
                ),
                SortSpec::new(
                    IrExpr::Field(IrFieldRef::new("status".to_owned())),
                    false,
                ),
            ])
        );
    }

    #[test]
    fn sort_empty_specs_error() {
        let cmd = Command::Sort(vec![]);
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::EmptySortSpec);
    }

    // ── head command ───────────────────────────────────────────────────

    #[test]
    fn head_positive() {
        let cmd = Command::Head(10);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(result, PipelineStage::Limit(10));
    }

    #[test]
    fn head_one() {
        let cmd = Command::Head(1);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(result, PipelineStage::Limit(1));
    }

    #[test]
    fn head_zero_error() {
        let cmd = Command::Head(0);
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::InvalidLimitValue { value: 0 });
    }

    #[test]
    fn head_negative_error() {
        let cmd = Command::Head(-1);
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::InvalidLimitValue { value: -1 });
    }

    #[test]
    fn head_large_negative_error() {
        let cmd = Command::Head(-999);
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::InvalidLimitValue { value: -999 });
    }

    // ── fields command ─────────────────────────────────────────────────

    #[test]
    fn fields_single() {
        let cmd = Command::Fields(vec![Expression::Field("name".to_owned())]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Project(vec![IrFieldRef::new("name".to_owned())])
        );
    }

    #[test]
    fn fields_multiple() {
        // fields name, age
        let cmd = Command::Fields(vec![
            Expression::Field("name".to_owned()),
            Expression::Field("age".to_owned()),
        ]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Project(vec![
                IrFieldRef::new("name".to_owned()),
                IrFieldRef::new("age".to_owned()),
            ])
        );
    }

    #[test]
    fn fields_empty_error() {
        let cmd = Command::Fields(vec![]);
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::EmptyFieldList);
    }

    #[test]
    fn fields_non_field_expression_error() {
        // fields 42
        let cmd = Command::Fields(vec![Expression::Number(42.0)]);
        let err = convert_command(cmd).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn fields_mixed_valid_and_invalid() {
        // fields name, 42
        let cmd = Command::Fields(vec![
            Expression::Field("name".to_owned()),
            Expression::Number(42.0),
        ]);
        let err = convert_command(cmd).expect_err("should fail on second expr");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── stats command ──────────────────────────────────────────────────

    #[test]
    fn stats_count_no_args_no_group_by() {
        // stats count()
        let cmd = Command::Stats {
            aggregates: vec![(Expression::Call("count".to_owned(), vec![]), None)],
            by: vec![],
        };
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Aggregate {
                measures: vec![AggregateExpr::new(
                    "count".to_owned(),
                    None,
                    None,
                )],
                group_by: vec![],
            }
        );
    }

    #[test]
    fn stats_sum_with_alias_and_group_by() {
        // stats total = sum(bytes) by method
        let cmd = Command::Stats {
            aggregates: vec![(
                Expression::Call(
                    "sum".to_owned(),
                    vec![Expression::Field("bytes".to_owned())],
                ),
                Some("total".to_owned()),
            )],
            by: vec![Expression::Field("method".to_owned())],
        };
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Aggregate {
                measures: vec![AggregateExpr::new(
                    "sum".to_owned(),
                    Some(IrExpr::Field(IrFieldRef::new("bytes".to_owned()))),
                    Some("total".to_owned()),
                )],
                group_by: vec![IrFieldRef::new("method".to_owned())],
            }
        );
    }

    #[test]
    fn stats_multiple_aggregates_with_group_by() {
        // stats total = sum(bytes), count() by method
        let cmd = Command::Stats {
            aggregates: vec![
                (
                    Expression::Call(
                        "sum".to_owned(),
                        vec![Expression::Field("bytes".to_owned())],
                    ),
                    Some("total".to_owned()),
                ),
                (Expression::Call("count".to_owned(), vec![]), None),
            ],
            by: vec![Expression::Field("method".to_owned())],
        };
        let result = convert_command(cmd).expect("should convert");

        let PipelineStage::Aggregate { measures, group_by } = result else {
            panic!("expected Aggregate stage");
        };
        assert_eq!(measures.len(), 2);
        assert_eq!(group_by.len(), 1);
        assert_eq!(group_by[0].as_str(), "method");
    }

    #[test]
    fn stats_empty_aggregates_error() {
        let cmd = Command::Stats {
            aggregates: vec![],
            by: vec![],
        };
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::EmptyAggregateMeasures);
    }

    #[test]
    fn stats_non_call_aggregate_error() {
        // stats 42
        let cmd = Command::Stats {
            aggregates: vec![(Expression::Number(42.0), None)],
            by: vec![],
        };
        let err = convert_command(cmd).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn stats_non_field_group_by_error() {
        // stats count() by 42
        let cmd = Command::Stats {
            aggregates: vec![(Expression::Call("count".to_owned(), vec![]), None)],
            by: vec![Expression::Number(42.0)],
        };
        let err = convert_command(cmd).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── helper: convert_sort_expr ──────────────────────────────────────

    #[test]
    fn convert_sort_expr_ascending() {
        let spec = SortExpression {
            expression: Expression::Field("time".to_owned()),
            order: SortOrder::Ascending,
        };
        let result = convert_sort_expr(spec);
        assert!(!result.is_descending());
    }

    #[test]
    fn convert_sort_expr_descending() {
        let spec = SortExpression {
            expression: Expression::Field("count".to_owned()),
            order: SortOrder::Descending,
        };
        let result = convert_sort_expr(spec);
        assert!(result.is_descending());
    }

    // ── helper: convert_field_expr ─────────────────────────────────────

    #[test]
    fn convert_field_expr_valid() {
        let expr = Expression::Field("status".to_owned());
        let result = convert_field_expr(expr).expect("should convert");
        assert_eq!(result, IrFieldRef::new("status".to_owned()));
    }

    #[test]
    fn convert_field_expr_number_error() {
        let expr = Expression::Number(42.0);
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_string_literal_error() {
        let expr = Expression::String("not_a_field".to_owned());
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_null_error() {
        let err = convert_field_expr(Expression::Null).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_boolean_error() {
        let err = convert_field_expr(Expression::Boolean(true)).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_binary_error() {
        let expr = Expression::Binary(
            BinaryOperator::Add,
            Box::new(Expression::Field("a".to_owned())),
            Box::new(Expression::Field("b".to_owned())),
        );
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_not_error() {
        let expr = Expression::Not(Box::new(Expression::Field("a".to_owned())));
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_call_error() {
        let expr = Expression::Call("count".to_owned(), vec![]);
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── helper: convert_aggregate_expr ─────────────────────────────────

    #[test]
    fn convert_aggregate_expr_call_no_args() {
        let expr = Expression::Call("count".to_owned(), vec![]);
        let result = convert_aggregate_expr(expr, None).expect("should convert");
        assert_eq!(result.function(), "count");
        assert!(result.argument().is_none());
        assert!(result.alias().is_none());
    }

    #[test]
    fn convert_aggregate_expr_call_with_arg_and_alias() {
        let expr = Expression::Call(
            "sum".to_owned(),
            vec![Expression::Field("bytes".to_owned())],
        );
        let result = convert_aggregate_expr(expr, Some("total".to_owned()))
            .expect("should convert");
        assert_eq!(result.function(), "sum");
        assert!(result.argument().is_some());
        assert_eq!(result.alias(), Some("total"));
    }

    #[test]
    fn convert_aggregate_expr_non_call_error() {
        let expr = Expression::Number(42.0);
        let err = convert_aggregate_expr(expr, None).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_aggregate_expr_field_error() {
        let expr = Expression::Field("count".to_owned());
        let err = convert_aggregate_expr(expr, None).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── helper: describe_expression_kind ───────────────────────────────

    #[test]
    fn describe_all_expression_kinds() {
        assert_eq!(
            describe_expression_kind(&Expression::Null),
            "null literal"
        );
        assert_eq!(
            describe_expression_kind(&Expression::Boolean(true)),
            "boolean literal"
        );
        assert_eq!(
            describe_expression_kind(&Expression::Number(1.0)),
            "number literal"
        );
        assert_eq!(
            describe_expression_kind(&Expression::String("x".to_owned())),
            "string literal"
        );
        assert_eq!(
            describe_expression_kind(&Expression::Field("x".to_owned())),
            "field reference"
        );
        assert_eq!(
            describe_expression_kind(&Expression::Binary(
                BinaryOperator::Add,
                Box::new(Expression::Number(1.0)),
                Box::new(Expression::Number(2.0)),
            )),
            "binary expression"
        );
        assert_eq!(
            describe_expression_kind(&Expression::Not(Box::new(Expression::Null))),
            "negation expression"
        );
        assert_eq!(
            describe_expression_kind(&Expression::Call("f".to_owned(), vec![])),
            "function call"
        );
    }

    #[test]
    fn stats_call_with_multiple_args_error() {
        // stats sum(a, b) — two args, should fail
        let cmd = Command::Stats {
            aggregates: vec![(
                Expression::Call(
                    "sum".to_owned(),
                    vec![
                        Expression::Field("a".to_owned()),
                        Expression::Field("b".to_owned()),
                    ],
                ),
                None,
            )],
            by: vec![],
        };
        let result = convert_command(cmd);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, SemanticError::ConversionError(ref msg) if msg.contains("at most one argument")),
            "expected ConversionError mentioning 'at most one argument', got: {err:?}"
        );
    }

    // ── snapshot tests ─────────────────────────────────────────────────

    #[test]
    fn snapshot_where_command() {
        let cmd = Command::Where(Expression::Binary(
            BinaryOperator::NotEqual,
            Box::new(Expression::Field("status".to_owned())),
            Box::new(Expression::Number(404.0)),
        ));
        let result = convert_command(cmd).expect("should convert");
        insta::assert_debug_snapshot!("where_command", result);
    }

    #[test]
    fn snapshot_stats_command() {
        let cmd = Command::Stats {
            aggregates: vec![
                (
                    Expression::Call(
                        "sum".to_owned(),
                        vec![Expression::Field("bytes".to_owned())],
                    ),
                    Some("total_bytes".to_owned()),
                ),
                (Expression::Call("count".to_owned(), vec![]), None),
            ],
            by: vec![Expression::Field("method".to_owned())],
        };
        let result = convert_command(cmd).expect("should convert");
        insta::assert_debug_snapshot!("stats_command", result);
    }
}
