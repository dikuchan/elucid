use super::error::SemanticError;
use crate::ast;
use crate::ir::{AggregateExpr, FieldRef, PipelineStage, SortOrder, SortSpec};

use super::expression::convert_expression;

/// Converts an [`ast::Command`] from the AST into a [`PipelineStage`].
///
/// Performs structural validation (e.g. non-empty field lists, positive limits)
/// and delegates expression conversion to [`convert_expression`].
///
/// # Errors
///
/// Returns a [`SemanticError`] when the command fails structural validation.
pub(crate) fn convert_command(cmd: ast::Command) -> Result<PipelineStage, SemanticError> {
    match cmd {
        ast::Command::Where(expr) => Ok(PipelineStage::Filter(convert_expression(expr))),
        ast::Command::Sort(specs) => {
            if specs.is_empty() {
                return Err(SemanticError::EmptySortSpec);
            }
            let ir_specs = specs.into_iter().map(convert_sort_expr).collect();
            Ok(PipelineStage::Sort(ir_specs))
        }
        ast::Command::Head(n) => {
            if n <= 0 {
                return Err(SemanticError::InvalidLimitValue { value: n });
            }
            Ok(PipelineStage::Limit(n as usize))
        }
        ast::Command::Fields(exprs) => {
            if exprs.is_empty() {
                return Err(SemanticError::EmptyFieldList);
            }
            let fields = exprs
                .into_iter()
                .map(convert_field_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PipelineStage::Project(fields))
        }
        ast::Command::Stats { aggregates, by } => {
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

/// Converts an [`ast::SortExpression`] from the AST into a [`SortSpec`].
fn convert_sort_expr(spec: ast::SortExpression) -> SortSpec {
    let (expression, order) = spec.into_parts();
    let order = match order {
        ast::SortOrder::Ascending => SortOrder::Ascending,
        ast::SortOrder::Descending => SortOrder::Descending,
    };
    SortSpec::new(convert_expression(expression), order)
}

/// Extracts a field reference from a field-like [`ast::Expr`].
///
/// Only [`ast::Expr::Field`] is accepted; any other variant produces a
/// [`SemanticError::ConversionError`].
fn convert_field_expr(expr: ast::Expr) -> Result<FieldRef, SemanticError> {
    match expr {
        ast::Expr::Field(name) => Ok(FieldRef::new(name)),
        other => Err(SemanticError::ConversionError(format!(
            "expected field name, got {}",
            describe_expression_kind(&other)
        ))),
    }
}

/// Converts an aggregate expression pair into an [`AggregateExpr`].
///
/// The `expr` must be [`ast::Expr::Call`]; otherwise a
/// [`SemanticError::ConversionError`] is returned.
fn convert_aggregate_expr(
    expr: ast::Expr,
    alias: Option<String>,
) -> Result<AggregateExpr, SemanticError> {
    match expr {
        ast::Expr::Call(name, args) => {
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

/// Returns a human-readable label for an [`ast::Expr`] variant, used in
/// error messages.
fn describe_expression_kind(expr: &ast::Expr) -> &'static str {
    match expr {
        ast::Expr::Null => "null literal",
        ast::Expr::Boolean(_) => "boolean literal",
        ast::Expr::Number(_) => "number literal",
        ast::Expr::String(_) => "string literal",
        ast::Expr::Field(_) => "field reference",
        ast::Expr::Binary(_, _, _) => "binary expression",
        ast::Expr::Not(_) => "negation expression",
        ast::Expr::Call(_, _) => "function call",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{BinaryOp, Expr, FieldRef, Literal};

    // ── where command ──────────────────────────────────────────────────

    #[test]
    fn where_simple_comparison() {
        // where status == 200
        let cmd = ast::Command::Where(ast::Expr::Binary(
            ast::BinaryOp::Equal,
            Box::new(ast::Expr::Field("status".to_owned())),
            Box::new(ast::Expr::Number(200.0)),
        ));
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Filter(Expr::Binary(
                BinaryOp::Equal,
                Box::new(Expr::Field(FieldRef::new("status".to_owned()))),
                Box::new(Expr::Literal(Literal::Number(200.0))),
            ))
        );
    }

    #[test]
    fn where_field_only() {
        // where active
        let cmd = ast::Command::Where(ast::Expr::Field("active".to_owned()));
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Filter(Expr::Field(FieldRef::new("active".to_owned())))
        );
    }

    #[test]
    fn where_negation() {
        // where not error
        let cmd = ast::Command::Where(ast::Expr::Not(Box::new(ast::Expr::Field(
            "error".to_owned(),
        ))));
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Filter(Expr::Not(Box::new(Expr::Field(FieldRef::new(
                "error".to_owned()
            )))))
        );
    }

    // ── sort command ───────────────────────────────────────────────────

    #[test]
    fn sort_ascending() {
        // sort by +time
        let cmd = ast::Command::Sort(vec![ast::SortExpression::new(
            ast::Expr::Field("time".to_owned()),
            ast::SortOrder::Ascending,
        )]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Sort(vec![SortSpec::new(
                Expr::Field(FieldRef::new("time".to_owned())),
                SortOrder::Ascending,
            )])
        );
    }

    #[test]
    fn sort_descending() {
        // sort by -count
        let cmd = ast::Command::Sort(vec![ast::SortExpression::new(
            ast::Expr::Field("count".to_owned()),
            ast::SortOrder::Descending,
        )]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Sort(vec![SortSpec::new(
                Expr::Field(FieldRef::new("count".to_owned())),
                SortOrder::Descending,
            )])
        );
    }

    #[test]
    fn sort_multiple_specs() {
        // sort by -count, +status
        let cmd = ast::Command::Sort(vec![
            ast::SortExpression::new(
                ast::Expr::Field("count".to_owned()),
                ast::SortOrder::Descending,
            ),
            ast::SortExpression::new(
                ast::Expr::Field("status".to_owned()),
                ast::SortOrder::Ascending,
            ),
        ]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Sort(vec![
                SortSpec::new(
                    Expr::Field(FieldRef::new("count".to_owned())),
                    SortOrder::Descending,
                ),
                SortSpec::new(
                    Expr::Field(FieldRef::new("status".to_owned())),
                    SortOrder::Ascending,
                ),
            ])
        );
    }

    #[test]
    fn sort_empty_specs_error() {
        let cmd = ast::Command::Sort(vec![]);
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::EmptySortSpec);
    }

    // ── head command ───────────────────────────────────────────────────

    #[test]
    fn head_positive() {
        let cmd = ast::Command::Head(10);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(result, PipelineStage::Limit(10));
    }

    #[test]
    fn head_one() {
        let cmd = ast::Command::Head(1);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(result, PipelineStage::Limit(1));
    }

    #[test]
    fn head_zero_error() {
        let cmd = ast::Command::Head(0);
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::InvalidLimitValue { value: 0 });
    }

    #[test]
    fn head_negative_error() {
        let cmd = ast::Command::Head(-1);
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::InvalidLimitValue { value: -1 });
    }

    #[test]
    fn head_large_negative_error() {
        let cmd = ast::Command::Head(-999);
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::InvalidLimitValue { value: -999 });
    }

    // ── fields command ─────────────────────────────────────────────────

    #[test]
    fn fields_single() {
        let cmd = ast::Command::Fields(vec![ast::Expr::Field("name".to_owned())]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Project(vec![FieldRef::new("name".to_owned())])
        );
    }

    #[test]
    fn fields_multiple() {
        // fields name, age
        let cmd = ast::Command::Fields(vec![
            ast::Expr::Field("name".to_owned()),
            ast::Expr::Field("age".to_owned()),
        ]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Project(vec![
                FieldRef::new("name".to_owned()),
                FieldRef::new("age".to_owned()),
            ])
        );
    }

    #[test]
    fn fields_empty_error() {
        let cmd = ast::Command::Fields(vec![]);
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::EmptyFieldList);
    }

    #[test]
    fn fields_non_field_expression_error() {
        // fields 42
        let cmd = ast::Command::Fields(vec![ast::Expr::Number(42.0)]);
        let err = convert_command(cmd).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn fields_mixed_valid_and_invalid() {
        // fields name, 42
        let cmd = ast::Command::Fields(vec![
            ast::Expr::Field("name".to_owned()),
            ast::Expr::Number(42.0),
        ]);
        let err = convert_command(cmd).expect_err("should fail on second expr");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── stats command ──────────────────────────────────────────────────

    #[test]
    fn stats_count_no_args_no_group_by() {
        // stats count()
        let cmd = ast::Command::Stats {
            aggregates: vec![(ast::Expr::Call("count".to_owned(), vec![]), None)],
            by: vec![],
        };
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Aggregate {
                measures: vec![AggregateExpr::new("count".to_owned(), None, None,)],
                group_by: vec![],
            }
        );
    }

    #[test]
    fn stats_sum_with_alias_and_group_by() {
        // stats total = sum(bytes) by method
        let cmd = ast::Command::Stats {
            aggregates: vec![(
                ast::Expr::Call("sum".to_owned(), vec![ast::Expr::Field("bytes".to_owned())]),
                Some("total".to_owned()),
            )],
            by: vec![ast::Expr::Field("method".to_owned())],
        };
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            PipelineStage::Aggregate {
                measures: vec![AggregateExpr::new(
                    "sum".to_owned(),
                    Some(Expr::Field(FieldRef::new("bytes".to_owned()))),
                    Some("total".to_owned()),
                )],
                group_by: vec![FieldRef::new("method".to_owned())],
            }
        );
    }

    #[test]
    fn stats_multiple_aggregates_with_group_by() {
        // stats total = sum(bytes), count() by method
        let cmd = ast::Command::Stats {
            aggregates: vec![
                (
                    ast::Expr::Call("sum".to_owned(), vec![ast::Expr::Field("bytes".to_owned())]),
                    Some("total".to_owned()),
                ),
                (ast::Expr::Call("count".to_owned(), vec![]), None),
            ],
            by: vec![ast::Expr::Field("method".to_owned())],
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
        let cmd = ast::Command::Stats {
            aggregates: vec![],
            by: vec![],
        };
        let err = convert_command(cmd).expect_err("should fail");
        assert_eq!(err, SemanticError::EmptyAggregateMeasures);
    }

    #[test]
    fn stats_non_call_aggregate_error() {
        // stats 42
        let cmd = ast::Command::Stats {
            aggregates: vec![(ast::Expr::Number(42.0), None)],
            by: vec![],
        };
        let err = convert_command(cmd).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn stats_non_field_group_by_error() {
        // stats count() by 42
        let cmd = ast::Command::Stats {
            aggregates: vec![(ast::Expr::Call("count".to_owned(), vec![]), None)],
            by: vec![ast::Expr::Number(42.0)],
        };
        let err = convert_command(cmd).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── helper: convert_sort_expr ──────────────────────────────────────

    #[test]
    fn convert_sort_expr_ascending() {
        let spec = ast::SortExpression::new(
            ast::Expr::Field("time".to_owned()),
            ast::SortOrder::Ascending,
        );
        let result = convert_sort_expr(spec);
        assert_eq!(result.order(), SortOrder::Ascending);
    }

    #[test]
    fn convert_sort_expr_descending() {
        let spec = ast::SortExpression::new(
            ast::Expr::Field("count".to_owned()),
            ast::SortOrder::Descending,
        );
        let result = convert_sort_expr(spec);
        assert_eq!(result.order(), SortOrder::Descending);
    }

    // ── helper: convert_field_expr ─────────────────────────────────────

    #[test]
    fn convert_field_expr_valid() {
        let expr = ast::Expr::Field("status".to_owned());
        let result = convert_field_expr(expr).expect("should convert");
        assert_eq!(result, FieldRef::new("status".to_owned()));
    }

    #[test]
    fn convert_field_expr_number_error() {
        let expr = ast::Expr::Number(42.0);
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_string_literal_error() {
        let expr = ast::Expr::String("not_a_field".to_owned());
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_null_error() {
        let err = convert_field_expr(ast::Expr::Null).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_boolean_error() {
        let err = convert_field_expr(ast::Expr::Boolean(true)).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_binary_error() {
        let expr = ast::Expr::Binary(
            ast::BinaryOp::Add,
            Box::new(ast::Expr::Field("a".to_owned())),
            Box::new(ast::Expr::Field("b".to_owned())),
        );
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_not_error() {
        let expr = ast::Expr::Not(Box::new(ast::Expr::Field("a".to_owned())));
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_call_error() {
        let expr = ast::Expr::Call("count".to_owned(), vec![]);
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── helper: convert_aggregate_expr ─────────────────────────────────

    #[test]
    fn convert_aggregate_expr_call_no_args() {
        let expr = ast::Expr::Call("count".to_owned(), vec![]);
        let result = convert_aggregate_expr(expr, None).expect("should convert");
        assert_eq!(result.function(), "count");
        assert!(result.argument().is_none());
        assert!(result.alias().is_none());
    }

    #[test]
    fn convert_aggregate_expr_call_with_arg_and_alias() {
        let expr = ast::Expr::Call("sum".to_owned(), vec![ast::Expr::Field("bytes".to_owned())]);
        let result =
            convert_aggregate_expr(expr, Some("total".to_owned())).expect("should convert");
        assert_eq!(result.function(), "sum");
        assert!(result.argument().is_some());
        assert_eq!(result.alias(), Some("total"));
    }

    #[test]
    fn convert_aggregate_expr_non_call_error() {
        let expr = ast::Expr::Number(42.0);
        let err = convert_aggregate_expr(expr, None).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_aggregate_expr_field_error() {
        let expr = ast::Expr::Field("count".to_owned());
        let err = convert_aggregate_expr(expr, None).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── helper: describe_expression_kind ───────────────────────────────

    #[test]
    fn describe_all_expression_kinds() {
        assert_eq!(describe_expression_kind(&ast::Expr::Null), "null literal");
        assert_eq!(
            describe_expression_kind(&ast::Expr::Boolean(true)),
            "boolean literal"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expr::Number(1.0)),
            "number literal"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expr::String("x".to_owned())),
            "string literal"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expr::Field("x".to_owned())),
            "field reference"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expr::Binary(
                ast::BinaryOp::Add,
                Box::new(ast::Expr::Number(1.0)),
                Box::new(ast::Expr::Number(2.0)),
            )),
            "binary expression"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expr::Not(Box::new(ast::Expr::Null))),
            "negation expression"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expr::Call("f".to_owned(), vec![])),
            "function call"
        );
    }

    #[test]
    fn stats_call_with_multiple_args_error() {
        // stats sum(a, b) — two args, should fail
        let cmd = ast::Command::Stats {
            aggregates: vec![(
                ast::Expr::Call(
                    "sum".to_owned(),
                    vec![
                        ast::Expr::Field("a".to_owned()),
                        ast::Expr::Field("b".to_owned()),
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
        let cmd = ast::Command::Where(ast::Expr::Binary(
            ast::BinaryOp::NotEqual,
            Box::new(ast::Expr::Field("status".to_owned())),
            Box::new(ast::Expr::Number(404.0)),
        ));
        let result = convert_command(cmd).expect("should convert");
        insta::assert_debug_snapshot!("where_command", result);
    }

    #[test]
    fn snapshot_stats_command() {
        let cmd = ast::Command::Stats {
            aggregates: vec![
                (
                    ast::Expr::Call("sum".to_owned(), vec![ast::Expr::Field("bytes".to_owned())]),
                    Some("total_bytes".to_owned()),
                ),
                (ast::Expr::Call("count".to_owned(), vec![]), None),
            ],
            by: vec![ast::Expr::Field("method".to_owned())],
        };
        let result = convert_command(cmd).expect("should convert");
        insta::assert_debug_snapshot!("stats_command", result);
    }
}
