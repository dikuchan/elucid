use super::error::SemanticError;
use crate::ast;
use crate::ir;

use super::expression::convert_expression;

/// Converts an [`ast::Command`] from the AST into an [`ir::PipelineStage`].
///
/// Performs structural validation (e.g. non-empty field lists, positive limits)
/// and delegates expression conversion to [`convert_expression`].
///
/// # Errors
///
/// Returns a [`SemanticError`] when the command fails structural validation.
pub(crate) fn convert_command(cmd: ast::Command) -> Result<ir::PipelineStage, SemanticError> {
    match cmd {
        ast::Command::Where(expr) => Ok(ir::PipelineStage::Filter(convert_expression(expr))),
        ast::Command::Sort(specs) => {
            if specs.is_empty() {
                return Err(SemanticError::EmptySortSpec);
            }
            let ir_specs = specs.into_iter().map(convert_sort_expr).collect();
            Ok(ir::PipelineStage::Sort(ir_specs))
        }
        ast::Command::Head(n) => {
            if n <= 0 {
                return Err(SemanticError::InvalidLimitValue { value: n });
            }
            Ok(ir::PipelineStage::Limit(n as usize))
        }
        ast::Command::Fields(exprs) => {
            if exprs.is_empty() {
                return Err(SemanticError::EmptyFieldList);
            }
            let fields = exprs
                .into_iter()
                .map(convert_field_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ir::PipelineStage::Project(fields))
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
            Ok(ir::PipelineStage::Aggregate { measures, group_by })
        }
    }
}

/// Converts an [`ast::SortExpression`] from the AST into an [`ir::SortSpec`].
fn convert_sort_expr(spec: ast::SortExpression) -> ir::SortSpec {
    let (expression, order) = spec.into_parts();
    let order = match order {
        ast::SortOrder::Ascending => ir::SortOrder::Ascending,
        ast::SortOrder::Descending => ir::SortOrder::Descending,
    };
    ir::SortSpec::new(convert_expression(expression), order)
}

/// Extracts a field reference from a field-like [`ast::Expr`].
///
/// Only [`ast::Expr::Field`] is accepted; any other variant produces a
/// [`SemanticError::ConversionError`].
fn convert_field_expr(expr: ast::Expression) -> Result<ir::FieldRef, SemanticError> {
    match expr {
        ast::Expression::Field(name) => Ok(ir::FieldRef::new(name)),
        other => Err(SemanticError::ConversionError(format!(
            "expected field name, got {}",
            describe_expression_kind(&other)
        ))),
    }
}

/// Converts an aggregate expression pair into an [`ir::AggregateExpr`].
///
/// The `expr` must be [`ast::Expr::Call`]; otherwise a
/// [`SemanticError::ConversionError`] is returned.
fn convert_aggregate_expr(
    expr: ast::Expression,
    alias: Option<String>,
) -> Result<ir::AggregateExpr, SemanticError> {
    match expr {
        ast::Expression::Call(name, args) => {
            if args.len() > 1 {
                return Err(SemanticError::ConversionError(format!(
                    "aggregate function '{name}' expects at most one argument, got {}",
                    args.len()
                )));
            }
            let argument = args.into_iter().next().map(convert_expression);
            Ok(ir::AggregateExpr::new(name, argument, alias))
        }
        other => Err(SemanticError::ConversionError(format!(
            "expected aggregate function call, got {}",
            describe_expression_kind(&other)
        ))),
    }
}

/// Returns a human-readable label for an [`ast::Expr`] variant, used in
/// error messages.
fn describe_expression_kind(expr: &ast::Expression) -> &'static str {
    match expr {
        ast::Expression::Null => "null literal",
        ast::Expression::Boolean(_) => "boolean literal",
        ast::Expression::Number(_) => "number literal",
        ast::Expression::String(_) => "string literal",
        ast::Expression::Field(_) => "field reference",
        ast::Expression::Binary(_, _, _) => "binary expression",
        ast::Expression::Not(_) => "negation expression",
        ast::Expression::Call(_, _) => "function call",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir;

    // ── where command ──────────────────────────────────────────────────

    #[test]
    fn where_simple_comparison() {
        // where status == 200
        let cmd = ast::Command::Where(ast::Expression::Binary(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Field("status".to_owned())),
            Box::new(ast::Expression::Number(200.0)),
        ));
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            ir::PipelineStage::Filter(ir::Expression::Binary(
                ir::BinaryOperator::Equal,
                Box::new(ir::Expression::Field(ir::FieldRef::new(
                    "status".to_owned()
                ))),
                Box::new(ir::Expression::Literal(ir::Literal::Number(200.0))),
            ))
        );
    }

    #[test]
    fn where_field_only() {
        // where active
        let cmd = ast::Command::Where(ast::Expression::Field("active".to_owned()));
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            ir::PipelineStage::Filter(ir::Expression::Field(ir::FieldRef::new(
                "active".to_owned()
            )))
        );
    }

    #[test]
    fn where_negation() {
        // where not error
        let cmd = ast::Command::Where(ast::Expression::Not(Box::new(ast::Expression::Field(
            "error".to_owned(),
        ))));
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            ir::PipelineStage::Filter(ir::Expression::Not(Box::new(ir::Expression::Field(
                ir::FieldRef::new("error".to_owned())
            ))))
        );
    }

    // ── sort command ───────────────────────────────────────────────────

    #[test]
    fn sort_ascending() {
        // sort by +time
        let cmd = ast::Command::Sort(vec![ast::SortExpression::new(
            ast::Expression::Field("time".to_owned()),
            ast::SortOrder::Ascending,
        )]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            ir::PipelineStage::Sort(vec![ir::SortSpec::new(
                ir::Expression::Field(ir::FieldRef::new("time".to_owned())),
                ir::SortOrder::Ascending,
            )])
        );
    }

    #[test]
    fn sort_descending() {
        // sort by -count
        let cmd = ast::Command::Sort(vec![ast::SortExpression::new(
            ast::Expression::Field("count".to_owned()),
            ast::SortOrder::Descending,
        )]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            ir::PipelineStage::Sort(vec![ir::SortSpec::new(
                ir::Expression::Field(ir::FieldRef::new("count".to_owned())),
                ir::SortOrder::Descending,
            )])
        );
    }

    #[test]
    fn sort_multiple_specs() {
        // sort by -count, +status
        let cmd = ast::Command::Sort(vec![
            ast::SortExpression::new(
                ast::Expression::Field("count".to_owned()),
                ast::SortOrder::Descending,
            ),
            ast::SortExpression::new(
                ast::Expression::Field("status".to_owned()),
                ast::SortOrder::Ascending,
            ),
        ]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            ir::PipelineStage::Sort(vec![
                ir::SortSpec::new(
                    ir::Expression::Field(ir::FieldRef::new("count".to_owned())),
                    ir::SortOrder::Descending,
                ),
                ir::SortSpec::new(
                    ir::Expression::Field(ir::FieldRef::new("status".to_owned())),
                    ir::SortOrder::Ascending,
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
        assert_eq!(result, ir::PipelineStage::Limit(10));
    }

    #[test]
    fn head_one() {
        let cmd = ast::Command::Head(1);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(result, ir::PipelineStage::Limit(1));
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
        let cmd = ast::Command::Fields(vec![ast::Expression::Field("name".to_owned())]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            ir::PipelineStage::Project(vec![ir::FieldRef::new("name".to_owned())])
        );
    }

    #[test]
    fn fields_multiple() {
        // fields name, age
        let cmd = ast::Command::Fields(vec![
            ast::Expression::Field("name".to_owned()),
            ast::Expression::Field("age".to_owned()),
        ]);
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            ir::PipelineStage::Project(vec![
                ir::FieldRef::new("name".to_owned()),
                ir::FieldRef::new("age".to_owned()),
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
        let cmd = ast::Command::Fields(vec![ast::Expression::Number(42.0)]);
        let err = convert_command(cmd).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn fields_mixed_valid_and_invalid() {
        // fields name, 42
        let cmd = ast::Command::Fields(vec![
            ast::Expression::Field("name".to_owned()),
            ast::Expression::Number(42.0),
        ]);
        let err = convert_command(cmd).expect_err("should fail on second expr");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── stats command ──────────────────────────────────────────────────

    #[test]
    fn stats_count_no_args_no_group_by() {
        // stats count()
        let cmd = ast::Command::Stats {
            aggregates: vec![(ast::Expression::Call("count".to_owned(), vec![]), None)],
            by: vec![],
        };
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            ir::PipelineStage::Aggregate {
                measures: vec![ir::AggregateExpr::new("count".to_owned(), None, None,)],
                group_by: vec![],
            }
        );
    }

    #[test]
    fn stats_sum_with_alias_and_group_by() {
        // stats total = sum(bytes) by method
        let cmd = ast::Command::Stats {
            aggregates: vec![(
                ast::Expression::Call(
                    "sum".to_owned(),
                    vec![ast::Expression::Field("bytes".to_owned())],
                ),
                Some("total".to_owned()),
            )],
            by: vec![ast::Expression::Field("method".to_owned())],
        };
        let result = convert_command(cmd).expect("should convert");
        assert_eq!(
            result,
            ir::PipelineStage::Aggregate {
                measures: vec![ir::AggregateExpr::new(
                    "sum".to_owned(),
                    Some(ir::Expression::Field(ir::FieldRef::new("bytes".to_owned()))),
                    Some("total".to_owned()),
                )],
                group_by: vec![ir::FieldRef::new("method".to_owned())],
            }
        );
    }

    #[test]
    fn stats_multiple_aggregates_with_group_by() {
        // stats total = sum(bytes), count() by method
        let cmd = ast::Command::Stats {
            aggregates: vec![
                (
                    ast::Expression::Call(
                        "sum".to_owned(),
                        vec![ast::Expression::Field("bytes".to_owned())],
                    ),
                    Some("total".to_owned()),
                ),
                (ast::Expression::Call("count".to_owned(), vec![]), None),
            ],
            by: vec![ast::Expression::Field("method".to_owned())],
        };
        let result = convert_command(cmd).expect("should convert");

        let ir::PipelineStage::Aggregate { measures, group_by } = result else {
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
            aggregates: vec![(ast::Expression::Number(42.0), None)],
            by: vec![],
        };
        let err = convert_command(cmd).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn stats_non_field_group_by_error() {
        // stats count() by 42
        let cmd = ast::Command::Stats {
            aggregates: vec![(ast::Expression::Call("count".to_owned(), vec![]), None)],
            by: vec![ast::Expression::Number(42.0)],
        };
        let err = convert_command(cmd).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── helper: convert_sort_expr ──────────────────────────────────────

    #[test]
    fn convert_sort_expr_ascending() {
        let spec = ast::SortExpression::new(
            ast::Expression::Field("time".to_owned()),
            ast::SortOrder::Ascending,
        );
        let result = convert_sort_expr(spec);
        assert_eq!(result.order(), ir::SortOrder::Ascending);
    }

    #[test]
    fn convert_sort_expr_descending() {
        let spec = ast::SortExpression::new(
            ast::Expression::Field("count".to_owned()),
            ast::SortOrder::Descending,
        );
        let result = convert_sort_expr(spec);
        assert_eq!(result.order(), ir::SortOrder::Descending);
    }

    // ── helper: convert_field_expr ─────────────────────────────────────

    #[test]
    fn convert_field_expr_valid() {
        let expr = ast::Expression::Field("status".to_owned());
        let result = convert_field_expr(expr).expect("should convert");
        assert_eq!(result, ir::FieldRef::new("status".to_owned()));
    }

    #[test]
    fn convert_field_expr_number_error() {
        let expr = ast::Expression::Number(42.0);
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_string_literal_error() {
        let expr = ast::Expression::String("not_a_field".to_owned());
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_null_error() {
        let err = convert_field_expr(ast::Expression::Null).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_boolean_error() {
        let err = convert_field_expr(ast::Expression::Boolean(true)).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_binary_error() {
        let expr = ast::Expression::Binary(
            ast::BinaryOperator::Add,
            Box::new(ast::Expression::Field("a".to_owned())),
            Box::new(ast::Expression::Field("b".to_owned())),
        );
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_not_error() {
        let expr = ast::Expression::Not(Box::new(ast::Expression::Field("a".to_owned())));
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_field_expr_call_error() {
        let expr = ast::Expression::Call("count".to_owned(), vec![]);
        let err = convert_field_expr(expr).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── helper: convert_aggregate_expr ─────────────────────────────────

    #[test]
    fn convert_aggregate_expr_call_no_args() {
        let expr = ast::Expression::Call("count".to_owned(), vec![]);
        let result = convert_aggregate_expr(expr, None).expect("should convert");
        assert_eq!(result.function(), "count");
        assert!(result.argument().is_none());
        assert!(result.alias().is_none());
    }

    #[test]
    fn convert_aggregate_expr_call_with_arg_and_alias() {
        let expr = ast::Expression::Call(
            "sum".to_owned(),
            vec![ast::Expression::Field("bytes".to_owned())],
        );
        let result =
            convert_aggregate_expr(expr, Some("total".to_owned())).expect("should convert");
        assert_eq!(result.function(), "sum");
        assert!(result.argument().is_some());
        assert_eq!(result.alias(), Some("total"));
    }

    #[test]
    fn convert_aggregate_expr_non_call_error() {
        let expr = ast::Expression::Number(42.0);
        let err = convert_aggregate_expr(expr, None).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    #[test]
    fn convert_aggregate_expr_field_error() {
        let expr = ast::Expression::Field("count".to_owned());
        let err = convert_aggregate_expr(expr, None).expect_err("should fail");
        assert!(matches!(err, SemanticError::ConversionError(_)));
    }

    // ── helper: describe_expression_kind ───────────────────────────────

    #[test]
    fn describe_all_expression_kinds() {
        assert_eq!(
            describe_expression_kind(&ast::Expression::Null),
            "null literal"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expression::Boolean(true)),
            "boolean literal"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expression::Number(1.0)),
            "number literal"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expression::String("x".to_owned())),
            "string literal"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expression::Field("x".to_owned())),
            "field reference"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expression::Binary(
                ast::BinaryOperator::Add,
                Box::new(ast::Expression::Number(1.0)),
                Box::new(ast::Expression::Number(2.0)),
            )),
            "binary expression"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expression::Not(Box::new(ast::Expression::Null))),
            "negation expression"
        );
        assert_eq!(
            describe_expression_kind(&ast::Expression::Call("f".to_owned(), vec![])),
            "function call"
        );
    }

    #[test]
    fn stats_call_with_multiple_args_error() {
        // stats sum(a, b) — two args, should fail
        let cmd = ast::Command::Stats {
            aggregates: vec![(
                ast::Expression::Call(
                    "sum".to_owned(),
                    vec![
                        ast::Expression::Field("a".to_owned()),
                        ast::Expression::Field("b".to_owned()),
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
        let cmd = ast::Command::Where(ast::Expression::Binary(
            ast::BinaryOperator::NotEqual,
            Box::new(ast::Expression::Field("status".to_owned())),
            Box::new(ast::Expression::Number(404.0)),
        ));
        let result = convert_command(cmd).expect("should convert");
        insta::assert_debug_snapshot!("where_command", result);
    }

    #[test]
    fn snapshot_stats_command() {
        let cmd = ast::Command::Stats {
            aggregates: vec![
                (
                    ast::Expression::Call(
                        "sum".to_owned(),
                        vec![ast::Expression::Field("bytes".to_owned())],
                    ),
                    Some("total_bytes".to_owned()),
                ),
                (ast::Expression::Call("count".to_owned(), vec![]), None),
            ],
            by: vec![ast::Expression::Field("method".to_owned())],
        };
        let result = convert_command(cmd).expect("should convert");
        insta::assert_debug_snapshot!("stats_command", result);
    }
}
