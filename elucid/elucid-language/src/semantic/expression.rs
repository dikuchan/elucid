use crate::ast;
use crate::ir;

/// Converts an [`ast::Expr`] from the AST into an [`ir::Expr`].
///
/// This is a pure recursive transformation with no validation. Every AST
/// expression variant maps directly to an IR expression variant, so the
/// conversion is infallible.
pub(crate) fn convert_expression(expr: ast::Expression) -> ir::Expression {
    match expr {
        ast::Expression::Null => ir::Expression::Literal(ir::Literal::Null),
        ast::Expression::Boolean(b) => ir::Expression::Literal(ir::Literal::Boolean(b)),
        ast::Expression::Number(n) => ir::Expression::Literal(ir::Literal::Number(n)),
        ast::Expression::String(s) => ir::Expression::Literal(ir::Literal::String(s)),
        ast::Expression::Field(name) => ir::Expression::Field(ir::FieldRef::new(name)),
        ast::Expression::Binary(op, lhs, rhs) => ir::Expression::Binary(
            convert_binary_op(op),
            Box::new(convert_expression(*lhs)),
            Box::new(convert_expression(*rhs)),
        ),
        ast::Expression::Not(inner) => ir::Expression::Not(Box::new(convert_expression(*inner))),
        ast::Expression::Call(name, args) => {
            ir::Expression::Call(name, args.into_iter().map(convert_expression).collect())
        }
    }
}

/// Converts an [`ast::BinaryOp`] from the AST into an [`ir::BinaryOp`].
///
/// This is a 1:1 mapping — every AST operator has a corresponding IR operator.
fn convert_binary_op(op: ast::BinaryOperator) -> ir::BinaryOperator {
    match op {
        ast::BinaryOperator::Add => ir::BinaryOperator::Add,
        ast::BinaryOperator::Subtract => ir::BinaryOperator::Subtract,
        ast::BinaryOperator::Multiply => ir::BinaryOperator::Multiply,
        ast::BinaryOperator::Divide => ir::BinaryOperator::Divide,
        ast::BinaryOperator::Equal => ir::BinaryOperator::Equal,
        ast::BinaryOperator::NotEqual => ir::BinaryOperator::NotEqual,
        ast::BinaryOperator::GreaterThan => ir::BinaryOperator::GreaterThan,
        ast::BinaryOperator::GreaterThanOrEqual => ir::BinaryOperator::GreaterThanOrEqual,
        ast::BinaryOperator::LessThan => ir::BinaryOperator::LessThan,
        ast::BinaryOperator::LessThanOrEqual => ir::BinaryOperator::LessThanOrEqual,
        ast::BinaryOperator::And => ir::BinaryOperator::And,
        ast::BinaryOperator::Or => ir::BinaryOperator::Or,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Literal conversion tests ---

    #[test]
    fn convert_null() {
        let result = convert_expression(ast::Expression::Null);
        assert_eq!(result, ir::Expression::Literal(ir::Literal::Null));
    }

    #[test]
    fn convert_boolean_true() {
        let result = convert_expression(ast::Expression::Boolean(true));
        assert_eq!(result, ir::Expression::Literal(ir::Literal::Boolean(true)));
    }

    #[test]
    fn convert_boolean_false() {
        let result = convert_expression(ast::Expression::Boolean(false));
        assert_eq!(result, ir::Expression::Literal(ir::Literal::Boolean(false)));
    }

    #[test]
    fn convert_number() {
        let result = convert_expression(ast::Expression::Number(42.5));
        assert_eq!(result, ir::Expression::Literal(ir::Literal::Number(42.5)));
    }

    #[test]
    fn convert_number_zero() {
        let result = convert_expression(ast::Expression::Number(0.0));
        assert_eq!(result, ir::Expression::Literal(ir::Literal::Number(0.0)));
    }

    #[test]
    fn convert_string() {
        let result = convert_expression(ast::Expression::String("hello".to_owned()));
        assert_eq!(
            result,
            ir::Expression::Literal(ir::Literal::String("hello".to_owned()))
        );
    }

    #[test]
    fn convert_string_empty() {
        let result = convert_expression(ast::Expression::String(String::new()));
        assert_eq!(
            result,
            ir::Expression::Literal(ir::Literal::String(String::new()))
        );
    }

    // --- Field reference conversion ---

    #[test]
    fn convert_field() {
        let result = convert_expression(ast::Expression::Field("status".to_owned()));
        assert_eq!(
            result,
            ir::Expression::Field(ir::FieldRef::new("status".to_owned()))
        );
    }

    // --- Binary expression conversion ---

    #[test]
    fn convert_binary_add() {
        let expr = ast::Expression::Binary(
            ast::BinaryOperator::Add,
            Box::new(ast::Expression::Field("a".to_owned())),
            Box::new(ast::Expression::Number(1.0)),
        );
        let result = convert_expression(expr);
        assert_eq!(
            result,
            ir::Expression::Binary(
                ir::BinaryOperator::Add,
                Box::new(ir::Expression::Field(ir::FieldRef::new("a".to_owned()))),
                Box::new(ir::Expression::Literal(ir::Literal::Number(1.0))),
            )
        );
    }

    #[test]
    fn convert_binary_all_operators() {
        let operators = [
            (ast::BinaryOperator::Add, ir::BinaryOperator::Add),
            (ast::BinaryOperator::Subtract, ir::BinaryOperator::Subtract),
            (ast::BinaryOperator::Multiply, ir::BinaryOperator::Multiply),
            (ast::BinaryOperator::Divide, ir::BinaryOperator::Divide),
            (ast::BinaryOperator::Equal, ir::BinaryOperator::Equal),
            (ast::BinaryOperator::NotEqual, ir::BinaryOperator::NotEqual),
            (
                ast::BinaryOperator::GreaterThan,
                ir::BinaryOperator::GreaterThan,
            ),
            (
                ast::BinaryOperator::GreaterThanOrEqual,
                ir::BinaryOperator::GreaterThanOrEqual,
            ),
            (ast::BinaryOperator::LessThan, ir::BinaryOperator::LessThan),
            (
                ast::BinaryOperator::LessThanOrEqual,
                ir::BinaryOperator::LessThanOrEqual,
            ),
            (ast::BinaryOperator::And, ir::BinaryOperator::And),
            (ast::BinaryOperator::Or, ir::BinaryOperator::Or),
        ];

        for (ast_op, expected_ir_op) in operators {
            let expr = ast::Expression::Binary(
                ast_op,
                Box::new(ast::Expression::Number(1.0)),
                Box::new(ast::Expression::Number(2.0)),
            );
            let result = convert_expression(expr);
            if let ir::Expression::Binary(ir_op, _, _) = result {
                assert_eq!(ir_op, expected_ir_op);
            } else {
                panic!("expected Binary variant for operator {expected_ir_op:?}");
            }
        }
    }

    // --- Not expression conversion ---

    #[test]
    fn convert_not() {
        let expr = ast::Expression::Not(Box::new(ast::Expression::Boolean(true)));
        let result = convert_expression(expr);
        assert_eq!(
            result,
            ir::Expression::Not(Box::new(ir::Expression::Literal(ir::Literal::Boolean(
                true
            ))))
        );
    }

    // --- Call expression conversion ---

    #[test]
    fn convert_call_no_args() {
        let expr = ast::Expression::Call("count".to_owned(), vec![]);
        let result = convert_expression(expr);
        assert_eq!(result, ir::Expression::Call("count".to_owned(), vec![]));
    }

    #[test]
    fn convert_call_with_args() {
        let expr = ast::Expression::Call(
            "sum".to_owned(),
            vec![ast::Expression::Field("bytes".to_owned())],
        );
        let result = convert_expression(expr);
        assert_eq!(
            result,
            ir::Expression::Call(
                "sum".to_owned(),
                vec![ir::Expression::Field(ir::FieldRef::new("bytes".to_owned()))],
            )
        );
    }

    // --- Snapshot tests for non-trivial expressions ---

    /// Constructs the AST for: `a + b > 10`
    /// i.e. `(a + b) > 10`
    fn make_binary_tree_expr() -> ast::Expression {
        // (a + b) > 10
        ast::Expression::Binary(
            ast::BinaryOperator::GreaterThan,
            Box::new(ast::Expression::Binary(
                ast::BinaryOperator::Add,
                Box::new(ast::Expression::Field("a".to_owned())),
                Box::new(ast::Expression::Field("b".to_owned())),
            )),
            Box::new(ast::Expression::Number(10.0)),
        )
    }

    #[test]
    fn snapshot_binary_expression_tree() {
        let expr = make_binary_tree_expr();
        let result = convert_expression(expr);
        insta::assert_debug_snapshot!("binary_expression_tree", result);
    }

    /// Constructs the AST for: `not (a and b)`
    fn make_nested_not_expr() -> ast::Expression {
        ast::Expression::Not(Box::new(ast::Expression::Binary(
            ast::BinaryOperator::And,
            Box::new(ast::Expression::Field("a".to_owned())),
            Box::new(ast::Expression::Field("b".to_owned())),
        )))
    }

    #[test]
    fn snapshot_nested_not() {
        let expr = make_nested_not_expr();
        let result = convert_expression(expr);
        insta::assert_debug_snapshot!("nested_not", result);
    }

    /// Constructs the AST for: `count()` (zero-arg call)
    fn make_call_count_expr() -> ast::Expression {
        ast::Expression::Call("count".to_owned(), vec![])
    }

    #[test]
    fn snapshot_function_call_count() {
        let expr = make_call_count_expr();
        let result = convert_expression(expr);
        insta::assert_debug_snapshot!("function_call_count", result);
    }

    /// Constructs the AST for: `sum(bytes)` (call with arg)
    fn make_call_sum_expr() -> ast::Expression {
        ast::Expression::Call(
            "sum".to_owned(),
            vec![ast::Expression::Field("bytes".to_owned())],
        )
    }

    #[test]
    fn snapshot_function_call_sum() {
        let expr = make_call_sum_expr();
        let result = convert_expression(expr);
        insta::assert_debug_snapshot!("function_call_sum", result);
    }

    // --- Complex nested expression ---

    /// Constructs: `not ((a > 1) and (b < 10))`
    #[test]
    fn convert_deeply_nested_expression() {
        let expr = ast::Expression::Not(Box::new(ast::Expression::Binary(
            ast::BinaryOperator::And,
            Box::new(ast::Expression::Binary(
                ast::BinaryOperator::GreaterThan,
                Box::new(ast::Expression::Field("a".to_owned())),
                Box::new(ast::Expression::Number(1.0)),
            )),
            Box::new(ast::Expression::Binary(
                ast::BinaryOperator::LessThan,
                Box::new(ast::Expression::Field("b".to_owned())),
                Box::new(ast::Expression::Number(10.0)),
            )),
        )));

        let result = convert_expression(expr);

        // Verify the top-level is Not
        let ir::Expression::Not(inner) = result else {
            panic!("expected Not variant");
        };

        // Verify inner is And with two Binary children
        let ir::Expression::Binary(op, left, right) = *inner else {
            panic!("expected Binary variant inside Not");
        };
        assert_eq!(op, ir::BinaryOperator::And);

        // Left: a > 1
        if let ir::Expression::Binary(l_op, _, _) = *left {
            assert_eq!(l_op, ir::BinaryOperator::GreaterThan);
        } else {
            panic!("expected Binary (GreaterThan) on left");
        }

        // Right: b < 10
        if let ir::Expression::Binary(r_op, _, _) = *right {
            assert_eq!(r_op, ir::BinaryOperator::LessThan);
        } else {
            panic!("expected Binary (LessThan) on right");
        }
    }
}
