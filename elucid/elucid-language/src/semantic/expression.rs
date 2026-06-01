use crate::ast;
use crate::ir;

/// Converts an [`ast::Expr`] from the AST into an [`ir::Expr`].
///
/// This is a pure recursive transformation with no validation. Every AST
/// expression variant maps directly to an IR expression variant, so the
/// conversion is infallible.
pub(crate) fn convert_expression(expr: ast::Expr) -> ir::Expr {
    match expr {
        ast::Expr::Null => ir::Expr::Literal(ir::Literal::Null),
        ast::Expr::Boolean(b) => ir::Expr::Literal(ir::Literal::Boolean(b)),
        ast::Expr::Number(n) => ir::Expr::Literal(ir::Literal::Number(n)),
        ast::Expr::String(s) => ir::Expr::Literal(ir::Literal::String(s)),
        ast::Expr::Field(name) => ir::Expr::Field(ir::FieldRef::new(name)),
        ast::Expr::Binary(op, lhs, rhs) => ir::Expr::Binary(
            convert_binary_op(op),
            Box::new(convert_expression(*lhs)),
            Box::new(convert_expression(*rhs)),
        ),
        ast::Expr::Not(inner) => ir::Expr::Not(Box::new(convert_expression(*inner))),
        ast::Expr::Call(name, args) => {
            ir::Expr::Call(name, args.into_iter().map(convert_expression).collect())
        }
    }
}

/// Converts an [`ast::BinaryOp`] from the AST into an [`ir::BinaryOp`].
///
/// This is a 1:1 mapping — every AST operator has a corresponding IR operator.
fn convert_binary_op(op: ast::BinaryOp) -> ir::BinaryOp {
    match op {
        ast::BinaryOp::Add => ir::BinaryOp::Add,
        ast::BinaryOp::Subtract => ir::BinaryOp::Subtract,
        ast::BinaryOp::Multiply => ir::BinaryOp::Multiply,
        ast::BinaryOp::Divide => ir::BinaryOp::Divide,
        ast::BinaryOp::Equal => ir::BinaryOp::Equal,
        ast::BinaryOp::NotEqual => ir::BinaryOp::NotEqual,
        ast::BinaryOp::GreaterThan => ir::BinaryOp::GreaterThan,
        ast::BinaryOp::GreaterThanOrEqual => ir::BinaryOp::GreaterThanOrEqual,
        ast::BinaryOp::LessThan => ir::BinaryOp::LessThan,
        ast::BinaryOp::LessThanOrEqual => ir::BinaryOp::LessThanOrEqual,
        ast::BinaryOp::And => ir::BinaryOp::And,
        ast::BinaryOp::Or => ir::BinaryOp::Or,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Literal conversion tests ---

    #[test]
    fn convert_null() {
        let result = convert_expression(ast::Expr::Null);
        assert_eq!(result, ir::Expr::Literal(ir::Literal::Null));
    }

    #[test]
    fn convert_boolean_true() {
        let result = convert_expression(ast::Expr::Boolean(true));
        assert_eq!(result, ir::Expr::Literal(ir::Literal::Boolean(true)));
    }

    #[test]
    fn convert_boolean_false() {
        let result = convert_expression(ast::Expr::Boolean(false));
        assert_eq!(result, ir::Expr::Literal(ir::Literal::Boolean(false)));
    }

    #[test]
    fn convert_number() {
        let result = convert_expression(ast::Expr::Number(42.5));
        assert_eq!(result, ir::Expr::Literal(ir::Literal::Number(42.5)));
    }

    #[test]
    fn convert_number_zero() {
        let result = convert_expression(ast::Expr::Number(0.0));
        assert_eq!(result, ir::Expr::Literal(ir::Literal::Number(0.0)));
    }

    #[test]
    fn convert_string() {
        let result = convert_expression(ast::Expr::String("hello".to_owned()));
        assert_eq!(result, ir::Expr::Literal(ir::Literal::String("hello".to_owned())));
    }

    #[test]
    fn convert_string_empty() {
        let result = convert_expression(ast::Expr::String(String::new()));
        assert_eq!(result, ir::Expr::Literal(ir::Literal::String(String::new())));
    }

    // --- Field reference conversion ---

    #[test]
    fn convert_field() {
        let result = convert_expression(ast::Expr::Field("status".to_owned()));
        assert_eq!(result, ir::Expr::Field(ir::FieldRef::new("status".to_owned())));
    }

    // --- Binary expression conversion ---

    #[test]
    fn convert_binary_add() {
        let expr = ast::Expr::Binary(
            ast::BinaryOp::Add,
            Box::new(ast::Expr::Field("a".to_owned())),
            Box::new(ast::Expr::Number(1.0)),
        );
        let result = convert_expression(expr);
        assert_eq!(
            result,
            ir::Expr::Binary(
                ir::BinaryOp::Add,
                Box::new(ir::Expr::Field(ir::FieldRef::new("a".to_owned()))),
                Box::new(ir::Expr::Literal(ir::Literal::Number(1.0))),
            )
        );
    }

    #[test]
    fn convert_binary_all_operators() {
        let operators = [
            (ast::BinaryOp::Add, ir::BinaryOp::Add),
            (ast::BinaryOp::Subtract, ir::BinaryOp::Subtract),
            (ast::BinaryOp::Multiply, ir::BinaryOp::Multiply),
            (ast::BinaryOp::Divide, ir::BinaryOp::Divide),
            (ast::BinaryOp::Equal, ir::BinaryOp::Equal),
            (ast::BinaryOp::NotEqual, ir::BinaryOp::NotEqual),
            (ast::BinaryOp::GreaterThan, ir::BinaryOp::GreaterThan),
            (
                ast::BinaryOp::GreaterThanOrEqual,
                ir::BinaryOp::GreaterThanOrEqual,
            ),
            (ast::BinaryOp::LessThan, ir::BinaryOp::LessThan),
            (ast::BinaryOp::LessThanOrEqual, ir::BinaryOp::LessThanOrEqual),
            (ast::BinaryOp::And, ir::BinaryOp::And),
            (ast::BinaryOp::Or, ir::BinaryOp::Or),
        ];

        for (ast_op, expected_ir_op) in operators {
            let expr = ast::Expr::Binary(
                ast_op,
                Box::new(ast::Expr::Number(1.0)),
                Box::new(ast::Expr::Number(2.0)),
            );
            let result = convert_expression(expr);
            if let ir::Expr::Binary(ir_op, _, _) = result {
                assert_eq!(ir_op, expected_ir_op);
            } else {
                panic!("expected Binary variant for operator {expected_ir_op:?}");
            }
        }
    }

    // --- Not expression conversion ---

    #[test]
    fn convert_not() {
        let expr = ast::Expr::Not(Box::new(ast::Expr::Boolean(true)));
        let result = convert_expression(expr);
        assert_eq!(
            result,
            ir::Expr::Not(Box::new(ir::Expr::Literal(ir::Literal::Boolean(true))))
        );
    }

    // --- Call expression conversion ---

    #[test]
    fn convert_call_no_args() {
        let expr = ast::Expr::Call("count".to_owned(), vec![]);
        let result = convert_expression(expr);
        assert_eq!(result, ir::Expr::Call("count".to_owned(), vec![]));
    }

    #[test]
    fn convert_call_with_args() {
        let expr = ast::Expr::Call("sum".to_owned(), vec![ast::Expr::Field("bytes".to_owned())]);
        let result = convert_expression(expr);
        assert_eq!(
            result,
            ir::Expr::Call(
                "sum".to_owned(),
                vec![ir::Expr::Field(ir::FieldRef::new("bytes".to_owned()))],
            )
        );
    }

    // --- Snapshot tests for non-trivial expressions ---

    /// Constructs the AST for: `a + b > 10`
    /// i.e. `(a + b) > 10`
    fn make_binary_tree_expr() -> ast::Expr {
        // (a + b) > 10
        ast::Expr::Binary(
            ast::BinaryOp::GreaterThan,
            Box::new(ast::Expr::Binary(
                ast::BinaryOp::Add,
                Box::new(ast::Expr::Field("a".to_owned())),
                Box::new(ast::Expr::Field("b".to_owned())),
            )),
            Box::new(ast::Expr::Number(10.0)),
        )
    }

    #[test]
    fn snapshot_binary_expression_tree() {
        let expr = make_binary_tree_expr();
        let result = convert_expression(expr);
        insta::assert_debug_snapshot!("binary_expression_tree", result);
    }

    /// Constructs the AST for: `not (a and b)`
    fn make_nested_not_expr() -> ast::Expr {
        ast::Expr::Not(Box::new(ast::Expr::Binary(
            ast::BinaryOp::And,
            Box::new(ast::Expr::Field("a".to_owned())),
            Box::new(ast::Expr::Field("b".to_owned())),
        )))
    }

    #[test]
    fn snapshot_nested_not() {
        let expr = make_nested_not_expr();
        let result = convert_expression(expr);
        insta::assert_debug_snapshot!("nested_not", result);
    }

    /// Constructs the AST for: `count()` (zero-arg call)
    fn make_call_count_expr() -> ast::Expr {
        ast::Expr::Call("count".to_owned(), vec![])
    }

    #[test]
    fn snapshot_function_call_count() {
        let expr = make_call_count_expr();
        let result = convert_expression(expr);
        insta::assert_debug_snapshot!("function_call_count", result);
    }

    /// Constructs the AST for: `sum(bytes)` (call with arg)
    fn make_call_sum_expr() -> ast::Expr {
        ast::Expr::Call("sum".to_owned(), vec![ast::Expr::Field("bytes".to_owned())])
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
        let expr = ast::Expr::Not(Box::new(ast::Expr::Binary(
            ast::BinaryOp::And,
            Box::new(ast::Expr::Binary(
                ast::BinaryOp::GreaterThan,
                Box::new(ast::Expr::Field("a".to_owned())),
                Box::new(ast::Expr::Number(1.0)),
            )),
            Box::new(ast::Expr::Binary(
                ast::BinaryOp::LessThan,
                Box::new(ast::Expr::Field("b".to_owned())),
                Box::new(ast::Expr::Number(10.0)),
            )),
        )));

        let result = convert_expression(expr);

        // Verify the top-level is Not
        let ir::Expr::Not(inner) = result else {
            panic!("expected Not variant");
        };

        // Verify inner is And with two Binary children
        let ir::Expr::Binary(op, left, right) = *inner else {
            panic!("expected Binary variant inside Not");
        };
        assert_eq!(op, ir::BinaryOp::And);

        // Left: a > 1
        if let ir::Expr::Binary(l_op, _, _) = *left {
            assert_eq!(l_op, ir::BinaryOp::GreaterThan);
        } else {
            panic!("expected Binary (GreaterThan) on left");
        }

        // Right: b < 10
        if let ir::Expr::Binary(r_op, _, _) = *right {
            assert_eq!(r_op, ir::BinaryOp::LessThan);
        } else {
            panic!("expected Binary (LessThan) on right");
        }
    }
}
