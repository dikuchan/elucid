use crate::ast::{BinaryOperator, Expression};
use crate::ir::{IrBinaryOp, IrExpr, IrFieldRef, IrLiteral};

/// Converts an [`Expression`] from the AST into an [`IrExpr`].
///
/// This is a pure recursive transformation with no validation. Every AST
/// expression variant maps directly to an IR expression variant, so the
/// conversion is infallible.
pub(crate) fn convert_expression(expr: Expression) -> IrExpr {
    match expr {
        Expression::Null => IrExpr::Literal(IrLiteral::Null),
        Expression::Boolean(b) => IrExpr::Literal(IrLiteral::Boolean(b)),
        Expression::Number(n) => IrExpr::Literal(IrLiteral::Number(n)),
        Expression::String(s) => IrExpr::Literal(IrLiteral::String(s)),
        Expression::Field(name) => IrExpr::Field(IrFieldRef::new(name)),
        Expression::Binary(op, lhs, rhs) => IrExpr::Binary(
            convert_binary_op(op),
            Box::new(convert_expression(*lhs)),
            Box::new(convert_expression(*rhs)),
        ),
        Expression::Not(inner) => IrExpr::Not(Box::new(convert_expression(*inner))),
        Expression::Call(name, args) => IrExpr::Call(
            name,
            args.into_iter().map(convert_expression).collect(),
        ),
    }
}

/// Converts an [`BinaryOperator`] from the AST into an [`IrBinaryOp`].
///
/// This is a 1:1 mapping — every AST operator has a corresponding IR operator.
fn convert_binary_op(op: BinaryOperator) -> IrBinaryOp {
    match op {
        BinaryOperator::Add => IrBinaryOp::Add,
        BinaryOperator::Subtract => IrBinaryOp::Subtract,
        BinaryOperator::Multiply => IrBinaryOp::Multiply,
        BinaryOperator::Divide => IrBinaryOp::Divide,
        BinaryOperator::Equal => IrBinaryOp::Equal,
        BinaryOperator::NotEqual => IrBinaryOp::NotEqual,
        BinaryOperator::GreaterThan => IrBinaryOp::GreaterThan,
        BinaryOperator::GreaterThanOrEqual => IrBinaryOp::GreaterThanOrEqual,
        BinaryOperator::LessThan => IrBinaryOp::LessThan,
        BinaryOperator::LessThanOrEqual => IrBinaryOp::LessThanOrEqual,
        BinaryOperator::And => IrBinaryOp::And,
        BinaryOperator::Or => IrBinaryOp::Or,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Literal conversion tests ---

    #[test]
    fn convert_null() {
        let result = convert_expression(Expression::Null);
        assert_eq!(result, IrExpr::Literal(IrLiteral::Null));
    }

    #[test]
    fn convert_boolean_true() {
        let result = convert_expression(Expression::Boolean(true));
        assert_eq!(result, IrExpr::Literal(IrLiteral::Boolean(true)));
    }

    #[test]
    fn convert_boolean_false() {
        let result = convert_expression(Expression::Boolean(false));
        assert_eq!(result, IrExpr::Literal(IrLiteral::Boolean(false)));
    }

    #[test]
    fn convert_number() {
        let result = convert_expression(Expression::Number(42.5));
        assert_eq!(result, IrExpr::Literal(IrLiteral::Number(42.5)));
    }

    #[test]
    fn convert_number_zero() {
        let result = convert_expression(Expression::Number(0.0));
        assert_eq!(result, IrExpr::Literal(IrLiteral::Number(0.0)));
    }

    #[test]
    fn convert_string() {
        let result = convert_expression(Expression::String("hello".to_owned()));
        assert_eq!(
            result,
            IrExpr::Literal(IrLiteral::String("hello".to_owned()))
        );
    }

    #[test]
    fn convert_string_empty() {
        let result = convert_expression(Expression::String(String::new()));
        assert_eq!(
            result,
            IrExpr::Literal(IrLiteral::String(String::new()))
        );
    }

    // --- Field reference conversion ---

    #[test]
    fn convert_field() {
        let result = convert_expression(Expression::Field("status".to_owned()));
        assert_eq!(
            result,
            IrExpr::Field(IrFieldRef::new("status".to_owned()))
        );
    }

    // --- Binary expression conversion ---

    #[test]
    fn convert_binary_add() {
        let expr = Expression::Binary(
            BinaryOperator::Add,
            Box::new(Expression::Field("a".to_owned())),
            Box::new(Expression::Number(1.0)),
        );
        let result = convert_expression(expr);
        assert_eq!(
            result,
            IrExpr::Binary(
                IrBinaryOp::Add,
                Box::new(IrExpr::Field(IrFieldRef::new("a".to_owned()))),
                Box::new(IrExpr::Literal(IrLiteral::Number(1.0))),
            )
        );
    }

    #[test]
    fn convert_binary_all_operators() {
        let operators = [
            (BinaryOperator::Add, IrBinaryOp::Add),
            (BinaryOperator::Subtract, IrBinaryOp::Subtract),
            (BinaryOperator::Multiply, IrBinaryOp::Multiply),
            (BinaryOperator::Divide, IrBinaryOp::Divide),
            (BinaryOperator::Equal, IrBinaryOp::Equal),
            (BinaryOperator::NotEqual, IrBinaryOp::NotEqual),
            (BinaryOperator::GreaterThan, IrBinaryOp::GreaterThan),
            (BinaryOperator::GreaterThanOrEqual, IrBinaryOp::GreaterThanOrEqual),
            (BinaryOperator::LessThan, IrBinaryOp::LessThan),
            (BinaryOperator::LessThanOrEqual, IrBinaryOp::LessThanOrEqual),
            (BinaryOperator::And, IrBinaryOp::And),
            (BinaryOperator::Or, IrBinaryOp::Or),
        ];

        for (ast_op, expected_ir_op) in operators {
            let expr = Expression::Binary(
                ast_op,
                Box::new(Expression::Number(1.0)),
                Box::new(Expression::Number(2.0)),
            );
            let result = convert_expression(expr);
            if let IrExpr::Binary(ir_op, _, _) = result {
                assert_eq!(ir_op, expected_ir_op);
            } else {
                panic!("expected Binary variant for operator {expected_ir_op:?}");
            }
        }
    }

    // --- Not expression conversion ---

    #[test]
    fn convert_not() {
        let expr = Expression::Not(Box::new(Expression::Boolean(true)));
        let result = convert_expression(expr);
        assert_eq!(
            result,
            IrExpr::Not(Box::new(IrExpr::Literal(IrLiteral::Boolean(true))))
        );
    }

    // --- Call expression conversion ---

    #[test]
    fn convert_call_no_args() {
        let expr = Expression::Call("count".to_owned(), vec![]);
        let result = convert_expression(expr);
        assert_eq!(
            result,
            IrExpr::Call("count".to_owned(), vec![])
        );
    }

    #[test]
    fn convert_call_with_args() {
        let expr = Expression::Call(
            "sum".to_owned(),
            vec![Expression::Field("bytes".to_owned())],
        );
        let result = convert_expression(expr);
        assert_eq!(
            result,
            IrExpr::Call(
                "sum".to_owned(),
                vec![IrExpr::Field(IrFieldRef::new("bytes".to_owned()))],
            )
        );
    }

    // --- Snapshot tests for non-trivial expressions ---

    /// Constructs the AST for: `a + b > 10`
    /// i.e. `(a + b) > 10`
    fn make_binary_tree_expr() -> Expression {
        // (a + b) > 10
        Expression::Binary(
            BinaryOperator::GreaterThan,
            Box::new(Expression::Binary(
                BinaryOperator::Add,
                Box::new(Expression::Field("a".to_owned())),
                Box::new(Expression::Field("b".to_owned())),
            )),
            Box::new(Expression::Number(10.0)),
        )
    }

    #[test]
    fn snapshot_binary_expression_tree() {
        let expr = make_binary_tree_expr();
        let result = convert_expression(expr);
        insta::assert_debug_snapshot!("binary_expression_tree", result);
    }

    /// Constructs the AST for: `not (a and b)`
    fn make_nested_not_expr() -> Expression {
        Expression::Not(Box::new(Expression::Binary(
            BinaryOperator::And,
            Box::new(Expression::Field("a".to_owned())),
            Box::new(Expression::Field("b".to_owned())),
        )))
    }

    #[test]
    fn snapshot_nested_not() {
        let expr = make_nested_not_expr();
        let result = convert_expression(expr);
        insta::assert_debug_snapshot!("nested_not", result);
    }

    /// Constructs the AST for: `count()` (zero-arg call)
    fn make_call_count_expr() -> Expression {
        Expression::Call("count".to_owned(), vec![])
    }

    #[test]
    fn snapshot_function_call_count() {
        let expr = make_call_count_expr();
        let result = convert_expression(expr);
        insta::assert_debug_snapshot!("function_call_count", result);
    }

    /// Constructs the AST for: `sum(bytes)` (call with arg)
    fn make_call_sum_expr() -> Expression {
        Expression::Call(
            "sum".to_owned(),
            vec![Expression::Field("bytes".to_owned())],
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
        let expr = Expression::Not(Box::new(Expression::Binary(
            BinaryOperator::And,
            Box::new(Expression::Binary(
                BinaryOperator::GreaterThan,
                Box::new(Expression::Field("a".to_owned())),
                Box::new(Expression::Number(1.0)),
            )),
            Box::new(Expression::Binary(
                BinaryOperator::LessThan,
                Box::new(Expression::Field("b".to_owned())),
                Box::new(Expression::Number(10.0)),
            )),
        )));

        let result = convert_expression(expr);

        // Verify the top-level is Not
        let IrExpr::Not(inner) = result else {
            panic!("expected Not variant");
        };

        // Verify inner is And with two Binary children
        let IrExpr::Binary(op, left, right) = *inner else {
            panic!("expected Binary variant inside Not");
        };
        assert_eq!(op, IrBinaryOp::And);

        // Left: a > 1
        if let IrExpr::Binary(l_op, _, _) = *left {
            assert_eq!(l_op, IrBinaryOp::GreaterThan);
        } else {
            panic!("expected Binary (GreaterThan) on left");
        }

        // Right: b < 10
        if let IrExpr::Binary(r_op, _, _) = *right {
            assert_eq!(r_op, IrBinaryOp::LessThan);
        } else {
            panic!("expected Binary (LessThan) on right");
        }
    }
}
