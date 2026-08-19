use crate::ast::{self, ExpressionKind, LiteralKind, UnaryOperator};
use crate::ir;

use super::error::SemanticError;

pub(crate) fn convert_expression(
    expression: &ast::Expression,
    relation: &ir::Relation,
) -> Result<ir::Expression, SemanticError> {
    match expression.kind() {
        ExpressionKind::Literal(literal) => convert_literal(literal.kind()),
        ExpressionKind::Field(reference) => {
            Ok(ir::Expression::Field(resolve_field(reference, relation)?))
        }
        ExpressionKind::Unary { operator, operand } => match operator {
            UnaryOperator::Not => Ok(ir::Expression::Not(Box::new(convert_expression(
                operand, relation,
            )?))),
            UnaryOperator::Negate => Err(unsupported_syntax("numeric negation")),
        },
        ExpressionKind::Binary {
            operator,
            left,
            right,
        } => Ok(ir::Expression::Binary(
            convert_binary_operator(*operator),
            Box::new(convert_expression(left, relation)?),
            Box::new(convert_expression(right, relation)?),
        )),
        ExpressionKind::Constructor(_) => Err(unsupported_syntax("constructor")),
        ExpressionKind::Cast(_) => Err(unsupported_syntax("cast")),
        ExpressionKind::Remainder(_) => Err(unsupported_syntax("remainder access")),
    }
}

pub(crate) fn resolve_field(
    reference: &ast::FieldReference,
    relation: &ir::Relation,
) -> Result<ir::Field, SemanticError> {
    relation
        .field(reference.as_str())
        .cloned()
        .ok_or_else(|| SemanticError::FieldNotFound {
            name: reference.as_str().to_owned(),
            span: reference.span(),
        })
}

fn convert_literal(literal: &LiteralKind) -> Result<ir::Expression, SemanticError> {
    let literal = match literal {
        LiteralKind::Null => ir::Literal::Null,
        LiteralKind::Boolean(value) => ir::Literal::Boolean(*value),
        LiteralKind::Integer(value) if is_exact_binary64_integer(*value) => {
            ir::Literal::Number(*value as f64)
        }
        LiteralKind::Integer(_) => {
            return Err(unsupported_syntax("exact integer literal"));
        }
        LiteralKind::FloatingPoint(value) => {
            let value = value.parse::<f64>().map_err(|error| {
                SemanticError::ConversionError(format!(
                    "validated floating-point literal could not be lowered: {error}"
                ))
            })?;
            ir::Literal::Number(value)
        }
        LiteralKind::String(value) => ir::Literal::String(value.to_string()),
    };
    Ok(ir::Expression::Literal(literal))
}

fn is_exact_binary64_integer(value: u64) -> bool {
    let significant_bits = u64::BITS - value.leading_zeros();
    significant_bits <= f64::MANTISSA_DIGITS
        || value.trailing_zeros() >= significant_bits - f64::MANTISSA_DIGITS
}

fn convert_binary_operator(operator: ast::BinaryOperator) -> ir::BinaryOperator {
    match operator {
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

fn unsupported_syntax(feature: &str) -> SemanticError {
    SemanticError::ConversionError(format!("{feature} is not supported by semantic analysis"))
}
