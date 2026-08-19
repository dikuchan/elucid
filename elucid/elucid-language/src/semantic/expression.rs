use chrono::DateTime;
use elucid_catalog::{LogicalType, Nullability};

use crate::ast::{
    self, ConstructorKind, ExpressionKind as AstExpressionKind, LiteralKind, NumericLiteralKind,
    NumericSign,
};
use crate::ir;
use crate::{Diagnostic, DiagnosticCode, Span};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntegerLiteralSite {
    Expression,
    Constructor,
}

pub(crate) fn convert_expression(
    expression: &ast::Expression,
    relation: &ir::Relation,
) -> Result<ir::Expression, Diagnostic> {
    convert_expression_with_expected(expression, relation, None)
}

pub(crate) fn resolve_field(
    reference: &ast::FieldReference,
    relation: &ir::Relation,
) -> Result<ir::Field, Diagnostic> {
    relation.field(reference.as_str()).cloned().ok_or_else(|| {
        Diagnostic::error(
            DiagnosticCode::FieldNotFound,
            format!(
                "field {:?} was not found in the current relation",
                reference.as_str()
            ),
            reference.span(),
        )
    })
}

fn convert_expression_with_expected(
    expression: &ast::Expression,
    relation: &ir::Relation,
    expected: Option<LogicalType>,
) -> Result<ir::Expression, Diagnostic> {
    match expression.kind() {
        AstExpressionKind::Literal(literal) => {
            convert_literal(literal, expected, expression.span())
        }
        AstExpressionKind::Field(reference) => {
            resolve_field(reference, relation).map(ir::Expression::field)
        }
        AstExpressionKind::Constructor(constructor) => convert_constructor(constructor),
        AstExpressionKind::Cast(cast) => convert_cast(cast, relation),
        AstExpressionKind::Remainder(remainder) => convert_remainder(remainder, relation),
        AstExpressionKind::Unary { operator, operand } => {
            convert_unary(*operator, operand, expression.span(), relation, expected)
        }
        AstExpressionKind::Binary {
            operator,
            left,
            right,
        } => convert_binary(
            *operator,
            left,
            right,
            expression.span(),
            relation,
            expected,
        ),
    }
}

fn convert_literal(
    literal: &ast::Literal,
    expected: Option<LogicalType>,
    span: Span,
) -> Result<ir::Expression, Diagnostic> {
    let literal = match literal.kind() {
        LiteralKind::Null => {
            let logical_type = expected
                .ok_or_else(|| type_mismatch("null literal has no unique contextual type", span))?;
            ir::Literal::Null(logical_type)
        }
        LiteralKind::Boolean(value) => ir::Literal::Boolean(*value),
        LiteralKind::Integer(magnitude) => integer_literal(
            *magnitude,
            NumericSign::NonNegative,
            expected,
            span,
            IntegerLiteralSite::Expression,
        )?,
        LiteralKind::FloatingPoint(value) => {
            let value = value.parse::<f64>().map_err(|_| {
                literal_invalid("floating-point literal is not representable", span)
            })?;
            if !value.is_finite() {
                return Err(literal_invalid(
                    "floating-point literal must be finite",
                    span,
                ));
            }
            ir::Literal::Float64(value)
        }
        LiteralKind::String(value) => ir::Literal::Utf8(value.clone()),
    };
    Ok(ir::Expression::literal(literal))
}

fn convert_constructor(constructor: &ast::Constructor) -> Result<ir::Expression, Diagnostic> {
    let literal = match constructor.kind() {
        ConstructorKind::Numeric { target, literal } => {
            let target = numeric_type(*target);
            match literal.kind() {
                NumericLiteralKind::Integer(magnitude) if is_integer(target) => integer_literal(
                    *magnitude,
                    literal.sign(),
                    Some(target),
                    constructor.span(),
                    IntegerLiteralSite::Constructor,
                )?,
                NumericLiteralKind::Integer(magnitude) => floating_constructor_from_integer(
                    *magnitude,
                    literal.sign(),
                    target,
                    constructor.span(),
                )?,
                NumericLiteralKind::FloatingPoint(value) => {
                    floating_constructor(value, literal.sign(), target, constructor.span())?
                }
            }
        }
        ConstructorKind::Datetime(value) => {
            ir::Literal::Datetime(parse_datetime_literal(value.value(), constructor.span())?)
        }
        ConstructorKind::Eid(value) => {
            ir::Literal::Eid(parse_eid(value.value(), constructor.span())?)
        }
    };
    Ok(ir::Expression::literal(literal))
}

fn convert_cast(
    cast: &ast::CastExpression,
    relation: &ir::Relation,
) -> Result<ir::Expression, Diagnostic> {
    let target = logical_type(cast.target());
    let expected = match cast.expression().kind() {
        AstExpressionKind::Literal(literal) if matches!(literal.kind(), LiteralKind::Null) => {
            Some(target)
        }
        _ if is_numeric(target) => Some(target),
        _ => None,
    };
    let expression = convert_expression_with_expected(cast.expression(), relation, expected)?;

    if !cast_is_defined(expression.logical_type(), target) {
        return Err(Diagnostic::error(
            DiagnosticCode::CastInvalid,
            format!(
                "cast from {} to {} is not defined",
                expression.logical_type(),
                target
            ),
            cast.span(),
        ));
    }

    if matches!(
        expression.kind(),
        ir::ExpressionKind::Literal(ir::Literal::Null(_))
    ) {
        return Ok(ir::Expression::literal(ir::Literal::Null(target)));
    }

    let kind = match cast.kind() {
        ast::CastKind::Strict => ir::CastKind::Strict,
        ast::CastKind::NullOnFailure => ir::CastKind::NullOnFailure,
    };
    let nullability = match kind {
        ir::CastKind::NullOnFailure => Nullability::Nullable,
        ir::CastKind::Strict if expression.logical_type() == LogicalType::Json => {
            Nullability::Nullable
        }
        ir::CastKind::Strict | ir::CastKind::Lossless => expression.nullability(),
    };
    Ok(ir::Expression::new(
        ir::ExpressionKind::Cast {
            kind,
            expression: Box::new(expression),
            target,
        },
        target,
        nullability,
    ))
}

fn convert_remainder(
    access: &ast::RemainderExpression,
    relation: &ir::Relation,
) -> Result<ir::Expression, Diagnostic> {
    let remainder = relation.field("@rest").cloned().ok_or_else(|| {
        Diagnostic::error(
            DiagnosticCode::FieldNotFound,
            "@rest is not present in the current relation",
            access.span(),
        )
    })?;
    let (function, logical_type, nullability) = match access.function() {
        ast::RemainderFunction::Value => (
            ir::RemainderFunction::Value,
            LogicalType::Json,
            Nullability::Nullable,
        ),
        ast::RemainderFunction::Exists => (
            ir::RemainderFunction::Exists,
            LogicalType::Bool,
            Nullability::NonNull,
        ),
    };
    Ok(ir::Expression::new(
        ir::ExpressionKind::Remainder {
            function,
            remainder,
            key: access.key().value().to_owned(),
        },
        logical_type,
        nullability,
    ))
}

fn convert_unary(
    operator: ast::UnaryOperator,
    operand: &ast::Expression,
    span: Span,
    relation: &ir::Relation,
    expected: Option<LogicalType>,
) -> Result<ir::Expression, Diagnostic> {
    if operator == ast::UnaryOperator::Negate
        && let AstExpressionKind::Literal(literal) = operand.kind()
        && let LiteralKind::Integer(magnitude) = literal.kind()
    {
        let literal = integer_literal(
            *magnitude,
            NumericSign::Negative,
            expected,
            span,
            IntegerLiteralSite::Expression,
        )?;
        return Ok(ir::Expression::literal(literal));
    }

    let expected_operand = match operator {
        ast::UnaryOperator::Not => Some(LogicalType::Bool),
        ast::UnaryOperator::Negate => expected.filter(|logical_type| is_numeric(*logical_type)),
    };
    let operand = convert_expression_with_expected(operand, relation, expected_operand)?;
    let ir_operator = match operator {
        ast::UnaryOperator::Not if operand.logical_type() == LogicalType::Bool => {
            ir::UnaryOperator::Not
        }
        ast::UnaryOperator::Negate
            if matches!(
                operand.logical_type(),
                LogicalType::Int32
                    | LogicalType::Int64
                    | LogicalType::Float32
                    | LogicalType::Float64
            ) =>
        {
            ir::UnaryOperator::Negate
        }
        ast::UnaryOperator::Not => {
            return Err(type_mismatch("not requires a bool operand", span));
        }
        ast::UnaryOperator::Negate => {
            return Err(type_mismatch(
                "unary minus requires a signed numeric operand",
                span,
            ));
        }
    };

    if let Some(folded) = fold_unary(ir_operator, &operand, span)? {
        return Ok(folded);
    }
    let logical_type = operand.logical_type();
    let nullability = operand.nullability();
    Ok(ir::Expression::new(
        ir::ExpressionKind::Unary {
            operator: ir_operator,
            operand: Box::new(operand),
        },
        logical_type,
        nullability,
    ))
}

fn convert_binary(
    operator: ast::BinaryOperator,
    left: &ast::Expression,
    right: &ast::Expression,
    span: Span,
    relation: &ir::Relation,
    expected: Option<LogicalType>,
) -> Result<ir::Expression, Diagnostic> {
    if matches!(
        operator,
        ast::BinaryOperator::Equal | ast::BinaryOperator::NotEqual
    ) {
        if is_null_literal(left) {
            return convert_null_predicate(operator, right, span, relation);
        }
        if is_null_literal(right) {
            return convert_null_predicate(operator, left, span, relation);
        }
    }

    match operator {
        ast::BinaryOperator::And | ast::BinaryOperator::Or => {
            let left = convert_expression_with_expected(left, relation, Some(LogicalType::Bool))?;
            let right = convert_expression_with_expected(right, relation, Some(LogicalType::Bool))?;
            if left.logical_type() != LogicalType::Bool || right.logical_type() != LogicalType::Bool
            {
                return Err(type_mismatch(
                    "logical operators require bool operands",
                    span,
                ));
            }
            build_binary(operator, left, right, LogicalType::Bool, span)
        }
        ast::BinaryOperator::Add
        | ast::BinaryOperator::Subtract
        | ast::BinaryOperator::Multiply
        | ast::BinaryOperator::Divide => {
            let (left, right, common) = convert_numeric_pair(
                left,
                right,
                expected.filter(|logical_type| is_numeric(*logical_type)),
                span,
                relation,
            )?;
            build_binary(operator, left, right, common, span)
        }
        ast::BinaryOperator::Equal
        | ast::BinaryOperator::NotEqual
        | ast::BinaryOperator::GreaterThan
        | ast::BinaryOperator::GreaterThanOrEqual
        | ast::BinaryOperator::LessThan
        | ast::BinaryOperator::LessThanOrEqual => {
            let (left, right) = convert_comparison_pair(left, right, operator, span, relation)?;
            build_binary(operator, left, right, LogicalType::Bool, span)
        }
    }
}

fn convert_null_predicate(
    operator: ast::BinaryOperator,
    value: &ast::Expression,
    span: Span,
    relation: &ir::Relation,
) -> Result<ir::Expression, Diagnostic> {
    if is_null_literal(value) {
        return Err(type_mismatch(
            "two null literals have no unique contextual type",
            span,
        ));
    }
    let expression = convert_expression(value, relation)?;
    let predicate = match operator {
        ast::BinaryOperator::Equal => ir::NullPredicate::IsNull,
        ast::BinaryOperator::NotEqual => ir::NullPredicate::IsNotNull,
        _ => unreachable!("only equality operators lower to null predicates"),
    };
    if let ir::ExpressionKind::Literal(literal) = expression.kind() {
        return Ok(ir::Expression::literal(ir::Literal::Boolean(
            matches!(literal, ir::Literal::Null(_))
                == matches!(predicate, ir::NullPredicate::IsNull),
        )));
    }
    Ok(ir::Expression::new(
        ir::ExpressionKind::NullPredicate {
            expression: Box::new(expression),
            predicate,
        },
        LogicalType::Bool,
        Nullability::NonNull,
    ))
}

fn convert_numeric_pair(
    left: &ast::Expression,
    right: &ast::Expression,
    expected: Option<LogicalType>,
    span: Span,
    relation: &ir::Relation,
) -> Result<(ir::Expression, ir::Expression, LogicalType), Diagnostic> {
    let (left, right) = convert_contextual_pair(left, right, expected, relation)?;
    if !is_numeric(left.logical_type()) || !is_numeric(right.logical_type()) {
        return Err(type_mismatch("arithmetic requires numeric operands", span));
    }
    let common = common_numeric_type(left.logical_type(), right.logical_type())
        .ok_or_else(|| type_mismatch("numeric operands have no lossless common type", span))?;
    Ok((
        coerce_losslessly(left, common),
        coerce_losslessly(right, common),
        common,
    ))
}

fn convert_comparison_pair(
    left: &ast::Expression,
    right: &ast::Expression,
    operator: ast::BinaryOperator,
    span: Span,
    relation: &ir::Relation,
) -> Result<(ir::Expression, ir::Expression), Diagnostic> {
    let (left, right) = convert_contextual_pair(left, right, None, relation)?;
    if is_numeric(left.logical_type()) && is_numeric(right.logical_type()) {
        let common = common_numeric_type(left.logical_type(), right.logical_type())
            .ok_or_else(|| type_mismatch("numeric operands have no lossless common type", span))?;
        return Ok((
            coerce_losslessly(left, common),
            coerce_losslessly(right, common),
        ));
    }

    let ordered = !matches!(
        operator,
        ast::BinaryOperator::Equal | ast::BinaryOperator::NotEqual
    );
    let valid_same_type = left.logical_type() == right.logical_type()
        && matches!(
            left.logical_type(),
            LogicalType::Utf8 | LogicalType::Datetime | LogicalType::Eid
        );
    let valid_equality = !ordered
        && left.logical_type() == LogicalType::Bool
        && right.logical_type() == LogicalType::Bool;
    if valid_same_type || valid_equality {
        Ok((left, right))
    } else {
        Err(type_mismatch("comparison operands are incompatible", span))
    }
}

fn convert_contextual_pair(
    left: &ast::Expression,
    right: &ast::Expression,
    expected: Option<LogicalType>,
    relation: &ir::Relation,
) -> Result<(ir::Expression, ir::Expression), Diagnostic> {
    let left_contextual = needs_context(left);
    let right_contextual = needs_context(right);
    match (left_contextual, right_contextual) {
        (true, false) => {
            let right = convert_expression_with_expected(right, relation, expected)?;
            let left =
                convert_expression_with_expected(left, relation, Some(right.logical_type()))?;
            Ok((left, right))
        }
        (false, true) => {
            let left = convert_expression_with_expected(left, relation, expected)?;
            let right =
                convert_expression_with_expected(right, relation, Some(left.logical_type()))?;
            Ok((left, right))
        }
        (true, true) if expected.is_some() => Ok((
            convert_expression_with_expected(left, relation, expected)?,
            convert_expression_with_expected(right, relation, expected)?,
        )),
        (true, true) if is_null_literal(left) => {
            let right = convert_expression_with_expected(right, relation, None)?;
            let left =
                convert_expression_with_expected(left, relation, Some(right.logical_type()))?;
            Ok((left, right))
        }
        (true, true) => {
            let left = convert_expression_with_expected(left, relation, None)?;
            let right =
                convert_expression_with_expected(right, relation, Some(left.logical_type()))?;
            Ok((left, right))
        }
        (false, false) => Ok((
            convert_expression_with_expected(left, relation, expected)?,
            convert_expression_with_expected(right, relation, expected)?,
        )),
    }
}

fn build_binary(
    operator: ast::BinaryOperator,
    left: ir::Expression,
    right: ir::Expression,
    logical_type: LogicalType,
    span: Span,
) -> Result<ir::Expression, Diagnostic> {
    let operator = binary_operator(operator);
    if let Some(folded) = fold_binary(operator, &left, &right, logical_type, span)? {
        return Ok(folded);
    }
    let nullability = combine_nullability(left.nullability(), right.nullability());
    Ok(ir::Expression::new(
        ir::ExpressionKind::Binary {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        },
        logical_type,
        nullability,
    ))
}

fn coerce_losslessly(expression: ir::Expression, target: LogicalType) -> ir::Expression {
    if expression.logical_type() == target {
        return expression;
    }
    if let ir::ExpressionKind::Literal(literal) = expression.kind()
        && let Some(literal) = widen_literal(literal, target)
    {
        return ir::Expression::literal(literal);
    }
    let nullability = expression.nullability();
    ir::Expression::new(
        ir::ExpressionKind::Cast {
            kind: ir::CastKind::Lossless,
            expression: Box::new(expression),
            target,
        },
        target,
        nullability,
    )
}

fn widen_literal(literal: &ir::Literal, target: LogicalType) -> Option<ir::Literal> {
    match (literal, target) {
        (ir::Literal::Int32(value), LogicalType::Int64) => {
            Some(ir::Literal::Int64(i64::from(*value)))
        }
        (ir::Literal::Int32(value), LogicalType::Float64) => {
            Some(ir::Literal::Float64(f64::from(*value)))
        }
        (ir::Literal::UInt32(value), LogicalType::UInt64) => {
            Some(ir::Literal::UInt64(u64::from(*value)))
        }
        (ir::Literal::UInt32(value), LogicalType::Float64) => {
            Some(ir::Literal::Float64(f64::from(*value)))
        }
        (ir::Literal::Float32(value), LogicalType::Float64) => {
            Some(ir::Literal::Float64(f64::from(*value)))
        }
        (ir::Literal::Null(_), target) => Some(ir::Literal::Null(target)),
        _ => None,
    }
}

fn integer_literal(
    magnitude: u64,
    sign: NumericSign,
    expected: Option<LogicalType>,
    span: Span,
    site: IntegerLiteralSite,
) -> Result<ir::Literal, Diagnostic> {
    let target = expected.unwrap_or(LogicalType::Int64);
    let invalid = |message| literal_invalid(message, span);
    let has_leading_minus = sign == NumericSign::Negative;
    let negative = has_leading_minus && magnitude != 0;
    match target {
        LogicalType::Int32 => signed_i128(magnitude, negative)
            .and_then(|value| i32::try_from(value).ok())
            .map(ir::Literal::Int32)
            .ok_or_else(|| invalid("integer literal is outside int32")),
        LogicalType::Int64 => signed_i128(magnitude, negative)
            .and_then(|value| i64::try_from(value).ok())
            .map(ir::Literal::Int64)
            .ok_or_else(|| invalid("integer literal is outside int64")),
        LogicalType::UInt32 if site == IntegerLiteralSite::Constructor && has_leading_minus => Err(
            literal_invalid("unsigned constructor does not accept a leading minus", span),
        ),
        LogicalType::UInt32 if negative => Err(type_mismatch(
            "negative expression cannot acquire an unsigned type",
            span,
        )),
        LogicalType::UInt32 => u32::try_from(magnitude)
            .map(ir::Literal::UInt32)
            .map_err(|_| invalid("integer literal is outside uint32")),
        LogicalType::UInt64 if site == IntegerLiteralSite::Constructor && has_leading_minus => Err(
            literal_invalid("unsigned constructor does not accept a leading minus", span),
        ),
        LogicalType::UInt64 if negative => Err(type_mismatch(
            "negative expression cannot acquire an unsigned type",
            span,
        )),
        LogicalType::UInt64 => Ok(ir::Literal::UInt64(magnitude)),
        LogicalType::Float32 if integer_is_exact(magnitude, f32::MANTISSA_DIGITS) => {
            let value = magnitude as f32;
            Ok(ir::Literal::Float32(if negative { -value } else { value }))
        }
        LogicalType::Float64 if integer_is_exact(magnitude, f64::MANTISSA_DIGITS) => {
            let value = magnitude as f64;
            Ok(ir::Literal::Float64(if negative { -value } else { value }))
        }
        LogicalType::Float32 | LogicalType::Float64 => Err(invalid(
            "integer literal is not exactly representable in the contextual floating type",
        )),
        LogicalType::Bool
        | LogicalType::Utf8
        | LogicalType::Datetime
        | LogicalType::Eid
        | LogicalType::Json => Err(type_mismatch(
            "integer literal requires a numeric contextual type",
            span,
        )),
        _ => Err(type_mismatch(
            "integer literal has an unsupported contextual type",
            span,
        )),
    }
}

fn floating_constructor_from_integer(
    magnitude: u64,
    sign: NumericSign,
    target: LogicalType,
    span: Span,
) -> Result<ir::Literal, Diagnostic> {
    let negative = sign == NumericSign::Negative;
    match target {
        LogicalType::Float32 => {
            let value = magnitude as f32;
            if value.is_finite() {
                Ok(ir::Literal::Float32(if negative { -value } else { value }))
            } else {
                Err(literal_invalid("float32 constructor is non-finite", span))
            }
        }
        LogicalType::Float64 => {
            let value = magnitude as f64;
            Ok(ir::Literal::Float64(if negative { -value } else { value }))
        }
        _ => Err(literal_invalid(
            "floating constructor requires a floating target",
            span,
        )),
    }
}

fn floating_constructor(
    token: &str,
    sign: NumericSign,
    target: LogicalType,
    span: Span,
) -> Result<ir::Literal, Diagnostic> {
    let negative = sign == NumericSign::Negative;
    match target {
        LogicalType::Float32 => {
            let value = token
                .parse::<f32>()
                .map_err(|_| literal_invalid("float32 constructor is invalid", span))?;
            if !value.is_finite() {
                return Err(literal_invalid("float32 constructor is non-finite", span));
            }
            Ok(ir::Literal::Float32(if negative { -value } else { value }))
        }
        LogicalType::Float64 => {
            let value = token
                .parse::<f64>()
                .map_err(|_| literal_invalid("float64 constructor is invalid", span))?;
            if !value.is_finite() {
                return Err(literal_invalid("float64 constructor is non-finite", span));
            }
            Ok(ir::Literal::Float64(if negative { -value } else { value }))
        }
        _ => Err(literal_invalid(
            "floating constructor requires a floating target",
            span,
        )),
    }
}

pub(crate) fn parse_datetime(value: &str, span: Span) -> Result<ir::UtcInstant, Diagnostic> {
    parse_datetime_with_code(value, span, DiagnosticCode::TimeExpressionInvalid)
}

fn parse_datetime_literal(value: &str, span: Span) -> Result<ir::UtcInstant, Diagnostic> {
    parse_datetime_with_code(value, span, DiagnosticCode::LiteralInvalid)
}

fn parse_datetime_with_code(
    value: &str,
    span: Span,
    code: DiagnosticCode,
) -> Result<ir::UtcInstant, Diagnostic> {
    let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| {
        Diagnostic::error(
            code,
            "datetime must be RFC 3339 with an explicit offset",
            span,
        )
    })?;
    if parsed.timestamp_subsec_nanos() % 1_000_000 != 0 {
        return Err(Diagnostic::error(
            code,
            "datetime contains sub-millisecond precision",
            span,
        ));
    }
    Ok(ir::UtcInstant::from_unix_milliseconds(
        parsed.timestamp_millis(),
    ))
}

fn parse_eid(value: &str, span: Span) -> Result<[u8; 16], Diagnostic> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(literal_invalid(
            "eid must contain exactly 32 lowercase hexadecimal characters",
            span,
        ));
    }
    let mut result = [0_u8; 16];
    for (index, output) in result.iter_mut().enumerate() {
        let high = hex_value(value.as_bytes()[index * 2]);
        let low = hex_value(value.as_bytes()[index * 2 + 1]);
        *output = (high << 4) | low;
    }
    Ok(result)
}

fn hex_value(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => unreachable!("eid validation accepts only lowercase hexadecimal bytes"),
    }
}

fn fold_unary(
    operator: ir::UnaryOperator,
    operand: &ir::Expression,
    span: Span,
) -> Result<Option<ir::Expression>, Diagnostic> {
    let ir::ExpressionKind::Literal(literal) = operand.kind() else {
        return Ok(None);
    };
    let folded = match (operator, literal) {
        (_, ir::Literal::Null(logical_type)) => ir::Literal::Null(*logical_type),
        (ir::UnaryOperator::Not, ir::Literal::Boolean(value)) => ir::Literal::Boolean(!value),
        (ir::UnaryOperator::Negate, ir::Literal::Int32(value)) => value
            .checked_neg()
            .map(ir::Literal::Int32)
            .ok_or_else(|| constant_evaluation_failed("integer negation overflowed", span))?,
        (ir::UnaryOperator::Negate, ir::Literal::Int64(value)) => value
            .checked_neg()
            .map(ir::Literal::Int64)
            .ok_or_else(|| constant_evaluation_failed("integer negation overflowed", span))?,
        (ir::UnaryOperator::Negate, ir::Literal::Float32(value)) => ir::Literal::Float32(-value),
        (ir::UnaryOperator::Negate, ir::Literal::Float64(value)) => ir::Literal::Float64(-value),
        _ => return Ok(None),
    };
    Ok(Some(ir::Expression::literal(folded)))
}

fn fold_binary(
    operator: ir::BinaryOperator,
    left: &ir::Expression,
    right: &ir::Expression,
    logical_type: LogicalType,
    span: Span,
) -> Result<Option<ir::Expression>, Diagnostic> {
    let (ir::ExpressionKind::Literal(left), ir::ExpressionKind::Literal(right)) =
        (left.kind(), right.kind())
    else {
        return Ok(None);
    };
    if matches!(operator, ir::BinaryOperator::And | ir::BinaryOperator::Or)
        && let Some(value) = fold_three_valued_boolean(operator, left, right)
    {
        return Ok(Some(ir::Expression::literal(value)));
    }
    if matches!(left, ir::Literal::Null(_)) || matches!(right, ir::Literal::Null(_)) {
        return Ok(Some(ir::Expression::literal(ir::Literal::Null(
            logical_type,
        ))));
    }

    let folded = match (left, right) {
        (ir::Literal::Int32(left), ir::Literal::Int32(right)) => {
            fold_i32(operator, *left, *right, span)?
        }
        (ir::Literal::Int64(left), ir::Literal::Int64(right)) => {
            fold_i64(operator, *left, *right, span)?
        }
        (ir::Literal::UInt32(left), ir::Literal::UInt32(right)) => {
            fold_u32(operator, *left, *right, span)?
        }
        (ir::Literal::UInt64(left), ir::Literal::UInt64(right)) => {
            fold_u64(operator, *left, *right, span)?
        }
        (ir::Literal::Float32(left), ir::Literal::Float32(right)) => {
            fold_f32(operator, *left, *right, span)?
        }
        (ir::Literal::Float64(left), ir::Literal::Float64(right)) => {
            fold_f64(operator, *left, *right, span)?
        }
        (ir::Literal::Boolean(left), ir::Literal::Boolean(right)) => match operator {
            ir::BinaryOperator::And => Some(ir::Literal::Boolean(*left && *right)),
            ir::BinaryOperator::Or => Some(ir::Literal::Boolean(*left || *right)),
            ir::BinaryOperator::Equal => Some(ir::Literal::Boolean(left == right)),
            ir::BinaryOperator::NotEqual => Some(ir::Literal::Boolean(left != right)),
            _ => None,
        },
        (ir::Literal::Utf8(left), ir::Literal::Utf8(right)) => {
            fold_ordered(operator, left.cmp(right))
        }
        (ir::Literal::Datetime(left), ir::Literal::Datetime(right)) => {
            fold_ordered(operator, left.cmp(right))
        }
        (ir::Literal::Eid(left), ir::Literal::Eid(right)) => {
            fold_ordered(operator, left.cmp(right))
        }
        _ => None,
    };
    Ok(folded.map(ir::Expression::literal))
}

macro_rules! fold_integer {
    ($name:ident, $type:ty, $variant:path) => {
        fn $name(
            operator: ir::BinaryOperator,
            left: $type,
            right: $type,
            span: Span,
        ) -> Result<Option<ir::Literal>, Diagnostic> {
            let value = match operator {
                ir::BinaryOperator::Add => left.checked_add(right),
                ir::BinaryOperator::Subtract => left.checked_sub(right),
                ir::BinaryOperator::Multiply => left.checked_mul(right),
                ir::BinaryOperator::Divide => left.checked_div(right),
                ir::BinaryOperator::Equal => {
                    return Ok(Some(ir::Literal::Boolean(left == right)));
                }
                ir::BinaryOperator::NotEqual => {
                    return Ok(Some(ir::Literal::Boolean(left != right)));
                }
                ir::BinaryOperator::GreaterThan => {
                    return Ok(Some(ir::Literal::Boolean(left > right)));
                }
                ir::BinaryOperator::GreaterThanOrEqual => {
                    return Ok(Some(ir::Literal::Boolean(left >= right)));
                }
                ir::BinaryOperator::LessThan => {
                    return Ok(Some(ir::Literal::Boolean(left < right)));
                }
                ir::BinaryOperator::LessThanOrEqual => {
                    return Ok(Some(ir::Literal::Boolean(left <= right)));
                }
                ir::BinaryOperator::And | ir::BinaryOperator::Or => return Ok(None),
            };
            value.map($variant).map(Some).ok_or_else(|| {
                constant_evaluation_failed("integer arithmetic overflowed or divided by zero", span)
            })
        }
    };
}

fold_integer!(fold_i32, i32, ir::Literal::Int32);
fold_integer!(fold_i64, i64, ir::Literal::Int64);
fold_integer!(fold_u32, u32, ir::Literal::UInt32);
fold_integer!(fold_u64, u64, ir::Literal::UInt64);

fn fold_f32(
    operator: ir::BinaryOperator,
    left: f32,
    right: f32,
    span: Span,
) -> Result<Option<ir::Literal>, Diagnostic> {
    let value = match operator {
        ir::BinaryOperator::Add => left + right,
        ir::BinaryOperator::Subtract => left - right,
        ir::BinaryOperator::Multiply => left * right,
        ir::BinaryOperator::Divide if right == 0.0 => {
            return Err(constant_evaluation_failed(
                "floating-point arithmetic divided by zero",
                span,
            ));
        }
        ir::BinaryOperator::Divide => left / right,
        ir::BinaryOperator::Equal => return Ok(Some(ir::Literal::Boolean(left == right))),
        ir::BinaryOperator::NotEqual => return Ok(Some(ir::Literal::Boolean(left != right))),
        ir::BinaryOperator::GreaterThan => return Ok(Some(ir::Literal::Boolean(left > right))),
        ir::BinaryOperator::GreaterThanOrEqual => {
            return Ok(Some(ir::Literal::Boolean(left >= right)));
        }
        ir::BinaryOperator::LessThan => return Ok(Some(ir::Literal::Boolean(left < right))),
        ir::BinaryOperator::LessThanOrEqual => {
            return Ok(Some(ir::Literal::Boolean(left <= right)));
        }
        ir::BinaryOperator::And | ir::BinaryOperator::Or => return Ok(None),
    };
    if value.is_finite() {
        Ok(Some(ir::Literal::Float32(value)))
    } else {
        Err(constant_evaluation_failed(
            "floating-point arithmetic produced a non-finite result",
            span,
        ))
    }
}

fn fold_f64(
    operator: ir::BinaryOperator,
    left: f64,
    right: f64,
    span: Span,
) -> Result<Option<ir::Literal>, Diagnostic> {
    fold_float(operator, left, right, span)
}

fn fold_float(
    operator: ir::BinaryOperator,
    left: f64,
    right: f64,
    span: Span,
) -> Result<Option<ir::Literal>, Diagnostic> {
    let value = match operator {
        ir::BinaryOperator::Add => left + right,
        ir::BinaryOperator::Subtract => left - right,
        ir::BinaryOperator::Multiply => left * right,
        ir::BinaryOperator::Divide if right == 0.0 => {
            return Err(constant_evaluation_failed(
                "floating-point arithmetic divided by zero",
                span,
            ));
        }
        ir::BinaryOperator::Divide => left / right,
        ir::BinaryOperator::Equal => return Ok(Some(ir::Literal::Boolean(left == right))),
        ir::BinaryOperator::NotEqual => return Ok(Some(ir::Literal::Boolean(left != right))),
        ir::BinaryOperator::GreaterThan => return Ok(Some(ir::Literal::Boolean(left > right))),
        ir::BinaryOperator::GreaterThanOrEqual => {
            return Ok(Some(ir::Literal::Boolean(left >= right)));
        }
        ir::BinaryOperator::LessThan => return Ok(Some(ir::Literal::Boolean(left < right))),
        ir::BinaryOperator::LessThanOrEqual => {
            return Ok(Some(ir::Literal::Boolean(left <= right)));
        }
        ir::BinaryOperator::And | ir::BinaryOperator::Or => return Ok(None),
    };
    if value.is_finite() {
        Ok(Some(ir::Literal::Float64(value)))
    } else {
        Err(constant_evaluation_failed(
            "floating-point arithmetic produced a non-finite result",
            span,
        ))
    }
}

fn fold_ordered(operator: ir::BinaryOperator, ordering: std::cmp::Ordering) -> Option<ir::Literal> {
    let value = match operator {
        ir::BinaryOperator::Equal => ordering.is_eq(),
        ir::BinaryOperator::NotEqual => !ordering.is_eq(),
        ir::BinaryOperator::GreaterThan => ordering.is_gt(),
        ir::BinaryOperator::GreaterThanOrEqual => !ordering.is_lt(),
        ir::BinaryOperator::LessThan => ordering.is_lt(),
        ir::BinaryOperator::LessThanOrEqual => !ordering.is_gt(),
        ir::BinaryOperator::Add
        | ir::BinaryOperator::Subtract
        | ir::BinaryOperator::Multiply
        | ir::BinaryOperator::Divide
        | ir::BinaryOperator::And
        | ir::BinaryOperator::Or => return None,
    };
    Some(ir::Literal::Boolean(value))
}

fn fold_three_valued_boolean(
    operator: ir::BinaryOperator,
    left: &ir::Literal,
    right: &ir::Literal,
) -> Option<ir::Literal> {
    let left = boolean_value(left)?;
    let right = boolean_value(right)?;
    let value = match operator {
        ir::BinaryOperator::And => match (left, right) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), value) | (value, Some(true)) => value,
            (None, None) => None,
        },
        ir::BinaryOperator::Or => match (left, right) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), value) | (value, Some(false)) => value,
            (None, None) => None,
        },
        _ => return None,
    };
    Some(match value {
        Some(value) => ir::Literal::Boolean(value),
        None => ir::Literal::Null(LogicalType::Bool),
    })
}

fn boolean_value(literal: &ir::Literal) -> Option<Option<bool>> {
    match literal {
        ir::Literal::Boolean(value) => Some(Some(*value)),
        ir::Literal::Null(LogicalType::Bool) => Some(None),
        _ => None,
    }
}

fn common_numeric_type(left: LogicalType, right: LogicalType) -> Option<LogicalType> {
    const CANDIDATES: [LogicalType; 6] = [
        LogicalType::Int32,
        LogicalType::UInt32,
        LogicalType::Int64,
        LogicalType::UInt64,
        LogicalType::Float32,
        LogicalType::Float64,
    ];
    CANDIDATES
        .into_iter()
        .find(|candidate| can_widen(left, *candidate) && can_widen(right, *candidate))
}

fn can_widen(source: LogicalType, target: LogicalType) -> bool {
    source == target
        || matches!(
            (source, target),
            (
                LogicalType::Int32,
                LogicalType::Int64 | LogicalType::Float64
            ) | (
                LogicalType::UInt32,
                LogicalType::UInt64 | LogicalType::Float64
            ) | (LogicalType::Float32, LogicalType::Float64)
        )
}

fn cast_is_defined(source: LogicalType, target: LogicalType) -> bool {
    source == target
        || is_numeric(source) && is_numeric(target)
        || target == LogicalType::Json
        || source == LogicalType::Json
        || target == LogicalType::Utf8
        || source == LogicalType::Utf8
            && matches!(
                target,
                LogicalType::Bool
                    | LogicalType::Int32
                    | LogicalType::Int64
                    | LogicalType::UInt32
                    | LogicalType::UInt64
                    | LogicalType::Float32
                    | LogicalType::Float64
                    | LogicalType::Datetime
                    | LogicalType::Eid
            )
}

fn signed_i128(magnitude: u64, negative: bool) -> Option<i128> {
    let magnitude = i128::from(magnitude);
    Some(if negative {
        magnitude.checked_neg()?
    } else {
        magnitude
    })
}

fn integer_is_exact(value: u64, mantissa_digits: u32) -> bool {
    let significant_bits = u64::BITS - value.leading_zeros();
    significant_bits <= mantissa_digits
        || value.trailing_zeros() >= significant_bits - mantissa_digits
}

fn is_null_literal(expression: &ast::Expression) -> bool {
    matches!(
        expression.kind(),
        AstExpressionKind::Literal(literal) if matches!(literal.kind(), LiteralKind::Null)
    )
}

fn needs_context(expression: &ast::Expression) -> bool {
    match expression.kind() {
        AstExpressionKind::Literal(literal) => {
            matches!(literal.kind(), LiteralKind::Integer(_) | LiteralKind::Null)
        }
        AstExpressionKind::Unary {
            operator: ast::UnaryOperator::Negate,
            operand,
        } => needs_context(operand),
        AstExpressionKind::Binary {
            operator:
                ast::BinaryOperator::Add
                | ast::BinaryOperator::Subtract
                | ast::BinaryOperator::Multiply
                | ast::BinaryOperator::Divide,
            left,
            right,
        } => needs_context(left) && needs_context(right),
        _ => false,
    }
}

fn is_numeric(logical_type: LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Int32
            | LogicalType::Int64
            | LogicalType::UInt32
            | LogicalType::UInt64
            | LogicalType::Float32
            | LogicalType::Float64
    )
}

fn is_integer(logical_type: LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Int32 | LogicalType::Int64 | LogicalType::UInt32 | LogicalType::UInt64
    )
}

fn numeric_type(logical_type: ast::NumericType) -> LogicalType {
    match logical_type {
        ast::NumericType::Int32 => LogicalType::Int32,
        ast::NumericType::Int64 => LogicalType::Int64,
        ast::NumericType::UInt32 => LogicalType::UInt32,
        ast::NumericType::UInt64 => LogicalType::UInt64,
        ast::NumericType::Float32 => LogicalType::Float32,
        ast::NumericType::Float64 => LogicalType::Float64,
    }
}

fn logical_type(logical_type: ast::LogicalType) -> LogicalType {
    match logical_type {
        ast::LogicalType::Bool => LogicalType::Bool,
        ast::LogicalType::Int32 => LogicalType::Int32,
        ast::LogicalType::Int64 => LogicalType::Int64,
        ast::LogicalType::UInt32 => LogicalType::UInt32,
        ast::LogicalType::UInt64 => LogicalType::UInt64,
        ast::LogicalType::Float32 => LogicalType::Float32,
        ast::LogicalType::Float64 => LogicalType::Float64,
        ast::LogicalType::Utf8 => LogicalType::Utf8,
        ast::LogicalType::Datetime => LogicalType::Datetime,
        ast::LogicalType::Eid => LogicalType::Eid,
        ast::LogicalType::Json => LogicalType::Json,
    }
}

fn binary_operator(operator: ast::BinaryOperator) -> ir::BinaryOperator {
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

fn combine_nullability(left: Nullability, right: Nullability) -> Nullability {
    if left == Nullability::Nullable || right == Nullability::Nullable {
        Nullability::Nullable
    } else {
        Nullability::NonNull
    }
}

fn literal_invalid(message: impl Into<String>, span: Span) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::LiteralInvalid, message, span)
}

fn type_mismatch(message: impl Into<String>, span: Span) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::TypeMismatch, message, span)
}

fn constant_evaluation_failed(message: impl Into<String>, span: Span) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::ConstantEvaluationFailed, message, span)
}
