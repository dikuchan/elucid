use chumsky::Parser;
use chumsky::input::ValueInput;
use chumsky::prelude::*;

use crate::ast::{
    BinaryOperator, CastExpression, CastKind, Constructor, ConstructorKind, Expression,
    ExpressionKind, Literal, LiteralKind, NumericLiteralKind, NumericSign, NumericType,
    RemainderExpression, RemainderFunction, SignedIntegerLiteral, SignedNumericLiteral,
    StringLiteral, UnaryOperator,
};
use crate::lexer::Token;
use crate::span::Span;

use super::{
    field_reference, floating_point_literal, integer_literal, logical_type, string_literal,
};

pub(super) fn signed_integer_literal<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, SignedIntegerLiteral, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    just(Token::OperatorSubtract)
        .to(NumericSign::Negative)
        .or_not()
        .then(integer_literal())
        .map_with(|(sign, magnitude), extra| {
            SignedIntegerLiteral::new(
                sign.unwrap_or(NumericSign::NonNegative),
                magnitude,
                extra.span(),
            )
        })
        .labelled("signed integer literal")
}

pub(super) fn datetime_constructor_value<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, StringLiteral, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    just(Token::KeywordDatetime)
        .ignore_then(
            string_literal()
                .delimited_by(just(Token::LeftParenthesis), just(Token::RightParenthesis)),
        )
        .labelled("datetime constructor")
}

pub(super) fn expression_parser<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Expression, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    recursive(|expression| {
        let integer = integer_literal()
            .map_with(|value, extra| {
                Expression::new(
                    ExpressionKind::Literal(Literal::new(
                        LiteralKind::Integer(value),
                        extra.span(),
                    )),
                    extra.span(),
                )
            })
            .labelled("integer literal")
            .boxed();
        let floating_point = floating_point_literal()
            .map_with(|value, extra| {
                Expression::new(
                    ExpressionKind::Literal(Literal::new(
                        LiteralKind::FloatingPoint(value),
                        extra.span(),
                    )),
                    extra.span(),
                )
            })
            .labelled("floating-point literal")
            .boxed();
        let string = string_literal()
            .map(|value| {
                let span = value.span();
                Expression::new(
                    ExpressionKind::Literal(Literal::new(
                        LiteralKind::String(value.value().into()),
                        span,
                    )),
                    span,
                )
            })
            .boxed();
        let null = just(Token::KeywordNull)
            .map_with(|_, extra| {
                Expression::new(
                    ExpressionKind::Literal(Literal::new(LiteralKind::Null, extra.span())),
                    extra.span(),
                )
            })
            .boxed();
        let boolean = select! {
            Token::KeywordTrue => true,
            Token::KeywordFalse => false,
        }
        .map_with(|value, extra| {
            Expression::new(
                ExpressionKind::Literal(Literal::new(LiteralKind::Boolean(value), extra.span())),
                extra.span(),
            )
        })
        .boxed();

        let field = field_reference()
            .map(|field| {
                let span = field.span();
                Expression::new(ExpressionKind::Field(field), span)
            })
            .boxed();

        let integer_target = select! {
            Token::KeywordInt32 => NumericType::Int32,
            Token::KeywordInt64 => NumericType::Int64,
            Token::KeywordUInt32 => NumericType::UInt32,
            Token::KeywordUInt64 => NumericType::UInt64,
        };
        let integer_constructor = integer_target
            .then_ignore(just(Token::LeftParenthesis))
            .then(signed_integer_literal())
            .then_ignore(just(Token::RightParenthesis))
            .map_with(|(target, literal), extra| {
                let literal = SignedNumericLiteral::new(
                    literal.sign(),
                    NumericLiteralKind::Integer(literal.magnitude()),
                    literal.span(),
                );
                let constructor =
                    Constructor::new(ConstructorKind::Numeric { target, literal }, extra.span());
                Expression::new(ExpressionKind::Constructor(constructor), extra.span())
            })
            .boxed();

        let numeric_magnitude = choice((
            integer_literal().map(NumericLiteralKind::Integer),
            floating_point_literal().map(NumericLiteralKind::FloatingPoint),
        ));
        let signed_numeric_literal = just(Token::OperatorSubtract)
            .to(NumericSign::Negative)
            .or_not()
            .then(numeric_magnitude)
            .map_with(|(sign, kind), extra| {
                SignedNumericLiteral::new(
                    sign.unwrap_or(NumericSign::NonNegative),
                    kind,
                    extra.span(),
                )
            });
        let floating_target = select! {
            Token::KeywordFloat32 => NumericType::Float32,
            Token::KeywordFloat64 => NumericType::Float64,
        };
        let floating_constructor = floating_target
            .then_ignore(just(Token::LeftParenthesis))
            .then(signed_numeric_literal)
            .then_ignore(just(Token::RightParenthesis))
            .map_with(|(target, literal), extra| {
                let constructor =
                    Constructor::new(ConstructorKind::Numeric { target, literal }, extra.span());
                Expression::new(ExpressionKind::Constructor(constructor), extra.span())
            })
            .boxed();

        let datetime_constructor = datetime_constructor_value()
            .map_with(|value, extra| {
                let constructor = Constructor::new(ConstructorKind::Datetime(value), extra.span());
                Expression::new(ExpressionKind::Constructor(constructor), extra.span())
            })
            .boxed();
        let eid_constructor = just(Token::KeywordEid)
            .ignore_then(
                string_literal()
                    .delimited_by(just(Token::LeftParenthesis), just(Token::RightParenthesis)),
            )
            .map_with(|value, extra| {
                let constructor = Constructor::new(ConstructorKind::Eid(value), extra.span());
                Expression::new(ExpressionKind::Constructor(constructor), extra.span())
            })
            .boxed();

        let cast = select! {
            Token::KeywordCast => CastKind::Strict,
            Token::KeywordTryCast => CastKind::NullOnFailure,
        }
        .then_ignore(just(Token::LeftParenthesis))
        .then(expression.clone())
        .then_ignore(just(Token::KeywordAs))
        .then(logical_type())
        .then_ignore(just(Token::RightParenthesis))
        .map_with(|((kind, expression), target), extra| {
            Expression::new(
                ExpressionKind::Cast(CastExpression::new(kind, expression, target, extra.span())),
                extra.span(),
            )
        })
        .boxed();

        let remainder = select! {
            Token::KeywordRest => RemainderFunction::Value,
            Token::KeywordRestExists => RemainderFunction::Exists,
        }
        .then_ignore(just(Token::LeftParenthesis))
        .then(string_literal())
        .then_ignore(just(Token::RightParenthesis))
        .map_with(|(function, key), extra| {
            Expression::new(
                ExpressionKind::Remainder(RemainderExpression::new(function, key, extra.span())),
                extra.span(),
            )
        })
        .boxed();

        let parenthesized = expression
            .clone()
            .delimited_by(just(Token::LeftParenthesis), just(Token::RightParenthesis))
            .map_with(|expression, extra| expression.with_span(extra.span()))
            .boxed();

        let primary = choice((
            integer_constructor,
            floating_constructor,
            datetime_constructor,
            eid_constructor,
            cast,
            remainder,
            integer,
            floating_point,
            string,
            null,
            boolean,
            field,
            parenthesized,
        ))
        .labelled("expression primary")
        .boxed();

        let unary = choice((
            just(Token::KeywordNot).to(UnaryOperator::Not),
            just(Token::OperatorSubtract).to(UnaryOperator::Negate),
        ))
        .or_not()
        .then(primary)
        .map_with(|(operator, operand), extra| match operator {
            Some(operator) => Expression::new(
                ExpressionKind::Unary {
                    operator,
                    operand: Box::new(operand),
                },
                extra.span(),
            ),
            None => operand,
        })
        .boxed();

        let multiplicative_operator = choice((
            just(Token::OperatorMultiply).to(BinaryOperator::Multiply),
            just(Token::OperatorDivide).to(BinaryOperator::Divide),
        ));
        let multiplicative = unary
            .clone()
            .foldl_with(
                multiplicative_operator.then(unary).repeated(),
                |left, (operator, right), extra| {
                    binary_expression(operator, left, right, extra.span())
                },
            )
            .boxed();

        let additive_operator = choice((
            just(Token::OperatorAdd).to(BinaryOperator::Add),
            just(Token::OperatorSubtract).to(BinaryOperator::Subtract),
        ));
        let additive = multiplicative
            .clone()
            .foldl_with(
                additive_operator.then(multiplicative).repeated(),
                |left, (operator, right), extra| {
                    binary_expression(operator, left, right, extra.span())
                },
            )
            .boxed();

        let comparison_operator = choice((
            just(Token::OperatorEqual).to(BinaryOperator::Equal),
            just(Token::OperatorNotEqual).to(BinaryOperator::NotEqual),
            just(Token::OperatorGreaterThanOrEqual).to(BinaryOperator::GreaterThanOrEqual),
            just(Token::OperatorGreaterThan).to(BinaryOperator::GreaterThan),
            just(Token::OperatorLessThanOrEqual).to(BinaryOperator::LessThanOrEqual),
            just(Token::OperatorLessThan).to(BinaryOperator::LessThan),
        ));
        let comparison = additive
            .clone()
            .then(comparison_operator.then(additive).or_not())
            .map_with(|(left, operation), extra| match operation {
                Some((operator, right)) => binary_expression(operator, left, right, extra.span()),
                None => left,
            })
            .boxed();

        let logical_and = comparison
            .clone()
            .foldl_with(
                just(Token::KeywordAnd)
                    .to(BinaryOperator::And)
                    .then(comparison)
                    .repeated(),
                |left, (operator, right), extra| {
                    binary_expression(operator, left, right, extra.span())
                },
            )
            .boxed();
        logical_and
            .clone()
            .foldl_with(
                just(Token::KeywordOr)
                    .to(BinaryOperator::Or)
                    .then(logical_and)
                    .repeated(),
                |left, (operator, right), extra| {
                    binary_expression(operator, left, right, extra.span())
                },
            )
            .labelled("expression")
            .boxed()
    })
}

fn binary_expression(
    operator: BinaryOperator,
    left: Expression,
    right: Expression,
    span: Span,
) -> Expression {
    Expression::new(
        ExpressionKind::Binary {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        },
        span,
    )
}
