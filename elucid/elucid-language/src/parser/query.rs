use std::num::NonZeroU64;

use chumsky::Parser;
use chumsky::input::ValueInput;
use chumsky::prelude::*;

use crate::ast::{
    Query, SourceExpression, TimeDirection, TimeExpression, TimeExpressionKind, TimeOperation,
    TimeOperationKind, TimeUnit,
};
use crate::lexer::Token;
use crate::span::Span;

use super::command::stage_parser;
use super::expression::datetime_constructor_value;
use super::identifier;

fn time_unit<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, TimeUnit, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    select! {
        Token::KeywordSecond => TimeUnit::Second,
        Token::KeywordMinute => TimeUnit::Minute,
        Token::KeywordHour => TimeUnit::Hour,
        Token::KeywordDay => TimeUnit::Day,
    }
    .labelled("time unit")
}

fn time_operation<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, TimeOperation, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let compact_truncation = select! {
        Token::SystemIdentifier("@s") => TimeUnit::Second,
        Token::SystemIdentifier("@m") => TimeUnit::Minute,
        Token::SystemIdentifier("@h") => TimeUnit::Hour,
        Token::SystemIdentifier("@d") => TimeUnit::Day,
    };
    let spaced_truncation = just(Token::At).ignore_then(time_unit());
    let truncation = choice((compact_truncation, spaced_truncation)).map_with(|unit, extra| {
        TimeOperation::new(TimeOperationKind::Truncate(unit), extra.span())
    });

    let direction = select! {
        Token::OperatorAdd => TimeDirection::Forward,
        Token::OperatorSubtract => TimeDirection::Backward,
    };
    let positive_integer = select! { Token::Integer(value) => value }
        .try_map(|value, span| {
            NonZeroU64::new(value)
                .ok_or_else(|| Rich::custom(span, "time magnitude must be positive"))
        })
        .labelled("positive integer");
    let shift = direction
        .then(positive_integer.or_not())
        .then(time_unit())
        .map_with(|((direction, magnitude), unit), extra| {
            TimeOperation::new(
                TimeOperationKind::Shift {
                    direction,
                    magnitude: magnitude.unwrap_or(NonZeroU64::MIN),
                    unit,
                },
                extra.span(),
            )
        });

    choice((truncation, shift)).labelled("relative time operation")
}

fn time_expression<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, TimeExpression, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let datetime = datetime_constructor_value().map_with(|value, extra| {
        TimeExpression::new(TimeExpressionKind::Datetime(value), extra.span())
    });
    let now = just(Token::KeywordNow)
        .map_with(|_, extra| TimeExpression::new(TimeExpressionKind::Now, extra.span()));
    let relative = time_operation()
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map_with(|operations, extra| {
            TimeExpression::new(TimeExpressionKind::Relative(operations), extra.span())
        });
    choice((datetime, now, relative)).labelled("time expression")
}

fn source_expression<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, SourceExpression, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let start_inclusive = just(Token::KeywordStartInclusive)
        .ignore_then(just(Token::OperatorAssign))
        .ignore_then(time_expression());
    let end_exclusive = just(Token::KeywordEndExclusive)
        .ignore_then(just(Token::OperatorAssign))
        .ignore_then(time_expression());

    just(Token::KeywordSource)
        .ignore_then(identifier())
        .then(start_inclusive.or_not())
        .then(end_exclusive.or_not())
        .map_with(|((name, start_inclusive), end_exclusive), extra| {
            SourceExpression::new(name, start_inclusive, end_exclusive, extra.span())
        })
        .labelled("source expression")
}

pub(super) fn parser<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Query, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    source_expression()
        .then(
            just(Token::Pipe)
                .ignore_then(stage_parser())
                .repeated()
                .collect::<Vec<_>>(),
        )
        .then_ignore(end())
        .map_with(|(source, stages), extra| Query::new(source, stages, extra.span()))
        .labelled("query")
}
