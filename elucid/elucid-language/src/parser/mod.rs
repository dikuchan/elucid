mod command;
mod error;
mod expression;
mod query;
#[cfg(test)]
mod tests;

use chumsky::Parser;
use chumsky::input::{Stream, ValueInput};
use chumsky::prelude::*;

use crate::ast::{FieldReference, Identifier, LogicalType, StringLiteral, SystemIdentifier};
use crate::lexer::{Token, tokenizer};
use crate::span::Span;

pub use error::ParserError;

fn identifier<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Identifier, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    select! {
        Token::Identifier(value) => value.into(),
        Token::QuotedIdentifier(value) => value.into(),
        Token::KeywordSource => "source".into(),
        Token::KeywordFilter => "filter".into(),
        Token::KeywordProject => "project".into(),
        Token::KeywordSort => "sort".into(),
        Token::KeywordTake => "take".into(),
        Token::KeywordSummarize => "summarize".into(),
        Token::KeywordSecond => "s".into(),
        Token::KeywordMinute => "m".into(),
        Token::KeywordHour => "h".into(),
        Token::KeywordDay => "d".into(),
    }
    .map_with(|value: Box<str>, extra| Identifier::new(value, extra.span()))
    .labelled("identifier")
}

fn field_reference<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, FieldReference, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let user = identifier().map(FieldReference::User);
    let system = select! { Token::SystemIdentifier(value) => value }.map_with(|value, extra| {
        FieldReference::System(SystemIdentifier::new(value, extra.span()))
    });
    choice((system, user)).labelled("field reference")
}

fn string_literal<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, StringLiteral, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    select! { Token::StringLiteral(value) => value }
        .map_with(|value, extra| StringLiteral::new(value, extra.span()))
        .labelled("string literal")
}

fn integer_literal<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, u64, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    select! { Token::Integer(value) => value }.labelled("integer literal")
}

fn floating_point_literal<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Box<str>, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    select! { Token::FloatingPoint(value) => Box::<str>::from(value) }
        .labelled("floating-point literal")
}

fn logical_type<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, LogicalType, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    select! {
        Token::KeywordBool => LogicalType::Bool,
        Token::KeywordInt32 => LogicalType::Int32,
        Token::KeywordInt64 => LogicalType::Int64,
        Token::KeywordUInt32 => LogicalType::UInt32,
        Token::KeywordUInt64 => LogicalType::UInt64,
        Token::KeywordFloat32 => LogicalType::Float32,
        Token::KeywordFloat64 => LogicalType::Float64,
        Token::KeywordUtf8 => LogicalType::Utf8,
        Token::KeywordDatetime => LogicalType::Datetime,
        Token::KeywordEid => LogicalType::Eid,
        Token::KeywordJson => LogicalType::Json,
    }
    .labelled("logical type")
}

pub fn parse(source: &str) -> Result<crate::ast::Query, ParserError<'_>> {
    query::parser()
        .parse(new_input(source))
        .into_result()
        .map_err(ParserError::new)
}

pub fn check(source: &str) -> Result<(), ParserError<'_>> {
    query::parser()
        .check(new_input(source))
        .into_result()
        .map_err(ParserError::new)
}

fn new_input(source: &str) -> impl ValueInput<'_, Token = Token<'_>, Span = Span> {
    Stream::from_iter(tokenizer(source))
        .map((0..source.len()).into(), |(token, span)| (token, span))
}
