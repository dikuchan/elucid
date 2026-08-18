use std::fmt::{Display, Formatter};

use logos::{Lexer, Logos};

use crate::span::Span;

pub(crate) fn tokenizer(source: &str) -> impl Iterator<Item = (Token<'_>, Span)> {
    Token::lexer(source)
        .spanned()
        .map(|(token, span)| match token {
            Ok(token) => (token, span.into()),
            Err(()) => (Token::Error, span.into()),
        })
}

#[derive(Clone, Debug, Logos, PartialEq)]
pub(crate) enum Token<'source> {
    Error,

    #[token("source")]
    KeywordSource,
    #[token("filter")]
    KeywordFilter,
    #[token("project")]
    KeywordProject,
    #[token("sort")]
    KeywordSort,
    #[token("take")]
    KeywordTake,
    #[token("summarize")]
    KeywordSummarize,

    #[token("s")]
    KeywordSecond,
    #[token("m")]
    KeywordMinute,
    #[token("h")]
    KeywordHour,
    #[token("d")]
    KeywordDay,

    #[token("and")]
    KeywordAnd,
    #[token("or")]
    KeywordOr,
    #[token("not")]
    KeywordNot,
    #[token("true")]
    KeywordTrue,
    #[token("false")]
    KeywordFalse,
    #[token("null")]
    KeywordNull,
    #[token("now")]
    KeywordNow,

    #[token("start_inclusive")]
    KeywordStartInclusive,
    #[token("end_exclusive")]
    KeywordEndExclusive,
    #[token("by")]
    KeywordBy,
    #[token("as")]
    KeywordAs,

    #[token("cast")]
    KeywordCast,
    #[token("try_cast")]
    KeywordTryCast,
    #[token("rest")]
    KeywordRest,
    #[token("rest_exists")]
    KeywordRestExists,
    #[token("count")]
    KeywordCount,
    #[token("sum")]
    KeywordSum,
    #[token("min")]
    KeywordMin,
    #[token("max")]
    KeywordMax,
    #[token("avg")]
    KeywordAvg,

    #[token("bool")]
    KeywordBool,
    #[token("int32")]
    KeywordInt32,
    #[token("int64")]
    KeywordInt64,
    #[token("uint32")]
    KeywordUInt32,
    #[token("uint64")]
    KeywordUInt64,
    #[token("float32")]
    KeywordFloat32,
    #[token("float64")]
    KeywordFloat64,
    #[token("utf8")]
    KeywordUtf8,
    #[token("datetime")]
    KeywordDatetime,
    #[token("eid")]
    KeywordEid,
    #[token("json")]
    KeywordJson,

    #[token("|")]
    Pipe,
    #[token("(")]
    LeftParenthesis,
    #[token(")")]
    RightParenthesis,
    #[token(",")]
    Comma,
    #[token("@")]
    At,

    #[token("+")]
    OperatorAdd,
    #[token("-")]
    OperatorSubtract,
    #[token("*")]
    OperatorMultiply,
    #[token("/")]
    OperatorDivide,
    #[token("==")]
    OperatorEqual,
    #[token("!=")]
    OperatorNotEqual,
    #[token(">=")]
    OperatorGreaterThanOrEqual,
    #[token(">")]
    OperatorGreaterThan,
    #[token("<=")]
    OperatorLessThanOrEqual,
    #[token("<")]
    OperatorLessThan,
    #[token("=")]
    OperatorAssign,

    #[regex(
        r"(?:0|[1-9][0-9]*)(?:\.[0-9]+(?:[eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)",
        callback_floating_point
    )]
    FloatingPoint(&'source str),
    #[regex(r"[0-9]+", callback_integer)]
    Integer(u64),
    #[regex(
        r#""(?:[^"\\\x00-\x1F]|\\(?:["\\/bfnrt]|u[0-9A-Fa-f]{4}))*""#,
        callback_string
    )]
    StringLiteral(String),
    #[regex(r"`[A-Za-z_][A-Za-z0-9_]*`", callback_quoted_identifier)]
    QuotedIdentifier(&'source str),
    #[regex(r"@[A-Za-z_][A-Za-z0-9_]*", priority = 1)]
    SystemIdentifier(&'source str),
    #[regex(r"[A-Za-z_][A-Za-z0-9_]*", priority = 1)]
    Identifier(&'source str),

    #[regex(r"[ \t\f\r\n]+", logos::skip)]
    Whitespace,
}

impl Display for Token<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error => formatter.write_str("<invalid token>"),
            Self::FloatingPoint(value)
            | Self::QuotedIdentifier(value)
            | Self::SystemIdentifier(value)
            | Self::Identifier(value) => formatter.write_str(value),
            Self::Integer(value) => write!(formatter, "{value}"),
            Self::StringLiteral(_) => formatter.write_str("<string literal>"),
            token => formatter.write_str(token.fixed_spelling()),
        }
    }
}

impl Token<'_> {
    fn fixed_spelling(&self) -> &'static str {
        match self {
            Self::KeywordSource => "source",
            Self::KeywordFilter => "filter",
            Self::KeywordProject => "project",
            Self::KeywordSort => "sort",
            Self::KeywordTake => "take",
            Self::KeywordSummarize => "summarize",
            Self::KeywordSecond => "s",
            Self::KeywordMinute => "m",
            Self::KeywordHour => "h",
            Self::KeywordDay => "d",
            Self::KeywordAnd => "and",
            Self::KeywordOr => "or",
            Self::KeywordNot => "not",
            Self::KeywordTrue => "true",
            Self::KeywordFalse => "false",
            Self::KeywordNull => "null",
            Self::KeywordNow => "now",
            Self::KeywordStartInclusive => "start_inclusive",
            Self::KeywordEndExclusive => "end_exclusive",
            Self::KeywordBy => "by",
            Self::KeywordAs => "as",
            Self::KeywordCast => "cast",
            Self::KeywordTryCast => "try_cast",
            Self::KeywordRest => "rest",
            Self::KeywordRestExists => "rest_exists",
            Self::KeywordCount => "count",
            Self::KeywordSum => "sum",
            Self::KeywordMin => "min",
            Self::KeywordMax => "max",
            Self::KeywordAvg => "avg",
            Self::KeywordBool => "bool",
            Self::KeywordInt32 => "int32",
            Self::KeywordInt64 => "int64",
            Self::KeywordUInt32 => "uint32",
            Self::KeywordUInt64 => "uint64",
            Self::KeywordFloat32 => "float32",
            Self::KeywordFloat64 => "float64",
            Self::KeywordUtf8 => "utf8",
            Self::KeywordDatetime => "datetime",
            Self::KeywordEid => "eid",
            Self::KeywordJson => "json",
            Self::Pipe => "|",
            Self::LeftParenthesis => "(",
            Self::RightParenthesis => ")",
            Self::Comma => ",",
            Self::At => "@",
            Self::OperatorAdd => "+",
            Self::OperatorSubtract => "-",
            Self::OperatorMultiply => "*",
            Self::OperatorDivide => "/",
            Self::OperatorEqual => "==",
            Self::OperatorNotEqual => "!=",
            Self::OperatorGreaterThanOrEqual => ">=",
            Self::OperatorGreaterThan => ">",
            Self::OperatorLessThanOrEqual => "<=",
            Self::OperatorLessThan => "<",
            Self::OperatorAssign => "=",
            Self::Whitespace => "<whitespace>",
            Self::Error
            | Self::FloatingPoint(_)
            | Self::Integer(_)
            | Self::StringLiteral(_)
            | Self::QuotedIdentifier(_)
            | Self::SystemIdentifier(_)
            | Self::Identifier(_) => "<dynamic token>",
        }
    }
}

fn callback_integer<'source>(lexer: &mut Lexer<'source, Token<'source>>) -> Option<u64> {
    lexer.slice().parse().ok()
}

fn callback_floating_point<'source>(
    lexer: &mut Lexer<'source, Token<'source>>,
) -> Option<&'source str> {
    let source = lexer.slice();
    source
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map(|_| source)
}

fn callback_string<'source>(lexer: &mut Lexer<'source, Token<'source>>) -> Option<String> {
    serde_json::from_str(lexer.slice()).ok()
}

fn callback_quoted_identifier<'source>(lexer: &mut Lexer<'source, Token<'source>>) -> &'source str {
    let source = lexer.slice();
    &source[1..source.len() - 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_tokens_preserve_exact_source_values_and_byte_spans() {
        let source = "18446744073709551615 0.1 1e-2";
        let tokens = tokenizer(source).collect::<Vec<_>>();

        assert_eq!(
            tokens,
            vec![
                (Token::Integer(u64::MAX), Span::new(0..20)),
                (Token::FloatingPoint("0.1"), Span::new(21..24)),
                (Token::FloatingPoint("1e-2"), Span::new(25..29)),
            ]
        );
    }

    #[test]
    fn json_strings_are_decoded_without_changing_their_byte_spans() {
        let source = r#""ошибка\n""#;
        let tokens = tokenizer(source).collect::<Vec<_>>();

        assert_eq!(
            tokens,
            vec![(
                Token::StringLiteral("ошибка\n".to_owned()),
                Span::new(0..source.len()),
            )]
        );
    }

    #[test]
    fn out_of_domain_numeric_tokens_are_rejected_at_the_lexical_boundary() {
        for source in ["18446744073709551616", "1e400"] {
            assert_eq!(
                tokenizer(source).collect::<Vec<_>>(),
                vec![(Token::Error, Span::new(0..source.len()))]
            );
        }
    }
}
