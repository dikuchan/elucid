use std::fmt;

use logos::{Lexer, Logos};

use crate::span::Span;

pub fn tokenizer(source: &'_ str) -> impl Iterator<Item = (Token<'_>, Span)> {
    Token::lexer(source)
        .spanned()
        .map(|(token, span)| match token {
            Ok(token) => (token, span.into()),
            Err(()) => (Token::Error, span.into()),
        })
}

#[derive(Logos, Clone, Debug, PartialEq)]
pub enum Token<'a> {
    Error,

    #[token("dataset")]
    KeywordDataset,
    #[token("where")]
    KeywordWhere,
    #[token("sort")]
    KeywordSort,
    #[token("head")]
    KeywordHead,
    #[token("aggr")]
    KeywordAggregate,
    #[token("by")]
    KeywordBy,
    #[token("null")]
    KeywordNull,

    #[token("|")]
    Pipe,
    #[token("(")]
    LeftParenthesis,
    #[token(")")]
    RightParenthesis,

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
    #[token(">")]
    OperatorGreaterThan,
    #[token(">=")]
    OperatorGreaterThanOrEqual,
    #[token("<")]
    OperatorLessThan,
    #[token("<=")]
    OperatorLessThanOrEqual,
    #[token("and")]
    OperatorAnd,
    #[token("or")]
    OperatorOr,
    #[token("=")]
    OperatorAssign,

    #[regex("-?[0-9]+", callback_integer)]
    Integer(i64),
    #[regex(r#""([^"\\]|\\t|\\u|\\n|\\")*""#, callback_string)]
    StringLiteral(&'a str),

    #[regex("[a-zA-Z_][a-zA-Z0-9_]*", callback_string)]
    Identifier(&'a str),

    #[token(",")]
    Comma,

    #[regex(r"[ \t\f\n]+", logos::skip)]
    Whitespace,
}

impl fmt::Display for Token<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::KeywordDataset => write!(f, "dataset"),
            Self::KeywordWhere => write!(f, "where"),
            Self::KeywordSort => write!(f, "sort"),
            Self::KeywordHead => write!(f, "head"),
            Self::KeywordAggregate => write!(f, "aggr"),
            Self::KeywordBy => write!(f, "by"),
            Self::KeywordNull => write!(f, "null"),
            Self::Pipe => write!(f, "|"),
            Self::LeftParenthesis => write!(f, "("),
            Self::RightParenthesis => write!(f, ")"),
            Self::OperatorAdd => write!(f, "+"),
            Self::OperatorSubtract => write!(f, "-"),
            Self::OperatorMultiply => write!(f, "*"),
            Self::OperatorDivide => write!(f, "/"),
            Self::OperatorEqual => write!(f, "=="),
            Self::OperatorNotEqual => write!(f, "!="),
            Self::OperatorGreaterThan => write!(f, ">"),
            Self::OperatorGreaterThanOrEqual => write!(f, ">="),
            Self::OperatorLessThan => write!(f, "<"),
            Self::OperatorLessThanOrEqual => write!(f, "<="),
            Self::OperatorAnd => write!(f, "and"),
            Self::OperatorOr => write!(f, "or"),
            Self::OperatorAssign => write!(f, "="),
            Self::Integer(i) => write!(f, "{}", i),
            Self::StringLiteral(s) => write!(f, "\"{}\"", s),
            Self::Identifier(i) => write!(f, "{}", i),
            Self::Comma => write!(f, ","),
            Self::Whitespace => write!(f, "<whitespace>"),
            Self::Error => write!(f, "<error>"),
        }
    }
}

fn callback_integer<'a>(lexer: &mut Lexer<'a, Token<'a>>) -> Option<i64> {
    lexer.slice().parse::<i64>().ok()
}

fn callback_string<'a>(lexer: &mut Lexer<'a, Token<'a>>) -> &'a str {
    lexer.slice()
}
