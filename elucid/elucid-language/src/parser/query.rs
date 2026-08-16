use chumsky::Parser;
use chumsky::input::ValueInput;
use chumsky::prelude::*;

use crate::ast::Query;
use crate::lexer::Token;
use crate::span::Span;

use super::{command::command_parser, identifier};

pub fn parser<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Query, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let command = command_parser();

    just(Token::KeywordSource)
        .ignore_then(identifier().labelled("source name"))
        .then(just(Token::Pipe).ignore_then(command).repeated().collect())
        .map(|(source, commands)| Query::new(source, commands))
        .labelled("query")
}
