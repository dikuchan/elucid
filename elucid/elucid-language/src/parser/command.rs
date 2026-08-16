use chumsky::Parser;
use chumsky::input::ValueInput;
use chumsky::prelude::*;

use crate::ast;
use crate::lexer::Token;
use crate::span::Span;

use super::{expression::expression_parser, identifier};

fn call_or_field<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Expression, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let identifier = identifier();

    let call = identifier
        .clone()
        .then_ignore(just(Token::LeftParenthesis))
        .then(
            expression_parser()
                .separated_by(just(Token::Comma))
                .collect(),
        )
        .then_ignore(just(Token::RightParenthesis))
        .map(|(name, arguments)| ast::Expression::Call(name, arguments));

    let field = identifier.map(ast::Expression::Field);

    call.or(field).labelled("field or function call")
}

fn project_cmd<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    just(Token::KeywordProject)
        .ignore_then(
            expression_parser()
                .separated_by(just(Token::Comma))
                .collect(),
        )
        .map(ast::Command::Project)
        .labelled("project")
}

fn filter_cmd<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    just(Token::KeywordFilter)
        .ignore_then(expression_parser())
        .map(ast::Command::Filter)
        .labelled("filter")
}

fn sort_cmd<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let sort_atom = call_or_field()
        .or(expression_parser()
            .delimited_by(just(Token::LeftParenthesis), just(Token::RightParenthesis)))
        .labelled("sort expression");

    let sort_item = choice((
        just(Token::OperatorSubtract)
            .ignore_then(sort_atom.clone())
            .map(|expression| ast::SortExpression::new(expression, ast::SortOrder::Descending)),
        just(Token::OperatorAdd)
            .ignore_then(sort_atom.clone())
            .map(|expression| ast::SortExpression::new(expression, ast::SortOrder::Ascending)),
        sort_atom.map(|expression| ast::SortExpression::new(expression, ast::SortOrder::Ascending)),
    ));

    just(Token::KeywordSort)
        .ignore_then(just(Token::KeywordBy).or_not())
        .ignore_then(sort_item.separated_by(just(Token::Comma)).collect())
        .map(ast::Command::Sort)
        .labelled("sort")
}

fn take_cmd<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    just(Token::KeywordTake)
        .ignore_then(select! { Token::Integer(n) => n }.labelled("integer"))
        .map(ast::Command::Take)
        .labelled("take")
}

fn summarize_cmd<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let aliased = identifier()
        .then_ignore(just(Token::OperatorAssign))
        .then(expression_parser())
        .map(|(alias, expression)| (expression, Some(alias)));

    let unaliased = call_or_field().map(|expression| (expression, None));

    let summarize_item = choice((aliased, unaliased)).labelled("summarize item");

    let by_clause = just(Token::KeywordBy)
        .ignore_then(
            expression_parser()
                .separated_by(just(Token::Comma))
                .collect(),
        )
        .or_not()
        .map(|option| option.unwrap_or_default());

    just(Token::KeywordSummarize)
        .ignore_then(summarize_item.separated_by(just(Token::Comma)).collect())
        .then(by_clause)
        .map(|(aggregates, by)| ast::Command::Summarize { aggregates, by })
        .labelled("summarize")
}

pub fn command_parser<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    choice((
        project_cmd(),
        filter_cmd(),
        sort_cmd(),
        take_cmd(),
        summarize_cmd(),
    ))
    .labelled("command")
}
