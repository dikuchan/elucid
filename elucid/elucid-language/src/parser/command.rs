use chumsky::Parser;
use chumsky::input::ValueInput;
use chumsky::prelude::*;

use crate::ast;
use crate::lexer::Token;
use crate::span::Span;

use super::expression::expression_parser;

fn identifier<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, String, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    select! { Token::Identifier(i) => i.to_string() }.labelled("identifier")
}

fn call_or_field<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Expr, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
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
        .map(|(name, arguments)| ast::Expr::Call(name, arguments));

    let field = identifier.map(ast::Expr::Field);

    call.or(field).labelled("field or function call")
}

fn fields_cmd<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    just(Token::KeywordFields)
        .ignore_then(
            expression_parser()
                .separated_by(just(Token::Comma))
                .collect(),
        )
        .map(ast::Command::Fields)
        .labelled("fields")
}

fn where_cmd<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    just(Token::KeywordWhere)
        .ignore_then(expression_parser())
        .map(ast::Command::Where)
        .labelled("where")
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
            .map(|expression| ast::SortExpression::new(
                expression,
                ast::SortOrder::Descending,
            )),
        just(Token::OperatorAdd)
            .ignore_then(sort_atom.clone())
            .map(|expression| ast::SortExpression::new(
                expression,
                ast::SortOrder::Ascending,
            )),
        sort_atom.map(|expression| ast::SortExpression::new(
            expression,
            ast::SortOrder::Ascending,
        )),
    ));

    just(Token::KeywordSort)
        .ignore_then(just(Token::KeywordBy).or_not())
        .ignore_then(sort_item.separated_by(just(Token::Comma)).collect())
        .map(ast::Command::Sort)
        .labelled("sort")
}

fn head_cmd<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    just(Token::KeywordHead)
        .ignore_then(select! { Token::Integer(n) => n }.labelled("integer"))
        .map(ast::Command::Head)
        .labelled("head")
}

fn stats_cmd<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let aliased = identifier()
        .then_ignore(just(Token::OperatorAssign))
        .then(expression_parser())
        .map(|(alias, expression)| (expression, Some(alias)));

    let unaliased = call_or_field().map(|expression| (expression, None));

    let stats_item = choice((aliased, unaliased)).labelled("stats item");

    let by_clause = just(Token::KeywordBy)
        .ignore_then(
            expression_parser()
                .separated_by(just(Token::Comma))
                .collect(),
        )
        .or_not()
        .map(|option| option.unwrap_or_default());

    just(Token::KeywordStats)
        .ignore_then(stats_item.separated_by(just(Token::Comma)).collect())
        .then(by_clause)
        .map(|(aggregates, by)| ast::Command::Stats { aggregates, by })
        .labelled("stats")
}

pub fn command_parser<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Command, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    choice((
        fields_cmd(),
        where_cmd(),
        sort_cmd(),
        head_cmd(),
        stats_cmd(),
    ))
    .labelled("command")
}
