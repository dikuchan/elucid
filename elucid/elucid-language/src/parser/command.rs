use chumsky::Parser;
use chumsky::input::ValueInput;
use chumsky::prelude::*;

use crate::ast::{
    AggregateCall, AggregateFunction, ComputedProjection, Measure, Projection, SortDirection,
    SortItem, Stage, StageKind,
};
use crate::lexer::Token;
use crate::span::Span;

use super::expression::{expression_parser, signed_integer_literal};
use super::{field_reference, identifier};

fn filter_stage<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Stage, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    just(Token::KeywordFilter)
        .ignore_then(expression_parser())
        .map_with(|expression, extra| Stage::new(StageKind::Filter(expression), extra.span()))
        .labelled("filter stage")
}

fn project_stage<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Stage, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let computed = identifier()
        .then_ignore(just(Token::OperatorAssign))
        .then(expression_parser())
        .map_with(|(alias, expression), extra| {
            Projection::Computed(ComputedProjection::new(alias, expression, extra.span()))
        });
    let field = field_reference().map(Projection::Field);
    let projection = choice((computed, field)).labelled("projection");

    just(Token::KeywordProject)
        .ignore_then(
            projection
                .separated_by(just(Token::Comma))
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .map_with(|projections, extra| Stage::new(StageKind::Project(projections), extra.span()))
        .labelled("project stage")
}

fn sort_stage<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Stage, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let direction = choice((
        just(Token::OperatorAdd).to(SortDirection::Ascending),
        just(Token::OperatorSubtract).to(SortDirection::Descending),
    ))
    .or_not();
    let item = direction
        .then(field_reference())
        .map_with(|(direction, field), extra| {
            SortItem::new(
                field,
                direction.unwrap_or(SortDirection::Ascending),
                extra.span(),
            )
        });

    just(Token::KeywordSort)
        .ignore_then(just(Token::KeywordBy).or_not())
        .ignore_then(
            item.separated_by(just(Token::Comma))
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .map_with(|items, extra| Stage::new(StageKind::Sort(items), extra.span()))
        .labelled("sort stage")
}

fn take_stage<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Stage, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    just(Token::KeywordTake)
        .ignore_then(signed_integer_literal())
        .map_with(|value, extra| Stage::new(StageKind::Take(value), extra.span()))
        .labelled("take stage")
}

fn aggregate_function<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, AggregateFunction, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    select! {
        Token::KeywordCount => AggregateFunction::Count,
        Token::KeywordSum => AggregateFunction::Sum,
        Token::KeywordMin => AggregateFunction::Min,
        Token::KeywordMax => AggregateFunction::Max,
        Token::KeywordAvg => AggregateFunction::Avg,
    }
    .labelled("aggregate function")
}

fn aggregate_call<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, AggregateCall, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    aggregate_function()
        .then_ignore(just(Token::LeftParenthesis))
        .then(field_reference().or_not())
        .then_ignore(just(Token::RightParenthesis))
        .map_with(|(function, argument), extra| {
            AggregateCall::new(function, argument, extra.span())
        })
        .labelled("aggregate call")
}

fn summarize_stage<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Stage, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let measure = identifier()
        .then_ignore(just(Token::OperatorAssign))
        .then(aggregate_call())
        .map_with(|(alias, aggregate), extra| Measure::new(alias, aggregate, extra.span()));
    let measures = measure
        .separated_by(just(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>();
    let group_by = just(Token::KeywordBy)
        .ignore_then(
            field_reference()
                .separated_by(just(Token::Comma))
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .or_not()
        .map(|fields| fields.unwrap_or_default());

    just(Token::KeywordSummarize)
        .ignore_then(measures)
        .then(group_by)
        .map_with(|(measures, group_by), extra| {
            Stage::new(StageKind::Summarize { measures, group_by }, extra.span())
        })
        .labelled("summarize stage")
}

pub(super) fn stage_parser<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Stage, extra::Err<Rich<'tokens, Token<'source>, Span>>>
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    choice((
        filter_stage(),
        project_stage(),
        sort_stage(),
        take_stage(),
        summarize_stage(),
    ))
    .labelled("pipeline stage")
}
