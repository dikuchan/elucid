use chumsky::Parser;
use chumsky::input::ValueInput;
use chumsky::pratt::{infix, left, prefix};
use chumsky::prelude::*;

use crate::ast::{BinaryOperator, Expression};
use crate::lexer::Token;
use crate::span::Span;

pub fn expression_parser<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, Expression, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let identifier = select! { Token::Identifier(i) => i.to_string() }.labelled("identifier");
    let number = select! { Token::Integer(n) => Expression::Number(n as f64) }.labelled("number");
    let string =
        select! { Token::StringLiteral(s) => Expression::String(s.to_owned()) }.labelled("string");
    let null = just(Token::KeywordNull)
        .to(Expression::Null)
        .labelled("null");
    let boolean = select! {
        Token::KeywordTrue => Expression::Boolean(true),
        Token::KeywordFalse => Expression::Boolean(false),
    }
    .labelled("boolean");

    recursive(|expression| {
        let call = identifier
            .clone()
            .then_ignore(just(Token::LeftParenthesis))
            .then(
                expression
                    .clone()
                    .separated_by(just(Token::Comma))
                    .collect(),
            )
            .then_ignore(just(Token::RightParenthesis))
            .map(|(name, arguments)| Expression::Call(name, arguments))
            .labelled("function call");

        let field = identifier.map(Expression::Field).labelled("field");

        let atom = choice((
            number,
            string,
            null,
            boolean,
            call,
            field,
            expression.delimited_by(just(Token::LeftParenthesis), just(Token::RightParenthesis)),
        ));

        atom.pratt((
            prefix(
                6,
                just(Token::KeywordNot),
                |_, expr, _| Expression::Not(Box::new(expr)),
            ),
            infix(
                left(1),
                just(Token::OperatorOr).to(BinaryOperator::Or),
                |l, _, r, _| Expression::Binary(BinaryOperator::Or, Box::new(l), Box::new(r)),
            ),
            infix(
                left(2),
                just(Token::OperatorAnd).to(BinaryOperator::And),
                |l, _, r, _| Expression::Binary(BinaryOperator::And, Box::new(l), Box::new(r)),
            ),
            infix(
                left(3),
                choice((
                    just(Token::OperatorEqual).to(BinaryOperator::Equal),
                    just(Token::OperatorNotEqual).to(BinaryOperator::NotEqual),
                    just(Token::OperatorGreaterThan).to(BinaryOperator::GreaterThan),
                    just(Token::OperatorGreaterThanOrEqual).to(BinaryOperator::GreaterThanOrEqual),
                    just(Token::OperatorLessThan).to(BinaryOperator::LessThan),
                    just(Token::OperatorLessThanOrEqual).to(BinaryOperator::LessThanOrEqual),
                )),
                |l, op, r, _| Expression::Binary(op, Box::new(l), Box::new(r)),
            ),
            infix(
                left(4),
                choice((
                    just(Token::OperatorAdd).to(BinaryOperator::Add),
                    just(Token::OperatorSubtract).to(BinaryOperator::Subtract),
                )),
                |l, op, r, _| Expression::Binary(op, Box::new(l), Box::new(r)),
            ),
            infix(
                left(5),
                choice((
                    just(Token::OperatorMultiply).to(BinaryOperator::Multiply),
                    just(Token::OperatorDivide).to(BinaryOperator::Divide),
                )),
                |l, op, r, _| Expression::Binary(op, Box::new(l), Box::new(r)),
            ),
        ))
        .labelled("expression")
    })
}
