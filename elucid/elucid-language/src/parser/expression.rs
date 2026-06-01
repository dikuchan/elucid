use chumsky::Parser;
use chumsky::input::ValueInput;
use chumsky::pratt::{infix, left, prefix};
use chumsky::prelude::*;

use crate::ast;
use crate::lexer::Token;
use crate::span::Span;

pub fn expression_parser<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Expression, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let identifier = select! { Token::Identifier(i) => i.to_string() }.labelled("identifier");
    let number = select! { Token::Integer(n) => ast::Expression::Number(n as f64) }.labelled("number");
    let string =
        select! { Token::StringLiteral(s) => ast::Expression::String(s.to_owned()) }.labelled("string");
    let null = just(Token::KeywordNull)
        .to(ast::Expression::Null)
        .labelled("null");
    let boolean = select! {
        Token::KeywordTrue => ast::Expression::Boolean(true),
        Token::KeywordFalse => ast::Expression::Boolean(false),
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
            .map(|(name, arguments)| ast::Expression::Call(name, arguments))
            .labelled("function call");

        let field = identifier.map(ast::Expression::Field).labelled("field");

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
            prefix(6, just(Token::KeywordNot), |_, expr, _| {
                ast::Expression::Not(Box::new(expr))
            }),
            infix(
                left(1),
                just(Token::OperatorOr).to(ast::BinaryOperator::Or),
                |l, _, r, _| ast::Expression::Binary(ast::BinaryOperator::Or, Box::new(l), Box::new(r)),
            ),
            infix(
                left(2),
                just(Token::OperatorAnd).to(ast::BinaryOperator::And),
                |l, _, r, _| ast::Expression::Binary(ast::BinaryOperator::And, Box::new(l), Box::new(r)),
            ),
            infix(
                left(3),
                choice((
                    just(Token::OperatorEqual).to(ast::BinaryOperator::Equal),
                    just(Token::OperatorNotEqual).to(ast::BinaryOperator::NotEqual),
                    just(Token::OperatorGreaterThan).to(ast::BinaryOperator::GreaterThan),
                    just(Token::OperatorGreaterThanOrEqual)
                        .to(ast::BinaryOperator::GreaterThanOrEqual),
                    just(Token::OperatorLessThan).to(ast::BinaryOperator::LessThan),
                    just(Token::OperatorLessThanOrEqual).to(ast::BinaryOperator::LessThanOrEqual),
                )),
                |l, op, r, _| ast::Expression::Binary(op, Box::new(l), Box::new(r)),
            ),
            infix(
                left(4),
                choice((
                    just(Token::OperatorAdd).to(ast::BinaryOperator::Add),
                    just(Token::OperatorSubtract).to(ast::BinaryOperator::Subtract),
                )),
                |l, op, r, _| ast::Expression::Binary(op, Box::new(l), Box::new(r)),
            ),
            infix(
                left(5),
                choice((
                    just(Token::OperatorMultiply).to(ast::BinaryOperator::Multiply),
                    just(Token::OperatorDivide).to(ast::BinaryOperator::Divide),
                )),
                |l, op, r, _| ast::Expression::Binary(op, Box::new(l), Box::new(r)),
            ),
        ))
        .labelled("expression")
    })
}
