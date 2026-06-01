use chumsky::Parser;
use chumsky::input::ValueInput;
use chumsky::pratt::{infix, left, prefix};
use chumsky::prelude::*;

use crate::ast;
use crate::lexer::Token;
use crate::span::Span;

pub fn expression_parser<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, ast::Expr, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    let identifier = select! { Token::Identifier(i) => i.to_string() }.labelled("identifier");
    let number = select! { Token::Integer(n) => ast::Expr::Number(n as f64) }.labelled("number");
    let string =
        select! { Token::StringLiteral(s) => ast::Expr::String(s.to_owned()) }.labelled("string");
    let null = just(Token::KeywordNull)
        .to(ast::Expr::Null)
        .labelled("null");
    let boolean = select! {
        Token::KeywordTrue => ast::Expr::Boolean(true),
        Token::KeywordFalse => ast::Expr::Boolean(false),
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
            .map(|(name, arguments)| ast::Expr::Call(name, arguments))
            .labelled("function call");

        let field = identifier.map(ast::Expr::Field).labelled("field");

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
                ast::Expr::Not(Box::new(expr))
            }),
            infix(
                left(1),
                just(Token::OperatorOr).to(ast::BinaryOp::Or),
                |l, _, r, _| ast::Expr::Binary(ast::BinaryOp::Or, Box::new(l), Box::new(r)),
            ),
            infix(
                left(2),
                just(Token::OperatorAnd).to(ast::BinaryOp::And),
                |l, _, r, _| ast::Expr::Binary(ast::BinaryOp::And, Box::new(l), Box::new(r)),
            ),
            infix(
                left(3),
                choice((
                    just(Token::OperatorEqual).to(ast::BinaryOp::Equal),
                    just(Token::OperatorNotEqual).to(ast::BinaryOp::NotEqual),
                    just(Token::OperatorGreaterThan).to(ast::BinaryOp::GreaterThan),
                    just(Token::OperatorGreaterThanOrEqual).to(ast::BinaryOp::GreaterThanOrEqual),
                    just(Token::OperatorLessThan).to(ast::BinaryOp::LessThan),
                    just(Token::OperatorLessThanOrEqual).to(ast::BinaryOp::LessThanOrEqual),
                )),
                |l, op, r, _| ast::Expr::Binary(op, Box::new(l), Box::new(r)),
            ),
            infix(
                left(4),
                choice((
                    just(Token::OperatorAdd).to(ast::BinaryOp::Add),
                    just(Token::OperatorSubtract).to(ast::BinaryOp::Subtract),
                )),
                |l, op, r, _| ast::Expr::Binary(op, Box::new(l), Box::new(r)),
            ),
            infix(
                left(5),
                choice((
                    just(Token::OperatorMultiply).to(ast::BinaryOp::Multiply),
                    just(Token::OperatorDivide).to(ast::BinaryOp::Divide),
                )),
                |l, op, r, _| ast::Expr::Binary(op, Box::new(l), Box::new(r)),
            ),
        ))
        .labelled("expression")
    })
}
