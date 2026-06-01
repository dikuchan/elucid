mod command;
mod error;
mod expression;
mod query;

use chumsky::Parser;
use chumsky::input::{Stream, ValueInput};
use chumsky::prelude::*;

use crate::ast::Query;
use crate::lexer::{Token, tokenizer};
use crate::span::Span;

pub use error::ParserError;

pub fn parse(source: &'_ str) -> Result<Query, ParserError<'_>> {
    let input = new_input(source);
    query::parser()
        .parse(input)
        .into_result()
        .map_err(ParserError::new)
}

pub fn check(source: &'_ str) -> Result<(), ParserError<'_>> {
    let input = new_input(source);
    query::parser()
        .check(input)
        .into_result()
        .map_err(ParserError::new)
}

fn new_input(source: &'_ str) -> impl ValueInput<'_, Token = Token<'_>, Span = Span> {
    let tokens = tokenizer(source);
    Stream::from_iter(tokens).map((0..source.len()).into(), |(t, s)| (t, s))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(source: &str) -> Query {
        match parse(source) {
            Ok(query) => query,
            Err(error) => {
                error.eprint(source).unwrap();
                panic!("Parse failed for input: '{source}'");
            }
        }
    }

    macro_rules! test_snapshots {
        ( $($name:ident: $input:expr),* $(,)? ) => {
            $(
                #[test]
                fn $name() {
                    let input = $input;
                    let ast = parse_ok(input);
                    insta::assert_debug_snapshot!(ast);
                }
            )*
        }
    }

    test_snapshots! {
        basic_dataset:
            "dataset test",

        basic_literal_filter:
            "dataset test | where status == 200",

        math_precedence:
            "dataset test | where a + b * c > 10",

        parenthesis_precedence:
            "dataset test | where (a or b) and c",

        and_or_precedence:
            "dataset test | where a or b and c",

        string_quoting:
            r#"dataset test | where name == "O'Conner" "#,

        null_literal:
            "dataset test | where value == null",

        boolean_literal:
            "dataset test | where active == true and deleted == false",

        not_operator:
            "dataset test | where not active and not deleted == false",

        fields_cmd:
            "dataset test | fields name, age, active",

        sort_mixed:
            "dataset test | sort by -count, +status, time",

        sort_parenthesized:
            "dataset test | sort by -(a + b)",

        head:
            "dataset test | head 10",

        stats_aliased:
            "dataset test | stats total = sum(bytes), count() by method",

        stats_field:
            "dataset test | stats count() by method, status",
    }

    #[test]
    fn test_should_fail() {
        let input = "dataset |";
        let ast = parse(input);
        assert!(ast.is_err());

        insta::assert_debug_snapshot!(ast.unwrap_err());
    }

    #[test]
    fn test_sort_rejects_literal() {
        let input = "dataset test | sort by 3";
        assert!(parse(input).is_err());
    }

    #[test]
    fn test_stats_rejects_literal() {
        let input = "dataset test | stats 1 + 2";
        assert!(parse(input).is_err());
    }
}
