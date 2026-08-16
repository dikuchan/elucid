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

fn identifier<'tokens, 'source: 'tokens, I>()
-> impl Parser<'tokens, I, String, extra::Err<Rich<'tokens, Token<'source>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'source>, Span = Span>,
{
    select! {
        Token::Identifier(identifier) => identifier.to_owned(),
        Token::KeywordSource => "source".to_owned(),
        Token::KeywordProject => "project".to_owned(),
        Token::KeywordFilter => "filter".to_owned(),
        Token::KeywordSort => "sort".to_owned(),
        Token::KeywordTake => "take".to_owned(),
        Token::KeywordSummarize => "summarize".to_owned(),
    }
    .labelled("identifier")
}

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
        basic_source:
            "source test",

        basic_literal_filter:
            "source test | filter status == 200",

        math_precedence:
            "source test | filter a + b * c > 10",

        parenthesis_precedence:
            "source test | filter (a or b) and c",

        and_or_precedence:
            "source test | filter a or b and c",

        string_quoting:
            r#"source test | filter name == "O'Conner" "#,

        null_literal:
            "source test | filter value == null",

        boolean_literal:
            "source test | filter active == true and deleted == false",

        not_operator:
            "source test | filter not active and not deleted == false",

        project_cmd:
            "source test | project name, age, active",

        sort_mixed:
            "source test | sort by -count, +status, time",

        sort_parenthesized:
            "source test | sort by -(a + b)",

        take:
            "source test | take 10",

        summarize_aliased:
            "source test | summarize total = sum(bytes), count() by method",

        summarize_field:
            "source test | summarize count() by method, status",
    }

    #[test]
    fn test_should_fail() {
        let input = "source |";
        let ast = parse(input);
        assert!(ast.is_err());

        insta::assert_debug_snapshot!(ast.unwrap_err());
    }

    #[test]
    fn test_sort_rejects_literal() {
        let input = "source test | sort by 3";
        assert!(parse(input).is_err());
    }

    #[test]
    fn test_summarize_rejects_literal() {
        let input = "source test | summarize 1 + 2";
        assert!(parse(input).is_err());
    }

    #[test]
    fn command_keywords_are_contextual_identifiers() {
        let input = "source source | filter source == 1 | project filter";
        assert!(parse(input).is_ok());
    }

    #[test]
    fn legacy_command_names_are_rejected() {
        for input in [
            "dataset test",
            "source test | where status == 200",
            "source test | fields status",
            "source test | head 10",
            "source test | stats count()",
        ] {
            assert!(
                parse(input).is_err(),
                "legacy query unexpectedly parsed: {input}"
            );
        }
    }
}
