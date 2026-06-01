use std::fmt;
use std::fmt::Debug;

use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::prelude::*;

use crate::lexer::Token;
use crate::span::Span;

type RichError<'a> = Rich<'a, Token<'a>, Span>;

type RichErrors<'a> = Vec<RichError<'a>>;

#[derive(Debug)]
pub struct ParserError<'a>(RichErrors<'a>);

impl<'a> ParserError<'a> {
    pub fn new(errors: RichErrors<'a>) -> Self {
        Self(errors)
    }

    /// Renders a pretty visual report to `stderr`.
    pub fn eprint(&self, source: &str) -> std::io::Result<()> {
        for error in self.0.iter() {
            let error_report = Self::new_error_report(error);
            error_report.eprint(Source::from(source))?;
        }
        Ok(())
    }

    fn new_error_report(error: &Rich<Token<'a>, Span>) -> Report<'a, Span> {
        let span = error.span().to_owned();
        Report::build(ReportKind::Error, span.clone())
            .with_message(error.to_string())
            .with_label(
                Label::new(span)
                    .with_message(error.reason())
                    .with_color(Color::Red),
            )
            .finish()
    }
}

impl fmt::Display for ParserError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for error in self.0.iter() {
            let report = Self::new_error_report(error);
            report.fmt(f)?;
        }
        Ok(())
    }
}

impl std::error::Error for ParserError<'_> {}
