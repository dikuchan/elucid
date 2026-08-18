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
    pub(crate) fn new(errors: RichErrors<'a>) -> Self {
        Self(errors)
    }

    #[must_use]
    pub fn span(&self) -> Span {
        self.0
            .first()
            .map_or_else(|| Span::new(0..0), |error| *error.span())
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
        Report::build(ReportKind::Error, span)
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
        for (i, error) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            let span = error.span().to_owned();
            let reason = error.reason().to_string();
            write!(
                f,
                "parse error at {}..{}: {reason}",
                span.start(),
                span.end()
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for ParserError<'_> {}
