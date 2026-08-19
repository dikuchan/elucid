use std::fmt;

use crate::Span;
use crate::parser::ParserError;

/// Errors produced during semantic validation of a parsed query.
///
/// These are distinct from [`ParserError`](crate::parser::ParserError), which
/// represents syntax-level failures. `SemanticError` covers structural
/// and logical problems detected after a successful parse.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SemanticError {
    /// The source name is absent from the captured catalog.
    SourceNotFound { name: String, span: Span },

    /// The field name is absent from the current relation.
    FieldNotFound { name: String, span: Span },

    /// Two outputs in one relation use the same name.
    DuplicateOutputField { name: String, span: Span },

    /// A stage is forbidden after the preceding stage.
    StageOrderInvalid { span: Span },

    /// The `take` value is negative.
    InvalidLimitValue {
        /// The invalid take value that was provided.
        value: i64,
    },

    /// A catch-all for unexpected conversion failures.
    ConversionError(String),
}

impl fmt::Display for SemanticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceNotFound { name, .. } => {
                write!(f, "source {name:?} was not found in the catalog snapshot")
            }
            Self::FieldNotFound { name, .. } => {
                write!(f, "field {name:?} was not found in the current relation")
            }
            Self::DuplicateOutputField { name, .. } => {
                write!(f, "output field {name:?} occurs more than once")
            }
            Self::StageOrderInvalid { .. } => {
                write!(
                    f,
                    "only 'sort' and 'take' commands may follow a 'summarize' command"
                )
            }
            Self::InvalidLimitValue { value } => {
                write!(f, "take value must be a non-negative integer, got {value}")
            }
            Self::ConversionError(msg) => {
                write!(f, "conversion error: {msg}")
            }
        }
    }
}

impl std::error::Error for SemanticError {}

/// An owned representation of a parse error.
///
/// Created at the [`analyze`](crate::analyze) boundary from the
/// internal borrowed parser error type ([`ParserError`]).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ParseError {
    messages: Vec<String>,
}

impl ParseError {
    /// Converts a borrowed [`ParserError`] into an owned [`ParseError`]
    /// by capturing human-readable error messages.
    pub(crate) fn from_parser_error(error: &ParserError<'_>, _source: &str) -> Self {
        let messages = vec![error.to_string()];
        Self { messages }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for msg in &self.messages {
            writeln!(f, "{msg}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ParseError {}

/// Error produced by [`analyze`](crate::analyze).
///
/// Wraps both parse-level and semantic-level errors into a single owned type
/// returned by the public entry point. No lifetime parameters — the error can
/// be stored and freely moved independently of the input string.
#[derive(Debug)]
#[non_exhaustive]
pub enum AnalyzeError {
    /// The query failed to parse.
    Parse(ParseError),
    /// The query parsed but failed semantic validation.
    Semantic(Vec<SemanticError>),
}

impl fmt::Display for AnalyzeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(err) => write!(f, "{err}"),
            Self::Semantic(errors) => {
                for (i, err) in errors.iter().enumerate() {
                    if i > 0 {
                        writeln!(f)?;
                    }
                    write!(f, "{err}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for AnalyzeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Parse(err) => Some(err),
            Self::Semantic(errors) => errors.first().map(|e| e as &dyn std::error::Error),
        }
    }
}
