use std::fmt;

use crate::parser::ParserError;

/// Errors produced during semantic validation of a parsed query.
///
/// These are distinct from [`ParserError`](crate::parser::ParserError), which
/// represents syntax-level failures. `SemanticError` covers structural
/// and logical problems detected after a successful parse.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SemanticError {
    /// The query pipeline contains no stages.
    EmptyPipeline,

    /// More than one `summarize` command was found in the pipeline.
    MultipleAggregates,

    /// A non-sort/take command appeared after a `summarize` command.
    AggregateAfterAggregate,

    /// The `take` value is zero or negative.
    InvalidLimitValue {
        /// The invalid take value that was provided.
        value: i64,
    },

    /// A `project` command was given with no field names.
    EmptyFieldList,

    /// A `summarize` command was given with no aggregate expressions.
    EmptyAggregateMeasures,

    /// A `sort` command was given with no sort expressions.
    EmptySortSpec,

    /// A catch-all for unexpected conversion failures.
    ConversionError(String),
}

impl fmt::Display for SemanticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPipeline => write!(f, "query pipeline must contain at least one stage"),
            Self::MultipleAggregates => {
                write!(
                    f,
                    "pipeline must not contain more than one 'summarize' command"
                )
            }
            Self::AggregateAfterAggregate => {
                write!(
                    f,
                    "only 'sort' and 'take' commands may follow a 'summarize' command"
                )
            }
            Self::InvalidLimitValue { value } => {
                write!(f, "take value must be a positive integer, got {value}")
            }
            Self::EmptyFieldList => {
                write!(f, "'project' command requires at least one field name")
            }
            Self::EmptyAggregateMeasures => {
                write!(
                    f,
                    "'summarize' command requires at least one aggregate expression"
                )
            }
            Self::EmptySortSpec => {
                write!(f, "'sort' command requires at least one sort expression")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_empty_pipeline() {
        let err = SemanticError::EmptyPipeline;
        assert_eq!(
            err.to_string(),
            "query pipeline must contain at least one stage"
        );
    }

    #[test]
    fn display_multiple_aggregates() {
        let err = SemanticError::MultipleAggregates;
        assert_eq!(
            err.to_string(),
            "pipeline must not contain more than one 'summarize' command"
        );
    }

    #[test]
    fn display_aggregate_after_aggregate() {
        let err = SemanticError::AggregateAfterAggregate;
        assert_eq!(
            err.to_string(),
            "only 'sort' and 'take' commands may follow a 'summarize' command"
        );
    }

    #[test]
    fn display_invalid_limit_value() {
        let err = SemanticError::InvalidLimitValue { value: -3 };
        assert_eq!(
            err.to_string(),
            "take value must be a positive integer, got -3"
        );
    }

    #[test]
    fn display_invalid_limit_value_zero() {
        let err = SemanticError::InvalidLimitValue { value: 0 };
        assert_eq!(
            err.to_string(),
            "take value must be a positive integer, got 0"
        );
    }

    #[test]
    fn display_empty_field_list() {
        let err = SemanticError::EmptyFieldList;
        assert_eq!(
            err.to_string(),
            "'project' command requires at least one field name"
        );
    }

    #[test]
    fn display_empty_aggregate_measures() {
        let err = SemanticError::EmptyAggregateMeasures;
        assert_eq!(
            err.to_string(),
            "'summarize' command requires at least one aggregate expression"
        );
    }

    #[test]
    fn display_empty_sort_spec() {
        let err = SemanticError::EmptySortSpec;
        assert_eq!(
            err.to_string(),
            "'sort' command requires at least one sort expression"
        );
    }

    #[test]
    fn display_conversion_error() {
        let err = SemanticError::ConversionError("unsupported cast from X to Y".into());
        assert_eq!(
            err.to_string(),
            "conversion error: unsupported cast from X to Y"
        );
    }

    #[test]
    fn implements_std_error() {
        fn assert_error<E: std::error::Error>(_: &E) {}
        assert_error(&SemanticError::EmptyPipeline);
        assert_error(&SemanticError::ConversionError("test".into()));
    }

    #[test]
    fn equality_works() {
        assert_eq!(
            SemanticError::InvalidLimitValue { value: 5 },
            SemanticError::InvalidLimitValue { value: 5 }
        );
        assert_ne!(
            SemanticError::InvalidLimitValue { value: 5 },
            SemanticError::InvalidLimitValue { value: 10 }
        );
        assert_ne!(SemanticError::EmptyPipeline, SemanticError::EmptyFieldList);
    }

    #[test]
    fn analyze_error_display_semantic_single() {
        let err = AnalyzeError::Semantic(vec![SemanticError::MultipleAggregates]);
        assert_eq!(
            err.to_string(),
            "pipeline must not contain more than one 'summarize' command"
        );
    }

    #[test]
    fn analyze_error_display_semantic_multiple() {
        let err = AnalyzeError::Semantic(vec![
            SemanticError::EmptyFieldList,
            SemanticError::EmptySortSpec,
        ]);
        let displayed = err.to_string();
        assert!(
            displayed.contains("'project' command requires at least one field name"),
            "missing project error: {displayed}"
        );
        assert!(
            displayed.contains("'sort' command requires at least one sort expression"),
            "missing sort error: {displayed}"
        );
    }

    #[test]
    fn analyze_error_source_semantic_returns_first() {
        let err = AnalyzeError::Semantic(vec![
            SemanticError::EmptyPipeline,
            SemanticError::EmptyFieldList,
        ]);
        let source = std::error::Error::source(&err);
        assert!(source.is_some());
        let msg = source.unwrap().to_string();
        assert!(msg.contains("at least one stage"));
    }

    #[test]
    fn analyze_error_source_parse_returns_inner() {
        let err = AnalyzeError::Parse(ParseError::from_parser_error(
            &crate::parser::parse("").unwrap_err(),
            "",
        ));
        let source = std::error::Error::source(&err);
        assert!(source.is_some());
    }

    #[test]
    fn analyze_error_source_semantic_empty_returns_none() {
        let err = AnalyzeError::Semantic(vec![]);
        let source = std::error::Error::source(&err);
        assert!(source.is_none());
    }

    #[test]
    fn analyze_error_implements_std_error() {
        fn assert_error<E: std::error::Error>(_: &E) {}
        assert_error(&AnalyzeError::Semantic(vec![SemanticError::EmptyPipeline]));
    }
}
