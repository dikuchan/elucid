pub mod ast;
mod semantic;
mod span;

pub mod ir;
mod lexer;
pub mod parser;

pub use semantic::{AnalyzeError, ParseError, SemanticError};
pub use span::Span;

/// Analyzes a query string, producing a validated [`ir::Pipeline`].
///
/// This is the primary entry point for query compilation. It chains:
/// 1. Lexing and parsing → AST.
/// 2. Semantic analysis → IR pipeline.
/// 3. Validation → structural rule checks.
///
/// # Errors
///
/// Returns [`AnalyzeError::Parse`] if the query string has syntax errors,
/// or [`AnalyzeError::Semantic`] if the parsed query violates structural rules
/// (e.g., multiple aggregate stages, invalid limit values).
///
/// # Examples
///
/// ```
/// use elucid_language::analyze;
///
/// let pipeline = analyze("source test | filter status == 200 | take 10")?;
/// assert_eq!(pipeline.source().dataset(), "test");
/// assert_eq!(pipeline.stages().len(), 2);
/// # Ok::<(), elucid_language::AnalyzeError>(())
/// ```
pub fn analyze(source: &str) -> Result<ir::Pipeline, AnalyzeError> {
    let ast = parser::parse(source)
        .map_err(|e| AnalyzeError::Parse(ParseError::from_parser_error(&e, source)))?;
    let pipeline = semantic::convert_query(&ast).map_err(AnalyzeError::Semantic)?;
    Ok(pipeline)
}
