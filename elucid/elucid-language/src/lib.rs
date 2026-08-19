pub mod ast;
mod catalog;
mod semantic;
mod span;

pub mod ir;
mod lexer;
pub mod parser;

pub use catalog::CatalogSnapshot;
pub use semantic::{AnalyzeError, ParseError, SemanticError};
pub use span::Span;

/// Analyzes a query string against an immutable catalog snapshot.
///
/// This is the primary entry point for query compilation. It chains:
/// 1. Lexing and parsing → AST.
/// 2. Semantic analysis → IR pipeline.
/// 3. Left-to-right relation and stage validation.
///
/// # Errors
///
/// Returns [`AnalyzeError::Parse`] if the query string has syntax errors,
/// or [`AnalyzeError::Semantic`] if source or field resolution fails, an output
/// relation is invalid, or the pipeline violates a stage rule.
///
pub fn analyze(query: &str, catalog: &CatalogSnapshot<'_>) -> Result<ir::Pipeline, AnalyzeError> {
    let ast = parser::parse(query)
        .map_err(|error| AnalyzeError::Parse(ParseError::from_parser_error(&error, query)))?;
    let pipeline = semantic::convert_query(&ast, catalog).map_err(AnalyzeError::Semantic)?;
    Ok(pipeline)
}
