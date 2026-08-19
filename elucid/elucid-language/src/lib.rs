pub mod ast;
mod catalog;
mod semantic;
mod span;

pub mod ir;
mod lexer;
pub mod parser;

pub use catalog::CatalogSnapshot;
pub use semantic::{
    Analysis, AnalyzeError, AnalyzeErrorCode, Diagnostic, DiagnosticCode, DiagnosticSeverity,
    SourcePosition, SourceRange,
};
pub use span::Span;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTimeContext {
    reference_time: ir::UtcInstant,
    request_start_inclusive: Option<ir::UtcInstant>,
    request_end_exclusive: Option<ir::UtcInstant>,
}

impl QueryTimeContext {
    #[must_use]
    pub const fn new(
        reference_time: ir::UtcInstant,
        request_start_inclusive: Option<ir::UtcInstant>,
        request_end_exclusive: Option<ir::UtcInstant>,
    ) -> Self {
        Self {
            reference_time,
            request_start_inclusive,
            request_end_exclusive,
        }
    }

    #[must_use]
    pub const fn reference_time(self) -> ir::UtcInstant {
        self.reference_time
    }

    #[must_use]
    pub const fn request_start_inclusive(self) -> Option<ir::UtcInstant> {
        self.request_start_inclusive
    }

    #[must_use]
    pub const fn request_end_exclusive(self) -> Option<ir::UtcInstant> {
        self.request_end_exclusive
    }
}

/// Analyzes a query string against an immutable catalog snapshot.
///
/// This is the primary entry point for query compilation. It chains:
/// 1. Lexing and parsing → AST.
/// 2. Semantic analysis → IR pipeline.
/// 3. Left-to-right relation and stage validation.
///
/// # Errors
///
/// Returns an [`AnalyzeError`] with [`AnalyzeErrorCode::Syntax`] when parsing
/// fails or [`AnalyzeErrorCode::Semantic`] when typed analysis fails.
///
pub fn analyze(
    query: &str,
    catalog: &CatalogSnapshot<'_>,
    time_context: &QueryTimeContext,
) -> Result<Analysis, AnalyzeError> {
    let ast = parser::parse(query).map_err(|error| {
        let mut error = AnalyzeError::syntax(error.to_string(), error.span());
        semantic::error::finish_diagnostics(error.diagnostics_mut(), query);
        error
    })?;
    let mut result = semantic::convert_query(&ast, catalog, time_context);
    if let Err(error) = &mut result {
        semantic::error::finish_diagnostics(error.diagnostics_mut(), query);
    }
    result
}
