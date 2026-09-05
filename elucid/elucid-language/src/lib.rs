pub mod ast;
mod catalog;
mod semantic;
mod span;

pub mod ir;
mod lexer;
pub mod parser;

pub use catalog::CatalogSnapshot;
pub use semantic::{
    Analysis, AnalyzeError, AnalyzeErrorKind, Diagnostic, DiagnosticCode, DiagnosticSeverity,
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

/// Parses a query and attaches stable diagnostics to syntax failures.
///
/// # Errors
///
/// Returns an [`AnalyzeError`] with [`AnalyzeErrorKind::Syntax`] when parsing fails.
pub fn parse(query: &str) -> Result<ast::Query, AnalyzeError> {
    parser::parse(query).map_err(|error| {
        let mut error = AnalyzeError::syntax(error.to_string(), error.span());
        semantic::error::finish_diagnostics(error.diagnostics_mut(), query);
        error
    })
}

/// Analyzes a parsed query against an immutable catalog snapshot.
///
/// # Errors
///
/// Returns an [`AnalyzeError`] with [`AnalyzeErrorKind::Semantic`] when typed analysis fails.
pub fn analyze_parsed(
    query: &str,
    ast: &ast::Query,
    catalog: &CatalogSnapshot<'_>,
    time_context: &QueryTimeContext,
) -> Result<Analysis, AnalyzeError> {
    let mut result = semantic::convert_query(ast, catalog, time_context);
    match &mut result {
        Ok(analysis) => {
            semantic::error::finish_diagnostics(analysis.diagnostics_mut(), query);
        }
        Err(error) => semantic::error::finish_diagnostics(error.diagnostics_mut(), query),
    }
    result
}

/// Parses and analyzes a query against an immutable catalog snapshot.
///
/// # Errors
///
/// Returns an [`AnalyzeError`] with [`AnalyzeErrorKind::Syntax`] when parsing fails or [`AnalyzeErrorKind::Semantic`] when typed analysis fails.
pub fn analyze(
    query: &str,
    catalog: &CatalogSnapshot<'_>,
    time_context: &QueryTimeContext,
) -> Result<Analysis, AnalyzeError> {
    let ast = parse(query)?;
    analyze_parsed(query, &ast, catalog, time_context)
}
