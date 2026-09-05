use std::cmp::Ordering;
use std::fmt;

use elucid_core::{CodedError, ErrorCode};

use crate::Span;
use crate::ir;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AnalyzeErrorKind {
    Syntax,
    Semantic,
}

impl From<AnalyzeErrorKind> for ErrorCode {
    fn from(value: AnalyzeErrorKind) -> Self {
        match value {
            AnalyzeErrorKind::Syntax => Self::QuerySyntaxError,
            AnalyzeErrorKind::Semantic => Self::QuerySemanticError,
        }
    }
}

impl AnalyzeErrorKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        ErrorCode::from(self).as_str()
    }
}

impl fmt::Display for AnalyzeErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DiagnosticSeverity {
    Error,
    Warning,
}

impl DiagnosticSeverity {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Warning => "WARNING",
        }
    }

    const fn sort_order(self) -> u8 {
        match self {
            Self::Error => 0,
            Self::Warning => 1,
        }
    }
}

impl fmt::Display for DiagnosticSeverity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DiagnosticCode {
    SyntaxError,
    SourceNotFound,
    FieldNotFound,
    TimeExpressionInvalid,
    TimeBoundUnresolved,
    TimeRangeInvalid,
    LiteralInvalid,
    FunctionArityInvalid,
    FunctionArgumentTypeInvalid,
    CastInvalid,
    ProjectionAliasRequired,
    AggregateAliasRequired,
    DuplicateOutputField,
    StageOrderInvalid,
    TakeInvalid,
    TypeMismatch,
    ConstantEvaluationFailed,
}

impl DiagnosticCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SyntaxError => "QUERY_SYNTAX_ERROR",
            Self::SourceNotFound => "QUERY_SOURCE_NOT_FOUND",
            Self::FieldNotFound => "QUERY_FIELD_NOT_FOUND",
            Self::TimeExpressionInvalid => "QUERY_TIME_EXPRESSION_INVALID",
            Self::TimeBoundUnresolved => "QUERY_TIME_BOUND_UNRESOLVED",
            Self::TimeRangeInvalid => "QUERY_TIME_RANGE_INVALID",
            Self::LiteralInvalid => "QUERY_LITERAL_INVALID",
            Self::FunctionArityInvalid => "QUERY_FUNCTION_ARITY_INVALID",
            Self::FunctionArgumentTypeInvalid => "QUERY_FUNCTION_ARGUMENT_TYPE_INVALID",
            Self::CastInvalid => "QUERY_CAST_INVALID",
            Self::ProjectionAliasRequired => "QUERY_PROJECTION_ALIAS_REQUIRED",
            Self::AggregateAliasRequired => "QUERY_AGGREGATE_ALIAS_REQUIRED",
            Self::DuplicateOutputField => "QUERY_DUPLICATE_OUTPUT_FIELD",
            Self::StageOrderInvalid => "QUERY_STAGE_ORDER_INVALID",
            Self::TakeInvalid => "QUERY_TAKE_INVALID",
            Self::TypeMismatch => "QUERY_TYPE_MISMATCH",
            Self::ConstantEvaluationFailed => "QUERY_CONSTANT_EVALUATION_FAILED",
        }
    }
}

impl fmt::Display for DiagnosticCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourcePosition {
    line: usize,
    column: usize,
}

impl SourcePosition {
    #[must_use]
    pub const fn line(self) -> usize {
        self.line
    }

    #[must_use]
    pub const fn column(self) -> usize {
        self.column
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceRange {
    start: SourcePosition,
    end: SourcePosition,
}

impl SourceRange {
    #[must_use]
    pub const fn start(self) -> SourcePosition {
        self.start
    }

    #[must_use]
    pub const fn end(self) -> SourcePosition {
        self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    code: DiagnosticCode,
    severity: DiagnosticSeverity,
    message: String,
    span: Option<Span>,
    source_range: Option<SourceRange>,
}

impl Diagnostic {
    pub(crate) fn error(code: DiagnosticCode, message: impl Into<String>, span: Span) -> Self {
        Self::new(code, DiagnosticSeverity::Error, message, Some(span))
    }

    fn new(
        code: DiagnosticCode,
        severity: DiagnosticSeverity,
        message: impl Into<String>,
        span: Option<Span>,
    ) -> Self {
        Self {
            code,
            severity,
            message: message.into(),
            span,
            source_range: None,
        }
    }

    pub(crate) fn attach_source_range(&mut self, source: &str) {
        self.source_range = self.span.and_then(|span| {
            Some(SourceRange {
                start: source_position(source, span.start())?,
                end: source_position(source, span.end())?,
            })
        });
    }

    #[must_use]
    pub const fn code(&self) -> DiagnosticCode {
        self.code
    }

    #[must_use]
    pub const fn severity(&self) -> DiagnosticSeverity {
        self.severity
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn span(&self) -> Option<Span> {
        self.span
    }

    #[must_use]
    pub const fn source_range(&self) -> Option<SourceRange> {
        self.source_range
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Analysis {
    pipeline: ir::Pipeline,
    diagnostics: Vec<Diagnostic>,
}

impl Analysis {
    pub(crate) fn new(pipeline: ir::Pipeline, diagnostics: Vec<Diagnostic>) -> Self {
        debug_assert!(
            diagnostics
                .iter()
                .all(|diagnostic| diagnostic.severity == DiagnosticSeverity::Warning)
        );
        Self {
            pipeline,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn pipeline(&self) -> &ir::Pipeline {
        &self.pipeline
    }

    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    pub(crate) fn diagnostics_mut(&mut self) -> &mut Vec<Diagnostic> {
        &mut self.diagnostics
    }

    #[must_use]
    pub fn into_pipeline(self) -> ir::Pipeline {
        self.pipeline
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzeError {
    kind: AnalyzeErrorKind,
    diagnostics: Vec<Diagnostic>,
}

impl AnalyzeError {
    pub(crate) fn syntax(message: impl Into<String>, span: Span) -> Self {
        Self {
            kind: AnalyzeErrorKind::Syntax,
            diagnostics: vec![Diagnostic::error(
                DiagnosticCode::SyntaxError,
                message,
                span,
            )],
        }
    }

    pub(crate) fn semantic(diagnostics: Vec<Diagnostic>) -> Self {
        debug_assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
        );
        Self {
            kind: AnalyzeErrorKind::Semantic,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> AnalyzeErrorKind {
        self.kind
    }

    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    pub(crate) fn diagnostics_mut(&mut self) -> &mut Vec<Diagnostic> {
        &mut self.diagnostics
    }
}

impl fmt::Display for AnalyzeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.kind)?;
        for diagnostic in &self.diagnostics {
            write!(
                formatter,
                "\n{} {}: {}",
                diagnostic.severity, diagnostic.code, diagnostic.message
            )?;
            if let Some(range) = diagnostic.source_range {
                write!(formatter, " at {}:{}", range.start.line, range.start.column)?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for AnalyzeError {}

impl CodedError for AnalyzeError {
    fn error_code(&self) -> ErrorCode {
        self.kind().into()
    }
}

pub(crate) fn finish_diagnostics(diagnostics: &mut [Diagnostic], source: &str) {
    for diagnostic in diagnostics.iter_mut() {
        diagnostic.attach_source_range(source);
    }
    diagnostics.sort_by(compare_diagnostics);
}

fn compare_diagnostics(left: &Diagnostic, right: &Diagnostic) -> Ordering {
    let left_span = left.span.unwrap_or(Span::new(usize::MAX..usize::MAX));
    let right_span = right.span.unwrap_or(Span::new(usize::MAX..usize::MAX));
    left_span
        .start()
        .cmp(&right_span.start())
        .then_with(|| left_span.end().cmp(&right_span.end()))
        .then_with(|| left.severity.sort_order().cmp(&right.severity.sort_order()))
        .then_with(|| left.code.as_str().cmp(right.code.as_str()))
}

fn source_position(source: &str, byte_offset: usize) -> Option<SourcePosition> {
    let prefix = source.get(..byte_offset)?;
    let mut line = 1;
    let mut column = 1;
    for character in prefix.chars() {
        if character == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    Some(SourcePosition { line, column })
}
