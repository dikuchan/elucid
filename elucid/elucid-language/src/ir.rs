//! Intermediate representation (IR) types for query pipelines.
//!
//! The IR is the output of semantic analysis on the parsed AST. It represents
//! a validated, normalized query pipeline.

use std::fmt;

/// A literal value in the IR.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Literal {
    /// The null value.
    Null,
    /// A boolean value.
    Boolean(bool),
    /// A numeric (floating-point) value.
    Number(f64),
    /// A string value.
    String(String),
}

/// Binary operator for IR expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    And,
    Or,
}

/// A reference to a field in the IR.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct FieldRef {
    name: String,
}

impl FieldRef {
    /// Creates a new field reference from a name.
    pub fn new(name: String) -> Self {
        Self { name }
    }

    /// Returns the field name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.name
    }

    /// Consumes the field reference and returns the inner name.
    pub fn into_inner(self) -> String {
        self.name
    }
}

impl From<String> for FieldRef {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

impl From<&str> for FieldRef {
    fn from(name: &str) -> Self {
        Self::new(name.to_owned())
    }
}

impl fmt::Display for FieldRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// An expression in the IR.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Expression {
    /// A literal value.
    Literal(Literal),
    /// A reference to a field.
    Field(FieldRef),
    /// A binary operation on two sub-expressions.
    Binary(BinaryOperator, Box<Expression>, Box<Expression>),
    /// Logical negation.
    Not(Box<Expression>),
    /// A function call.
    Call(String, Vec<Expression>),
}

/// Specifies the data source for a query pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct SourceSpec {
    dataset: String,
}

impl SourceSpec {
    /// Creates a new source specification for the given dataset name.
    pub fn new(dataset: String) -> Self {
        Self { dataset }
    }

    /// Returns the dataset name.
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    /// Consumes the source spec and returns the dataset name.
    pub fn into_dataset(self) -> String {
        self.dataset
    }
}

impl From<&str> for SourceSpec {
    fn from(dataset: &str) -> Self {
        Self {
            dataset: dataset.to_owned(),
        }
    }
}

impl fmt::Display for SourceSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.dataset)
    }
}

/// A time range constraint for the query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct TimeRange {
    earliest: Option<String>,
    latest: Option<String>,
}

impl TimeRange {
    /// Creates a new time range with optional earliest and latest bounds.
    pub fn new(earliest: Option<String>, latest: Option<String>) -> Self {
        Self { earliest, latest }
    }

    /// Returns the earliest time bound, if specified.
    pub fn earliest(&self) -> Option<&str> {
        self.earliest.as_deref()
    }

    /// Returns the latest time bound, if specified.
    pub fn latest(&self) -> Option<&str> {
        self.latest.as_deref()
    }
}

/// A sort specification within a pipeline stage.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SortSpec {
    expr: Expression,
    order: SortOrder,
}

impl SortSpec {
    /// Creates a new sort specification.
    pub fn new(expr: Expression, order: SortOrder) -> Self {
        Self { expr, order }
    }

    /// Returns the expression to sort by.
    pub fn expr(&self) -> &Expression {
        &self.expr
    }

    /// Returns the sort order.
    pub fn order(&self) -> SortOrder {
        self.order
    }

    /// Consumes the sort spec and returns its components.
    pub fn into_parts(self) -> (Expression, SortOrder) {
        (self.expr, self.order)
    }
}

/// Sort direction for sort expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum SortOrder {
    /// Ascending order.
    #[default]
    Ascending,
    /// Descending order.
    Descending,
}

impl fmt::Display for SortOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ascending => write!(f, "asc"),
            Self::Descending => write!(f, "desc"),
        }
    }
}

/// An aggregate expression (e.g., `count()`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct AggregateExpr {
    function: String,
    argument: Option<Expression>,
    alias: Option<String>,
}

impl AggregateExpr {
    /// Creates a new aggregate expression.
    pub fn new(function: String, argument: Option<Expression>, alias: Option<String>) -> Self {
        Self {
            function,
            argument,
            alias,
        }
    }

    /// Returns the aggregate function name.
    pub fn function(&self) -> &str {
        &self.function
    }

    /// Returns the aggregate argument expression, if any.
    pub fn argument(&self) -> Option<&Expression> {
        self.argument.as_ref()
    }

    /// Returns the output alias, if any.
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    /// Consumes the aggregate expression and returns its components.
    pub fn into_parts(self) -> (String, Option<Expression>, Option<String>) {
        (self.function, self.argument, self.alias)
    }
}

/// A single stage in a query pipeline.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum PipelineStage {
    /// Filter rows by a predicate expression.
    Filter(Expression),
    /// Project specific fields.
    Project(Vec<FieldRef>),
    /// Aggregate with measures and optional group-by fields.
    Aggregate {
        measures: Vec<AggregateExpr>,
        group_by: Vec<FieldRef>,
    },
    /// Sort by one or more specifications.
    Sort(Vec<SortSpec>),
    /// Limit the number of output rows.
    Limit(usize),
    /// Placeholder for future full-text search.
    Search,
}

/// A complete query pipeline.
///
/// A pipeline starts from a data source, optionally constrained by a time
/// range, and processes data through a sequence of stages.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Pipeline {
    source: SourceSpec,
    time_range: TimeRange,
    stages: Vec<PipelineStage>,
}

impl Pipeline {
    /// Creates a new query pipeline.
    pub fn new(source: SourceSpec, time_range: TimeRange, stages: Vec<PipelineStage>) -> Self {
        Self {
            source,
            time_range,
            stages,
        }
    }

    /// Returns the data source specification.
    pub fn source(&self) -> &SourceSpec {
        &self.source
    }

    /// Returns the time range constraint.
    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    /// Returns the pipeline stages.
    pub fn stages(&self) -> &[PipelineStage] {
        &self.stages
    }

    /// Consumes the pipeline and returns its components.
    pub fn into_parts(self) -> (SourceSpec, TimeRange, Vec<PipelineStage>) {
        (self.source, self.time_range, self.stages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_null() {
        let lit = Literal::Null;
        assert_eq!(lit, Literal::Null);
    }

    #[test]
    fn literal_boolean() {
        assert_eq!(Literal::Boolean(true), Literal::Boolean(true));
        assert_ne!(Literal::Boolean(true), Literal::Boolean(false));
    }

    #[test]
    fn literal_number() {
        assert_eq!(Literal::Number(42.0), Literal::Number(42.0));
    }

    #[test]
    fn literal_string() {
        assert_eq!(
            Literal::String("hello".to_owned()),
            Literal::String("hello".to_owned())
        );
    }

    #[test]
    fn field_ref_new() {
        let field = FieldRef::new("status".to_owned());
        assert_eq!(field.as_str(), "status");
    }

    #[test]
    fn field_ref_from_string() {
        let field: FieldRef = "host".to_owned().into();
        assert_eq!(field.as_str(), "host");
    }

    #[test]
    fn field_ref_from_str() {
        let field: FieldRef = "method".into();
        assert_eq!(field.as_str(), "method");
    }

    #[test]
    fn field_ref_into_inner() {
        let field = FieldRef::new("path".to_owned());
        assert_eq!(field.into_inner(), "path");
    }

    #[test]
    fn field_ref_display() {
        let field = FieldRef::new("status".to_owned());
        assert_eq!(format!("{field}"), "status");
    }

    #[test]
    fn field_ref_equality() {
        let a = FieldRef::new("host".to_owned());
        let b = FieldRef::new("host".to_owned());
        let c = FieldRef::new("port".to_owned());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn expr_literal() {
        let expr = Expression::Literal(Literal::Number(1.0));
        assert_eq!(expr, Expression::Literal(Literal::Number(1.0)));
    }

    #[test]
    fn expr_field() {
        let expr = Expression::Field(FieldRef::new("count".to_owned()));
        assert_eq!(expr, Expression::Field(FieldRef::new("count".to_owned())));
    }

    #[test]
    fn expr_binary() {
        let expr = Expression::Binary(
            BinaryOperator::Add,
            Box::new(Expression::Field(FieldRef::new("a".to_owned()))),
            Box::new(Expression::Literal(Literal::Number(1.0))),
        );
        if let Expression::Binary(op, _, _) = &expr {
            assert_eq!(*op, BinaryOperator::Add);
        } else {
            panic!("expected Binary variant");
        }
    }

    #[test]
    fn expr_not() {
        let expr = Expression::Not(Box::new(Expression::Literal(Literal::Boolean(true))));
        if let Expression::Not(inner) = &expr {
            assert_eq!(**inner, Expression::Literal(Literal::Boolean(true)));
        } else {
            panic!("expected Not variant");
        }
    }

    #[test]
    fn expr_call() {
        let expr = Expression::Call(
            "count".to_owned(),
            vec![Expression::Field(FieldRef::new("bytes".to_owned()))],
        );
        if let Expression::Call(name, args) = &expr {
            assert_eq!(name, "count");
            assert_eq!(args.len(), 1);
        } else {
            panic!("expected Call variant");
        }
    }

    #[test]
    fn source_spec_new() {
        let spec = SourceSpec::new("logs".to_owned());
        assert_eq!(spec.dataset(), "logs");
    }

    #[test]
    fn source_spec_into_dataset() {
        let spec = SourceSpec::new("logs".to_owned());
        assert_eq!(spec.into_dataset(), "logs");
    }

    #[test]
    fn source_spec_display() {
        let spec = SourceSpec::new("logs".to_owned());
        assert_eq!(format!("{spec}"), "logs");
    }

    #[test]
    fn time_range_default() {
        let range = TimeRange::default();
        assert!(range.earliest().is_none());
        assert!(range.latest().is_none());
    }

    #[test]
    fn time_range_with_bounds() {
        let range = TimeRange::new(Some("-1h".to_owned()), Some("now".to_owned()));
        assert_eq!(range.earliest(), Some("-1h"));
        assert_eq!(range.latest(), Some("now"));
    }

    #[test]
    fn sort_order_default() {
        assert_eq!(SortOrder::default(), SortOrder::Ascending);
    }

    #[test]
    fn sort_order_display() {
        assert_eq!(format!("{}", SortOrder::Ascending), "asc");
        assert_eq!(format!("{}", SortOrder::Descending), "desc");
    }

    #[test]
    fn sort_spec_ascending() {
        let spec = SortSpec::new(
            Expression::Field(FieldRef::new("time".to_owned())),
            SortOrder::Ascending,
        );
        assert_eq!(spec.order(), SortOrder::Ascending);
        assert!(matches!(spec.expr(), Expression::Field(_)));
    }

    #[test]
    fn sort_spec_descending() {
        let spec = SortSpec::new(
            Expression::Field(FieldRef::new("count".to_owned())),
            SortOrder::Descending,
        );
        assert_eq!(spec.order(), SortOrder::Descending);
    }

    #[test]
    fn sort_spec_into_parts() {
        let expr = Expression::Field(FieldRef::new("time".to_owned()));
        let spec = SortSpec::new(expr.clone(), SortOrder::Descending);
        let (e, order) = spec.into_parts();
        assert_eq!(e, expr);
        assert_eq!(order, SortOrder::Descending);
    }

    #[test]
    fn aggregate_expr_with_alias() {
        let agg = AggregateExpr::new(
            "sum".to_owned(),
            Some(Expression::Field(FieldRef::new("bytes".to_owned()))),
            Some("total_bytes".to_owned()),
        );
        assert_eq!(agg.function(), "sum");
        assert!(agg.argument().is_some());
        assert_eq!(agg.alias(), Some("total_bytes"));
    }

    #[test]
    fn aggregate_expr_without_alias() {
        let agg = AggregateExpr::new("count".to_owned(), None, None);
        assert_eq!(agg.function(), "count");
        assert!(agg.argument().is_none());
        assert!(agg.alias().is_none());
    }

    #[test]
    fn aggregate_expr_into_parts() {
        let agg = AggregateExpr::new(
            "avg".to_owned(),
            Some(Expression::Field(FieldRef::new("latency".to_owned()))),
            Some("avg_latency".to_owned()),
        );
        let (func, arg, alias) = agg.into_parts();
        assert_eq!(func, "avg");
        assert!(arg.is_some());
        assert_eq!(alias, Some("avg_latency".to_owned()));
    }

    #[test]
    fn pipeline_stage_filter() {
        let stage = PipelineStage::Filter(Expression::Literal(Literal::Boolean(true)));
        assert!(matches!(stage, PipelineStage::Filter(_)));
    }

    #[test]
    fn pipeline_stage_project() {
        let stage = PipelineStage::Project(vec![
            FieldRef::new("host".to_owned()),
            FieldRef::new("status".to_owned()),
        ]);
        assert!(matches!(stage, PipelineStage::Project(_)));
    }

    #[test]
    fn pipeline_stage_aggregate() {
        let stage = PipelineStage::Aggregate {
            measures: vec![AggregateExpr::new("count".to_owned(), None, None)],
            group_by: vec![FieldRef::new("method".to_owned())],
        };
        assert!(matches!(stage, PipelineStage::Aggregate { .. }));
    }

    #[test]
    fn pipeline_stage_sort() {
        let stage = PipelineStage::Sort(vec![SortSpec::new(
            Expression::Field(FieldRef::new("time".to_owned())),
            SortOrder::Descending,
        )]);
        assert!(matches!(stage, PipelineStage::Sort(_)));
    }

    #[test]
    fn pipeline_stage_limit() {
        let stage = PipelineStage::Limit(10);
        assert!(matches!(stage, PipelineStage::Limit(10)));
    }

    #[test]
    fn pipeline_stage_search() {
        let stage = PipelineStage::Search;
        assert!(matches!(stage, PipelineStage::Search));
    }

    #[test]
    fn pipeline_full_construction() {
        let pipeline = Pipeline::new(
            SourceSpec::new("access_logs".to_owned()),
            TimeRange::new(Some("-24h".to_owned()), None),
            vec![
                PipelineStage::Filter(Expression::Binary(
                    BinaryOperator::Equal,
                    Box::new(Expression::Field(FieldRef::new("status".to_owned()))),
                    Box::new(Expression::Literal(Literal::Number(200.0))),
                )),
                PipelineStage::Sort(vec![SortSpec::new(
                    Expression::Field(FieldRef::new("time".to_owned())),
                    SortOrder::Descending,
                )]),
                PipelineStage::Limit(100),
            ],
        );

        assert_eq!(pipeline.source().dataset(), "access_logs");
        assert_eq!(pipeline.time_range().earliest(), Some("-24h"));
        assert_eq!(pipeline.stages().len(), 3);
    }

    #[test]
    fn pipeline_into_parts() {
        let pipeline = Pipeline::new(
            SourceSpec::new("logs".to_owned()),
            TimeRange::default(),
            vec![PipelineStage::Search],
        );
        let (source, time_range, stages) = pipeline.into_parts();
        assert_eq!(source.dataset(), "logs");
        assert!(time_range.earliest().is_none());
        assert_eq!(stages.len(), 1);
    }

    #[test]
    fn pipeline_empty_stages_is_valid() {
        let pipeline = Pipeline::new(
            SourceSpec::new("logs".to_owned()),
            TimeRange::default(),
            vec![],
        );
        assert_eq!(pipeline.source().dataset(), "logs");
        assert!(pipeline.stages().is_empty());
    }

    #[test]
    fn expr_call_zero_arguments() {
        let expr = Expression::Call("count".to_owned(), vec![]);
        if let Expression::Call(name, args) = &expr {
            assert_eq!(name, "count");
            assert!(args.is_empty());
        } else {
            panic!("expected Call variant");
        }
    }

    #[test]
    fn source_spec_from_str() {
        let spec = SourceSpec::from("metrics");
        assert_eq!(spec.dataset(), "metrics");
    }
}
