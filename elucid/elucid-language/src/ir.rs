//! Resolved intermediate representation for query pipelines.

use std::fmt;
use std::sync::Arc;

use elucid_catalog::{FieldId, LogicalType, Nullability, SchemaId, SourceId};

use crate::Span;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Literal {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
}

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FieldOrigin {
    Schema { field_id: FieldId },
    System { field_id: FieldId },
    Remainder { key: Box<str> },
    Derived { declaration_span: Span },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Field {
    name: Box<str>,
    logical_type: LogicalType,
    nullability: Nullability,
    origin: FieldOrigin,
}

impl Field {
    pub(crate) fn new(
        name: impl Into<Box<str>>,
        logical_type: LogicalType,
        nullability: Nullability,
        origin: FieldOrigin,
    ) -> Self {
        Self {
            name: name.into(),
            logical_type,
            nullability,
            origin,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn logical_type(&self) -> LogicalType {
        self.logical_type
    }

    #[must_use]
    pub const fn nullability(&self) -> Nullability {
        self.nullability
    }

    #[must_use]
    pub const fn origin(&self) -> &FieldOrigin {
        &self.origin
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Relation {
    fields: Arc<[Field]>,
}

impl Relation {
    pub(crate) fn new(fields: Vec<Field>) -> Self {
        Self {
            fields: fields.into(),
        }
    }

    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    #[must_use]
    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name() == name)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Expression {
    Literal(Literal),
    Field(Field),
    Binary(BinaryOperator, Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Source {
    source_id: SourceId,
    name: Box<str>,
    active_schema_id: SchemaId,
}

impl Source {
    pub(crate) fn new(
        source_id: SourceId,
        name: impl Into<Box<str>>,
        active_schema_id: SchemaId,
    ) -> Self {
        Self {
            source_id,
            name: name.into(),
            active_schema_id,
        }
    }

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn active_schema_id(&self) -> SchemaId {
        self.active_schema_id
    }
}

impl fmt::Display for Source {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct TimeRange {
    start_inclusive: Option<Box<str>>,
    end_exclusive: Option<Box<str>>,
}

impl TimeRange {
    #[must_use]
    pub fn start_inclusive(&self) -> Option<&str> {
        self.start_inclusive.as_deref()
    }

    #[must_use]
    pub fn end_exclusive(&self) -> Option<&str> {
        self.end_exclusive.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Projection {
    expression: Expression,
    output_field: Field,
}

impl Projection {
    pub(crate) const fn new(expression: Expression, output_field: Field) -> Self {
        Self {
            expression,
            output_field,
        }
    }

    #[must_use]
    pub const fn expression(&self) -> &Expression {
        &self.expression
    }

    #[must_use]
    pub const fn output_field(&self) -> &Field {
        &self.output_field
    }

    #[must_use]
    pub fn into_parts(self) -> (Expression, Field) {
        (self.expression, self.output_field)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SortSpec {
    field: Field,
    order: SortOrder,
}

impl SortSpec {
    pub(crate) const fn new(field: Field, order: SortOrder) -> Self {
        Self { field, order }
    }

    #[must_use]
    pub const fn field(&self) -> &Field {
        &self.field
    }

    #[must_use]
    pub const fn order(&self) -> SortOrder {
        self.order
    }

    #[must_use]
    pub fn into_parts(self) -> (Field, SortOrder) {
        (self.field, self.order)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum SortOrder {
    #[default]
    Ascending,
    Descending,
}

impl fmt::Display for SortOrder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ascending => formatter.write_str("asc"),
            Self::Descending => formatter.write_str("desc"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Average,
}

impl AggregateFunction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Average => "avg",
        }
    }
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct AggregateExpression {
    function: AggregateFunction,
    argument: Option<Field>,
    output_field: Field,
}

impl AggregateExpression {
    pub(crate) const fn new(
        function: AggregateFunction,
        argument: Option<Field>,
        output_field: Field,
    ) -> Self {
        Self {
            function,
            argument,
            output_field,
        }
    }

    #[must_use]
    pub const fn function(&self) -> AggregateFunction {
        self.function
    }

    #[must_use]
    pub const fn argument(&self) -> Option<&Field> {
        self.argument.as_ref()
    }

    #[must_use]
    pub const fn output_field(&self) -> &Field {
        &self.output_field
    }

    #[must_use]
    pub fn into_parts(self) -> (AggregateFunction, Option<Field>, Field) {
        (self.function, self.argument, self.output_field)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum StageKind {
    Filter(Expression),
    Project(Vec<Projection>),
    Aggregate {
        measures: Vec<AggregateExpression>,
        group_by: Vec<Field>,
    },
    Sort(Vec<SortSpec>),
    Take(usize),
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Stage {
    kind: StageKind,
    output_relation: Relation,
}

impl Stage {
    pub(crate) const fn new(kind: StageKind, output_relation: Relation) -> Self {
        Self {
            kind,
            output_relation,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> &StageKind {
        &self.kind
    }

    #[must_use]
    pub const fn output_relation(&self) -> &Relation {
        &self.output_relation
    }

    #[must_use]
    pub fn into_parts(self) -> (StageKind, Relation) {
        (self.kind, self.output_relation)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Pipeline {
    source: Source,
    time_range: TimeRange,
    source_relation: Relation,
    stages: Vec<Stage>,
}

impl Pipeline {
    pub(crate) fn new(
        source: Source,
        time_range: TimeRange,
        source_relation: Relation,
        stages: Vec<Stage>,
    ) -> Self {
        Self {
            source,
            time_range,
            source_relation,
            stages,
        }
    }

    #[must_use]
    pub const fn source(&self) -> &Source {
        &self.source
    }

    #[must_use]
    pub const fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    #[must_use]
    pub const fn source_relation(&self) -> &Relation {
        &self.source_relation
    }

    #[must_use]
    pub fn stages(&self) -> &[Stage] {
        &self.stages
    }

    #[must_use]
    pub fn output_relation(&self) -> &Relation {
        self.stages
            .last()
            .map_or(&self.source_relation, Stage::output_relation)
    }

    #[must_use]
    pub fn into_parts(self) -> (Source, TimeRange, Relation, Vec<Stage>) {
        (
            self.source,
            self.time_range,
            self.source_relation,
            self.stages,
        )
    }
}
