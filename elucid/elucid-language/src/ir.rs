//! Fully resolved and typed intermediate representation for query pipelines.

use std::fmt;

use elucid_catalog::{FieldId, LogicalType, Nullability, SchemaId, SourceId};

use crate::Span;

/// A UTC instant represented exactly as milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UtcInstant(i64);

impl UtcInstant {
    pub const UNIX_EPOCH: Self = Self(0);

    #[must_use]
    pub const fn from_unix_milliseconds(value: i64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn unix_milliseconds(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Literal {
    Null(LogicalType),
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Datetime(UtcInstant),
    Eid([u8; 16]),
}

impl Literal {
    #[must_use]
    pub const fn logical_type(&self) -> LogicalType {
        match self {
            Self::Null(logical_type) => *logical_type,
            Self::Boolean(_) => LogicalType::Bool,
            Self::Int32(_) => LogicalType::Int32,
            Self::Int64(_) => LogicalType::Int64,
            Self::UInt32(_) => LogicalType::UInt32,
            Self::UInt64(_) => LogicalType::UInt64,
            Self::Float32(_) => LogicalType::Float32,
            Self::Float64(_) => LogicalType::Float64,
            Self::Utf8(_) => LogicalType::Utf8,
            Self::Datetime(_) => LogicalType::Datetime,
            Self::Eid(_) => LogicalType::Eid,
        }
    }

    #[must_use]
    pub const fn nullability(&self) -> Nullability {
        match self {
            Self::Null(_) => Nullability::Nullable,
            Self::Boolean(_)
            | Self::Int32(_)
            | Self::Int64(_)
            | Self::UInt32(_)
            | Self::UInt64(_)
            | Self::Float32(_)
            | Self::Float64(_)
            | Self::Utf8(_)
            | Self::Datetime(_)
            | Self::Eid(_) => Nullability::NonNull,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum UnaryOperator {
    Not,
    Negate,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CastKind {
    Lossless,
    Strict,
    NullOnFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RemainderFunction {
    Value,
    Exists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum NullPredicate {
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FieldOrigin {
    Schema { field_id: FieldId },
    System { field_id: FieldId },
    Derived { declaration_span: Span },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Field {
    name: String,
    logical_type: LogicalType,
    nullability: Nullability,
    origin: FieldOrigin,
}

impl Field {
    pub(crate) fn new(
        name: impl Into<String>,
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
    fields: Vec<Field>,
}

impl Relation {
    pub(crate) const fn new(fields: Vec<Field>) -> Self {
        Self { fields }
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
pub enum ExpressionKind {
    Literal(Literal),
    Field(Field),
    Unary {
        operator: UnaryOperator,
        operand: Box<Expression>,
    },
    Binary {
        operator: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Cast {
        kind: CastKind,
        expression: Box<Expression>,
        target: LogicalType,
    },
    Remainder {
        function: RemainderFunction,
        remainder: Field,
        key: String,
    },
    NullPredicate {
        expression: Box<Expression>,
        predicate: NullPredicate,
    },
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Expression {
    kind: ExpressionKind,
    logical_type: LogicalType,
    nullability: Nullability,
}

impl Expression {
    pub(crate) const fn new(
        kind: ExpressionKind,
        logical_type: LogicalType,
        nullability: Nullability,
    ) -> Self {
        Self {
            kind,
            logical_type,
            nullability,
        }
    }

    pub(crate) fn literal(literal: Literal) -> Self {
        let logical_type = literal.logical_type();
        let nullability = literal.nullability();
        Self::new(ExpressionKind::Literal(literal), logical_type, nullability)
    }

    pub(crate) fn field(field: Field) -> Self {
        let logical_type = field.logical_type();
        let nullability = field.nullability();
        Self::new(ExpressionKind::Field(field), logical_type, nullability)
    }

    #[must_use]
    pub const fn kind(&self) -> &ExpressionKind {
        &self.kind
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
    pub fn into_parts(self) -> (ExpressionKind, LogicalType, Nullability) {
        (self.kind, self.logical_type, self.nullability)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Source {
    source_id: SourceId,
    name: String,
    active_schema_id: SchemaId,
}

impl Source {
    pub(crate) fn new(
        source_id: SourceId,
        name: impl Into<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct TimeRange {
    start_inclusive: UtcInstant,
    end_exclusive: UtcInstant,
}

impl TimeRange {
    pub(crate) const fn new(
        start_inclusive: UtcInstant,
        end_exclusive: UtcInstant,
    ) -> Option<Self> {
        if start_inclusive.0 >= end_exclusive.0 {
            return None;
        }
        Some(Self {
            start_inclusive,
            end_exclusive,
        })
    }

    #[must_use]
    pub const fn start_inclusive(self) -> UtcInstant {
        self.start_inclusive
    }

    #[must_use]
    pub const fn end_exclusive(self) -> UtcInstant {
        self.end_exclusive
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
    Take(u64),
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
