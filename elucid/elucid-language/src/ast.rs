use std::num::NonZeroU64;

use crate::span::Span;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct Identifier {
    value: String,
    span: Span,
}

impl Identifier {
    pub(crate) fn new(value: impl Into<String>, span: Span) -> Self {
        Self {
            value: value.into(),
            span,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct SystemIdentifier {
    value: String,
    span: Span,
}

impl SystemIdentifier {
    pub(crate) fn new(value: impl Into<String>, span: Span) -> Self {
        Self {
            value: value.into(),
            span,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum FieldReference {
    User(Identifier),
    System(SystemIdentifier),
}

impl FieldReference {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::User(identifier) => identifier.as_str(),
            Self::System(identifier) => identifier.as_str(),
        }
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::User(identifier) => identifier.span(),
            Self::System(identifier) => identifier.span(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StringLiteral {
    value: String,
    span: Span,
}

impl StringLiteral {
    pub(crate) fn new(value: impl Into<String>, span: Span) -> Self {
        Self {
            value: value.into(),
            span,
        }
    }

    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LiteralKind {
    Null,
    Boolean(bool),
    Integer(u64),
    FloatingPoint(String),
    String(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Literal {
    kind: LiteralKind,
    span: Span,
}

impl Literal {
    pub(crate) const fn new(kind: LiteralKind, span: Span) -> Self {
        Self { kind, span }
    }

    #[must_use]
    pub const fn kind(&self) -> &LiteralKind {
        &self.kind
    }

    #[must_use]
    pub fn as_string(&self) -> Option<&str> {
        match &self.kind {
            LiteralKind::String(value) => Some(value),
            LiteralKind::Null
            | LiteralKind::Boolean(_)
            | LiteralKind::Integer(_)
            | LiteralKind::FloatingPoint(_) => None,
        }
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum NumericSign {
    NonNegative,
    Negative,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NumericLiteralKind {
    Integer(u64),
    FloatingPoint(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SignedNumericLiteral {
    sign: NumericSign,
    kind: NumericLiteralKind,
    span: Span,
}

impl SignedNumericLiteral {
    pub(crate) const fn new(sign: NumericSign, kind: NumericLiteralKind, span: Span) -> Self {
        Self { sign, kind, span }
    }

    #[must_use]
    pub const fn sign(&self) -> NumericSign {
        self.sign
    }

    #[must_use]
    pub const fn kind(&self) -> &NumericLiteralKind {
        &self.kind
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SignedIntegerLiteral {
    sign: NumericSign,
    magnitude: u64,
    span: Span,
}

impl SignedIntegerLiteral {
    pub(crate) const fn new(sign: NumericSign, magnitude: u64, span: Span) -> Self {
        Self {
            sign,
            magnitude,
            span,
        }
    }

    #[must_use]
    pub const fn sign(&self) -> NumericSign {
        self.sign
    }

    #[must_use]
    pub const fn magnitude(&self) -> u64 {
        self.magnitude
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum NumericType {
    Int32,
    Int64,
    UInt32,
    UInt64,
    Float32,
    Float64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum LogicalType {
    Bool,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Datetime,
    Eid,
    Json,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConstructorKind {
    Numeric {
        target: NumericType,
        literal: SignedNumericLiteral,
    },
    Datetime(StringLiteral),
    Eid(StringLiteral),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Constructor {
    kind: ConstructorKind,
    span: Span,
}

impl Constructor {
    pub(crate) const fn new(kind: ConstructorKind, span: Span) -> Self {
        Self { kind, span }
    }

    #[must_use]
    pub const fn kind(&self) -> &ConstructorKind {
        &self.kind
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum CastKind {
    Strict,
    NullOnFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CastExpression {
    kind: CastKind,
    expression: Box<Expression>,
    target: LogicalType,
    span: Span,
}

impl CastExpression {
    pub(crate) fn new(
        kind: CastKind,
        expression: Expression,
        target: LogicalType,
        span: Span,
    ) -> Self {
        Self {
            kind,
            expression: Box::new(expression),
            target,
            span,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> CastKind {
        self.kind
    }

    #[must_use]
    pub const fn expression(&self) -> &Expression {
        &self.expression
    }

    #[must_use]
    pub const fn target(&self) -> LogicalType {
        self.target
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum RemainderFunction {
    Value,
    Exists,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RemainderExpression {
    function: RemainderFunction,
    key: StringLiteral,
    span: Span,
}

impl RemainderExpression {
    pub(crate) const fn new(function: RemainderFunction, key: StringLiteral, span: Span) -> Self {
        Self {
            function,
            key,
            span,
        }
    }

    #[must_use]
    pub const fn function(&self) -> RemainderFunction {
        self.function
    }

    #[must_use]
    pub const fn key(&self) -> &StringLiteral {
        &self.key
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum UnaryOperator {
    Not,
    Negate,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExpressionKind {
    Literal(Literal),
    Field(FieldReference),
    Constructor(Constructor),
    Cast(CastExpression),
    Remainder(RemainderExpression),
    Unary {
        operator: UnaryOperator,
        operand: Box<Expression>,
    },
    Binary {
        operator: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Expression {
    kind: ExpressionKind,
    span: Span,
}

impl Expression {
    pub(crate) const fn new(kind: ExpressionKind, span: Span) -> Self {
        Self { kind, span }
    }

    #[must_use]
    pub const fn kind(&self) -> &ExpressionKind {
        &self.kind
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }

    pub(crate) const fn with_span(mut self, span: Span) -> Self {
        self.span = span;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum TimeDirection {
    Forward,
    Backward,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TimeOperationKind {
    Truncate(TimeUnit),
    Shift {
        direction: TimeDirection,
        magnitude: NonZeroU64,
        unit: TimeUnit,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TimeOperation {
    kind: TimeOperationKind,
    span: Span,
}

impl TimeOperation {
    pub(crate) const fn new(kind: TimeOperationKind, span: Span) -> Self {
        Self { kind, span }
    }

    #[must_use]
    pub const fn kind(&self) -> &TimeOperationKind {
        &self.kind
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TimeExpressionKind {
    Datetime(StringLiteral),
    Now,
    Relative(Vec<TimeOperation>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TimeExpression {
    kind: TimeExpressionKind,
    span: Span,
}

impl TimeExpression {
    pub(crate) const fn new(kind: TimeExpressionKind, span: Span) -> Self {
        Self { kind, span }
    }

    #[must_use]
    pub const fn kind(&self) -> &TimeExpressionKind {
        &self.kind
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SourceExpression {
    name: Identifier,
    start_inclusive: Option<TimeExpression>,
    end_exclusive: Option<TimeExpression>,
    span: Span,
}

impl SourceExpression {
    pub(crate) const fn new(
        name: Identifier,
        start_inclusive: Option<TimeExpression>,
        end_exclusive: Option<TimeExpression>,
        span: Span,
    ) -> Self {
        Self {
            name,
            start_inclusive,
            end_exclusive,
            span,
        }
    }

    #[must_use]
    pub const fn name(&self) -> &Identifier {
        &self.name
    }

    #[must_use]
    pub const fn start_inclusive(&self) -> Option<&TimeExpression> {
        self.start_inclusive.as_ref()
    }

    #[must_use]
    pub const fn end_exclusive(&self) -> Option<&TimeExpression> {
        self.end_exclusive.as_ref()
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ComputedProjection {
    alias: Identifier,
    expression: Expression,
    span: Span,
}

impl ComputedProjection {
    pub(crate) const fn new(alias: Identifier, expression: Expression, span: Span) -> Self {
        Self {
            alias,
            expression,
            span,
        }
    }

    #[must_use]
    pub const fn alias(&self) -> &Identifier {
        &self.alias
    }

    #[must_use]
    pub const fn expression(&self) -> &Expression {
        &self.expression
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Projection {
    Field(FieldReference),
    Computed(ComputedProjection),
    Unaliased(Expression),
}

impl Projection {
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Field(field) => field.span(),
            Self::Computed(projection) => projection.span(),
            Self::Unaliased(expression) => expression.span(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SortItem {
    field: FieldReference,
    direction: SortDirection,
    span: Span,
}

impl SortItem {
    pub(crate) const fn new(field: FieldReference, direction: SortDirection, span: Span) -> Self {
        Self {
            field,
            direction,
            span,
        }
    }

    #[must_use]
    pub const fn field(&self) -> &FieldReference {
        &self.field
    }

    #[must_use]
    pub const fn direction(&self) -> SortDirection {
        self.direction
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl AggregateFunction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct AggregateCall {
    function: AggregateFunction,
    argument: Option<FieldReference>,
    span: Span,
}

impl AggregateCall {
    pub(crate) const fn new(
        function: AggregateFunction,
        argument: Option<FieldReference>,
        span: Span,
    ) -> Self {
        Self {
            function,
            argument,
            span,
        }
    }

    #[must_use]
    pub const fn function(&self) -> AggregateFunction {
        self.function
    }

    #[must_use]
    pub const fn argument(&self) -> Option<&FieldReference> {
        self.argument.as_ref()
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Measure {
    alias: Option<Identifier>,
    aggregate: AggregateCall,
    span: Span,
}

impl Measure {
    pub(crate) const fn new(
        alias: Option<Identifier>,
        aggregate: AggregateCall,
        span: Span,
    ) -> Self {
        Self {
            alias,
            aggregate,
            span,
        }
    }

    #[must_use]
    pub const fn alias(&self) -> Option<&Identifier> {
        self.alias.as_ref()
    }

    #[must_use]
    pub const fn aggregate(&self) -> &AggregateCall {
        &self.aggregate
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StageKind {
    Filter(Expression),
    Project(Vec<Projection>),
    Sort(Vec<SortItem>),
    Take(SignedIntegerLiteral),
    Summarize {
        measures: Vec<Measure>,
        group_by: Vec<FieldReference>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Stage {
    kind: StageKind,
    span: Span,
}

impl Stage {
    pub(crate) const fn new(kind: StageKind, span: Span) -> Self {
        Self { kind, span }
    }

    #[must_use]
    pub const fn kind(&self) -> &StageKind {
        &self.kind
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Query {
    source: SourceExpression,
    stages: Vec<Stage>,
    span: Span,
}

impl Query {
    pub(crate) const fn new(source: SourceExpression, stages: Vec<Stage>, span: Span) -> Self {
        Self {
            source,
            stages,
            span,
        }
    }

    #[must_use]
    pub const fn source(&self) -> &SourceExpression {
        &self.source
    }

    #[must_use]
    pub fn stages(&self) -> &[Stage] {
        &self.stages
    }

    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}
