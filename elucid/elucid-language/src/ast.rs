/// Binary operator in an expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinaryOperator {
    /// Addition.
    Add,
    /// Subtraction.
    Subtract,
    /// Multiplication.
    Multiply,
    /// Division.
    Divide,
    /// Equality.
    Equal,
    /// Inequality.
    NotEqual,
    /// Greater than.
    GreaterThan,
    /// Greater than or equal.
    GreaterThanOrEqual,
    /// Less than.
    LessThan,
    /// Less than or equal.
    LessThanOrEqual,
    /// Logical AND.
    And,
    /// Logical OR.
    Or,
}

/// Expression node in a parsed query.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Expression {
    /// The null literal.
    Null,
    /// A boolean literal (`true` / `false`).
    Boolean(bool),
    /// A numeric literal.
    Number(f64),
    /// A string literal.
    String(String),
    /// A field reference.
    Field(String),
    /// A binary operation.
    Binary(BinaryOperator, Box<Expression>, Box<Expression>),
    /// Logical negation.
    Not(Box<Expression>),
    /// A function call.
    Call(String, Vec<Expression>),
}

/// Sort direction for sort expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SortOrder {
    /// Ascending order.
    Ascending,
    /// Descending order.
    Descending,
}

/// A single sort key with its direction.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SortExpression {
    expression: Expression,
    order: SortOrder,
}

impl SortExpression {
    /// Creates a new sort expression.
    pub fn new(expression: Expression, order: SortOrder) -> Self {
        Self { expression, order }
    }

    /// Returns the expression to sort by.
    pub fn expression(&self) -> &Expression {
        &self.expression
    }

    /// Returns the sort direction.
    pub fn order(&self) -> SortOrder {
        self.order
    }

    /// Consumes the sort expression and returns its components.
    pub fn into_parts(self) -> (Expression, SortOrder) {
        (self.expression, self.order)
    }
}

/// A single piped command in a query.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Command {
    /// `where <expr>`. Filter rows by a predicate.
    Where(Expression),
    /// `sort by <specs>`. Sort by one or more expressions.
    Sort(Vec<SortExpression>),
    /// `head <n>`. Limit output rows.
    Head(i64),
    /// `fields <expr>, ...`. Project specific fields.
    Fields(Vec<Expression>),
    /// `stats <aggregates> [by <fields>]`. Aggregate with optional group-by.
    Stats {
        /// Aggregate expressions paired with optional aliases.
        aggregates: Vec<(Expression, Option<String>)>,
        /// Group-by field expressions.
        by: Vec<Expression>,
    },
}

/// A complete parsed query with source dataset and pipeline commands.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Query {
    source: String,
    commands: Vec<Command>,
}

impl Query {
    /// Creates a new query from a source dataset name and a list of commands.
    pub fn new(source: String, commands: Vec<Command>) -> Self {
        Self { source, commands }
    }

    /// Returns the source dataset name.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Returns the pipeline commands.
    pub fn commands(&self) -> &[Command] {
        &self.commands
    }

    /// Consumes the query and returns its components.
    pub fn into_parts(self) -> (String, Vec<Command>) {
        (self.source, self.commands)
    }
}
