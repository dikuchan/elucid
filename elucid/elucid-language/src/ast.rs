/// Binary operator in an expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinaryOp {
    /// Addition (`+`).
    Add,
    /// Subtraction (`-`).
    Subtract,
    /// Multiplication (`*`).
    Multiply,
    /// Division (`/`).
    Divide,
    /// Equality (`==`).
    Equal,
    /// Inequality (`!=`).
    NotEqual,
    /// Greater than (`>`).
    GreaterThan,
    /// Greater than or equal (`>=`).
    GreaterThanOrEqual,
    /// Less than (`<`).
    LessThan,
    /// Less than or equal (`<=`).
    LessThanOrEqual,
    /// Logical AND (`and`).
    And,
    /// Logical OR (`or`).
    Or,
}

/// Expression node in a parsed query.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Expr {
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
    /// A binary operation (`lhs op rhs`).
    Binary(BinaryOp, Box<Expr>, Box<Expr>),
    /// Logical negation (`not expr`).
    Not(Box<Expr>),
    /// A function call (e.g., `count()`, `sum(bytes)`).
    Call(String, Vec<Expr>),
}

/// Sort direction for sort expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SortOrder {
    /// Ascending order (smallest first).
    Ascending,
    /// Descending order (largest first).
    Descending,
}

/// A single sort key with its direction.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SortExpression {
    expression: Expr,
    order: SortOrder,
}

impl SortExpression {
    /// Creates a new sort expression.
    pub fn new(expression: Expr, order: SortOrder) -> Self {
        Self { expression, order }
    }

    /// Returns the expression to sort by.
    pub fn expression(&self) -> &Expr {
        &self.expression
    }

    /// Returns the sort direction.
    pub fn order(&self) -> SortOrder {
        self.order
    }

    /// Consumes the sort expression and returns its components.
    pub fn into_parts(self) -> (Expr, SortOrder) {
        (self.expression, self.order)
    }
}

/// A single piped command in a query.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Command {
    /// `where <expr>` — filter rows by a predicate.
    Where(Expr),
    /// `sort by <specs>` — sort by one or more expressions.
    Sort(Vec<SortExpression>),
    /// `head <n>` — limit output rows.
    Head(i64),
    /// `fields <expr>, ...` — project specific fields.
    Fields(Vec<Expr>),
    /// `stats <aggregates> [by <fields>]` — aggregate with optional group-by.
    Stats {
        /// Aggregate expressions paired with optional aliases.
        aggregates: Vec<(Expr, Option<String>)>,
        /// Group-by field expressions.
        by: Vec<Expr>,
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
