#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub enum Expression {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
    Field(String),
    Binary(BinaryOperator, Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Call(String, Vec<Expression>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

#[derive(Debug, Clone)]
pub struct SortExpression {
    pub expression: Expression,
    pub order: SortOrder,
}

#[derive(Debug, Clone)]
pub enum Command {
    Where(Expression),
    Sort(Vec<SortExpression>),
    Head(i64),
    Select(Vec<Expression>),
    Aggregate {
        aggregates: Vec<(Expression, Option<String>)>,
        by: Vec<Expression>,
    },
}

#[derive(Debug, Clone)]
pub struct Query {
    pub source: String,
    pub commands: Vec<Command>,
}
