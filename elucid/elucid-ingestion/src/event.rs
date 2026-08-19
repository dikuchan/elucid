use std::fmt::Debug;

pub trait EventContext: Debug + Clone + Send + Sync + 'static {
    fn to_json(&self) -> serde_json::Value;
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RawEvent<C: EventContext> {
    pub raw: String,
    pub context: C,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Event<C: EventContext> {
    pub row: EventRow,
    pub context: C,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EventRow {
    pub values: Vec<EventValue>,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum EventValue {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(String),
    Timestamp(i64),
}
