use arrow::datatypes::{DataType, TimeUnit};

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

impl LogicalType {
    #[must_use]
    pub fn arrow_data_type(self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Utf8 | Self::Json => DataType::Utf8,
            Self::Datetime => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            Self::Eid => DataType::FixedSizeBinary(16),
        }
    }

    pub(crate) const fn metadata_value(self) -> Option<&'static str> {
        match self {
            Self::Eid => Some("eid"),
            Self::Json => Some("json"),
            Self::Bool
            | Self::Int32
            | Self::Int64
            | Self::UInt32
            | Self::UInt64
            | Self::Float32
            | Self::Float64
            | Self::Utf8
            | Self::Datetime => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum UserLogicalType {
    Bool,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Datetime,
}

impl From<UserLogicalType> for LogicalType {
    fn from(value: UserLogicalType) -> Self {
        match value {
            UserLogicalType::Bool => Self::Bool,
            UserLogicalType::Int32 => Self::Int32,
            UserLogicalType::Int64 => Self::Int64,
            UserLogicalType::UInt32 => Self::UInt32,
            UserLogicalType::UInt64 => Self::UInt64,
            UserLogicalType::Float32 => Self::Float32,
            UserLogicalType::Float64 => Self::Float64,
            UserLogicalType::Utf8 => Self::Utf8,
            UserLogicalType::Datetime => Self::Datetime,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Nullability {
    NonNull,
    Nullable,
}

impl Nullability {
    pub(crate) const fn is_nullable(self) -> bool {
        match self {
            Self::NonNull => false,
            Self::Nullable => true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum FieldRole {
    EventTime,
    IngestTime,
    EventId,
    Data,
    Remainder,
}
