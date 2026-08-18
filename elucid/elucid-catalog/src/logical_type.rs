use std::fmt::{Display, Formatter};

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
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::UInt32 => "uint32",
            Self::UInt64 => "uint64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Utf8 => "utf8",
            Self::Datetime => "datetime",
            Self::Eid => "eid",
            Self::Json => "json",
        }
    }

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

impl Display for LogicalType {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
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

impl UserLogicalType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::UInt32 => "uint32",
            Self::UInt64 => "uint64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Utf8 => "utf8",
            Self::Datetime => "datetime",
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
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NonNull => "NON_NULL",
            Self::Nullable => "NULLABLE",
        }
    }

    pub(crate) const fn is_nullable(self) -> bool {
        match self {
            Self::NonNull => false,
            Self::Nullable => true,
        }
    }
}

impl Display for Nullability {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
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

impl FieldRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EventTime => "EVENT_TIME",
            Self::IngestTime => "INGEST_TIME",
            Self::EventId => "EVENT_ID",
            Self::Data => "DATA",
            Self::Remainder => "REMAINDER",
        }
    }
}
