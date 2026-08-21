use std::sync::atomic::{AtomicU64, Ordering};

use elucid_catalog::LogicalType;

const LOGICAL_TYPE_COUNT: usize = 9;

#[derive(Debug, Default)]
pub struct HistoricalConversionMetrics {
    failures: [AtomicU64; LOGICAL_TYPE_COUNT],
}

impl HistoricalConversionMetrics {
    #[must_use]
    pub fn failures(&self, logical_type: LogicalType) -> u64 {
        logical_type_index(logical_type)
            .map_or(0, |index| self.failures[index].load(Ordering::Relaxed))
    }

    pub(crate) fn increment(&self, logical_type: LogicalType) {
        if let Some(index) = logical_type_index(logical_type) {
            self.failures[index].fetch_add(1, Ordering::Relaxed);
        }
    }
}

const fn logical_type_index(logical_type: LogicalType) -> Option<usize> {
    match logical_type {
        LogicalType::Bool => Some(0),
        LogicalType::Int32 => Some(1),
        LogicalType::Int64 => Some(2),
        LogicalType::UInt32 => Some(3),
        LogicalType::UInt64 => Some(4),
        LogicalType::Float32 => Some(5),
        LogicalType::Float64 => Some(6),
        LogicalType::Utf8 => Some(7),
        LogicalType::Datetime => Some(8),
        LogicalType::Eid | LogicalType::Json => None,
        _ => None,
    }
}
