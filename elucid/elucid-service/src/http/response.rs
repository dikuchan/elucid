use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use utoipa::ToSchema;

pub(super) const MAXIMUM_OPERATIONAL_LIST_ITEMS: u64 = 100;

pub(super) fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[derive(Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum ListCompletion {
    Complete,
    Truncated,
}

impl ListCompletion {
    pub(super) const fn from_truncated(truncated: bool) -> Self {
        if truncated {
            Self::Truncated
        } else {
            Self::Complete
        }
    }
}
