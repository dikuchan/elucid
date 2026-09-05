use axum::body::{Body, Bytes as BodyBytes};
use axum::http::HeaderMap;
use axum::http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use futures::StreamExt as _;

use super::error::ApiError;

pub(super) async fn read_bounded_body(
    body: Body,
    content_length: Option<u64>,
    maximum_body_bytes: u64,
) -> Result<BodyBytes, BodyReadFailure> {
    let initial_capacity = content_length.unwrap_or(0).min(maximum_body_bytes);
    let initial_capacity =
        usize::try_from(initial_capacity).map_err(|_| BodyReadFailure::Internal)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(initial_capacity)
        .map_err(|_| BodyReadFailure::Internal)?;
    let mut body_bytes = 0_u64;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BodyReadFailure::Invalid)?;
        let chunk_bytes = u64::try_from(chunk.len()).map_err(|_| BodyReadFailure::Internal)?;
        body_bytes = body_bytes
            .checked_add(chunk_bytes)
            .ok_or(BodyReadFailure::LimitExceeded)?;
        if body_bytes > maximum_body_bytes {
            return Err(BodyReadFailure::LimitExceeded);
        }
        bytes
            .try_reserve_exact(chunk.len())
            .map_err(|_| BodyReadFailure::Internal)?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(BodyBytes::from(bytes))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BodyReadFailure {
    Invalid,
    LimitExceeded,
    Internal,
}

pub(super) fn has_yaml_content_type(headers: &HeaderMap) -> bool {
    has_content_type(headers, "application/yaml")
}

pub(super) fn has_ndjson_content_type(headers: &HeaderMap) -> bool {
    has_content_type(headers, "application/x-ndjson")
}

pub(super) fn has_json_content_type(headers: &HeaderMap) -> bool {
    has_content_type(headers, "application/json")
}

fn has_content_type(headers: &HeaderMap, expected: &str) -> bool {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case(expected))
}

pub(super) fn has_identity_content_encoding(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(CONTENT_ENCODING).iter();
    let Some(value) = values.next() else {
        return true;
    };
    if values.next().is_some() {
        return false;
    }
    value
        .to_str()
        .is_ok_and(|value| value.trim().eq_ignore_ascii_case("identity"))
}

pub(super) fn parse_content_length(headers: &HeaderMap) -> Result<Option<u64>, ApiError> {
    let mut values = headers.get_all(CONTENT_LENGTH).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(ApiError::invalid_request());
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .map(Some)
        .ok_or_else(ApiError::invalid_request)
}
