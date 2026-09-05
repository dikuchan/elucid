use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use uuid::Uuid;

use super::error::ApiError;

const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

pub(super) async fn enforce_request_timeout(
    State(timeout): State<Duration>,
    request: Request,
    next: Next,
) -> Response {
    match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => ApiError::request_timed_out().into_response(),
    }
}

pub(super) async fn request_identity(request: Request, next: Next) -> Response {
    let request_id = request
        .headers()
        .get(&X_REQUEST_ID)
        .filter(|value| {
            value
                .to_str()
                .ok()
                .is_some_and(|value| Uuid::parse_str(value).is_ok())
        })
        .cloned()
        .unwrap_or_else(generated_request_identity);
    let mut response = next.run(request).await;
    response.headers_mut().insert(X_REQUEST_ID, request_id);
    response
}

fn generated_request_identity() -> HeaderValue {
    HeaderValue::from_str(&Uuid::now_v7().to_string())
        .unwrap_or_else(|_| HeaderValue::from_static("00000000-0000-0000-0000-000000000000"))
}
