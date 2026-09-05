use std::sync::Arc;

use axum::Json;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use elucid_metastore::{OperationalLimit, OperationalSegmentState, SegmentInspection};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::runtime::ApplicationState;

use super::catalog::parse_source_id;
use super::error::{ApiError, ErrorEnvelope};
use super::health::readiness_details;
use super::response::{ListCompletion, MAXIMUM_OPERATIONAL_LIST_ITEMS, format_timestamp};

#[utoipa::path(
    get,
    path = "/api/v1/segments",
    tag = "operations",
    summary = "List published segments",
    params(SegmentListQuery),
    responses(
        (status = 200, description = "Bounded operational segment list", body = SegmentListResponse),
        (status = 400, description = "Query parameters are invalid", body = ErrorEnvelope),
        (status = 404, description = "Source does not exist", body = ErrorEnvelope),
        (status = 409, description = "Publication state changed concurrently", body = ErrorEnvelope),
        (status = 500, description = "Publication metadata is corrupt or an internal error occurred", body = ErrorEnvelope),
        (status = 503, description = "Catalog or PostgreSQL is unavailable", body = ErrorEnvelope),
    )
)]
pub(super) async fn list_segments(
    State(state): State<Arc<ApplicationState>>,
    query: Result<Query<SegmentListQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return ApiError::invalid_request().into_response(),
    };
    let source_id = match parse_source_id(&query.source_id) {
        Ok(source_id) => source_id,
        Err(error) => return error.into_response(),
    };
    let state_filter = match query
        .state
        .as_deref()
        .map(OperationalSegmentState::try_from)
    {
        Some(Ok(state_filter)) => Some(state_filter),
        Some(Err(_)) => return ApiError::invalid_request().into_response(),
        None => None,
    };
    let runtime = state.snapshot();
    let Some(dependencies) = runtime.dependencies() else {
        return ApiError::server_not_ready(readiness_details(&runtime)).into_response();
    };
    if dependencies
        .catalog
        .snapshot()
        .source_by_id(source_id)
        .is_none()
    {
        return ApiError::not_found().into_response();
    }
    let limit = match OperationalLimit::new(MAXIMUM_OPERATIONAL_LIST_ITEMS) {
        Ok(limit) => limit,
        Err(_) => return ApiError::internal().into_response(),
    };
    match dependencies
        .operations
        .segments(source_id, state_filter, limit)
        .await
    {
        Ok(segments) => Json(SegmentListResponse {
            completion: ListCompletion::from_truncated(segments.is_truncated()),
            limit: segments.limit(),
            segments: segments.items().iter().map(SegmentResponse::from).collect(),
        })
        .into_response(),
        Err(error) => ApiError::publication(error).into_response(),
    }
}

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub(super) struct SegmentListQuery {
    #[param(format = Uuid)]
    source_id: String,
    state: Option<String>,
}

#[derive(Serialize, ToSchema)]
struct SegmentListResponse {
    completion: ListCompletion,
    limit: usize,
    segments: Vec<SegmentResponse>,
}

#[derive(Serialize, ToSchema)]
struct SegmentResponse {
    #[schema(format = Uuid)]
    segment_id: String,
    #[schema(format = Uuid)]
    source_id: String,
    #[schema(format = Uuid)]
    schema_id: String,
    schema_version: u64,
    state: &'static str,
    origin: &'static str,
    #[schema(format = Date)]
    event_day: String,
    row_count: u64,
    uncompressed_bytes: u64,
    parquet_bytes: u64,
    #[schema(format = DateTime)]
    minimum_event_time: String,
    #[schema(format = DateTime)]
    maximum_event_time: String,
    #[schema(format = DateTime)]
    minimum_ingestion_time: String,
    #[schema(format = DateTime)]
    maximum_ingestion_time: String,
    #[schema(format = DateTime)]
    published_at: Option<String>,
    #[schema(format = DateTime)]
    retired_at: Option<String>,
}

impl From<&SegmentInspection> for SegmentResponse {
    fn from(segment: &SegmentInspection) -> Self {
        Self {
            segment_id: segment.segment_id().to_string(),
            source_id: segment.source_id().to_string(),
            schema_id: segment.schema_id().to_string(),
            schema_version: segment.schema_version().get(),
            state: segment.state().as_str(),
            origin: segment.origin().as_str(),
            event_day: segment.event_day().to_string(),
            row_count: segment.row_count(),
            uncompressed_bytes: segment.uncompressed_bytes(),
            parquet_bytes: segment.parquet_bytes(),
            minimum_event_time: format_timestamp(segment.minimum_event_time()),
            maximum_event_time: format_timestamp(segment.maximum_event_time()),
            minimum_ingestion_time: format_timestamp(segment.minimum_ingestion_time()),
            maximum_ingestion_time: format_timestamp(segment.maximum_ingestion_time()),
            published_at: segment.published_at().map(format_timestamp),
            retired_at: segment.retired_at().map(format_timestamp),
        }
    }
}
