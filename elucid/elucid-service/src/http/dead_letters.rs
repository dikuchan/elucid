use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use elucid_metastore::{DeadLetterSummary, OperationalLimit};
use elucid_storage::{ObjectReadRange, StoredObjectId, TransferLimit};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::dead_letter::{DeadLetterDocumentEntry, decode_dead_letters};
use crate::runtime::ApplicationState;

use super::catalog::parse_source_id;
use super::error::{ApiError, ErrorEnvelope};
use super::health::readiness_details;
use super::response::{ListCompletion, MAXIMUM_OPERATIONAL_LIST_ITEMS, format_timestamp};

const MAXIMUM_DEAD_LETTER_RESPONSE_BYTES: u64 = 1_048_576;

#[utoipa::path(
    get,
    path = "/api/v1/dead-letters",
    tag = "ingestion",
    summary = "List published dead-letter objects",
    params(DeadLetterListQuery),
    responses(
        (status = 200, description = "Bounded published dead-letter object list", body = DeadLetterListResponse),
        (status = 400, description = "Query parameters are invalid", body = ErrorEnvelope),
        (status = 404, description = "Source does not exist", body = ErrorEnvelope),
        (status = 409, description = "Publication state changed concurrently", body = ErrorEnvelope),
        (status = 500, description = "Publication metadata is corrupt or an internal error occurred", body = ErrorEnvelope),
        (status = 503, description = "Catalog or PostgreSQL is unavailable", body = ErrorEnvelope),
    )
)]
pub(super) async fn list_dead_letters(
    State(state): State<Arc<ApplicationState>>,
    query: Result<Query<DeadLetterListQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return ApiError::invalid_request().into_response(),
    };
    let source_id = match parse_source_id(&query.source_id) {
        Ok(source_id) => source_id,
        Err(error) => return error.into_response(),
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
    match dependencies.operations.dead_letters(source_id, limit).await {
        Ok(dead_letters) => Json(DeadLetterListResponse {
            completion: ListCompletion::from_truncated(dead_letters.is_truncated()),
            limit: dead_letters.limit(),
            dead_letters: dead_letters
                .items()
                .iter()
                .map(DeadLetterSummaryResponse::from)
                .collect(),
        })
        .into_response(),
        Err(error) => ApiError::publication(error).into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/dead-letters/{object_id}",
    tag = "ingestion",
    summary = "Read one dead-letter object",
    params(("object_id" = String, Path, format = Uuid, description = "Dead-letter object UUID")),
    responses(
        (status = 200, description = "Bounded decoded dead-letter entries", body = DeadLetterReadResponse),
        (status = 400, description = "Object identity is not a UUID", body = ErrorEnvelope),
        (status = 404, description = "Dead-letter object does not exist", body = ErrorEnvelope),
        (status = 500, description = "Stored object is corrupt or an internal error occurred", body = ErrorEnvelope),
        (status = 503, description = "PostgreSQL or object storage is unavailable", body = ErrorEnvelope),
    )
)]
pub(super) async fn read_dead_letter(
    State(state): State<Arc<ApplicationState>>,
    Path(object_id): Path<String>,
) -> Response {
    let object_id = match Uuid::parse_str(&object_id) {
        Ok(object_id) => StoredObjectId::from(object_id),
        Err(_) => return ApiError::invalid_request().into_response(),
    };
    let runtime = state.snapshot();
    let Some(dependencies) = runtime.dependencies() else {
        return ApiError::server_not_ready(readiness_details(&runtime)).into_response();
    };
    let object = match dependencies.operations.dead_letter(object_id).await {
        Ok(Some(object)) => object,
        Ok(None) => return ApiError::not_found().into_response(),
        Err(error) => return ApiError::publication(error).into_response(),
    };
    let transfer_limit = match TransferLimit::new(MAXIMUM_DEAD_LETTER_RESPONSE_BYTES) {
        Ok(limit) => limit,
        Err(_) => return ApiError::internal().into_response(),
    };
    let object_bytes = object.descriptor().expected_byte_size().get();
    let (bytes, completion) = if object_bytes <= MAXIMUM_DEAD_LETTER_RESPONSE_BYTES {
        match dependencies
            .immutable_objects
            .read_exact(object.descriptor(), transfer_limit)
            .await
        {
            Ok(bytes) => (bytes, ListCompletion::Complete),
            Err(error) => return ApiError::storage(error).into_response(),
        }
    } else {
        let range = match ObjectReadRange::new(
            0,
            MAXIMUM_DEAD_LETTER_RESPONSE_BYTES,
            object.descriptor().expected_byte_size(),
        ) {
            Ok(range) => range,
            Err(_) => return ApiError::internal().into_response(),
        };
        let prefix = match dependencies
            .immutable_objects
            .read_range(object.descriptor(), range, transfer_limit)
            .await
        {
            Ok(prefix) => prefix,
            Err(error) => return ApiError::storage(error).into_response(),
        };
        let Some(last_delimiter) = prefix.iter().rposition(|byte| *byte == b'\n') else {
            return ApiError::object_integrity().into_response();
        };
        (prefix.slice(..=last_delimiter), ListCompletion::Truncated)
    };
    match decode_dead_letters(&bytes) {
        Ok(entries) => Json(DeadLetterReadResponse {
            object: DeadLetterSummaryResponse::from(object.summary()),
            completion,
            limit_bytes: MAXIMUM_DEAD_LETTER_RESPONSE_BYTES,
            entries,
        })
        .into_response(),
        Err(_) => ApiError::object_integrity().into_response(),
    }
}

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub(super) struct DeadLetterListQuery {
    #[param(format = Uuid)]
    source_id: String,
}

#[derive(Serialize, ToSchema)]
struct DeadLetterListResponse {
    completion: ListCompletion,
    limit: usize,
    dead_letters: Vec<DeadLetterSummaryResponse>,
}

#[derive(Serialize, ToSchema)]
struct DeadLetterSummaryResponse {
    #[schema(format = Uuid)]
    object_id: String,
    #[schema(format = Uuid)]
    source_id: String,
    #[schema(format = Uuid)]
    input_id: String,
    #[schema(format = Uuid)]
    batch_id: String,
    byte_size: u64,
    #[schema(format = DateTime)]
    published_at: String,
    #[schema(format = DateTime)]
    retention_deadline: String,
}

impl From<&DeadLetterSummary> for DeadLetterSummaryResponse {
    fn from(summary: &DeadLetterSummary) -> Self {
        Self {
            object_id: summary.object_id().to_string(),
            source_id: summary.source_id().to_string(),
            input_id: summary.input_id().to_string(),
            batch_id: summary.batch_id().to_string(),
            byte_size: summary.byte_size(),
            published_at: format_timestamp(summary.published_at()),
            retention_deadline: format_timestamp(summary.retention_deadline()),
        }
    }
}

#[derive(Serialize, ToSchema)]
struct DeadLetterReadResponse {
    object: DeadLetterSummaryResponse,
    completion: ListCompletion,
    limit_bytes: u64,
    entries: Vec<DeadLetterDocumentEntry>,
}
