use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes as BodyBytes};
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, SecondsFormat, Utc};
use futures::StreamExt as _;
use serde::ser::SerializeStruct as _;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use elucid_catalog::{
    CatalogApplicationError, CatalogErrorCode, CatalogManifest, Field, IngestionProfileRevision,
    Input, InputName, Schema, Source, SourceId, SourceName,
};
use elucid_engine::{
    EngineError, EngineErrorCode, QueryColumn, QueryExecutionStatistics, QueryOutputRowLimitError,
    QueryResult,
};
use elucid_ingestion::{
    BatchId, BatchMetadata, IngestionTime, MAXIMUM_BATCH_EVENT_DAYS, PinnedCatalogIdentities,
    SpoolErrorCode,
};
use elucid_metastore::{
    CatalogApplyOutcome, CatalogPersistenceError, CatalogPersistenceErrorKind, DeadLetterSummary,
    OperationalLimit, OperationalSegmentState, PublicationError, PublicationErrorKind,
    QueryRequestTimeRange, QuerySnapshotError, QuerySnapshotErrorKind, SegmentInspection,
};
use elucid_storage::{
    ObjectReadRange, StorageError, StorageErrorCode, StoredObjectId, TransferLimit,
};

use crate::dead_letter::{DeadLetterDocumentEntry, decode_dead_letters};
use crate::ingestion::{
    AdmissionFailure, AdmittedAppend, IngestionAvailability, IngestionStatus,
    MAXIMUM_HTTP_BATCH_RECORDS,
};
use crate::query::{CompletedQuery, QueryAdmissionFailure, QueryFailure};
use crate::runtime::{
    ApplicationState, ComponentHealth, ComponentStatus, MaintenanceOwnership, RuntimeSnapshot,
};

const MAXIMUM_CATALOG_DOCUMENT_BYTES: usize = 1_048_576;
const MAXIMUM_QUERY_REQUEST_BYTES: u64 = 1_048_576;
const MAXIMUM_SOURCE_LIST_ITEMS: usize = 100;
const MAXIMUM_OPERATIONAL_LIST_ITEMS: u64 = 100;
const MAXIMUM_DEAD_LETTER_RESPONSE_BYTES: u64 = 1_048_576;
const RETRY_AFTER_SECONDS: u64 = 1;
const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

pub(crate) fn router(state: Arc<ApplicationState>) -> Router {
    let request_timeout = Duration::from_secs(
        state
            .configuration()
            .server()
            .request_timeout_seconds()
            .get(),
    );
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/metrics", get(metrics))
        .route("/api/v1/status", get(status))
        .route(
            "/api/v1/catalog-applications",
            post(apply_catalog).layer(DefaultBodyLimit::max(MAXIMUM_CATALOG_DOCUMENT_BYTES)),
        )
        .route("/api/v1/sources", get(list_sources))
        .route("/api/v1/sources/{source_id}", get(get_source))
        .route("/api/v1/segments", get(list_segments))
        .route("/api/v1/dead-letters", get(list_dead_letters))
        .route("/api/v1/dead-letters/{object_id}", get(read_dead_letter))
        .route("/api/v1/query-executions", post(execute_query))
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(ui_or_not_found)
        .layer(middleware::from_fn_with_state(
            request_timeout,
            enforce_request_timeout,
        ))
        .route(
            "/api/v1/sources/{source_name}/inputs/{input_name}/events",
            post(ingest_events),
        )
        .layer(middleware::from_fn(request_identity))
        .with_state(state)
}

async fn liveness() -> Json<LivenessResponse> {
    Json(LivenessResponse { status: "UP" })
}

async fn readiness(State(state): State<Arc<ApplicationState>>) -> Response {
    let snapshot = state.snapshot();
    if snapshot.is_ready() {
        return Json(ReadinessResponse {
            status: "READY",
            components: snapshot.health(),
        })
        .into_response();
    }
    ApiError::server_not_ready(readiness_details(&snapshot)).into_response()
}

async fn status(State(state): State<Arc<ApplicationState>>) -> Json<StatusResponse> {
    let snapshot = state.snapshot();
    let configuration = state.configuration();
    let phase = service_phase(&snapshot);
    let (
        spool_used_bytes,
        pending_batches,
        oldest_queued_age_seconds,
        ingestion_availability,
        maintenance_ownership,
    ) = snapshot.dependencies().map_or(
        (
            0,
            0,
            None,
            IngestionAvailability::Unavailable,
            status_maintenance_without_dependencies(configuration),
        ),
        |dependencies| {
            let ingestion = dependencies.ingestion.status();
            (
                ingestion.used_bytes(),
                ingestion.pending_batches(),
                ingestion.oldest_queued_age_seconds(),
                dependencies.ingestion.availability(),
                StatusMaintenanceOwnership::from(dependencies.maintenance.ownership()),
            )
        },
    );
    let admission =
        if snapshot.is_ready() && ingestion_availability == IngestionAvailability::Available {
            AdmissionState::Open
        } else {
            AdmissionState::Closed
        };
    let publication_status = match snapshot.dependencies() {
        Some(dependencies) if snapshot.health().postgresql == ComponentStatus::Up => {
            match dependencies.operations.publication_backlog().await {
                Ok(backlog) => {
                    state.metrics().update_publication_backlog(backlog);
                    ComponentStatus::Up
                }
                Err(_) => ComponentStatus::Down,
            }
        }
        Some(_) | None => ComponentStatus::Down,
    };
    let publication_backlog = state.metrics().publication_backlog();
    Json(StatusResponse {
        phase,
        admission,
        components: snapshot.health(),
        limits: EffectiveLimits::from_configuration(configuration),
        spool: SpoolStatus {
            capacity_bytes: configuration.local_storage().spool_capacity_bytes().get(),
            used_bytes: spool_used_bytes,
            pending_batches,
            oldest_queued_age_seconds,
        },
        publication: PublicationStatus {
            status: publication_status,
            pending_batches,
            prepared_segments: publication_backlog.prepared_segments,
            planned_objects: publication_backlog.planned_objects,
            uploaded_objects: publication_backlog.uploaded_objects,
        },
        maintenance: MaintenanceStatus {
            ownership: maintenance_ownership,
            recent_compactions: [],
        },
    })
}

async fn metrics(State(state): State<Arc<ApplicationState>>) -> Response {
    let snapshot = state.snapshot();
    let ingestion = snapshot
        .dependencies()
        .map(|dependencies| dependencies.ingestion.status());
    state.metrics().update_spool(
        ingestion.map_or(0, IngestionStatus::used_bytes),
        ingestion.map_or(0, IngestionStatus::pending_batches),
    );
    match state.metrics().encode() {
        Ok(body) => (
            [(
                CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn apply_catalog(
    State(state): State<Arc<ApplicationState>>,
    headers: HeaderMap,
    body: Result<BodyBytes, BytesRejection>,
) -> Response {
    if !has_yaml_content_type(&headers) {
        return ApiError::unsupported_catalog_media_type().into_response();
    }
    let body = match body {
        Ok(body) => body,
        Err(_) => return ApiError::catalog_request_too_large().into_response(),
    };
    let manifest = match CatalogManifest::decode(&body) {
        Ok(manifest) => manifest,
        Err(error) => return ApiError::catalog(error).into_response(),
    };
    let snapshot = state.snapshot();
    let Some(dependencies) = snapshot.dependencies() else {
        return ApiError::server_not_ready(readiness_details(&snapshot)).into_response();
    };
    if snapshot.health().postgresql != ComponentStatus::Up {
        return ApiError::metastore_unavailable().into_response();
    }
    match dependencies.catalog.apply(&manifest).await {
        Ok(outcome) => match CatalogApplicationResponse::from_outcome(outcome) {
            Ok(response) => Json(response).into_response(),
            Err(error) => error.into_response(),
        },
        Err(error) => ApiError::catalog_persistence(error).into_response(),
    }
}

async fn ingest_events(
    State(state): State<Arc<ApplicationState>>,
    Path((source_name, input_name)): Path<(String, String)>,
    request: Request,
) -> Response {
    let request_timeout = Duration::from_secs(
        state
            .configuration()
            .server()
            .request_timeout_seconds()
            .get(),
    );
    let request_deadline = Instant::now() + request_timeout;
    if !has_ndjson_content_type(request.headers()) {
        return reject_ingestion(&state, ApiError::unsupported_ingestion_media_type());
    }
    if !has_identity_content_encoding(request.headers()) {
        return reject_ingestion(&state, ApiError::unsupported_ingestion_encoding());
    }
    let content_length = match parse_content_length(request.headers()) {
        Ok(content_length) => content_length,
        Err(error) => return reject_ingestion(&state, error),
    };
    let maximum_body_bytes = state
        .configuration()
        .ingestion()
        .maximum_http_batch_bytes()
        .get();
    if content_length.is_some_and(|length| length > maximum_body_bytes) {
        return reject_ingestion(&state, ApiError::ingestion_batch_limit_exceeded());
    }

    let snapshot = state.snapshot();
    if !snapshot.is_ready() {
        if snapshot.is_draining() {
            return reject_ingestion(&state, ApiError::server_draining());
        }
        return reject_ingestion(
            &state,
            ApiError::server_not_ready(readiness_details(&snapshot)),
        );
    }
    let Some(dependencies) = snapshot.dependencies() else {
        return reject_ingestion(
            &state,
            ApiError::server_not_ready(readiness_details(&snapshot)),
        );
    };
    let source_name = match SourceName::try_from(source_name) {
        Ok(source_name) => source_name,
        Err(_) => return reject_ingestion(&state, ApiError::invalid_request()),
    };
    let input_name = match InputName::try_from(input_name) {
        Ok(input_name) => input_name,
        Err(_) => return reject_ingestion(&state, ApiError::invalid_request()),
    };
    let catalog = dependencies.catalog.snapshot();
    let Some(source) = catalog.source_by_name(&source_name) else {
        return reject_ingestion(&state, ApiError::not_found());
    };
    let Some(input) = source
        .inputs()
        .iter()
        .find(|input| input.name() == &input_name)
    else {
        return reject_ingestion(&state, ApiError::not_found());
    };
    let profile = input.active_profile_revision();
    let pinned_catalog = PinnedCatalogIdentities::new(
        source.id(),
        input.id(),
        profile.id(),
        profile.target_schema_id(),
    );
    let admitted = match dependencies.ingestion.try_admit() {
        Ok(admitted) => admitted,
        Err(AdmissionFailure::CapacityExhausted) => {
            return reject_ingestion(&state, ApiError::capacity_exhausted());
        }
        Err(AdmissionFailure::Draining) => {
            return reject_ingestion(&state, ApiError::server_draining());
        }
        Err(AdmissionFailure::Unavailable) => {
            return reject_ingestion(
                &state,
                ApiError::server_not_ready(spool_unavailable_details(&snapshot)),
            );
        }
    };

    let batch_id = match BatchId::try_from(Uuid::now_v7()) {
        Ok(batch_id) => batch_id,
        Err(_) => return reject_ingestion(&state, ApiError::internal()),
    };
    let captured_time = Utc::now();
    let ingestion_time =
        match IngestionTime::from_unix_milliseconds(captured_time.timestamp_millis()) {
            Ok(ingestion_time) => ingestion_time,
            Err(_) => return reject_ingestion(&state, ApiError::internal()),
        };
    let metadata = BatchMetadata::new(batch_id, pinned_catalog, ingestion_time);
    let shutdown = dependencies.ingestion.shutdown_token();
    let (outcome_sender, outcome_receiver) = oneshot::channel();
    let _supervised_request = tokio::spawn(supervise_admitted_request(
        AdmittedRequest {
            append: admitted,
            metadata,
            captured_time,
            body: request.into_body(),
            content_length,
            maximum_body_bytes,
            deadline: request_deadline,
            shutdown,
        },
        outcome_sender,
    ));
    match outcome_receiver.await {
        Ok(outcome) => {
            if outcome.is_rejected() {
                state.metrics().record_http_rejected();
            }
            outcome.into_response()
        }
        Err(_) => {
            state.metrics().record_http_rejected();
            ApiError::internal().into_response()
        }
    }
}

fn reject_ingestion(state: &ApplicationState, error: ApiError) -> Response {
    state.metrics().record_http_rejected();
    error.into_response()
}

struct AdmittedRequest {
    append: AdmittedAppend,
    metadata: BatchMetadata,
    captured_time: chrono::DateTime<Utc>,
    body: Body,
    content_length: Option<u64>,
    maximum_body_bytes: u64,
    deadline: Instant,
    shutdown: CancellationToken,
}

async fn supervise_admitted_request(
    request: AdmittedRequest,
    outcome_sender: oneshot::Sender<AdmittedIngestionOutcome>,
) {
    let AdmittedRequest {
        append,
        metadata,
        captured_time,
        body,
        content_length,
        maximum_body_bytes,
        deadline,
        shutdown,
    } = request;
    let request_timeout = tokio::time::sleep_until(deadline);
    tokio::pin!(request_timeout);
    let body = tokio::select! {
        biased;
        result = read_bounded_ndjson(
            body,
            content_length,
            maximum_body_bytes,
            MAXIMUM_HTTP_BATCH_RECORDS,
        ) => result.map_err(AdmittedIngestionOutcome::from),
        () = shutdown.cancelled() => Err(AdmittedIngestionOutcome::ServerDraining),
        () = &mut request_timeout => Err(AdmittedIngestionOutcome::RequestTimedOut),
    };
    let body = match body {
        Ok(body) => body,
        Err(outcome) => {
            deliver_admitted_outcome(outcome_sender, outcome);
            return;
        }
    };

    let append_operation = append.append(metadata, body);
    tokio::pin!(append_operation);
    let append_result = tokio::select! {
        biased;
        result = &mut append_operation => Some(result),
        () = &mut request_timeout => None,
    };
    match append_result {
        Some(result) => deliver_admitted_outcome(
            outcome_sender,
            AdmittedIngestionOutcome::from_append(result, captured_time),
        ),
        None => {
            deliver_admitted_outcome(
                outcome_sender,
                AdmittedIngestionOutcome::AppendOutcomeAmbiguous,
            );
            // The client has its ambiguous 500. The spool records any later append failure in its
            // health state, and the supervised request still owns the append until it settles.
            drop(append_operation.await);
        }
    }
}

fn deliver_admitted_outcome(
    sender: oneshot::Sender<AdmittedIngestionOutcome>,
    outcome: AdmittedIngestionOutcome,
) {
    if let Err(_unobserved_outcome) = sender.send(outcome) {
        // The HTTP request disappeared. The append is deliberately not cancelled, so the caller's
        // outcome is ambiguous and a retry can duplicate the batch.
    }
}

async fn read_bounded_ndjson(
    body: Body,
    content_length: Option<u64>,
    maximum_body_bytes: u64,
    maximum_records: u64,
) -> Result<BodyBytes, BodyReadFailure> {
    let initial_capacity = content_length.unwrap_or(0).min(maximum_body_bytes);
    let initial_capacity =
        usize::try_from(initial_capacity).map_err(|_| BodyReadFailure::Internal)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(initial_capacity)
        .map_err(|_| BodyReadFailure::Internal)?;
    let mut body_bytes = 0_u64;
    let mut framed_records = 0_u64;
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
        let chunk_records = u64::try_from(chunk.iter().filter(|byte| **byte == b'\n').count())
            .map_err(|_| BodyReadFailure::LimitExceeded)?;
        framed_records = framed_records
            .checked_add(chunk_records)
            .ok_or(BodyReadFailure::LimitExceeded)?;
        if framed_records > maximum_records {
            return Err(BodyReadFailure::LimitExceeded);
        }
        bytes
            .try_reserve_exact(chunk.len())
            .map_err(|_| BodyReadFailure::Internal)?;
        bytes.extend_from_slice(&chunk);
    }

    if bytes.last().is_some_and(|byte| *byte != b'\n') {
        framed_records = framed_records
            .checked_add(1)
            .ok_or(BodyReadFailure::LimitExceeded)?;
    }
    if framed_records > maximum_records {
        return Err(BodyReadFailure::LimitExceeded);
    }
    Ok(BodyBytes::from(bytes))
}

async fn read_bounded_body(
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
enum BodyReadFailure {
    Invalid,
    LimitExceeded,
    Internal,
}

enum AdmittedIngestionOutcome {
    Accepted(IngestionAcceptedResponse),
    InvalidBody,
    LimitExceeded,
    RequestTimedOut,
    ServerDraining,
    AppendOutcomeAmbiguous,
    Internal,
}

impl AdmittedIngestionOutcome {
    const fn is_rejected(&self) -> bool {
        !matches!(self, Self::Accepted(_))
    }

    fn from_append(
        result: Result<elucid_ingestion::DurableAppend, elucid_ingestion::SpoolError>,
        captured_time: chrono::DateTime<Utc>,
    ) -> Self {
        match result {
            Ok(durable) => Self::Accepted(IngestionAcceptedResponse {
                batch_id: durable.metadata().batch_id().to_string(),
                state: IngestionAcceptedState::DurablyQueued,
                ingestion_time: captured_time.to_rfc3339_opts(SecondsFormat::Millis, true),
                body_bytes: durable.body_bytes().get(),
            }),
            Err(error) if error.code() == SpoolErrorCode::BatchLimitExceeded => Self::LimitExceeded,
            Err(_) => Self::Internal,
        }
    }

    fn into_response(self) -> Response {
        match self {
            Self::Accepted(response) => (StatusCode::ACCEPTED, Json(response)).into_response(),
            Self::InvalidBody => ApiError::invalid_request().into_response(),
            Self::LimitExceeded => ApiError::ingestion_batch_limit_exceeded().into_response(),
            Self::RequestTimedOut => ApiError::request_timed_out().into_response(),
            Self::ServerDraining => ApiError::server_draining().into_response(),
            Self::AppendOutcomeAmbiguous | Self::Internal => ApiError::internal().into_response(),
        }
    }
}

impl From<BodyReadFailure> for AdmittedIngestionOutcome {
    fn from(value: BodyReadFailure) -> Self {
        match value {
            BodyReadFailure::Invalid => Self::InvalidBody,
            BodyReadFailure::LimitExceeded => Self::LimitExceeded,
            BodyReadFailure::Internal => Self::Internal,
        }
    }
}

async fn list_sources(State(state): State<Arc<ApplicationState>>) -> Response {
    let runtime = state.snapshot();
    let Some(dependencies) = runtime.dependencies() else {
        return ApiError::server_not_ready(readiness_details(&runtime)).into_response();
    };
    let catalog = dependencies.catalog.snapshot();
    let completion = if catalog.len() > MAXIMUM_SOURCE_LIST_ITEMS {
        ListCompletion::Truncated
    } else {
        ListCompletion::Complete
    };
    let sources = catalog
        .sources()
        .take(MAXIMUM_SOURCE_LIST_ITEMS)
        .map(SourceSummary::from_source)
        .collect();
    Json(SourceListResponse {
        completion,
        limit: MAXIMUM_SOURCE_LIST_ITEMS,
        sources,
    })
    .into_response()
}

async fn get_source(
    State(state): State<Arc<ApplicationState>>,
    Path(source_id): Path<String>,
) -> Response {
    let source_id = match parse_source_id(&source_id) {
        Ok(source_id) => source_id,
        Err(error) => return error.into_response(),
    };
    let runtime = state.snapshot();
    let Some(dependencies) = runtime.dependencies() else {
        return ApiError::server_not_ready(readiness_details(&runtime)).into_response();
    };
    let catalog = dependencies.catalog.snapshot();
    let Some(source) = catalog.source_by_id(source_id) else {
        return ApiError::not_found().into_response();
    };
    Json(SourceDetail::from_source(source)).into_response()
}

async fn list_segments(
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

async fn execute_query(State(state): State<Arc<ApplicationState>>, request: Request) -> Response {
    if !has_json_content_type(request.headers()) {
        return ApiError::unsupported_query_media_type().into_response();
    }
    let content_length = match parse_content_length(request.headers()) {
        Ok(content_length) => content_length,
        Err(error) => return error.into_response(),
    };
    if content_length.is_some_and(|length| length > MAXIMUM_QUERY_REQUEST_BYTES) {
        return ApiError::query_request_too_large().into_response();
    }

    let runtime = state.snapshot();
    if !runtime.is_ready() {
        if runtime.is_draining() {
            return ApiError::server_draining().into_response();
        }
        return ApiError::server_not_ready(readiness_details(&runtime)).into_response();
    }
    let Some(dependencies) = runtime.dependencies() else {
        return ApiError::server_not_ready(readiness_details(&runtime)).into_response();
    };
    let admitted = match dependencies.queries.try_admit() {
        Ok(admitted) => admitted,
        Err(QueryAdmissionFailure::CapacityExhausted) => {
            return ApiError::capacity_exhausted().into_response();
        }
        Err(QueryAdmissionFailure::Draining) => {
            return ApiError::server_draining().into_response();
        }
    };
    let body = match read_bounded_body(
        request.into_body(),
        content_length,
        MAXIMUM_QUERY_REQUEST_BYTES,
    )
    .await
    {
        Ok(body) => body,
        Err(BodyReadFailure::LimitExceeded) => {
            return ApiError::query_request_too_large().into_response();
        }
        Err(BodyReadFailure::Invalid | BodyReadFailure::Internal) => {
            return ApiError::invalid_request().into_response();
        }
    };
    let request = match serde_json::from_slice::<QueryExecutionRequest>(&body) {
        Ok(request) => request,
        Err(_) => return ApiError::invalid_request().into_response(),
    };
    let request_range = match QueryRequestTimeRange::new(
        request.time_range.start_inclusive,
        request.time_range.end_exclusive,
    ) {
        Ok(request_range) => request_range,
        Err(_) => return ApiError::invalid_request().into_response(),
    };
    let completed = match admitted
        .execute(request.query, request_range, request.output_rows)
        .await
    {
        Ok(completed) => completed,
        Err(error) => return ApiError::query(error).into_response(),
    };
    match QueryExecutionResponse::new(Uuid::now_v7(), completed) {
        Ok(response) => Json(response).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn list_dead_letters(
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

async fn read_dead_letter(
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

async fn ui_or_not_found(request: Request) -> Response {
    crate::ui::response(request.method(), request.uri().path())
        .unwrap_or_else(|| ApiError::not_found().into_response())
}

async fn method_not_allowed() -> ApiError {
    ApiError::method_not_allowed()
}

async fn enforce_request_timeout(
    State(timeout): State<Duration>,
    request: Request,
    next: Next,
) -> Response {
    match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => ApiError::request_timed_out().into_response(),
    }
}

async fn request_identity(request: Request, next: Next) -> Response {
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

fn has_yaml_content_type(headers: &HeaderMap) -> bool {
    has_content_type(headers, "application/yaml")
}

fn has_ndjson_content_type(headers: &HeaderMap) -> bool {
    has_content_type(headers, "application/x-ndjson")
}

fn has_json_content_type(headers: &HeaderMap) -> bool {
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

fn has_identity_content_encoding(headers: &HeaderMap) -> bool {
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

fn parse_content_length(headers: &HeaderMap) -> Result<Option<u64>, ApiError> {
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

fn parse_source_id(value: &str) -> Result<SourceId, ApiError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(|value| SourceId::try_from(value).ok())
        .ok_or_else(ApiError::invalid_request)
}

fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn readiness_details(snapshot: &RuntimeSnapshot) -> ReadinessDetails {
    ReadinessDetails {
        phase: service_phase(snapshot),
        components: snapshot.health(),
    }
}

fn spool_unavailable_details(snapshot: &RuntimeSnapshot) -> ReadinessDetails {
    let mut components = snapshot.health();
    components.spool = ComponentStatus::Down;
    components.ingestion_worker = ComponentStatus::Down;
    ReadinessDetails {
        phase: ServicePhase::Degraded,
        components,
    }
}

fn service_phase(snapshot: &RuntimeSnapshot) -> ServicePhase {
    match snapshot {
        RuntimeSnapshot::Starting { .. } => ServicePhase::Starting,
        RuntimeSnapshot::Operational { .. } if snapshot.is_ready() => ServicePhase::Ready,
        RuntimeSnapshot::Operational { .. } => ServicePhase::Degraded,
        RuntimeSnapshot::Draining { .. } => ServicePhase::Draining,
    }
}

fn status_maintenance_without_dependencies(
    configuration: &crate::RuntimeConfiguration,
) -> StatusMaintenanceOwnership {
    match configuration.maintenance().mode() {
        crate::MaintenanceMode::Automatic => StatusMaintenanceOwnership::Starting,
        crate::MaintenanceMode::Disabled => StatusMaintenanceOwnership::Disabled,
    }
}

#[derive(Serialize)]
struct LivenessResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ReadinessResponse {
    status: &'static str,
    components: ComponentHealth,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ServicePhase {
    Starting,
    Ready,
    Degraded,
    Draining,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum AdmissionState {
    Open,
    Closed,
}

#[derive(Serialize)]
struct StatusResponse {
    phase: ServicePhase,
    admission: AdmissionState,
    components: ComponentHealth,
    limits: EffectiveLimits,
    spool: SpoolStatus,
    publication: PublicationStatus,
    maintenance: MaintenanceStatus,
}

#[derive(Serialize)]
struct EffectiveLimits {
    maximum_http_batch_bytes: u64,
    maximum_http_batch_records: u64,
    maximum_batch_event_days: usize,
    maximum_concurrent_ingestion_requests: u64,
    maximum_concurrent_queries: u64,
    query_timeout_seconds: u64,
    maximum_query_scan_bytes: u64,
    query_memory_bytes: u64,
    maximum_query_result_rows: u64,
    maximum_query_result_bytes: u64,
    spool_capacity_bytes: u64,
    scratch_capacity_bytes: u64,
}

impl EffectiveLimits {
    fn from_configuration(configuration: &crate::RuntimeConfiguration) -> Self {
        Self {
            maximum_http_batch_bytes: configuration.ingestion().maximum_http_batch_bytes().get(),
            maximum_http_batch_records: MAXIMUM_HTTP_BATCH_RECORDS,
            maximum_batch_event_days: MAXIMUM_BATCH_EVENT_DAYS,
            maximum_concurrent_ingestion_requests: configuration
                .ingestion()
                .maximum_concurrent_requests()
                .get(),
            maximum_concurrent_queries: configuration.query().maximum_concurrent_queries().get(),
            query_timeout_seconds: configuration.query().timeout_seconds().get(),
            maximum_query_scan_bytes: configuration.query().maximum_scan_bytes().get(),
            query_memory_bytes: configuration.query().memory_bytes().get(),
            maximum_query_result_rows: configuration.query().maximum_result_rows().get(),
            maximum_query_result_bytes: configuration.query().maximum_result_bytes().get(),
            spool_capacity_bytes: configuration.local_storage().spool_capacity_bytes().get(),
            scratch_capacity_bytes: configuration.local_storage().scratch_capacity_bytes().get(),
        }
    }
}

#[derive(Serialize)]
struct IngestionAcceptedResponse {
    batch_id: String,
    state: IngestionAcceptedState,
    ingestion_time: String,
    body_bytes: u64,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum IngestionAcceptedState {
    DurablyQueued,
}

#[derive(Serialize)]
struct SpoolStatus {
    capacity_bytes: u64,
    used_bytes: u64,
    pending_batches: u64,
    oldest_queued_age_seconds: Option<u64>,
}

#[derive(Serialize)]
struct PublicationStatus {
    status: ComponentStatus,
    pending_batches: u64,
    prepared_segments: u64,
    planned_objects: u64,
    uploaded_objects: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryExecutionRequest {
    query: String,
    time_range: QueryTimeRangeRequest,
    output_rows: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryTimeRangeRequest {
    start_inclusive: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
}

struct QueryExecutionResponse {
    query_id: String,
    source_id: String,
    active_schema_id: String,
    active_schema_version: u64,
    time_range: QueryTimeRangeResponse,
    columns: Vec<QueryColumnResponse>,
    diagnostics: Vec<QueryDiagnosticResponse>,
    result: QueryResult,
}

impl QueryExecutionResponse {
    fn new(query_id: Uuid, completed: CompletedQuery) -> Result<Self, ApiError> {
        let snapshot = completed.snapshot();
        let time_range = snapshot.time_range();
        let start_inclusive = DateTime::<Utc>::from_timestamp_millis(
            time_range.start_inclusive().unix_milliseconds(),
        )
        .ok_or_else(ApiError::internal)?;
        let end_exclusive =
            DateTime::<Utc>::from_timestamp_millis(time_range.end_exclusive().unix_milliseconds())
                .ok_or_else(ApiError::internal)?;
        let source_id = snapshot.source_id().to_string();
        let active_schema_id = snapshot.active_schema().id().to_string();
        let active_schema_version = snapshot.active_schema().version().get();
        let columns = completed
            .result()
            .columns()
            .iter()
            .map(QueryColumnResponse::from_column)
            .collect();
        let diagnostics = completed
            .result()
            .diagnostics()
            .iter()
            .map(QueryDiagnosticResponse::from_diagnostic)
            .collect();
        Ok(Self {
            query_id: query_id.to_string(),
            source_id,
            active_schema_id,
            active_schema_version,
            time_range: QueryTimeRangeResponse {
                start_inclusive: format_timestamp(start_inclusive),
                end_exclusive: format_timestamp(end_exclusive),
            },
            columns,
            diagnostics,
            result: completed.into_result(),
        })
    }
}

impl Serialize for QueryExecutionResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut response = serializer.serialize_struct("QueryExecutionResponse", 11)?;
        response.serialize_field("query_id", &self.query_id)?;
        response.serialize_field("source_id", &self.source_id)?;
        response.serialize_field("active_schema_id", &self.active_schema_id)?;
        response.serialize_field("active_schema_version", &self.active_schema_version)?;
        response.serialize_field("time_range", &self.time_range)?;
        response.serialize_field("columns", &self.columns)?;
        response.serialize_field("rows", self.result.rows())?;
        response.serialize_field("completion", self.result.completion().as_str())?;
        response.serialize_field(
            "truncation_reason",
            &self
                .result
                .completion()
                .truncation_reason()
                .map(|reason| reason.as_str()),
        )?;
        response.serialize_field("diagnostics", &self.diagnostics)?;
        response.serialize_field(
            "statistics",
            &QueryStatisticsResponse::from(self.result.statistics()),
        )?;
        response.end()
    }
}

#[derive(Serialize)]
struct QueryTimeRangeResponse {
    start_inclusive: String,
    end_exclusive: String,
}

#[derive(Serialize)]
struct QueryColumnResponse {
    name: String,
    logical_type: &'static str,
    nullability: &'static str,
}

impl QueryColumnResponse {
    fn from_column(column: &QueryColumn) -> Self {
        Self {
            name: column.name().to_owned(),
            logical_type: column.logical_type().as_str(),
            nullability: column.nullability().as_str(),
        }
    }
}

#[derive(Serialize)]
struct QueryDiagnosticResponse {
    code: &'static str,
    severity: &'static str,
    message: String,
    span: Option<QuerySpanResponse>,
    source_range: Option<QuerySourceRangeResponse>,
}

impl QueryDiagnosticResponse {
    fn from_diagnostic(diagnostic: &elucid_language::Diagnostic) -> Self {
        Self {
            code: diagnostic.code().as_str(),
            severity: diagnostic.severity().as_str(),
            message: diagnostic.message().to_owned(),
            span: diagnostic.span().map(|span| QuerySpanResponse {
                start_byte: span.start(),
                end_byte: span.end(),
            }),
            source_range: diagnostic
                .source_range()
                .map(QuerySourceRangeResponse::from),
        }
    }
}

#[derive(Serialize)]
struct QuerySpanResponse {
    start_byte: usize,
    end_byte: usize,
}

#[derive(Serialize)]
struct QuerySourceRangeResponse {
    start: QuerySourcePositionResponse,
    end: QuerySourcePositionResponse,
}

impl From<elucid_language::SourceRange> for QuerySourceRangeResponse {
    fn from(range: elucid_language::SourceRange) -> Self {
        Self {
            start: QuerySourcePositionResponse::from(range.start()),
            end: QuerySourcePositionResponse::from(range.end()),
        }
    }
}

#[derive(Serialize)]
struct QuerySourcePositionResponse {
    line: usize,
    column: usize,
}

impl From<elucid_language::SourcePosition> for QuerySourcePositionResponse {
    fn from(position: elucid_language::SourcePosition) -> Self {
        Self {
            line: position.line(),
            column: position.column(),
        }
    }
}

#[derive(Serialize)]
struct QueryStatisticsResponse {
    selected_segments: u64,
    selected_parquet_bytes: u64,
    output_rows: u64,
    output_bytes: u64,
    elapsed_milliseconds: u64,
}

impl From<QueryExecutionStatistics> for QueryStatisticsResponse {
    fn from(statistics: QueryExecutionStatistics) -> Self {
        Self {
            selected_segments: statistics.selected_segments(),
            selected_parquet_bytes: statistics.selected_parquet_bytes(),
            output_rows: statistics.output_rows(),
            output_bytes: statistics.output_bytes(),
            elapsed_milliseconds: statistics.elapsed_milliseconds(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentListQuery {
    source_id: String,
    state: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeadLetterListQuery {
    source_id: String,
}

#[derive(Serialize)]
struct SegmentListResponse {
    completion: ListCompletion,
    limit: usize,
    segments: Vec<SegmentResponse>,
}

#[derive(Serialize)]
struct SegmentResponse {
    segment_id: String,
    source_id: String,
    schema_id: String,
    schema_version: u64,
    state: &'static str,
    origin: &'static str,
    event_day: String,
    row_count: u64,
    uncompressed_bytes: u64,
    parquet_bytes: u64,
    minimum_event_time: String,
    maximum_event_time: String,
    minimum_ingestion_time: String,
    maximum_ingestion_time: String,
    published_at: Option<String>,
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

#[derive(Serialize)]
struct DeadLetterListResponse {
    completion: ListCompletion,
    limit: usize,
    dead_letters: Vec<DeadLetterSummaryResponse>,
}

#[derive(Serialize)]
struct DeadLetterSummaryResponse {
    object_id: String,
    source_id: String,
    input_id: String,
    batch_id: String,
    byte_size: u64,
    published_at: String,
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

#[derive(Serialize)]
struct DeadLetterReadResponse {
    object: DeadLetterSummaryResponse,
    completion: ListCompletion,
    limit_bytes: u64,
    entries: Vec<DeadLetterDocumentEntry>,
}

#[derive(Serialize)]
struct MaintenanceStatus {
    ownership: StatusMaintenanceOwnership,
    recent_compactions: [(); 0],
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StatusMaintenanceOwnership {
    Starting,
    Disabled,
    Owned,
    Standby,
}

impl From<MaintenanceOwnership> for StatusMaintenanceOwnership {
    fn from(value: MaintenanceOwnership) -> Self {
        match value {
            MaintenanceOwnership::Disabled => Self::Disabled,
            MaintenanceOwnership::Owned => Self::Owned,
            MaintenanceOwnership::Standby => Self::Standby,
        }
    }
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum CatalogApplicationResult {
    Applied,
    Unchanged,
}

#[derive(Serialize)]
struct CatalogApplicationResponse {
    outcome: CatalogApplicationResult,
    source_id: String,
    active_schema_version: u64,
    active_input_profile_revisions: Vec<ActiveInputProfile>,
}

impl CatalogApplicationResponse {
    fn from_outcome(outcome: CatalogApplyOutcome) -> Result<Self, ApiError> {
        let result = match &outcome {
            CatalogApplyOutcome::Applied { .. } => CatalogApplicationResult::Applied,
            CatalogApplyOutcome::Unchanged { .. } => CatalogApplicationResult::Unchanged,
            _ => return Err(ApiError::internal()),
        };
        let source = outcome.source();
        Ok(Self {
            outcome: result,
            source_id: source.id().to_string(),
            active_schema_version: source.active_schema().version().get(),
            active_input_profile_revisions: source
                .inputs()
                .iter()
                .map(ActiveInputProfile::from_input)
                .collect(),
        })
    }
}

#[derive(Serialize)]
struct ActiveInputProfile {
    input_id: String,
    input_name: String,
    profile_revision_id: String,
    revision: u64,
}

impl ActiveInputProfile {
    fn from_input(input: &Input) -> Self {
        let profile = input.active_profile_revision();
        Self {
            input_id: input.id().to_string(),
            input_name: input.name().as_str().to_owned(),
            profile_revision_id: profile.id().to_string(),
            revision: profile.revision().get(),
        }
    }
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ListCompletion {
    Complete,
    Truncated,
}

impl ListCompletion {
    const fn from_truncated(truncated: bool) -> Self {
        if truncated {
            Self::Truncated
        } else {
            Self::Complete
        }
    }
}

#[derive(Serialize)]
struct SourceListResponse {
    completion: ListCompletion,
    limit: usize,
    sources: Vec<SourceSummary>,
}

#[derive(Serialize)]
struct SourceSummary {
    source_id: String,
    name: String,
    display_name: String,
    active_schema_version: u64,
}

impl SourceSummary {
    fn from_source(source: &Source) -> Self {
        Self {
            source_id: source.id().to_string(),
            name: source.name().as_str().to_owned(),
            display_name: source.display_name().to_owned(),
            active_schema_version: source.active_schema().version().get(),
        }
    }
}

#[derive(Serialize)]
struct SourceDetail {
    source_id: String,
    name: String,
    display_name: String,
    active_schema: SchemaDetail,
    schema_versions: Vec<SchemaSummary>,
    inputs: Vec<InputSummary>,
}

impl SourceDetail {
    fn from_source(source: &Source) -> Self {
        Self {
            source_id: source.id().to_string(),
            name: source.name().as_str().to_owned(),
            display_name: source.display_name().to_owned(),
            active_schema: SchemaDetail::from_schema(source.active_schema()),
            schema_versions: source
                .schemas()
                .iter()
                .map(SchemaSummary::from_schema)
                .collect(),
            inputs: source
                .inputs()
                .iter()
                .map(InputSummary::from_input)
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct SchemaSummary {
    schema_id: String,
    version: u64,
}

impl SchemaSummary {
    fn from_schema(schema: &Schema) -> Self {
        Self {
            schema_id: schema.id().to_string(),
            version: schema.version().get(),
        }
    }
}

#[derive(Serialize)]
struct SchemaDetail {
    schema_id: String,
    version: u64,
    fields: Vec<FieldSummary>,
}

impl SchemaDetail {
    fn from_schema(schema: &Schema) -> Self {
        Self {
            schema_id: schema.id().to_string(),
            version: schema.version().get(),
            fields: schema
                .fields()
                .iter()
                .map(FieldSummary::from_field)
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct FieldSummary {
    field_id: String,
    name: String,
    logical_type: &'static str,
    nullability: &'static str,
    role: &'static str,
    description: Option<String>,
    historical_remainder_pointer: Option<String>,
}

impl FieldSummary {
    fn from_field(field: &Field) -> Self {
        Self {
            field_id: field.id().to_string(),
            name: field.name().to_owned(),
            logical_type: field.logical_type().as_str(),
            nullability: field.nullability().as_str(),
            role: field.role().as_str(),
            description: field.description().map(str::to_owned),
            historical_remainder_pointer: field
                .historical_remainder_pointer()
                .map(ToString::to_string),
        }
    }
}

#[derive(Serialize)]
struct InputSummary {
    input_id: String,
    name: String,
    active_profile: IngestionProfileSummary,
}

impl InputSummary {
    fn from_input(input: &Input) -> Self {
        Self {
            input_id: input.id().to_string(),
            name: input.name().as_str().to_owned(),
            active_profile: IngestionProfileSummary::from_profile(input.active_profile_revision()),
        }
    }
}

#[derive(Serialize)]
struct IngestionProfileSummary {
    profile_revision_id: String,
    revision: u64,
    target_schema_id: String,
    maximum_record_bytes: u64,
    event_time_json_pointer: String,
    event_time_format: &'static str,
}

impl IngestionProfileSummary {
    fn from_profile(profile: &IngestionProfileRevision) -> Self {
        Self {
            profile_revision_id: profile.id().to_string(),
            revision: profile.revision().get(),
            target_schema_id: profile.target_schema_id().to_string(),
            maximum_record_bytes: profile.profile().maximum_record_bytes().get(),
            event_time_json_pointer: profile.profile().event_time().json_pointer().to_string(),
            event_time_format: profile.profile().event_time().format().as_str(),
        }
    }
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    details: ErrorDetails,
    retry_after_seconds: Option<u64>,
}

impl ApiError {
    fn invalid_request() -> Self {
        Self::plain(
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
            "Request is invalid",
        )
    }

    fn unsupported_catalog_media_type() -> Self {
        Self::plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "INVALID_REQUEST",
            "Content-Type must be application/yaml",
        )
    }

    fn catalog_request_too_large() -> Self {
        Self::plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "INVALID_REQUEST",
            "Request body exceeds the catalog document limit",
        )
    }

    fn unsupported_ingestion_media_type() -> Self {
        Self::plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "INVALID_REQUEST",
            "Content-Type must be application/x-ndjson",
        )
    }

    fn unsupported_ingestion_encoding() -> Self {
        Self::plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "INVALID_REQUEST",
            "Content-Encoding must be identity",
        )
    }

    fn unsupported_query_media_type() -> Self {
        Self::plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "INVALID_REQUEST",
            "Content-Type must be application/json",
        )
    }

    fn query_request_too_large() -> Self {
        Self::plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "INVALID_REQUEST",
            "Request body exceeds the query request limit",
        )
    }

    fn ingestion_batch_limit_exceeded() -> Self {
        Self::plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "INGESTION_BATCH_LIMIT_EXCEEDED",
            "Ingestion batch exceeds an admission limit",
        )
    }

    fn capacity_exhausted() -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: "CAPACITY_EXHAUSTED",
            message: "Admission capacity is exhausted",
            details: ErrorDetails::Empty(EmptyDetails {}),
            retry_after_seconds: Some(RETRY_AFTER_SECONDS),
        }
    }

    fn request_timed_out() -> Self {
        Self::plain(
            StatusCode::REQUEST_TIMEOUT,
            "INVALID_REQUEST",
            "Request timed out",
        )
    }

    fn method_not_allowed() -> Self {
        Self::plain(
            StatusCode::METHOD_NOT_ALLOWED,
            "INVALID_REQUEST",
            "Method is not allowed",
        )
    }

    fn not_found() -> Self {
        Self::plain(StatusCode::NOT_FOUND, "NOT_FOUND", "Resource was not found")
    }

    fn internal() -> Self {
        Self::plain(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Internal server error",
        )
    }

    fn server_not_ready(details: ReadinessDetails) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "SERVER_NOT_READY",
            message: "Server is not ready",
            details: ErrorDetails::Readiness(details),
            retry_after_seconds: Some(RETRY_AFTER_SECONDS),
        }
    }

    fn server_draining() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "SERVER_DRAINING",
            message: "Server is draining",
            details: ErrorDetails::Empty(EmptyDetails {}),
            retry_after_seconds: Some(RETRY_AFTER_SECONDS),
        }
    }

    fn metastore_unavailable() -> Self {
        Self::plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "METASTORE_UNAVAILABLE",
            "Metastore is unavailable",
        )
    }

    fn publication(error: PublicationError) -> Self {
        match error.kind() {
            PublicationErrorKind::Unavailable => Self::metastore_unavailable(),
            PublicationErrorKind::Conflict => Self::plain(
                StatusCode::CONFLICT,
                "METASTORE_CONFLICT",
                "Metastore state changed concurrently",
            ),
            PublicationErrorKind::Corrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                "METASTORE_CORRUPT",
                "Metastore state is corrupt",
            ),
            _ => Self::internal(),
        }
    }

    fn storage(error: StorageError) -> Self {
        match error.code() {
            StorageErrorCode::ObjectStoreUnavailable
            | StorageErrorCode::ObjectUploadFailed
            | StorageErrorCode::ObjectVerificationFailed
            | StorageErrorCode::ObjectDeleteFailed => Self::plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "OBJECT_STORE_UNAVAILABLE",
                "Object store is unavailable",
            ),
            StorageErrorCode::ObjectIntegrityError | StorageErrorCode::ParquetInvalid => {
                Self::object_integrity()
            }
            StorageErrorCode::ParquetBuildFailed | StorageErrorCode::LocalCapacityExhausted => {
                Self::internal()
            }
            _ => Self::internal(),
        }
    }

    fn object_integrity() -> Self {
        Self::plain(
            StatusCode::INTERNAL_SERVER_ERROR,
            "OBJECT_INTEGRITY_ERROR",
            "Stored object failed integrity validation",
        )
    }

    fn query(error: QueryFailure) -> Self {
        match error {
            QueryFailure::Snapshot(error) => Self::query_snapshot(error),
            QueryFailure::Engine(error) => Self::query_engine(error),
            QueryFailure::OutputRowLimit(error) => match error {
                QueryOutputRowLimitError::MustBePositive
                | QueryOutputRowLimitError::ExceedsConfiguredMaximum { .. } => {
                    Self::invalid_request()
                }
                _ => Self::invalid_request(),
            },
            QueryFailure::Cancelled => Self::plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "QUERY_CANCELLED",
                "Query execution was cancelled",
            ),
        }
    }

    fn query_snapshot(error: QuerySnapshotError) -> Self {
        match error.kind() {
            QuerySnapshotErrorKind::Analysis => {
                let Some(analysis) = error.analysis_error() else {
                    return Self::internal();
                };
                Self {
                    status: StatusCode::BAD_REQUEST,
                    code: analysis.code().as_str(),
                    message: "Query is invalid",
                    details: ErrorDetails::Query(QueryErrorDetails {
                        diagnostics: analysis
                            .diagnostics()
                            .iter()
                            .map(QueryDiagnosticResponse::from_diagnostic)
                            .collect(),
                    }),
                    retry_after_seconds: None,
                }
            }
            QuerySnapshotErrorKind::ResourceLimit => Self::plain(
                StatusCode::UNPROCESSABLE_ENTITY,
                "QUERY_RESOURCE_LIMIT_EXCEEDED",
                "Query exceeds an execution limit",
            ),
            QuerySnapshotErrorKind::Unavailable => Self::metastore_unavailable(),
            QuerySnapshotErrorKind::Corrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                "CATALOG_CORRUPT",
                "Query metadata is corrupt",
            ),
            _ => Self::internal(),
        }
    }

    fn query_engine(error: EngineError) -> Self {
        match error.code() {
            EngineErrorCode::PublishedObjectMissing => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.code().as_str(),
                "A published query object is missing",
            ),
            EngineErrorCode::PublishedObjectCorrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.code().as_str(),
                "A published query object is corrupt",
            ),
            EngineErrorCode::CatalogCorrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.code().as_str(),
                "Query catalog state is corrupt",
            ),
            EngineErrorCode::QueryCastFailed => Self::plain(
                StatusCode::UNPROCESSABLE_ENTITY,
                error.code().as_str(),
                "Query cast failed",
            ),
            EngineErrorCode::QueryEvaluationFailed => Self::plain(
                StatusCode::UNPROCESSABLE_ENTITY,
                error.code().as_str(),
                "Query evaluation failed",
            ),
            EngineErrorCode::QueryResourceLimitExceeded => Self::plain(
                StatusCode::UNPROCESSABLE_ENTITY,
                error.code().as_str(),
                "Query exceeds an execution limit",
            ),
            EngineErrorCode::QueryTimeout => Self::plain(
                StatusCode::REQUEST_TIMEOUT,
                error.code().as_str(),
                "Query exceeded its execution timeout",
            ),
            EngineErrorCode::QueryCancelled => Self::plain(
                StatusCode::SERVICE_UNAVAILABLE,
                error.code().as_str(),
                "Query execution was cancelled",
            ),
            EngineErrorCode::QueryExecutionFailed => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.code().as_str(),
                "Query execution failed",
            ),
            _ => Self::internal(),
        }
    }

    fn catalog(error: CatalogApplicationError) -> Self {
        let status = match error.code() {
            CatalogErrorCode::ManifestInvalid | CatalogErrorCode::ProfileInvalid => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            CatalogErrorCode::DefinitionConflict | CatalogErrorCode::SchemaIncompatible => {
                StatusCode::CONFLICT
            }
            CatalogErrorCode::Corrupt => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            code: error.code().as_str(),
            message: "Catalog application was rejected",
            details: ErrorDetails::Catalog(CatalogErrorDetails {
                path: error.path().as_str().to_owned(),
            }),
            retry_after_seconds: None,
        }
    }

    fn catalog_persistence(error: CatalogPersistenceError) -> Self {
        if let Some(catalog_error) = error.catalog_error() {
            let status = match catalog_error.code() {
                CatalogErrorCode::ManifestInvalid | CatalogErrorCode::ProfileInvalid => {
                    StatusCode::UNPROCESSABLE_ENTITY
                }
                CatalogErrorCode::DefinitionConflict | CatalogErrorCode::SchemaIncompatible => {
                    StatusCode::CONFLICT
                }
                CatalogErrorCode::Corrupt => StatusCode::INTERNAL_SERVER_ERROR,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            return Self {
                status,
                code: catalog_error.code().as_str(),
                message: "Catalog application was rejected",
                details: ErrorDetails::Catalog(CatalogErrorDetails {
                    path: catalog_error.path().as_str().to_owned(),
                }),
                retry_after_seconds: None,
            };
        }
        match error.kind() {
            CatalogPersistenceErrorKind::Unavailable => Self::metastore_unavailable(),
            CatalogPersistenceErrorKind::Conflict => Self::plain(
                StatusCode::CONFLICT,
                "METASTORE_CONFLICT",
                "Metastore state changed concurrently",
            ),
            CatalogPersistenceErrorKind::Corrupt => Self::plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                "METASTORE_CORRUPT",
                "Metastore state is corrupt",
            ),
            _ => Self::internal(),
        }
    }

    fn plain(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
            details: ErrorDetails::Empty(EmptyDetails {}),
            retry_after_seconds: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(ErrorEnvelope {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                    details: self.details,
                },
            }),
        )
            .into_response();
        if let Some(seconds) = self.retry_after_seconds
            && let Ok(value) = HeaderValue::from_str(&seconds.to_string())
        {
            response.headers_mut().insert("retry-after", value);
        }
        response
    }
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
    details: ErrorDetails,
}

#[derive(Serialize)]
#[serde(untagged)]
enum ErrorDetails {
    Empty(EmptyDetails),
    Readiness(ReadinessDetails),
    Catalog(CatalogErrorDetails),
    Query(QueryErrorDetails),
}

#[derive(Serialize)]
struct EmptyDetails {}

#[derive(Serialize)]
struct ReadinessDetails {
    phase: ServicePhase,
    components: ComponentHealth,
}

#[derive(Serialize)]
struct CatalogErrorDetails {
    path: String,
}

#[derive(Serialize)]
struct QueryErrorDetails {
    diagnostics: Vec<QueryDiagnosticResponse>,
}
