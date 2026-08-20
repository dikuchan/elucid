use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes as BodyBytes};
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, Path, Request, State};
use axum::http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{SecondsFormat, Utc};
use futures::StreamExt as _;
use serde::Serialize;
use uuid::Uuid;

use elucid_catalog::{
    CatalogApplicationError, CatalogErrorCode, CatalogManifest, Field, IngestionProfileRevision,
    Input, InputName, Schema, Source, SourceId, SourceName,
};
use elucid_ingestion::{
    BatchId, BatchMetadata, IngestionTime, PinnedCatalogIdentities, SpoolErrorCode,
};
use elucid_metastore::{CatalogApplyOutcome, CatalogPersistenceError, CatalogPersistenceErrorKind};

use crate::ingestion::{AdmissionFailure, IngestionAvailability, MAXIMUM_HTTP_BATCH_RECORDS};
use crate::runtime::{
    ApplicationState, ComponentHealth, ComponentStatus, MaintenanceOwnership, RuntimeSnapshot,
};

const MAXIMUM_CATALOG_DOCUMENT_BYTES: usize = 1_048_576;
const MAXIMUM_SOURCE_LIST_ITEMS: usize = 100;
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
        .route("/api/v1/status", get(status))
        .route(
            "/api/v1/catalog-applications",
            post(apply_catalog).layer(DefaultBodyLimit::max(MAXIMUM_CATALOG_DOCUMENT_BYTES)),
        )
        .route("/api/v1/sources", get(list_sources))
        .route(
            "/api/v1/sources/{source_name}/inputs/{input_name}/events",
            post(ingest_events),
        )
        .route("/api/v1/sources/{source_id}", get(get_source))
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(
            request_timeout,
            enforce_request_timeout,
        ))
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
    let (spool_used_bytes, pending_batches, ingestion_availability, maintenance_ownership) =
        snapshot.dependencies().map_or(
            (
                0,
                0,
                IngestionAvailability::Unavailable,
                status_maintenance_without_dependencies(configuration),
            ),
            |dependencies| {
                let ingestion = dependencies.ingestion.status();
                (
                    ingestion.used_bytes(),
                    ingestion.pending_batches(),
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
    Json(StatusResponse {
        phase,
        admission,
        components: snapshot.health(),
        limits: EffectiveLimits::from_configuration(configuration),
        spool: SpoolStatus {
            capacity_bytes: configuration.local_storage().spool_capacity_bytes().get(),
            used_bytes: spool_used_bytes,
            oldest_queued_age_seconds: None,
        },
        publication: PublicationStatus {
            pending_batches,
            prepared_segments: 0,
        },
        maintenance: MaintenanceStatus {
            ownership: maintenance_ownership,
            recent_compactions: [],
        },
    })
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
    if !has_ndjson_content_type(request.headers()) {
        return ApiError::unsupported_ingestion_media_type().into_response();
    }
    if !has_identity_content_encoding(request.headers()) {
        return ApiError::unsupported_ingestion_encoding().into_response();
    }
    let content_length = match parse_content_length(request.headers()) {
        Ok(content_length) => content_length,
        Err(error) => return error.into_response(),
    };
    let maximum_body_bytes = state
        .configuration()
        .ingestion()
        .maximum_http_batch_bytes()
        .get();
    if content_length.is_some_and(|length| length > maximum_body_bytes) {
        return ApiError::ingestion_batch_limit_exceeded().into_response();
    }

    let snapshot = state.snapshot();
    if !snapshot.is_ready() {
        return ApiError::server_not_ready(readiness_details(&snapshot)).into_response();
    }
    let Some(dependencies) = snapshot.dependencies() else {
        return ApiError::server_not_ready(readiness_details(&snapshot)).into_response();
    };
    let source_name = match SourceName::try_from(source_name) {
        Ok(source_name) => source_name,
        Err(_) => return ApiError::invalid_request().into_response(),
    };
    let input_name = match InputName::try_from(input_name) {
        Ok(input_name) => input_name,
        Err(_) => return ApiError::invalid_request().into_response(),
    };
    let catalog = dependencies.catalog.snapshot();
    let Some(source) = catalog.source_by_name(&source_name) else {
        return ApiError::not_found().into_response();
    };
    let Some(input) = source
        .inputs()
        .iter()
        .find(|input| input.name() == &input_name)
    else {
        return ApiError::not_found().into_response();
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
            return ApiError::capacity_exhausted().into_response();
        }
        Err(AdmissionFailure::Unavailable) => {
            return ApiError::server_not_ready(spool_unavailable_details(&snapshot))
                .into_response();
        }
    };

    let batch_id = match BatchId::try_from(Uuid::now_v7()) {
        Ok(batch_id) => batch_id,
        Err(_) => return ApiError::internal().into_response(),
    };
    let captured_time = Utc::now();
    let ingestion_time =
        match IngestionTime::from_unix_milliseconds(captured_time.timestamp_millis()) {
            Ok(ingestion_time) => ingestion_time,
            Err(_) => return ApiError::internal().into_response(),
        };
    let metadata = BatchMetadata::new(batch_id, pinned_catalog, ingestion_time);
    let body = match read_bounded_ndjson(
        request.into_body(),
        content_length,
        maximum_body_bytes,
        MAXIMUM_HTTP_BATCH_RECORDS,
    )
    .await
    {
        Ok(body) => body,
        Err(BodyReadFailure::Invalid) => return ApiError::invalid_request().into_response(),
        Err(BodyReadFailure::LimitExceeded) => {
            return ApiError::ingestion_batch_limit_exceeded().into_response();
        }
        Err(BodyReadFailure::Internal) => return ApiError::internal().into_response(),
    };
    let durable = match admitted.append(metadata, body).await {
        Ok(durable) => durable,
        Err(error) if error.code() == SpoolErrorCode::BatchLimitExceeded => {
            return ApiError::ingestion_batch_limit_exceeded().into_response();
        }
        Err(_) => return ApiError::internal().into_response(),
    };

    (
        StatusCode::ACCEPTED,
        Json(IngestionAcceptedResponse {
            batch_id: durable.metadata().batch_id().to_string(),
            state: IngestionAcceptedState::DurablyQueued,
            ingestion_time: captured_time.to_rfc3339_opts(SecondsFormat::Millis, true),
            body_bytes: durable.body_bytes().get(),
        }),
    )
        .into_response()
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BodyReadFailure {
    Invalid,
    LimitExceeded,
    Internal,
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

async fn not_found() -> ApiError {
    ApiError::not_found()
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
    oldest_queued_age_seconds: Option<u64>,
}

#[derive(Serialize)]
struct PublicationStatus {
    pending_batches: u64,
    prepared_segments: u64,
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
            message: "Ingestion capacity is exhausted",
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

    fn metastore_unavailable() -> Self {
        Self::plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "METASTORE_UNAVAILABLE",
            "Metastore is unavailable",
        )
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
