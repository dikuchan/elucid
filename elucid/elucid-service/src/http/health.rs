use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use elucid_ingestion::MAXIMUM_BATCH_EVENT_DAYS;
use serde::Serialize;
use utoipa::ToSchema;

use crate::ingestion::{IngestionAvailability, IngestionStatus, MAXIMUM_HTTP_BATCH_RECORDS};
use crate::runtime::{
    ApplicationState, ComponentHealth, ComponentStatus, MaintenanceOwnership, RuntimeSnapshot,
};

use super::error::{ApiError, ErrorEnvelope};

#[utoipa::path(
    get,
    path = "/health/live",
    tag = "health",
    summary = "Check process liveness",
    responses((status = 200, description = "The process runtime can make progress", body = LivenessResponse))
)]
pub(super) async fn liveness() -> Json<LivenessResponse> {
    Json(LivenessResponse { status: "UP" })
}

#[utoipa::path(
    get,
    path = "/health/ready",
    tag = "health",
    summary = "Check request readiness",
    responses(
        (status = 200, description = "The server admits ingestion and query requests", body = ReadinessResponse),
        (status = 503, description = "A required dependency or local capability is unavailable", body = ErrorEnvelope),
    )
)]
pub(super) async fn readiness(State(state): State<Arc<ApplicationState>>) -> Response {
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

#[utoipa::path(
    get,
    path = "/api/v1/status",
    tag = "operations",
    summary = "Inspect runtime status",
    responses((status = 200, description = "Bounded runtime and backlog summary", body = StatusResponse))
)]
pub(super) async fn status(State(state): State<Arc<ApplicationState>>) -> Json<StatusResponse> {
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

#[utoipa::path(
    get,
    path = "/metrics",
    tag = "operations",
    summary = "Read OpenMetrics metrics",
    responses(
        (status = 200, description = "OpenMetrics 1.0 metrics", body = String, content_type = "application/openmetrics-text; version=1.0.0; charset=utf-8"),
        (status = 500, description = "Metrics encoding failed")
    )
)]
pub(super) async fn metrics(State(state): State<Arc<ApplicationState>>) -> Response {
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

pub(super) fn readiness_details(snapshot: &RuntimeSnapshot) -> ReadinessDetails {
    ReadinessDetails {
        phase: service_phase(snapshot),
        components: snapshot.health(),
    }
}

pub(super) fn spool_unavailable_details(snapshot: &RuntimeSnapshot) -> ReadinessDetails {
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

#[derive(Serialize, ToSchema)]
pub(super) struct LivenessResponse {
    status: &'static str,
}

#[derive(Serialize, ToSchema)]
struct ReadinessResponse {
    status: &'static str,
    components: ComponentHealth,
}

#[derive(Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ServicePhase {
    Starting,
    Ready,
    Degraded,
    Draining,
}

#[derive(Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum AdmissionState {
    Open,
    Closed,
}

#[derive(Serialize, ToSchema)]
pub(super) struct StatusResponse {
    phase: ServicePhase,
    admission: AdmissionState,
    components: ComponentHealth,
    limits: EffectiveLimits,
    spool: SpoolStatus,
    publication: PublicationStatus,
    maintenance: MaintenanceStatus,
}

#[derive(Serialize, ToSchema)]
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

#[derive(Serialize, ToSchema)]
struct SpoolStatus {
    capacity_bytes: u64,
    used_bytes: u64,
    pending_batches: u64,
    oldest_queued_age_seconds: Option<u64>,
}

#[derive(Serialize, ToSchema)]
struct PublicationStatus {
    status: ComponentStatus,
    pending_batches: u64,
    prepared_segments: u64,
    planned_objects: u64,
    uploaded_objects: u64,
}

#[derive(Serialize, ToSchema)]
struct MaintenanceStatus {
    ownership: StatusMaintenanceOwnership,
    #[schema(value_type = Vec<Object>)]
    recent_compactions: [(); 0],
}

#[derive(Clone, Copy, Serialize, ToSchema)]
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

#[derive(Serialize, ToSchema)]
pub(super) struct ReadinessDetails {
    phase: ServicePhase,
    components: ComponentHealth,
}
