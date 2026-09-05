use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::body::{Body, Bytes as BodyBytes};
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::{SecondsFormat, Utc};
use elucid_catalog::{InputName, SourceName};
use elucid_ingestion::{
    BatchId, BatchMetadata, IngestionTime, PinnedCatalogIdentities, SpoolErrorKind,
};
use futures::StreamExt as _;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ingestion::{AdmissionFailure, AdmittedAppend, MAXIMUM_HTTP_BATCH_RECORDS};
use crate::runtime::ApplicationState;

use super::body::{
    BodyReadFailure, has_identity_content_encoding, has_ndjson_content_type, parse_content_length,
};
use super::error::{ApiError, ErrorEnvelope};
use super::health::{readiness_details, spool_unavailable_details};

#[utoipa::path(
    post,
    path = "/api/v1/sources/{source_name}/inputs/{input_name}/events",
    tag = "ingestion",
    summary = "Durably queue NDJSON events",
    params(
        ("source_name" = String, Path, description = "Catalog source name"),
        ("input_name" = String, Path, description = "Catalog input name"),
    ),
    request_body(content = String, description = "Newline-delimited JSON events", content_type = "application/x-ndjson"),
    responses(
        (status = 202, description = "The complete request body is durable in the local spool", body = IngestionAcceptedResponse),
        (status = 400, description = "Malformed request or invalid path identity", body = ErrorEnvelope),
        (status = 404, description = "Source or input does not exist", body = ErrorEnvelope),
        (status = 408, description = "Request body did not become durable before the request deadline", body = ErrorEnvelope),
        (status = 413, description = "Batch exceeds an admission limit", body = ErrorEnvelope),
        (status = 415, description = "Content-Type or Content-Encoding is unsupported", body = ErrorEnvelope),
        (status = 429, description = "Ingestion capacity is exhausted before ownership", body = ErrorEnvelope),
        (status = 500, description = "Durability outcome is ambiguous", body = ErrorEnvelope),
        (status = 503, description = "The server is not ready or is draining", body = ErrorEnvelope),
    )
)]
pub(super) async fn ingest_events(
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
            Err(error) if error.kind() == SpoolErrorKind::BatchLimitExceeded => Self::LimitExceeded,
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

#[derive(Serialize, ToSchema)]
struct IngestionAcceptedResponse {
    #[schema(format = Uuid)]
    batch_id: String,
    state: IngestionAcceptedState,
    #[schema(format = DateTime)]
    ingestion_time: String,
    body_bytes: u64,
}

#[derive(Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum IngestionAcceptedState {
    DurablyQueued,
}
