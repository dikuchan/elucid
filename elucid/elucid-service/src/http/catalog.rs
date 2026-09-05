use std::sync::Arc;

use axum::Json;
use axum::body::Bytes as BodyBytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use elucid_catalog::{
    CatalogManifest, Field, IngestionProfileRevision, Input, Schema, Source, SourceId,
};
use elucid_metastore::CatalogApplyOutcome;
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::runtime::{ApplicationState, ComponentStatus};

use super::body::has_yaml_content_type;
use super::error::{ApiError, ErrorEnvelope};
use super::health::readiness_details;
use super::response::ListCompletion;

pub(super) const MAXIMUM_CATALOG_DOCUMENT_BYTES: usize = 1_048_576;

const MAXIMUM_SOURCE_LIST_ITEMS: usize = 100;

#[utoipa::path(
    post,
    path = "/api/v1/catalog-applications",
    tag = "catalog",
    summary = "Apply a complete catalog manifest",
    request_body(content = String, description = "Complete catalog manifest", content_type = "application/yaml"),
    responses(
        (status = 200, description = "Catalog state was applied or was already identical", body = CatalogApplicationResponse),
        (status = 400, description = "Malformed request", body = ErrorEnvelope),
        (status = 409, description = "Catalog history conflicts with durable state", body = ErrorEnvelope),
        (status = 413, description = "Catalog document exceeds the configured limit", body = ErrorEnvelope),
        (status = 415, description = "Content-Type is not application/yaml", body = ErrorEnvelope),
        (status = 422, description = "Catalog document violates the catalog contract", body = ErrorEnvelope),
        (status = 500, description = "Durable catalog state is corrupt or an internal error occurred", body = ErrorEnvelope),
        (status = 503, description = "PostgreSQL or server readiness is unavailable", body = ErrorEnvelope),
    )
)]
pub(super) async fn apply_catalog(
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

#[utoipa::path(
    get,
    path = "/api/v1/sources",
    tag = "catalog",
    summary = "List catalog sources",
    responses(
        (status = 200, description = "Bounded source list", body = SourceListResponse),
        (status = 503, description = "Catalog is not available", body = ErrorEnvelope),
    )
)]
pub(super) async fn list_sources(State(state): State<Arc<ApplicationState>>) -> Response {
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

#[utoipa::path(
    get,
    path = "/api/v1/sources/{source_id}",
    tag = "catalog",
    summary = "Inspect one catalog source",
    params(("source_id" = String, Path, format = Uuid, description = "Source UUID")),
    responses(
        (status = 200, description = "Source schema and active ingestion profiles", body = SourceDetail),
        (status = 400, description = "Source identity is not a UUID", body = ErrorEnvelope),
        (status = 404, description = "Source does not exist", body = ErrorEnvelope),
        (status = 503, description = "Catalog is not available", body = ErrorEnvelope),
    )
)]
pub(super) async fn get_source(
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

pub(super) fn parse_source_id(value: &str) -> Result<SourceId, ApiError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(|value| SourceId::try_from(value).ok())
        .ok_or_else(ApiError::invalid_request)
}

#[derive(Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum CatalogApplicationResult {
    Applied,
    Unchanged,
}

#[derive(Serialize, ToSchema)]
struct CatalogApplicationResponse {
    outcome: CatalogApplicationResult,
    #[schema(format = Uuid)]
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

#[derive(Serialize, ToSchema)]
struct ActiveInputProfile {
    #[schema(format = Uuid)]
    input_id: String,
    input_name: String,
    #[schema(format = Uuid)]
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

#[derive(Serialize, ToSchema)]
struct SourceListResponse {
    completion: ListCompletion,
    limit: usize,
    sources: Vec<SourceSummary>,
}

#[derive(Serialize, ToSchema)]
struct SourceSummary {
    #[schema(format = Uuid)]
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

#[derive(Serialize, ToSchema)]
struct SourceDetail {
    #[schema(format = Uuid)]
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

#[derive(Serialize, ToSchema)]
struct SchemaSummary {
    #[schema(format = Uuid)]
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

#[derive(Serialize, ToSchema)]
struct SchemaDetail {
    #[schema(format = Uuid)]
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

#[derive(Serialize, ToSchema)]
struct FieldSummary {
    #[schema(format = Uuid)]
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

#[derive(Serialize, ToSchema)]
struct InputSummary {
    #[schema(format = Uuid)]
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

#[derive(Serialize, ToSchema)]
struct IngestionProfileSummary {
    #[schema(format = Uuid)]
    profile_revision_id: String,
    revision: u64,
    #[schema(format = Uuid)]
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
