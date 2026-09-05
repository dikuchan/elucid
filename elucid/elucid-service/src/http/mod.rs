mod body;
mod catalog;
mod dead_letters;
mod error;
mod health;
mod ingestion;
mod middleware;
mod query;
mod response;
mod segments;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{DefaultBodyLimit, Request};
use axum::middleware::{from_fn, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::runtime::ApplicationState;

use self::error::ApiError;

#[derive(OpenApi)]
#[openapi(
    paths(
        health::liveness,
        health::readiness,
        health::status,
        health::metrics,
        catalog::apply_catalog,
        ingestion::ingest_events,
        catalog::list_sources,
        catalog::get_source,
        segments::list_segments,
        query::list_query_executions,
        query::execute_query,
        dead_letters::list_dead_letters,
        dead_letters::read_dead_letter,
    ),
    info(
        title = "Elucid API",
        description = "Elucid ingestion, catalog, query, and operational API. Every response includes X-Request-Id. This server has no authentication and must only be exposed in a trusted environment."
    ),
    tags(
        (name = "health", description = "Process and dependency health"),
        (name = "operations", description = "Runtime status and metrics"),
        (name = "catalog", description = "Catalog application and inspection"),
        (name = "ingestion", description = "Durable NDJSON ingestion and dead letters"),
        (name = "query", description = "Synchronous EQL execution and recent requests"),
    )
)]
struct ApiDocumentation;

pub(crate) fn router(state: Arc<ApplicationState>) -> Router {
    let request_timeout = Duration::from_secs(
        state
            .configuration()
            .server()
            .request_timeout_seconds()
            .get(),
    );
    Router::new()
        .route("/health/live", get(health::liveness))
        .route("/health/ready", get(health::readiness))
        .route("/metrics", get(health::metrics))
        .route("/api/v1/status", get(health::status))
        .route(
            "/api/v1/catalog-applications",
            post(catalog::apply_catalog).layer(DefaultBodyLimit::max(
                catalog::MAXIMUM_CATALOG_DOCUMENT_BYTES,
            )),
        )
        .route("/api/v1/sources", get(catalog::list_sources))
        .route("/api/v1/sources/{source_id}", get(catalog::get_source))
        .route("/api/v1/segments", get(segments::list_segments))
        .route("/api/v1/dead-letters", get(dead_letters::list_dead_letters))
        .route(
            "/api/v1/dead-letters/{object_id}",
            get(dead_letters::read_dead_letter),
        )
        .route(
            "/api/v1/query-executions",
            get(query::list_query_executions).post(query::execute_query),
        )
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(ui_or_not_found)
        .layer(from_fn_with_state(
            request_timeout,
            middleware::enforce_request_timeout,
        ))
        // Admitted ingestion owns its deadline and keeps an append alive until it settles.
        .route(
            "/api/v1/sources/{source_name}/inputs/{input_name}/events",
            post(ingestion::ingest_events),
        )
        .merge(SwaggerUi::new("/swagger").url("/openapi.json", ApiDocumentation::openapi()))
        .layer(from_fn(middleware::request_identity))
        .with_state(state)
}

async fn ui_or_not_found(request: Request) -> Response {
    crate::ui::response(request.method(), request.uri().path())
        .unwrap_or_else(|| ApiError::not_found().into_response())
}

async fn method_not_allowed() -> ApiError {
    ApiError::method_not_allowed()
}
