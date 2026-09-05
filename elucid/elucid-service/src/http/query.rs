use std::sync::Arc;

use axum::Json;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use elucid_engine::{QueryColumn, QueryExecutionStatistics, QueryResult};
use elucid_metastore::{
    QueryExecutionId, QueryExecutionListLimit, QueryExecutionRecord, QueryRequestTimeRange,
};
use serde::ser::SerializeStruct as _;
use serde::{Deserialize, Serialize};
use utoipa::openapi::schema::{ArrayBuilder, ObjectBuilder, SchemaType, Type};
use utoipa::openapi::{KnownFormat, RefOr, SchemaFormat};
use utoipa::{PartialSchema, ToSchema};
use uuid::Uuid;

use crate::query::{CompletedQuery, QueryAdmissionFailure};
use crate::runtime::ApplicationState;

use super::body::{
    BodyReadFailure, has_json_content_type, parse_content_length, read_bounded_body,
};
use super::error::{ApiError, ErrorEnvelope};
use super::health::readiness_details;
use super::response::{ListCompletion, format_timestamp};

const MAXIMUM_QUERY_REQUEST_BYTES: u64 = 1_048_576;

const MAXIMUM_QUERY_EXECUTION_LIST_ITEMS: u64 = 50;

#[utoipa::path(
    get,
    path = "/api/v1/query-executions",
    tag = "query",
    summary = "List recent query executions",
    responses(
        (status = 200, description = "Bounded recent query execution list", body = QueryExecutionListResponse),
        (status = 500, description = "Stored query execution metadata is invalid", body = ErrorEnvelope),
        (status = 503, description = "The server or PostgreSQL is unavailable", body = ErrorEnvelope),
    )
)]
pub(super) async fn list_query_executions(State(state): State<Arc<ApplicationState>>) -> Response {
    let runtime = state.snapshot();
    let Some(dependencies) = runtime.dependencies() else {
        return ApiError::server_not_ready(readiness_details(&runtime)).into_response();
    };
    let limit = match QueryExecutionListLimit::new(MAXIMUM_QUERY_EXECUTION_LIST_ITEMS) {
        Ok(limit) => limit,
        Err(_) => return ApiError::internal().into_response(),
    };
    match dependencies.queries.recent(limit).await {
        Ok(executions) => Json(QueryExecutionListResponse {
            completion: ListCompletion::from_truncated(executions.is_truncated()),
            limit: executions.limit(),
            query_executions: executions
                .items()
                .iter()
                .map(QueryExecutionSummaryResponse::from)
                .collect(),
        })
        .into_response(),
        Err(error) => ApiError::query_execution_persistence(error).into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/query-executions",
    tag = "query",
    summary = "Execute an EQL query synchronously",
    request_body(content = QueryExecutionRequest, description = "Synchronous EQL query", content_type = "application/json"),
    responses(
        (status = 200, description = "Completed or deliberately truncated query result", body = QueryExecutionResponse),
        (status = 400, description = "Malformed request or invalid query", body = ErrorEnvelope),
        (status = 408, description = "Query exceeded its execution timeout", body = ErrorEnvelope),
        (status = 413, description = "Query request exceeds the request limit", body = ErrorEnvelope),
        (status = 415, description = "Content-Type is not application/json", body = ErrorEnvelope),
        (status = 422, description = "Query evaluation or resource limit failed", body = ErrorEnvelope),
        (status = 429, description = "Query admission capacity is exhausted", body = ErrorEnvelope),
        (status = 500, description = "Published data or internal query state is invalid", body = ErrorEnvelope),
        (status = 503, description = "The server is not ready, is draining, or query execution was cancelled", body = ErrorEnvelope),
    )
)]
pub(super) async fn execute_query(
    State(state): State<Arc<ApplicationState>>,
    request: Request,
) -> Response {
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
    let query_id = QueryExecutionId::from(Uuid::now_v7());
    let completed = match admitted
        .execute(query_id, request.query, request_range, request.output_rows)
        .await
    {
        Ok(completed) => completed,
        Err(error) => return ApiError::query(error).into_response(),
    };
    match QueryExecutionResponse::new(query_id, completed) {
        Ok(response) => Json(response).into_response(),
        Err(error) => error.into_response(),
    }
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct QueryExecutionRequest {
    query: String,
    time_range: QueryTimeRangeRequest,
    output_rows: u64,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct QueryTimeRangeRequest {
    start_inclusive: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
}

#[derive(Serialize, ToSchema)]
struct QueryExecutionListResponse {
    completion: ListCompletion,
    limit: usize,
    query_executions: Vec<QueryExecutionSummaryResponse>,
}

#[derive(Serialize, ToSchema)]
struct QueryExecutionSummaryResponse {
    #[schema(format = Uuid)]
    query_id: String,
    query: String,
    time_range: QueryTimeRangeResponse,
    output_rows: String,
    #[schema(format = DateTime)]
    submitted_at: String,
}

impl From<&QueryExecutionRecord> for QueryExecutionSummaryResponse {
    fn from(execution: &QueryExecutionRecord) -> Self {
        let time_range = execution.time_range();
        Self {
            query_id: execution.query_id().to_string(),
            query: execution.query().to_owned(),
            time_range: QueryTimeRangeResponse {
                start_inclusive: format_timestamp(time_range.start_inclusive()),
                end_exclusive: format_timestamp(time_range.end_exclusive()),
            },
            output_rows: execution.output_rows().to_string(),
            submitted_at: format_timestamp(execution.submitted_at()),
        }
    }
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
    fn new(query_id: QueryExecutionId, completed: CompletedQuery) -> Result<Self, ApiError> {
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

impl PartialSchema for QueryExecutionResponse {
    fn schema() -> RefOr<utoipa::openapi::schema::Schema> {
        let uuid = || {
            ObjectBuilder::new()
                .schema_type(Type::String)
                .format(Some(SchemaFormat::KnownFormat(KnownFormat::Uuid)))
        };
        let completion = ObjectBuilder::new()
            .schema_type(Type::String)
            .enum_values(Some(["COMPLETE", "TRUNCATED"]));
        ObjectBuilder::new()
            .property("query_id", uuid())
            .property("source_id", uuid())
            .property("active_schema_id", uuid())
            .property("active_schema_version", u64::schema())
            .property("time_range", QueryTimeRangeResponse::schema())
            .property("columns", Vec::<QueryColumnResponse>::schema())
            .property(
                "rows",
                ArrayBuilder::new().items(
                    ArrayBuilder::new()
                        .items(ObjectBuilder::new().schema_type(SchemaType::AnyValue)),
                ),
            )
            .property("completion", completion)
            .property("truncation_reason", Option::<String>::schema())
            .property("diagnostics", Vec::<QueryDiagnosticResponse>::schema())
            .property("statistics", QueryStatisticsResponse::schema())
            .required("query_id")
            .required("source_id")
            .required("active_schema_id")
            .required("active_schema_version")
            .required("time_range")
            .required("columns")
            .required("rows")
            .required("completion")
            .required("truncation_reason")
            .required("diagnostics")
            .required("statistics")
            .into()
    }
}

impl ToSchema for QueryExecutionResponse {}

#[derive(Serialize, ToSchema)]
struct QueryTimeRangeResponse {
    #[schema(format = DateTime)]
    start_inclusive: String,
    #[schema(format = DateTime)]
    end_exclusive: String,
}

#[derive(Serialize, ToSchema)]
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

#[derive(Serialize, ToSchema)]
pub(super) struct QueryDiagnosticResponse {
    code: &'static str,
    severity: &'static str,
    message: String,
    span: Option<QuerySpanResponse>,
    source_range: Option<QuerySourceRangeResponse>,
}

impl QueryDiagnosticResponse {
    pub(super) fn from_diagnostic(diagnostic: &elucid_language::Diagnostic) -> Self {
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

#[derive(Serialize, ToSchema)]
struct QuerySpanResponse {
    start_byte: usize,
    end_byte: usize,
}

#[derive(Serialize, ToSchema)]
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

#[derive(Serialize, ToSchema)]
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

#[derive(Serialize, ToSchema)]
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
