use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;
use toml_edit::DocumentMut;
use url::Url;

use super::environment::{
    DIRECT_CURSOR_HMAC_KEY, DIRECT_OPERATOR_BEARER_TOKEN, DIRECT_POSTGRESQL_DSN,
    DIRECT_S3_ACCESS_KEY_ID, DIRECT_S3_SECRET_ACCESS_KEY, DIRECT_S3_SESSION_TOKEN, Environment,
    EnvironmentLookup,
};
use super::error::{
    ConfigurationError, ConfigurationField, InvalidValueReason, SecretInvalidReason, SecretKind,
};
use super::model::{
    AddressingStyle, Applications, Attempts, Buckets, Bytes, CatalogConfiguration,
    CompactionConfiguration, Connections, Depth, EnabledServices, GarbageCollectionConfiguration,
    IngestionConfiguration, Items, LogFormat, LogLevel, MetastoreConfiguration, NetworkTrust,
    NodeService, ObjectStoreConfiguration, Objects, Queries, QueryConfiguration, Requests,
    Reservations, RetentionConfiguration, Roots, Rows, Runs, RuntimeConfiguration, Seconds,
    SecretBytes, SecretReference, SecretString, Secrets, Segments, ServerConfiguration, Stages,
    TelemetryConfiguration, non_empty_string,
};
use super::validation;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawRuntimeConfiguration {
    server: RawServerConfiguration,
    metastore: RawMetastoreConfiguration,
    catalog: RawCatalogConfiguration,
    object_store: RawObjectStoreConfiguration,
    ingestion: RawIngestionConfiguration,
    compaction: RawCompactionConfiguration,
    garbage_collection: RawGarbageCollectionConfiguration,
    retention: RawRetentionConfiguration,
    query: RawQueryConfiguration,
    telemetry: RawTelemetryConfiguration,
}

impl RawRuntimeConfiguration {
    pub(super) fn from_document(document: DocumentMut) -> Result<Self, ConfigurationError> {
        toml_edit::de::from_document(document).map_err(|_| ConfigurationError::DocumentInvalid)
    }

    pub(super) fn materialize(
        self,
        environment: &Environment,
    ) -> Result<RuntimeConfiguration, ConfigurationError> {
        let candidate = RuntimeConfigurationCandidate {
            server: self.server.materialize()?,
            metastore: self.metastore.materialize()?,
            catalog: self.catalog.materialize()?,
            object_store: self.object_store.materialize()?,
            ingestion: self.ingestion.materialize()?,
            compaction: self.compaction.materialize()?,
            garbage_collection: self.garbage_collection.materialize()?,
            retention: self.retention.materialize()?,
            query: self.query.materialize()?,
            telemetry: self.telemetry.materialize()?,
        };
        validation::validate(&candidate)?;
        let secrets = resolve_secrets(&candidate, environment)?;
        Ok(candidate.into_runtime_configuration(secrets))
    }
}

pub(super) struct RuntimeConfigurationCandidate {
    pub(super) server: ServerConfiguration,
    pub(super) metastore: MetastoreConfiguration,
    pub(super) catalog: CatalogConfiguration,
    pub(super) object_store: ObjectStoreConfiguration,
    pub(super) ingestion: IngestionConfiguration,
    pub(super) compaction: CompactionConfiguration,
    pub(super) garbage_collection: GarbageCollectionConfiguration,
    pub(super) retention: RetentionConfiguration,
    pub(super) query: QueryConfiguration,
    pub(super) telemetry: TelemetryConfiguration,
}

impl RuntimeConfigurationCandidate {
    fn into_runtime_configuration(self, secrets: Secrets) -> RuntimeConfiguration {
        RuntimeConfiguration {
            server: self.server,
            metastore: self.metastore,
            catalog: self.catalog,
            object_store: self.object_store,
            ingestion: self.ingestion,
            compaction: self.compaction,
            garbage_collection: self.garbage_collection,
            retention: self.retention,
            query: self.query,
            telemetry: self.telemetry,
            secrets,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServerConfiguration {
    bind: String,
    browser_origin: String,
    network_trust: NetworkTrust,
    enabled_services: Vec<NodeService>,
    maximum_json_request_body_bytes: u64,
    maximum_request_header_bytes: u64,
    request_timeout_seconds: u64,
    header_timeout_seconds: u64,
    idle_timeout_seconds: u64,
    shutdown_timeout_seconds: u64,
    default_page_items: u64,
    maximum_page_items: u64,
    cursor_hmac_key_environment_variable: String,
    #[serde(default)]
    operator_bearer_token_environment_variable: Option<String>,
}

impl RawServerConfiguration {
    fn materialize(self) -> Result<ServerConfiguration, ConfigurationError> {
        Ok(ServerConfiguration {
            bind: parse_socket_address(self.bind, field("server.bind"))?,
            browser_origin: parse_url(self.browser_origin, field("server.browser_origin"))?,
            network_trust: self.network_trust,
            enabled_services: EnabledServices::from_configuration(self.enabled_services)?,
            maximum_json_request_body_bytes: Bytes::from_configuration(
                self.maximum_json_request_body_bytes,
                field("server.maximum_json_request_body_bytes"),
            )?,
            maximum_request_header_bytes: Bytes::from_configuration(
                self.maximum_request_header_bytes,
                field("server.maximum_request_header_bytes"),
            )?,
            request_timeout_seconds: Seconds::from_configuration(
                self.request_timeout_seconds,
                field("server.request_timeout_seconds"),
            )?,
            header_timeout_seconds: Seconds::from_configuration(
                self.header_timeout_seconds,
                field("server.header_timeout_seconds"),
            )?,
            idle_timeout_seconds: Seconds::from_configuration(
                self.idle_timeout_seconds,
                field("server.idle_timeout_seconds"),
            )?,
            shutdown_timeout_seconds: Seconds::from_configuration(
                self.shutdown_timeout_seconds,
                field("server.shutdown_timeout_seconds"),
            )?,
            default_page_items: Items::from_configuration(
                self.default_page_items,
                field("server.default_page_items"),
            )?,
            maximum_page_items: Items::from_configuration(
                self.maximum_page_items,
                field("server.maximum_page_items"),
            )?,
            cursor_hmac_key_environment_variable: SecretReference::from_configuration(
                self.cursor_hmac_key_environment_variable,
                field("server.cursor_hmac_key_environment_variable"),
            )?,
            operator_bearer_token_environment_variable: self
                .operator_bearer_token_environment_variable
                .map(|value| {
                    SecretReference::from_configuration(
                        value,
                        field("server.operator_bearer_token_environment_variable"),
                    )
                })
                .transpose()?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMetastoreConfiguration {
    postgresql_dsn_environment_variable: String,
    maximum_connections: u64,
    connection_timeout_seconds: u64,
    migration_lock_timeout_seconds: u64,
    statement_timeout_seconds: u64,
}

impl RawMetastoreConfiguration {
    fn materialize(self) -> Result<MetastoreConfiguration, ConfigurationError> {
        Ok(MetastoreConfiguration {
            postgresql_dsn_environment_variable: SecretReference::from_configuration(
                self.postgresql_dsn_environment_variable,
                field("metastore.postgresql_dsn_environment_variable"),
            )?,
            maximum_connections: Connections::from_configuration(
                self.maximum_connections,
                field("metastore.maximum_connections"),
            )?,
            connection_timeout_seconds: Seconds::from_configuration(
                self.connection_timeout_seconds,
                field("metastore.connection_timeout_seconds"),
            )?,
            migration_lock_timeout_seconds: Seconds::from_configuration(
                self.migration_lock_timeout_seconds,
                field("metastore.migration_lock_timeout_seconds"),
            )?,
            statement_timeout_seconds: Seconds::from_configuration(
                self.statement_timeout_seconds,
                field("metastore.statement_timeout_seconds"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCatalogConfiguration {
    maximum_manifest_bytes: u64,
    maximum_concurrent_applications: u64,
}

impl RawCatalogConfiguration {
    fn materialize(self) -> Result<CatalogConfiguration, ConfigurationError> {
        Ok(CatalogConfiguration {
            maximum_manifest_bytes: Bytes::from_configuration(
                self.maximum_manifest_bytes,
                field("catalog.maximum_manifest_bytes"),
            )?,
            maximum_concurrent_applications: Applications::from_configuration(
                self.maximum_concurrent_applications,
                field("catalog.maximum_concurrent_applications"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawObjectStoreConfiguration {
    alias: String,
    authority: String,
    endpoint: String,
    region: String,
    bucket: String,
    root_prefix: String,
    addressing_style: AddressingStyle,
    access_key_id_environment_variable: String,
    secret_access_key_environment_variable: String,
    request_timeout_seconds: u64,
    maximum_request_attempts: u64,
}

impl RawObjectStoreConfiguration {
    fn materialize(self) -> Result<ObjectStoreConfiguration, ConfigurationError> {
        let endpoint = parse_url(self.endpoint, field("object_store.endpoint"))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(ConfigurationError::ValueInvalid {
                field: field("object_store.endpoint"),
                reason: InvalidValueReason::InvalidObjectStoreEndpoint,
            });
        }
        Ok(ObjectStoreConfiguration {
            alias: non_empty_string(self.alias, field("object_store.alias"))?,
            authority: non_empty_string(self.authority, field("object_store.authority"))?,
            endpoint,
            region: non_empty_string(self.region, field("object_store.region"))?,
            bucket: non_empty_string(self.bucket, field("object_store.bucket"))?,
            root_prefix: non_empty_string(self.root_prefix, field("object_store.root_prefix"))?,
            addressing_style: self.addressing_style,
            access_key_id_environment_variable: SecretReference::from_configuration(
                self.access_key_id_environment_variable,
                field("object_store.access_key_id_environment_variable"),
            )?,
            secret_access_key_environment_variable: SecretReference::from_configuration(
                self.secret_access_key_environment_variable,
                field("object_store.secret_access_key_environment_variable"),
            )?,
            request_timeout_seconds: Seconds::from_configuration(
                self.request_timeout_seconds,
                field("object_store.request_timeout_seconds"),
            )?,
            maximum_request_attempts: Attempts::from_configuration(
                self.maximum_request_attempts,
                field("object_store.maximum_request_attempts"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIngestionConfiguration {
    staging_directory: String,
    staging_capacity_bytes: u64,
    maximum_request_body_bytes: u64,
    maximum_record_bytes: u64,
    dead_letter_complete_raw_maximum_bytes: u64,
    dead_letter_raw_prefix_bytes: u64,
    maximum_dead_letter_page_bytes: u64,
    target_segment_rows: u64,
    target_segment_uncompressed_bytes: u64,
    maximum_parquet_row_group_rows: u64,
    maximum_open_event_time_buckets: u64,
    maximum_concurrent_requests: u64,
    attempt_heartbeat_interval_seconds: u64,
    attempt_stale_after_seconds: u64,
    attempt_timeout_seconds: u64,
}

impl RawIngestionConfiguration {
    fn materialize(self) -> Result<IngestionConfiguration, ConfigurationError> {
        Ok(IngestionConfiguration {
            staging_directory: non_empty_path(
                self.staging_directory,
                field("ingestion.staging_directory"),
            )?,
            staging_capacity_bytes: Bytes::from_configuration(
                self.staging_capacity_bytes,
                field("ingestion.staging_capacity_bytes"),
            )?,
            maximum_request_body_bytes: Bytes::from_configuration(
                self.maximum_request_body_bytes,
                field("ingestion.maximum_request_body_bytes"),
            )?,
            maximum_record_bytes: Bytes::from_configuration(
                self.maximum_record_bytes,
                field("ingestion.maximum_record_bytes"),
            )?,
            dead_letter_complete_raw_maximum_bytes: Bytes::from_configuration(
                self.dead_letter_complete_raw_maximum_bytes,
                field("ingestion.dead_letter_complete_raw_maximum_bytes"),
            )?,
            dead_letter_raw_prefix_bytes: Bytes::from_configuration(
                self.dead_letter_raw_prefix_bytes,
                field("ingestion.dead_letter_raw_prefix_bytes"),
            )?,
            maximum_dead_letter_page_bytes: Bytes::from_configuration(
                self.maximum_dead_letter_page_bytes,
                field("ingestion.maximum_dead_letter_page_bytes"),
            )?,
            target_segment_rows: Rows::from_configuration(
                self.target_segment_rows,
                field("ingestion.target_segment_rows"),
            )?,
            target_segment_uncompressed_bytes: Bytes::from_configuration(
                self.target_segment_uncompressed_bytes,
                field("ingestion.target_segment_uncompressed_bytes"),
            )?,
            maximum_parquet_row_group_rows: Rows::from_configuration(
                self.maximum_parquet_row_group_rows,
                field("ingestion.maximum_parquet_row_group_rows"),
            )?,
            maximum_open_event_time_buckets: Buckets::from_configuration(
                self.maximum_open_event_time_buckets,
                field("ingestion.maximum_open_event_time_buckets"),
            )?,
            maximum_concurrent_requests: Requests::from_configuration(
                self.maximum_concurrent_requests,
                field("ingestion.maximum_concurrent_requests"),
            )?,
            attempt_heartbeat_interval_seconds: Seconds::from_configuration(
                self.attempt_heartbeat_interval_seconds,
                field("ingestion.attempt_heartbeat_interval_seconds"),
            )?,
            attempt_stale_after_seconds: Seconds::from_configuration(
                self.attempt_stale_after_seconds,
                field("ingestion.attempt_stale_after_seconds"),
            )?,
            attempt_timeout_seconds: Seconds::from_configuration(
                self.attempt_timeout_seconds,
                field("ingestion.attempt_timeout_seconds"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCompactionConfiguration {
    scan_interval_seconds: u64,
    working_directory: String,
    working_capacity_bytes: u64,
    memory_pool_bytes: u64,
    minimum_input_segments: u64,
    maximum_input_segments: u64,
    maximum_input_rows: u64,
    maximum_input_uncompressed_bytes: u64,
    maximum_input_parquet_bytes: u64,
    target_output_segment_uncompressed_bytes: u64,
    maximum_output_segment_rows: u64,
    maximum_parquet_row_group_rows: u64,
    maximum_output_segments: u64,
    maximum_output_parquet_object_bytes: u64,
    maximum_output_parquet_bytes: u64,
    maximum_concurrent_runs: u64,
    maximum_cluster_concurrent_runs: u64,
    maximum_recovery_batch_runs: u64,
    run_heartbeat_interval_seconds: u64,
    run_stale_after_seconds: u64,
    run_timeout_seconds: u64,
}

impl RawCompactionConfiguration {
    fn materialize(self) -> Result<CompactionConfiguration, ConfigurationError> {
        Ok(CompactionConfiguration {
            scan_interval_seconds: Seconds::from_configuration(
                self.scan_interval_seconds,
                field("compaction.scan_interval_seconds"),
            )?,
            working_directory: non_empty_path(
                self.working_directory,
                field("compaction.working_directory"),
            )?,
            working_capacity_bytes: Bytes::from_configuration(
                self.working_capacity_bytes,
                field("compaction.working_capacity_bytes"),
            )?,
            memory_pool_bytes: Bytes::from_configuration(
                self.memory_pool_bytes,
                field("compaction.memory_pool_bytes"),
            )?,
            minimum_input_segments: Segments::from_configuration(
                self.minimum_input_segments,
                field("compaction.minimum_input_segments"),
            )?,
            maximum_input_segments: Segments::from_configuration(
                self.maximum_input_segments,
                field("compaction.maximum_input_segments"),
            )?,
            maximum_input_rows: Rows::from_configuration(
                self.maximum_input_rows,
                field("compaction.maximum_input_rows"),
            )?,
            maximum_input_uncompressed_bytes: Bytes::from_configuration(
                self.maximum_input_uncompressed_bytes,
                field("compaction.maximum_input_uncompressed_bytes"),
            )?,
            maximum_input_parquet_bytes: Bytes::from_configuration(
                self.maximum_input_parquet_bytes,
                field("compaction.maximum_input_parquet_bytes"),
            )?,
            target_output_segment_uncompressed_bytes: Bytes::from_configuration(
                self.target_output_segment_uncompressed_bytes,
                field("compaction.target_output_segment_uncompressed_bytes"),
            )?,
            maximum_output_segment_rows: Rows::from_configuration(
                self.maximum_output_segment_rows,
                field("compaction.maximum_output_segment_rows"),
            )?,
            maximum_parquet_row_group_rows: Rows::from_configuration(
                self.maximum_parquet_row_group_rows,
                field("compaction.maximum_parquet_row_group_rows"),
            )?,
            maximum_output_segments: Segments::from_configuration(
                self.maximum_output_segments,
                field("compaction.maximum_output_segments"),
            )?,
            maximum_output_parquet_object_bytes: Bytes::from_configuration(
                self.maximum_output_parquet_object_bytes,
                field("compaction.maximum_output_parquet_object_bytes"),
            )?,
            maximum_output_parquet_bytes: Bytes::from_configuration(
                self.maximum_output_parquet_bytes,
                field("compaction.maximum_output_parquet_bytes"),
            )?,
            maximum_concurrent_runs: Runs::from_configuration(
                self.maximum_concurrent_runs,
                field("compaction.maximum_concurrent_runs"),
            )?,
            maximum_cluster_concurrent_runs: Runs::from_configuration(
                self.maximum_cluster_concurrent_runs,
                field("compaction.maximum_cluster_concurrent_runs"),
            )?,
            maximum_recovery_batch_runs: Runs::from_configuration(
                self.maximum_recovery_batch_runs,
                field("compaction.maximum_recovery_batch_runs"),
            )?,
            run_heartbeat_interval_seconds: Seconds::from_configuration(
                self.run_heartbeat_interval_seconds,
                field("compaction.run_heartbeat_interval_seconds"),
            )?,
            run_stale_after_seconds: Seconds::from_configuration(
                self.run_stale_after_seconds,
                field("compaction.run_stale_after_seconds"),
            )?,
            run_timeout_seconds: Seconds::from_configuration(
                self.run_timeout_seconds,
                field("compaction.run_timeout_seconds"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGarbageCollectionConfiguration {
    orphan_grace_period_seconds: u64,
    retired_object_grace_period_seconds: u64,
    scan_interval_seconds: u64,
    maximum_batch_objects: u64,
    maximum_concurrent_object_deletions: u64,
}

impl RawGarbageCollectionConfiguration {
    fn materialize(self) -> Result<GarbageCollectionConfiguration, ConfigurationError> {
        Ok(GarbageCollectionConfiguration {
            orphan_grace_period_seconds: Seconds::from_configuration(
                self.orphan_grace_period_seconds,
                field("garbage_collection.orphan_grace_period_seconds"),
            )?,
            retired_object_grace_period_seconds: Seconds::from_configuration(
                self.retired_object_grace_period_seconds,
                field("garbage_collection.retired_object_grace_period_seconds"),
            )?,
            scan_interval_seconds: Seconds::from_configuration(
                self.scan_interval_seconds,
                field("garbage_collection.scan_interval_seconds"),
            )?,
            maximum_batch_objects: Objects::from_configuration(
                self.maximum_batch_objects,
                field("garbage_collection.maximum_batch_objects"),
            )?,
            maximum_concurrent_object_deletions: Objects::from_configuration(
                self.maximum_concurrent_object_deletions,
                field("garbage_collection.maximum_concurrent_object_deletions"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRetentionConfiguration {
    idempotency_retention_seconds: u64,
    event_data_retention_seconds: u64,
    dead_letter_retention_seconds: u64,
    ingestion_provenance_retention_seconds: u64,
    compaction_provenance_retention_seconds: u64,
    scan_interval_seconds: u64,
    maximum_task_duration_seconds: u64,
    maximum_expiration_batch_segments: u64,
    maximum_retry_expiration_batch_requests: u64,
    maximum_idempotency_expiration_batch_reservations: u64,
    maximum_provenance_roots_per_batch: u64,
}

impl RawRetentionConfiguration {
    fn materialize(self) -> Result<RetentionConfiguration, ConfigurationError> {
        Ok(RetentionConfiguration {
            idempotency_retention_seconds: Seconds::from_configuration(
                self.idempotency_retention_seconds,
                field("retention.idempotency_retention_seconds"),
            )?,
            event_data_retention_seconds: Seconds::from_configuration(
                self.event_data_retention_seconds,
                field("retention.event_data_retention_seconds"),
            )?,
            dead_letter_retention_seconds: Seconds::from_configuration(
                self.dead_letter_retention_seconds,
                field("retention.dead_letter_retention_seconds"),
            )?,
            ingestion_provenance_retention_seconds: Seconds::from_configuration(
                self.ingestion_provenance_retention_seconds,
                field("retention.ingestion_provenance_retention_seconds"),
            )?,
            compaction_provenance_retention_seconds: Seconds::from_configuration(
                self.compaction_provenance_retention_seconds,
                field("retention.compaction_provenance_retention_seconds"),
            )?,
            scan_interval_seconds: Seconds::from_configuration(
                self.scan_interval_seconds,
                field("retention.scan_interval_seconds"),
            )?,
            maximum_task_duration_seconds: Seconds::from_configuration(
                self.maximum_task_duration_seconds,
                field("retention.maximum_task_duration_seconds"),
            )?,
            maximum_expiration_batch_segments: Segments::from_configuration(
                self.maximum_expiration_batch_segments,
                field("retention.maximum_expiration_batch_segments"),
            )?,
            maximum_retry_expiration_batch_requests: Requests::from_configuration(
                self.maximum_retry_expiration_batch_requests,
                field("retention.maximum_retry_expiration_batch_requests"),
            )?,
            maximum_idempotency_expiration_batch_reservations: Reservations::from_configuration(
                self.maximum_idempotency_expiration_batch_reservations,
                field("retention.maximum_idempotency_expiration_batch_reservations"),
            )?,
            maximum_provenance_roots_per_batch: Roots::from_configuration(
                self.maximum_provenance_roots_per_batch,
                field("retention.maximum_provenance_roots_per_batch"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawQueryConfiguration {
    default_output_rows: u64,
    maximum_output_rows: u64,
    maximum_result_bytes: u64,
    maximum_query_bytes: u64,
    maximum_pipeline_stages: u64,
    maximum_expression_depth: u64,
    maximum_selected_segments: u64,
    maximum_selected_object_bytes: u64,
    execution_timeout_seconds: u64,
    memory_pool_bytes: u64,
    spill_directory: String,
    spill_capacity_bytes: u64,
    maximum_concurrent_queries: u64,
    maximum_queued_queries: u64,
}

impl RawQueryConfiguration {
    fn materialize(self) -> Result<QueryConfiguration, ConfigurationError> {
        Ok(QueryConfiguration {
            default_output_rows: Rows::from_configuration(
                self.default_output_rows,
                field("query.default_output_rows"),
            )?,
            maximum_output_rows: Rows::from_configuration(
                self.maximum_output_rows,
                field("query.maximum_output_rows"),
            )?,
            maximum_result_bytes: Bytes::from_configuration(
                self.maximum_result_bytes,
                field("query.maximum_result_bytes"),
            )?,
            maximum_query_bytes: Bytes::from_configuration(
                self.maximum_query_bytes,
                field("query.maximum_query_bytes"),
            )?,
            maximum_pipeline_stages: Stages::from_configuration(
                self.maximum_pipeline_stages,
                field("query.maximum_pipeline_stages"),
            )?,
            maximum_expression_depth: Depth::from_configuration(
                self.maximum_expression_depth,
                field("query.maximum_expression_depth"),
            )?,
            maximum_selected_segments: Segments::from_configuration(
                self.maximum_selected_segments,
                field("query.maximum_selected_segments"),
            )?,
            maximum_selected_object_bytes: Bytes::from_configuration(
                self.maximum_selected_object_bytes,
                field("query.maximum_selected_object_bytes"),
            )?,
            execution_timeout_seconds: Seconds::from_configuration(
                self.execution_timeout_seconds,
                field("query.execution_timeout_seconds"),
            )?,
            memory_pool_bytes: Bytes::from_configuration(
                self.memory_pool_bytes,
                field("query.memory_pool_bytes"),
            )?,
            spill_directory: non_empty_path(self.spill_directory, field("query.spill_directory"))?,
            spill_capacity_bytes: Bytes::from_configuration(
                self.spill_capacity_bytes,
                field("query.spill_capacity_bytes"),
            )?,
            maximum_concurrent_queries: Queries::from_configuration(
                self.maximum_concurrent_queries,
                field("query.maximum_concurrent_queries"),
            )?,
            maximum_queued_queries: Queries::from_configuration(
                self.maximum_queued_queries,
                field("query.maximum_queued_queries"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTelemetryConfiguration {
    log_format: LogFormat,
    log_level: LogLevel,
    metrics_path: String,
}

impl RawTelemetryConfiguration {
    fn materialize(self) -> Result<TelemetryConfiguration, ConfigurationError> {
        if !self.metrics_path.starts_with('/') || self.metrics_path.starts_with("//") {
            return Err(ConfigurationError::ValueInvalid {
                field: field("telemetry.metrics_path"),
                reason: InvalidValueReason::InvalidMetricsPath,
            });
        }
        Ok(TelemetryConfiguration {
            log_format: self.log_format,
            log_level: self.log_level,
            metrics_path: self.metrics_path,
        })
    }
}

fn resolve_secrets(
    candidate: &RuntimeConfigurationCandidate,
    environment: &Environment,
) -> Result<Secrets, ConfigurationError> {
    let cursor_hmac_key = resolve_required_secret(
        environment,
        SecretKind::CursorHmacKey,
        DIRECT_CURSOR_HMAC_KEY,
        &candidate.server.cursor_hmac_key_environment_variable,
    )?;
    if cursor_hmac_key.as_bytes().len() < 32 {
        return Err(invalid_secret(
            SecretKind::CursorHmacKey,
            cursor_hmac_key.source_environment_variable,
            SecretInvalidReason::CursorKeyTooShort,
        ));
    }

    let postgresql_dsn = resolve_required_secret(
        environment,
        SecretKind::PostgreSqlDsn,
        DIRECT_POSTGRESQL_DSN,
        &candidate.metastore.postgresql_dsn_environment_variable,
    )?;
    let s3_access_key_id = resolve_required_secret(
        environment,
        SecretKind::S3AccessKeyId,
        DIRECT_S3_ACCESS_KEY_ID,
        &candidate.object_store.access_key_id_environment_variable,
    )?;
    let s3_secret_access_key = resolve_required_secret(
        environment,
        SecretKind::S3SecretAccessKey,
        DIRECT_S3_SECRET_ACCESS_KEY,
        &candidate
            .object_store
            .secret_access_key_environment_variable,
    )?;
    let s3_session_token = resolve_optional_direct_secret(
        environment,
        SecretKind::S3SessionToken,
        DIRECT_S3_SESSION_TOKEN,
    )?;
    let operator_bearer_token = candidate
        .server
        .operator_bearer_token_environment_variable
        .as_ref()
        .map(|reference| {
            resolve_required_secret(
                environment,
                SecretKind::OperatorBearerToken,
                DIRECT_OPERATOR_BEARER_TOKEN,
                reference,
            )
        })
        .transpose()?;
    if let Some(secret) = &operator_bearer_token {
        if secret.value.len() < 32 {
            return Err(invalid_secret(
                SecretKind::OperatorBearerToken,
                secret.source_environment_variable.clone(),
                SecretInvalidReason::OperatorTokenTooShort,
            ));
        }
        if !secret
            .value
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(invalid_secret(
                SecretKind::OperatorBearerToken,
                secret.source_environment_variable.clone(),
                SecretInvalidReason::OperatorTokenNotVisibleAscii,
            ));
        }
    }

    Ok(Secrets {
        cursor_hmac_key: SecretBytes::new(cursor_hmac_key.value.into_bytes()),
        postgresql_dsn: SecretString::new(postgresql_dsn.value),
        s3_access_key_id: SecretString::new(s3_access_key_id.value),
        s3_secret_access_key: SecretString::new(s3_secret_access_key.value),
        s3_session_token: s3_session_token.map(|secret| SecretString::new(secret.value)),
        operator_bearer_token: operator_bearer_token.map(|secret| SecretString::new(secret.value)),
    })
}

struct ResolvedSecret {
    source_environment_variable: String,
    value: String,
}

impl ResolvedSecret {
    fn as_bytes(&self) -> &[u8] {
        self.value.as_bytes()
    }
}

fn resolve_required_secret(
    environment: &Environment,
    kind: SecretKind,
    direct_environment_variable: &str,
    reference: &SecretReference,
) -> Result<ResolvedSecret, ConfigurationError> {
    match resolve_environment_value(environment, kind, direct_environment_variable)? {
        Some(value) => validate_non_empty_secret(kind, direct_environment_variable, value),
        None => match resolve_environment_value(environment, kind, reference.as_str())? {
            Some(value) => validate_non_empty_secret(kind, reference.as_str(), value),
            None => Err(ConfigurationError::SecretMissing {
                kind,
                environment_variable: reference.as_str().to_owned(),
            }),
        },
    }
}

fn resolve_optional_direct_secret(
    environment: &Environment,
    kind: SecretKind,
    direct_environment_variable: &str,
) -> Result<Option<ResolvedSecret>, ConfigurationError> {
    if let Some(value) = resolve_environment_value(environment, kind, direct_environment_variable)?
    {
        return validate_non_empty_secret(kind, direct_environment_variable, value).map(Some);
    }
    Ok(None)
}

fn resolve_environment_value<'a>(
    environment: &'a Environment,
    kind: SecretKind,
    environment_variable: &str,
) -> Result<Option<&'a str>, ConfigurationError> {
    match environment.value(environment_variable) {
        EnvironmentLookup::Missing => Ok(None),
        EnvironmentLookup::Unicode(value) => Ok(Some(value)),
        EnvironmentLookup::NotUnicode => Err(invalid_secret(
            kind,
            environment_variable.to_owned(),
            SecretInvalidReason::ValueNotUnicode,
        )),
    }
}

fn validate_non_empty_secret(
    kind: SecretKind,
    environment_variable: &str,
    value: &str,
) -> Result<ResolvedSecret, ConfigurationError> {
    if value.is_empty() {
        return Err(invalid_secret(
            kind,
            environment_variable.to_owned(),
            SecretInvalidReason::Empty,
        ));
    }
    Ok(ResolvedSecret {
        source_environment_variable: environment_variable.to_owned(),
        value: value.to_owned(),
    })
}

fn invalid_secret(
    kind: SecretKind,
    environment_variable: String,
    reason: SecretInvalidReason,
) -> ConfigurationError {
    ConfigurationError::SecretInvalid {
        kind,
        environment_variable,
        reason,
    }
}

fn parse_socket_address(
    value: String,
    field: ConfigurationField,
) -> Result<SocketAddr, ConfigurationError> {
    value.parse().map_err(|_| ConfigurationError::ValueInvalid {
        field,
        reason: InvalidValueReason::InvalidSocketAddress,
    })
}

fn parse_url(value: String, field: ConfigurationField) -> Result<Url, ConfigurationError> {
    Url::parse(&value).map_err(|_| ConfigurationError::ValueInvalid {
        field,
        reason: InvalidValueReason::InvalidUrl,
    })
}

fn non_empty_path(value: String, field: ConfigurationField) -> Result<PathBuf, ConfigurationError> {
    if value.is_empty() {
        return Err(ConfigurationError::ValueInvalid {
            field,
            reason: InvalidValueReason::Empty,
        });
    }
    Ok(PathBuf::from(value))
}

const fn field(path: &'static str) -> ConfigurationField {
    ConfigurationField::new(path)
}
