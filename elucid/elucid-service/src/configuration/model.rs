use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use url::Url;

use super::error::{
    ConfigurationError, ConfigurationField, ConfigurationViolation, InvalidValueReason,
};

macro_rules! positive_measurement {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[non_exhaustive]
        pub struct $name(NonZeroU64);

        impl $name {
            pub(super) fn from_configuration(
                value: u64,
                field: ConfigurationField,
            ) -> Result<Self, ConfigurationError> {
                NonZeroU64::new(value)
                    .map(Self)
                    .ok_or(ConfigurationError::ValueInvalid {
                        field,
                        reason: InvalidValueReason::RequiredPositive,
                    })
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }
    };
}

positive_measurement!(Applications);
positive_measurement!(Attempts);
positive_measurement!(Buckets);
positive_measurement!(Bytes);
positive_measurement!(Connections);
positive_measurement!(Depth);
positive_measurement!(Items);
positive_measurement!(Objects);
positive_measurement!(Queries);
positive_measurement!(Requests);
positive_measurement!(Reservations);
positive_measurement!(Roots);
positive_measurement!(Rows);
positive_measurement!(Runs);
positive_measurement!(Seconds);
positive_measurement!(Segments);
positive_measurement!(Stages);

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeService {
    Ingestion,
    Query,
    Maintenance,
}

impl std::fmt::Display for NodeService {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Ingestion => "INGESTION",
            Self::Query => "QUERY",
            Self::Maintenance => "MAINTENANCE",
        };
        formatter.write_str(name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct EnabledServices(Vec<NodeService>);

impl EnabledServices {
    pub(super) fn from_configuration(
        services: Vec<NodeService>,
    ) -> Result<Self, ConfigurationError> {
        if services.is_empty() {
            return Err(ConfigurationError::ConstraintViolation {
                violation: ConfigurationViolation::EmptyEnabledServices,
            });
        }

        let mut unique = BTreeSet::new();
        for service in services {
            if !unique.insert(service) {
                return Err(ConfigurationError::ConstraintViolation {
                    violation: ConfigurationViolation::DuplicateEnabledService { service },
                });
            }
        }
        Ok(Self(unique.into_iter().collect()))
    }

    #[must_use]
    pub fn contains(&self, service: NodeService) -> bool {
        self.0.binary_search(&service).is_ok()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[NodeService] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NetworkTrust {
    LoopbackOnly,
    LocalContainer,
    TrustedNetwork,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AddressingStyle {
    Path,
    VirtualHosted,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SecretReference(String);

impl SecretReference {
    pub(super) fn from_configuration(
        value: String,
        field: ConfigurationField,
    ) -> Result<Self, ConfigurationError> {
        if !is_portable_environment_variable_name(&value) {
            return Err(ConfigurationError::ValueInvalid {
                field,
                reason: InvalidValueReason::InvalidEnvironmentVariableName,
            });
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub struct SecretString(String);

impl SecretString {
    pub(super) fn new(value: String) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl Debug for SecretString {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

impl std::fmt::Display for SecretString {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub struct SecretBytes(Vec<u8>);

impl SecretBytes {
    pub(super) fn new(value: Vec<u8>) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn expose_secret(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for SecretBytes {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

impl std::fmt::Display for SecretBytes {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Secrets {
    pub(super) cursor_hmac_key: SecretBytes,
    pub(super) postgresql_dsn: SecretString,
    pub(super) s3_access_key_id: SecretString,
    pub(super) s3_secret_access_key: SecretString,
    pub(super) s3_session_token: Option<SecretString>,
    pub(super) operator_bearer_token: Option<SecretString>,
}

impl Secrets {
    #[must_use]
    pub const fn cursor_hmac_key(&self) -> &SecretBytes {
        &self.cursor_hmac_key
    }

    #[must_use]
    pub const fn postgresql_dsn(&self) -> &SecretString {
        &self.postgresql_dsn
    }

    #[must_use]
    pub const fn s3_access_key_id(&self) -> &SecretString {
        &self.s3_access_key_id
    }

    #[must_use]
    pub const fn s3_secret_access_key(&self) -> &SecretString {
        &self.s3_secret_access_key
    }

    #[must_use]
    pub const fn s3_session_token(&self) -> Option<&SecretString> {
        self.s3_session_token.as_ref()
    }

    #[must_use]
    pub const fn operator_bearer_token(&self) -> Option<&SecretString> {
        self.operator_bearer_token.as_ref()
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RuntimeConfiguration {
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
    pub(super) secrets: Secrets,
}

impl RuntimeConfiguration {
    #[must_use]
    pub const fn server(&self) -> &ServerConfiguration {
        &self.server
    }

    #[must_use]
    pub const fn metastore(&self) -> &MetastoreConfiguration {
        &self.metastore
    }

    #[must_use]
    pub const fn catalog(&self) -> &CatalogConfiguration {
        &self.catalog
    }

    #[must_use]
    pub const fn object_store(&self) -> &ObjectStoreConfiguration {
        &self.object_store
    }

    #[must_use]
    pub const fn ingestion(&self) -> &IngestionConfiguration {
        &self.ingestion
    }

    #[must_use]
    pub const fn compaction(&self) -> &CompactionConfiguration {
        &self.compaction
    }

    #[must_use]
    pub const fn garbage_collection(&self) -> &GarbageCollectionConfiguration {
        &self.garbage_collection
    }

    #[must_use]
    pub const fn retention(&self) -> &RetentionConfiguration {
        &self.retention
    }

    #[must_use]
    pub const fn query(&self) -> &QueryConfiguration {
        &self.query
    }

    #[must_use]
    pub const fn telemetry(&self) -> &TelemetryConfiguration {
        &self.telemetry
    }

    #[must_use]
    pub const fn secrets(&self) -> &Secrets {
        &self.secrets
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ServerConfiguration {
    pub(super) bind: SocketAddr,
    pub(super) browser_origin: Url,
    pub(super) network_trust: NetworkTrust,
    pub(super) enabled_services: EnabledServices,
    pub(super) maximum_json_request_body_bytes: Bytes,
    pub(super) maximum_request_header_bytes: Bytes,
    pub(super) request_timeout_seconds: Seconds,
    pub(super) header_timeout_seconds: Seconds,
    pub(super) idle_timeout_seconds: Seconds,
    pub(super) shutdown_timeout_seconds: Seconds,
    pub(super) default_page_items: Items,
    pub(super) maximum_page_items: Items,
    pub(super) cursor_hmac_key_environment_variable: SecretReference,
    pub(super) operator_bearer_token_environment_variable: Option<SecretReference>,
}

impl ServerConfiguration {
    #[must_use]
    pub const fn bind(&self) -> SocketAddr {
        self.bind
    }

    #[must_use]
    pub const fn browser_origin(&self) -> &Url {
        &self.browser_origin
    }

    #[must_use]
    pub const fn network_trust(&self) -> NetworkTrust {
        self.network_trust
    }

    #[must_use]
    pub const fn enabled_services(&self) -> &EnabledServices {
        &self.enabled_services
    }

    #[must_use]
    pub const fn maximum_json_request_body_bytes(&self) -> Bytes {
        self.maximum_json_request_body_bytes
    }

    #[must_use]
    pub const fn maximum_request_header_bytes(&self) -> Bytes {
        self.maximum_request_header_bytes
    }

    #[must_use]
    pub const fn request_timeout_seconds(&self) -> Seconds {
        self.request_timeout_seconds
    }

    #[must_use]
    pub const fn header_timeout_seconds(&self) -> Seconds {
        self.header_timeout_seconds
    }

    #[must_use]
    pub const fn idle_timeout_seconds(&self) -> Seconds {
        self.idle_timeout_seconds
    }

    #[must_use]
    pub const fn shutdown_timeout_seconds(&self) -> Seconds {
        self.shutdown_timeout_seconds
    }

    #[must_use]
    pub const fn default_page_items(&self) -> Items {
        self.default_page_items
    }

    #[must_use]
    pub const fn maximum_page_items(&self) -> Items {
        self.maximum_page_items
    }

    #[must_use]
    pub const fn cursor_hmac_key_environment_variable(&self) -> &SecretReference {
        &self.cursor_hmac_key_environment_variable
    }

    #[must_use]
    pub const fn operator_bearer_token_environment_variable(&self) -> Option<&SecretReference> {
        self.operator_bearer_token_environment_variable.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MetastoreConfiguration {
    pub(super) postgresql_dsn_environment_variable: SecretReference,
    pub(super) maximum_connections: Connections,
    pub(super) connection_timeout_seconds: Seconds,
    pub(super) migration_lock_timeout_seconds: Seconds,
    pub(super) statement_timeout_seconds: Seconds,
}

impl MetastoreConfiguration {
    #[must_use]
    pub const fn postgresql_dsn_environment_variable(&self) -> &SecretReference {
        &self.postgresql_dsn_environment_variable
    }

    #[must_use]
    pub const fn maximum_connections(&self) -> Connections {
        self.maximum_connections
    }

    #[must_use]
    pub const fn connection_timeout_seconds(&self) -> Seconds {
        self.connection_timeout_seconds
    }

    #[must_use]
    pub const fn migration_lock_timeout_seconds(&self) -> Seconds {
        self.migration_lock_timeout_seconds
    }

    #[must_use]
    pub const fn statement_timeout_seconds(&self) -> Seconds {
        self.statement_timeout_seconds
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CatalogConfiguration {
    pub(super) maximum_manifest_bytes: Bytes,
    pub(super) maximum_concurrent_applications: Applications,
}

impl CatalogConfiguration {
    #[must_use]
    pub const fn maximum_manifest_bytes(&self) -> Bytes {
        self.maximum_manifest_bytes
    }

    #[must_use]
    pub const fn maximum_concurrent_applications(&self) -> Applications {
        self.maximum_concurrent_applications
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ObjectStoreConfiguration {
    pub(super) alias: String,
    pub(super) authority: String,
    pub(super) endpoint: Url,
    pub(super) region: String,
    pub(super) bucket: String,
    pub(super) root_prefix: String,
    pub(super) addressing_style: AddressingStyle,
    pub(super) access_key_id_environment_variable: SecretReference,
    pub(super) secret_access_key_environment_variable: SecretReference,
    pub(super) request_timeout_seconds: Seconds,
    pub(super) maximum_request_attempts: Attempts,
}

impl ObjectStoreConfiguration {
    #[must_use]
    pub fn alias(&self) -> &str {
        &self.alias
    }

    #[must_use]
    pub fn authority(&self) -> &str {
        &self.authority
    }

    #[must_use]
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }

    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    #[must_use]
    pub fn root_prefix(&self) -> &str {
        &self.root_prefix
    }

    #[must_use]
    pub const fn addressing_style(&self) -> AddressingStyle {
        self.addressing_style
    }

    #[must_use]
    pub const fn access_key_id_environment_variable(&self) -> &SecretReference {
        &self.access_key_id_environment_variable
    }

    #[must_use]
    pub const fn secret_access_key_environment_variable(&self) -> &SecretReference {
        &self.secret_access_key_environment_variable
    }

    #[must_use]
    pub const fn request_timeout_seconds(&self) -> Seconds {
        self.request_timeout_seconds
    }

    #[must_use]
    pub const fn maximum_request_attempts(&self) -> Attempts {
        self.maximum_request_attempts
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct IngestionConfiguration {
    pub(super) staging_directory: PathBuf,
    pub(super) staging_capacity_bytes: Bytes,
    pub(super) maximum_request_body_bytes: Bytes,
    pub(super) maximum_record_bytes: Bytes,
    pub(super) dead_letter_complete_raw_maximum_bytes: Bytes,
    pub(super) dead_letter_raw_prefix_bytes: Bytes,
    pub(super) maximum_dead_letter_page_bytes: Bytes,
    pub(super) target_segment_rows: Rows,
    pub(super) target_segment_uncompressed_bytes: Bytes,
    pub(super) maximum_parquet_row_group_rows: Rows,
    pub(super) maximum_open_event_time_buckets: Buckets,
    pub(super) maximum_concurrent_requests: Requests,
    pub(super) attempt_heartbeat_interval_seconds: Seconds,
    pub(super) attempt_stale_after_seconds: Seconds,
    pub(super) attempt_timeout_seconds: Seconds,
}

impl IngestionConfiguration {
    #[must_use]
    pub fn staging_directory(&self) -> &Path {
        &self.staging_directory
    }

    #[must_use]
    pub const fn staging_capacity_bytes(&self) -> Bytes {
        self.staging_capacity_bytes
    }

    #[must_use]
    pub const fn maximum_request_body_bytes(&self) -> Bytes {
        self.maximum_request_body_bytes
    }

    #[must_use]
    pub const fn maximum_record_bytes(&self) -> Bytes {
        self.maximum_record_bytes
    }

    #[must_use]
    pub const fn dead_letter_complete_raw_maximum_bytes(&self) -> Bytes {
        self.dead_letter_complete_raw_maximum_bytes
    }

    #[must_use]
    pub const fn dead_letter_raw_prefix_bytes(&self) -> Bytes {
        self.dead_letter_raw_prefix_bytes
    }

    #[must_use]
    pub const fn maximum_dead_letter_page_bytes(&self) -> Bytes {
        self.maximum_dead_letter_page_bytes
    }

    #[must_use]
    pub const fn target_segment_rows(&self) -> Rows {
        self.target_segment_rows
    }

    #[must_use]
    pub const fn target_segment_uncompressed_bytes(&self) -> Bytes {
        self.target_segment_uncompressed_bytes
    }

    #[must_use]
    pub const fn maximum_parquet_row_group_rows(&self) -> Rows {
        self.maximum_parquet_row_group_rows
    }

    #[must_use]
    pub const fn maximum_open_event_time_buckets(&self) -> Buckets {
        self.maximum_open_event_time_buckets
    }

    #[must_use]
    pub const fn maximum_concurrent_requests(&self) -> Requests {
        self.maximum_concurrent_requests
    }

    #[must_use]
    pub const fn attempt_heartbeat_interval_seconds(&self) -> Seconds {
        self.attempt_heartbeat_interval_seconds
    }

    #[must_use]
    pub const fn attempt_stale_after_seconds(&self) -> Seconds {
        self.attempt_stale_after_seconds
    }

    #[must_use]
    pub const fn attempt_timeout_seconds(&self) -> Seconds {
        self.attempt_timeout_seconds
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CompactionConfiguration {
    pub(super) scan_interval_seconds: Seconds,
    pub(super) working_directory: PathBuf,
    pub(super) working_capacity_bytes: Bytes,
    pub(super) memory_pool_bytes: Bytes,
    pub(super) minimum_input_segments: Segments,
    pub(super) maximum_input_segments: Segments,
    pub(super) maximum_input_rows: Rows,
    pub(super) maximum_input_uncompressed_bytes: Bytes,
    pub(super) maximum_input_parquet_bytes: Bytes,
    pub(super) target_output_segment_uncompressed_bytes: Bytes,
    pub(super) maximum_output_segment_rows: Rows,
    pub(super) maximum_parquet_row_group_rows: Rows,
    pub(super) maximum_output_segments: Segments,
    pub(super) maximum_output_parquet_object_bytes: Bytes,
    pub(super) maximum_output_parquet_bytes: Bytes,
    pub(super) maximum_concurrent_runs: Runs,
    pub(super) maximum_cluster_concurrent_runs: Runs,
    pub(super) maximum_recovery_batch_runs: Runs,
    pub(super) run_heartbeat_interval_seconds: Seconds,
    pub(super) run_stale_after_seconds: Seconds,
    pub(super) run_timeout_seconds: Seconds,
}

impl CompactionConfiguration {
    #[must_use]
    pub const fn scan_interval_seconds(&self) -> Seconds {
        self.scan_interval_seconds
    }

    #[must_use]
    pub fn working_directory(&self) -> &Path {
        &self.working_directory
    }

    #[must_use]
    pub const fn working_capacity_bytes(&self) -> Bytes {
        self.working_capacity_bytes
    }

    #[must_use]
    pub const fn memory_pool_bytes(&self) -> Bytes {
        self.memory_pool_bytes
    }

    #[must_use]
    pub const fn minimum_input_segments(&self) -> Segments {
        self.minimum_input_segments
    }

    #[must_use]
    pub const fn maximum_input_segments(&self) -> Segments {
        self.maximum_input_segments
    }

    #[must_use]
    pub const fn maximum_input_rows(&self) -> Rows {
        self.maximum_input_rows
    }

    #[must_use]
    pub const fn maximum_input_uncompressed_bytes(&self) -> Bytes {
        self.maximum_input_uncompressed_bytes
    }

    #[must_use]
    pub const fn maximum_input_parquet_bytes(&self) -> Bytes {
        self.maximum_input_parquet_bytes
    }

    #[must_use]
    pub const fn target_output_segment_uncompressed_bytes(&self) -> Bytes {
        self.target_output_segment_uncompressed_bytes
    }

    #[must_use]
    pub const fn maximum_output_segment_rows(&self) -> Rows {
        self.maximum_output_segment_rows
    }

    #[must_use]
    pub const fn maximum_parquet_row_group_rows(&self) -> Rows {
        self.maximum_parquet_row_group_rows
    }

    #[must_use]
    pub const fn maximum_output_segments(&self) -> Segments {
        self.maximum_output_segments
    }

    #[must_use]
    pub const fn maximum_output_parquet_object_bytes(&self) -> Bytes {
        self.maximum_output_parquet_object_bytes
    }

    #[must_use]
    pub const fn maximum_output_parquet_bytes(&self) -> Bytes {
        self.maximum_output_parquet_bytes
    }

    #[must_use]
    pub const fn maximum_concurrent_runs(&self) -> Runs {
        self.maximum_concurrent_runs
    }

    #[must_use]
    pub const fn maximum_cluster_concurrent_runs(&self) -> Runs {
        self.maximum_cluster_concurrent_runs
    }

    #[must_use]
    pub const fn maximum_recovery_batch_runs(&self) -> Runs {
        self.maximum_recovery_batch_runs
    }

    #[must_use]
    pub const fn run_heartbeat_interval_seconds(&self) -> Seconds {
        self.run_heartbeat_interval_seconds
    }

    #[must_use]
    pub const fn run_stale_after_seconds(&self) -> Seconds {
        self.run_stale_after_seconds
    }

    #[must_use]
    pub const fn run_timeout_seconds(&self) -> Seconds {
        self.run_timeout_seconds
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct GarbageCollectionConfiguration {
    pub(super) orphan_grace_period_seconds: Seconds,
    pub(super) retired_object_grace_period_seconds: Seconds,
    pub(super) scan_interval_seconds: Seconds,
    pub(super) maximum_batch_objects: Objects,
    pub(super) maximum_concurrent_object_deletions: Objects,
}

impl GarbageCollectionConfiguration {
    #[must_use]
    pub const fn orphan_grace_period_seconds(&self) -> Seconds {
        self.orphan_grace_period_seconds
    }

    #[must_use]
    pub const fn retired_object_grace_period_seconds(&self) -> Seconds {
        self.retired_object_grace_period_seconds
    }

    #[must_use]
    pub const fn scan_interval_seconds(&self) -> Seconds {
        self.scan_interval_seconds
    }

    #[must_use]
    pub const fn maximum_batch_objects(&self) -> Objects {
        self.maximum_batch_objects
    }

    #[must_use]
    pub const fn maximum_concurrent_object_deletions(&self) -> Objects {
        self.maximum_concurrent_object_deletions
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RetentionConfiguration {
    pub(super) idempotency_retention_seconds: Seconds,
    pub(super) event_data_retention_seconds: Seconds,
    pub(super) dead_letter_retention_seconds: Seconds,
    pub(super) ingestion_provenance_retention_seconds: Seconds,
    pub(super) compaction_provenance_retention_seconds: Seconds,
    pub(super) scan_interval_seconds: Seconds,
    pub(super) maximum_task_duration_seconds: Seconds,
    pub(super) maximum_expiration_batch_segments: Segments,
    pub(super) maximum_retry_expiration_batch_requests: Requests,
    pub(super) maximum_idempotency_expiration_batch_reservations: Reservations,
    pub(super) maximum_provenance_roots_per_batch: Roots,
}

impl RetentionConfiguration {
    #[must_use]
    pub const fn idempotency_retention_seconds(&self) -> Seconds {
        self.idempotency_retention_seconds
    }

    #[must_use]
    pub const fn event_data_retention_seconds(&self) -> Seconds {
        self.event_data_retention_seconds
    }

    #[must_use]
    pub const fn dead_letter_retention_seconds(&self) -> Seconds {
        self.dead_letter_retention_seconds
    }

    #[must_use]
    pub const fn ingestion_provenance_retention_seconds(&self) -> Seconds {
        self.ingestion_provenance_retention_seconds
    }

    #[must_use]
    pub const fn compaction_provenance_retention_seconds(&self) -> Seconds {
        self.compaction_provenance_retention_seconds
    }

    #[must_use]
    pub const fn scan_interval_seconds(&self) -> Seconds {
        self.scan_interval_seconds
    }

    #[must_use]
    pub const fn maximum_task_duration_seconds(&self) -> Seconds {
        self.maximum_task_duration_seconds
    }

    #[must_use]
    pub const fn maximum_expiration_batch_segments(&self) -> Segments {
        self.maximum_expiration_batch_segments
    }

    #[must_use]
    pub const fn maximum_retry_expiration_batch_requests(&self) -> Requests {
        self.maximum_retry_expiration_batch_requests
    }

    #[must_use]
    pub const fn maximum_idempotency_expiration_batch_reservations(&self) -> Reservations {
        self.maximum_idempotency_expiration_batch_reservations
    }

    #[must_use]
    pub const fn maximum_provenance_roots_per_batch(&self) -> Roots {
        self.maximum_provenance_roots_per_batch
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct QueryConfiguration {
    pub(super) default_output_rows: Rows,
    pub(super) maximum_output_rows: Rows,
    pub(super) maximum_result_bytes: Bytes,
    pub(super) maximum_query_bytes: Bytes,
    pub(super) maximum_pipeline_stages: Stages,
    pub(super) maximum_expression_depth: Depth,
    pub(super) maximum_selected_segments: Segments,
    pub(super) maximum_selected_object_bytes: Bytes,
    pub(super) execution_timeout_seconds: Seconds,
    pub(super) memory_pool_bytes: Bytes,
    pub(super) spill_directory: PathBuf,
    pub(super) spill_capacity_bytes: Bytes,
    pub(super) maximum_concurrent_queries: Queries,
    pub(super) maximum_queued_queries: Queries,
}

impl QueryConfiguration {
    #[must_use]
    pub const fn default_output_rows(&self) -> Rows {
        self.default_output_rows
    }

    #[must_use]
    pub const fn maximum_output_rows(&self) -> Rows {
        self.maximum_output_rows
    }

    #[must_use]
    pub const fn maximum_result_bytes(&self) -> Bytes {
        self.maximum_result_bytes
    }

    #[must_use]
    pub const fn maximum_query_bytes(&self) -> Bytes {
        self.maximum_query_bytes
    }

    #[must_use]
    pub const fn maximum_pipeline_stages(&self) -> Stages {
        self.maximum_pipeline_stages
    }

    #[must_use]
    pub const fn maximum_expression_depth(&self) -> Depth {
        self.maximum_expression_depth
    }

    #[must_use]
    pub const fn maximum_selected_segments(&self) -> Segments {
        self.maximum_selected_segments
    }

    #[must_use]
    pub const fn maximum_selected_object_bytes(&self) -> Bytes {
        self.maximum_selected_object_bytes
    }

    #[must_use]
    pub const fn execution_timeout_seconds(&self) -> Seconds {
        self.execution_timeout_seconds
    }

    #[must_use]
    pub const fn memory_pool_bytes(&self) -> Bytes {
        self.memory_pool_bytes
    }

    #[must_use]
    pub fn spill_directory(&self) -> &Path {
        &self.spill_directory
    }

    #[must_use]
    pub const fn spill_capacity_bytes(&self) -> Bytes {
        self.spill_capacity_bytes
    }

    #[must_use]
    pub const fn maximum_concurrent_queries(&self) -> Queries {
        self.maximum_concurrent_queries
    }

    #[must_use]
    pub const fn maximum_queued_queries(&self) -> Queries {
        self.maximum_queued_queries
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TelemetryConfiguration {
    pub(super) log_format: LogFormat,
    pub(super) log_level: LogLevel,
    pub(super) metrics_path: String,
}

impl TelemetryConfiguration {
    #[must_use]
    pub const fn log_format(&self) -> LogFormat {
        self.log_format
    }

    #[must_use]
    pub const fn log_level(&self) -> LogLevel {
        self.log_level
    }

    #[must_use]
    pub fn metrics_path(&self) -> &str {
        &self.metrics_path
    }
}

pub(super) fn non_empty_string(
    value: String,
    field: ConfigurationField,
) -> Result<String, ConfigurationError> {
    if value.is_empty() {
        return Err(ConfigurationError::ValueInvalid {
            field,
            reason: InvalidValueReason::Empty,
        });
    }
    Ok(value)
}

fn is_portable_environment_variable_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
