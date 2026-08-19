use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use url::Url;

use super::error::{ConfigurationError, ConfigurationField, InvalidValueReason};

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

positive_measurement!(Bytes);
positive_measurement!(Connections);
positive_measurement!(Queries);
positive_measurement!(Requests);
positive_measurement!(Rows);
positive_measurement!(Seconds);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MaintenanceMode {
    Automatic,
    Disabled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogFormat {
    Json,
    Pretty,
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

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Secrets {
    pub(super) postgresql_url: SecretString,
    pub(super) object_store_access_key_id: SecretString,
    pub(super) object_store_secret_access_key: SecretString,
    pub(super) object_store_session_token: Option<SecretString>,
}

impl Secrets {
    #[must_use]
    pub const fn postgresql_url(&self) -> &SecretString {
        &self.postgresql_url
    }

    #[must_use]
    pub const fn object_store_access_key_id(&self) -> &SecretString {
        &self.object_store_access_key_id
    }

    #[must_use]
    pub const fn object_store_secret_access_key(&self) -> &SecretString {
        &self.object_store_secret_access_key
    }

    #[must_use]
    pub const fn object_store_session_token(&self) -> Option<&SecretString> {
        self.object_store_session_token.as_ref()
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RuntimeConfiguration {
    pub(super) server: ServerConfiguration,
    pub(super) metastore: MetastoreConfiguration,
    pub(super) object_store: ObjectStoreConfiguration,
    pub(super) local_storage: LocalStorageConfiguration,
    pub(super) ingestion: IngestionConfiguration,
    pub(super) query: QueryConfiguration,
    pub(super) maintenance: MaintenanceConfiguration,
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
    pub const fn object_store(&self) -> &ObjectStoreConfiguration {
        &self.object_store
    }

    #[must_use]
    pub const fn local_storage(&self) -> &LocalStorageConfiguration {
        &self.local_storage
    }

    #[must_use]
    pub const fn ingestion(&self) -> &IngestionConfiguration {
        &self.ingestion
    }

    #[must_use]
    pub const fn query(&self) -> &QueryConfiguration {
        &self.query
    }

    #[must_use]
    pub const fn maintenance(&self) -> &MaintenanceConfiguration {
        &self.maintenance
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
    pub(super) listen_address: SocketAddr,
    pub(super) request_timeout_seconds: Seconds,
    pub(super) shutdown_timeout_seconds: Seconds,
}

impl ServerConfiguration {
    #[must_use]
    pub const fn listen_address(&self) -> SocketAddr {
        self.listen_address
    }

    #[must_use]
    pub const fn request_timeout_seconds(&self) -> Seconds {
        self.request_timeout_seconds
    }

    #[must_use]
    pub const fn shutdown_timeout_seconds(&self) -> Seconds {
        self.shutdown_timeout_seconds
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MetastoreConfiguration {
    pub(super) maximum_connections: Connections,
}

impl MetastoreConfiguration {
    #[must_use]
    pub const fn maximum_connections(&self) -> Connections {
        self.maximum_connections
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ObjectStoreConfiguration {
    pub(super) endpoint: Url,
    pub(super) bucket: String,
    pub(super) root_prefix: String,
    pub(super) request_timeout_seconds: Seconds,
}

impl ObjectStoreConfiguration {
    #[must_use]
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
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
    pub const fn request_timeout_seconds(&self) -> Seconds {
        self.request_timeout_seconds
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct LocalStorageConfiguration {
    pub(super) spool_path: PathBuf,
    pub(super) spool_capacity_bytes: Bytes,
    pub(super) scratch_path: PathBuf,
    pub(super) scratch_capacity_bytes: Bytes,
}

impl LocalStorageConfiguration {
    #[must_use]
    pub fn spool_path(&self) -> &Path {
        &self.spool_path
    }

    #[must_use]
    pub const fn spool_capacity_bytes(&self) -> Bytes {
        self.spool_capacity_bytes
    }

    #[must_use]
    pub fn scratch_path(&self) -> &Path {
        &self.scratch_path
    }

    #[must_use]
    pub const fn scratch_capacity_bytes(&self) -> Bytes {
        self.scratch_capacity_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct IngestionConfiguration {
    pub(super) maximum_http_batch_bytes: Bytes,
    pub(super) maximum_concurrent_requests: Requests,
}

impl IngestionConfiguration {
    #[must_use]
    pub const fn maximum_http_batch_bytes(&self) -> Bytes {
        self.maximum_http_batch_bytes
    }

    #[must_use]
    pub const fn maximum_concurrent_requests(&self) -> Requests {
        self.maximum_concurrent_requests
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct QueryConfiguration {
    pub(super) maximum_concurrent_queries: Queries,
    pub(super) timeout_seconds: Seconds,
    pub(super) maximum_scan_bytes: Bytes,
    pub(super) memory_bytes: Bytes,
    pub(super) maximum_result_rows: Rows,
    pub(super) maximum_result_bytes: Bytes,
}

impl QueryConfiguration {
    #[must_use]
    pub const fn maximum_concurrent_queries(&self) -> Queries {
        self.maximum_concurrent_queries
    }

    #[must_use]
    pub const fn timeout_seconds(&self) -> Seconds {
        self.timeout_seconds
    }

    #[must_use]
    pub const fn maximum_scan_bytes(&self) -> Bytes {
        self.maximum_scan_bytes
    }

    #[must_use]
    pub const fn memory_bytes(&self) -> Bytes {
        self.memory_bytes
    }

    #[must_use]
    pub const fn maximum_result_rows(&self) -> Rows {
        self.maximum_result_rows
    }

    #[must_use]
    pub const fn maximum_result_bytes(&self) -> Bytes {
        self.maximum_result_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MaintenanceConfiguration {
    pub(super) mode: MaintenanceMode,
    pub(super) event_retention_seconds: Seconds,
    pub(super) dead_letter_retention_seconds: Seconds,
}

impl MaintenanceConfiguration {
    #[must_use]
    pub const fn mode(&self) -> MaintenanceMode {
        self.mode
    }

    #[must_use]
    pub const fn event_retention_seconds(&self) -> Seconds {
        self.event_retention_seconds
    }

    #[must_use]
    pub const fn dead_letter_retention_seconds(&self) -> Seconds {
        self.dead_letter_retention_seconds
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TelemetryConfiguration {
    pub(super) log_format: LogFormat,
}

impl TelemetryConfiguration {
    #[must_use]
    pub const fn log_format(&self) -> LogFormat {
        self.log_format
    }
}
