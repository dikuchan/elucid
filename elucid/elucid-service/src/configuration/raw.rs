use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;
use toml_edit::DocumentMut;
use url::Url;

use super::environment::{
    DIRECT_OBJECT_STORE_ACCESS_KEY_ID, DIRECT_OBJECT_STORE_SECRET_ACCESS_KEY,
    DIRECT_OBJECT_STORE_SESSION_TOKEN, DIRECT_POSTGRESQL_URL, Environment, EnvironmentLookup,
};
use super::error::{
    ConfigurationError, ConfigurationField, InvalidValueReason, SecretInvalidReason, SecretKind,
};
use super::model::{
    Bytes, Connections, IngestionConfiguration, LocalStorageConfiguration, LogFormat,
    MaintenanceConfiguration, MaintenanceMode, MetastoreConfiguration, ObjectStoreConfiguration,
    Queries, QueryConfiguration, Requests, Rows, RuntimeConfiguration, Seconds, SecretString,
    Secrets, ServerConfiguration, TelemetryConfiguration,
};
use super::validation;

const MAXIMUM_BUCKET_BYTES: usize = 255;
const MAXIMUM_ROOT_PREFIX_BYTES: usize = 1_024;
const MAXIMUM_POSTGRESQL_URL_BYTES: usize = 4_096;
const MAXIMUM_CREDENTIAL_BYTES: usize = 4_096;

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct RawRuntimeConfiguration {
    server: RawServerConfiguration,
    metastore: RawMetastoreConfiguration,
    object_store: RawObjectStoreConfiguration,
    local_storage: RawLocalStorageConfiguration,
    ingestion: RawIngestionConfiguration,
    query: RawQueryConfiguration,
    maintenance: RawMaintenanceConfiguration,
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
            object_store: self.object_store.materialize()?,
            local_storage: self.local_storage.materialize()?,
            ingestion: self.ingestion.materialize()?,
            query: self.query.materialize()?,
            maintenance: self.maintenance.materialize()?,
            telemetry: self.telemetry.materialize(),
        };
        validation::validate(&candidate)?;
        let secrets = resolve_secrets(environment)?;
        Ok(candidate.into_runtime_configuration(secrets))
    }
}

pub(super) struct RuntimeConfigurationCandidate {
    pub(super) server: ServerConfiguration,
    pub(super) metastore: MetastoreConfiguration,
    pub(super) object_store: ObjectStoreConfiguration,
    pub(super) local_storage: LocalStorageConfiguration,
    pub(super) ingestion: IngestionConfiguration,
    pub(super) query: QueryConfiguration,
    pub(super) maintenance: MaintenanceConfiguration,
    pub(super) telemetry: TelemetryConfiguration,
}

impl RuntimeConfigurationCandidate {
    fn into_runtime_configuration(self, secrets: Secrets) -> RuntimeConfiguration {
        RuntimeConfiguration {
            server: self.server,
            metastore: self.metastore,
            object_store: self.object_store,
            local_storage: self.local_storage,
            ingestion: self.ingestion,
            query: self.query,
            maintenance: self.maintenance,
            telemetry: self.telemetry,
            secrets,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawServerConfiguration {
    listen_address: String,
    request_timeout_seconds: u64,
    shutdown_timeout_seconds: u64,
}

impl Default for RawServerConfiguration {
    fn default() -> Self {
        Self {
            listen_address: "127.0.0.1:8080".to_owned(),
            request_timeout_seconds: 60,
            shutdown_timeout_seconds: 15,
        }
    }
}

impl RawServerConfiguration {
    fn materialize(self) -> Result<ServerConfiguration, ConfigurationError> {
        Ok(ServerConfiguration {
            listen_address: parse_socket_address(
                self.listen_address,
                field("server.listen_address"),
            )?,
            request_timeout_seconds: Seconds::from_configuration(
                self.request_timeout_seconds,
                field("server.request_timeout_seconds"),
            )?,
            shutdown_timeout_seconds: Seconds::from_configuration(
                self.shutdown_timeout_seconds,
                field("server.shutdown_timeout_seconds"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawMetastoreConfiguration {
    maximum_connections: u64,
}

impl Default for RawMetastoreConfiguration {
    fn default() -> Self {
        Self {
            maximum_connections: 10,
        }
    }
}

impl RawMetastoreConfiguration {
    fn materialize(self) -> Result<MetastoreConfiguration, ConfigurationError> {
        Ok(MetastoreConfiguration {
            maximum_connections: Connections::from_configuration(
                self.maximum_connections,
                field("metastore.maximum_connections"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawObjectStoreConfiguration {
    endpoint: String,
    bucket: String,
    root_prefix: String,
    request_timeout_seconds: u64,
}

impl Default for RawObjectStoreConfiguration {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:9000".to_owned(),
            bucket: "elucid".to_owned(),
            root_prefix: "elucid".to_owned(),
            request_timeout_seconds: 30,
        }
    }
}

impl RawObjectStoreConfiguration {
    fn materialize(self) -> Result<ObjectStoreConfiguration, ConfigurationError> {
        Ok(ObjectStoreConfiguration {
            endpoint: parse_http_origin(self.endpoint, field("object_store.endpoint"))?,
            bucket: non_empty_bounded_string(
                self.bucket,
                field("object_store.bucket"),
                MAXIMUM_BUCKET_BYTES,
            )?,
            root_prefix: bounded_string(
                self.root_prefix,
                field("object_store.root_prefix"),
                MAXIMUM_ROOT_PREFIX_BYTES,
            )?,
            request_timeout_seconds: Seconds::from_configuration(
                self.request_timeout_seconds,
                field("object_store.request_timeout_seconds"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawLocalStorageConfiguration {
    spool_path: PathBuf,
    spool_capacity_bytes: u64,
    scratch_path: PathBuf,
    scratch_capacity_bytes: u64,
}

impl Default for RawLocalStorageConfiguration {
    fn default() -> Self {
        Self {
            spool_path: PathBuf::from("/var/lib/elucid/spool"),
            spool_capacity_bytes: 2_147_483_648,
            scratch_path: PathBuf::from("/var/lib/elucid/scratch"),
            scratch_capacity_bytes: 2_147_483_648,
        }
    }
}

impl RawLocalStorageConfiguration {
    fn materialize(self) -> Result<LocalStorageConfiguration, ConfigurationError> {
        Ok(LocalStorageConfiguration {
            spool_path: absolute_path(self.spool_path, field("local_storage.spool_path"))?,
            spool_capacity_bytes: Bytes::from_configuration(
                self.spool_capacity_bytes,
                field("local_storage.spool_capacity_bytes"),
            )?,
            scratch_path: absolute_path(self.scratch_path, field("local_storage.scratch_path"))?,
            scratch_capacity_bytes: Bytes::from_configuration(
                self.scratch_capacity_bytes,
                field("local_storage.scratch_capacity_bytes"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawIngestionConfiguration {
    maximum_http_batch_bytes: u64,
    maximum_concurrent_requests: u64,
}

impl Default for RawIngestionConfiguration {
    fn default() -> Self {
        Self {
            maximum_http_batch_bytes: 16_777_216,
            maximum_concurrent_requests: 4,
        }
    }
}

impl RawIngestionConfiguration {
    fn materialize(self) -> Result<IngestionConfiguration, ConfigurationError> {
        Ok(IngestionConfiguration {
            maximum_http_batch_bytes: Bytes::from_configuration(
                self.maximum_http_batch_bytes,
                field("ingestion.maximum_http_batch_bytes"),
            )?,
            maximum_concurrent_requests: Requests::from_configuration(
                self.maximum_concurrent_requests,
                field("ingestion.maximum_concurrent_requests"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawQueryConfiguration {
    maximum_concurrent_queries: u64,
    timeout_seconds: u64,
    maximum_scan_bytes: u64,
    memory_bytes: u64,
    maximum_result_rows: u64,
    maximum_result_bytes: u64,
}

impl Default for RawQueryConfiguration {
    fn default() -> Self {
        Self {
            maximum_concurrent_queries: 2,
            timeout_seconds: 30,
            maximum_scan_bytes: 107_374_182_400,
            memory_bytes: 536_870_912,
            maximum_result_rows: 10_000,
            maximum_result_bytes: 16_777_216,
        }
    }
}

impl RawQueryConfiguration {
    fn materialize(self) -> Result<QueryConfiguration, ConfigurationError> {
        Ok(QueryConfiguration {
            maximum_concurrent_queries: Queries::from_configuration(
                self.maximum_concurrent_queries,
                field("query.maximum_concurrent_queries"),
            )?,
            timeout_seconds: Seconds::from_configuration(
                self.timeout_seconds,
                field("query.timeout_seconds"),
            )?,
            maximum_scan_bytes: Bytes::from_configuration(
                self.maximum_scan_bytes,
                field("query.maximum_scan_bytes"),
            )?,
            memory_bytes: Bytes::from_configuration(
                self.memory_bytes,
                field("query.memory_bytes"),
            )?,
            maximum_result_rows: Rows::from_configuration(
                self.maximum_result_rows,
                field("query.maximum_result_rows"),
            )?,
            maximum_result_bytes: Bytes::from_configuration(
                self.maximum_result_bytes,
                field("query.maximum_result_bytes"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawMaintenanceConfiguration {
    mode: MaintenanceMode,
    event_retention_seconds: u64,
    dead_letter_retention_seconds: u64,
}

impl Default for RawMaintenanceConfiguration {
    fn default() -> Self {
        Self {
            mode: MaintenanceMode::Automatic,
            event_retention_seconds: 2_592_000,
            dead_letter_retention_seconds: 604_800,
        }
    }
}

impl RawMaintenanceConfiguration {
    fn materialize(self) -> Result<MaintenanceConfiguration, ConfigurationError> {
        Ok(MaintenanceConfiguration {
            mode: self.mode,
            event_retention_seconds: Seconds::from_configuration(
                self.event_retention_seconds,
                field("maintenance.event_retention_seconds"),
            )?,
            dead_letter_retention_seconds: Seconds::from_configuration(
                self.dead_letter_retention_seconds,
                field("maintenance.dead_letter_retention_seconds"),
            )?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawTelemetryConfiguration {
    log_format: LogFormat,
}

impl Default for RawTelemetryConfiguration {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Pretty,
        }
    }
}

impl RawTelemetryConfiguration {
    const fn materialize(self) -> TelemetryConfiguration {
        TelemetryConfiguration {
            log_format: self.log_format,
        }
    }
}

fn resolve_secrets(environment: &Environment) -> Result<Secrets, ConfigurationError> {
    Ok(Secrets {
        postgresql_url: SecretString::new(resolve_postgresql_url(environment)?),
        object_store_access_key_id: SecretString::new(resolve_required_credential(
            environment,
            DIRECT_OBJECT_STORE_ACCESS_KEY_ID,
            SecretKind::ObjectStoreAccessKeyId,
        )?),
        object_store_secret_access_key: SecretString::new(resolve_required_credential(
            environment,
            DIRECT_OBJECT_STORE_SECRET_ACCESS_KEY,
            SecretKind::ObjectStoreSecretAccessKey,
        )?),
        object_store_session_token: resolve_optional_credential(
            environment,
            DIRECT_OBJECT_STORE_SESSION_TOKEN,
            SecretKind::ObjectStoreSessionToken,
        )?
        .map(SecretString::new),
    })
}

fn resolve_postgresql_url(environment: &Environment) -> Result<String, ConfigurationError> {
    let value = required_environment_value(
        environment,
        DIRECT_POSTGRESQL_URL,
        SecretKind::PostgreSqlUrl,
    )?;
    if value.len() > MAXIMUM_POSTGRESQL_URL_BYTES {
        return secret_invalid(SecretKind::PostgreSqlUrl, SecretInvalidReason::TooLong);
    }
    let valid = Url::parse(value).is_ok_and(|url| {
        matches!(url.scheme(), "postgres" | "postgresql") && url.host_str().is_some()
    });
    if !valid {
        return secret_invalid(
            SecretKind::PostgreSqlUrl,
            SecretInvalidReason::InvalidPostgreSqlUrl,
        );
    }
    Ok(value.to_owned())
}

fn resolve_required_credential(
    environment: &Environment,
    name: &str,
    kind: SecretKind,
) -> Result<String, ConfigurationError> {
    let value = required_environment_value(environment, name, kind)?;
    validate_credential(value, kind)?;
    Ok(value.to_owned())
}

fn resolve_optional_credential(
    environment: &Environment,
    name: &str,
    kind: SecretKind,
) -> Result<Option<String>, ConfigurationError> {
    match environment.value(name) {
        EnvironmentLookup::Missing => Ok(None),
        EnvironmentLookup::NotUnicode => secret_invalid(kind, SecretInvalidReason::NotUnicode),
        EnvironmentLookup::Unicode(value) => {
            validate_credential(value, kind)?;
            Ok(Some(value.to_owned()))
        }
    }
}

fn required_environment_value<'a>(
    environment: &'a Environment,
    name: &str,
    kind: SecretKind,
) -> Result<&'a str, ConfigurationError> {
    match environment.value(name) {
        EnvironmentLookup::Missing => Err(ConfigurationError::SecretMissing { kind }),
        EnvironmentLookup::NotUnicode => secret_invalid(kind, SecretInvalidReason::NotUnicode),
        EnvironmentLookup::Unicode(value) => Ok(value),
    }
}

fn validate_credential(value: &str, kind: SecretKind) -> Result<(), ConfigurationError> {
    if value.is_empty() {
        return secret_invalid(kind, SecretInvalidReason::Empty);
    }
    if value.len() > MAXIMUM_CREDENTIAL_BYTES {
        return secret_invalid(kind, SecretInvalidReason::TooLong);
    }
    if !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return secret_invalid(kind, SecretInvalidReason::ContainsWhitespaceOrControl);
    }
    Ok(())
}

fn secret_invalid<T>(
    kind: SecretKind,
    reason: SecretInvalidReason,
) -> Result<T, ConfigurationError> {
    Err(ConfigurationError::SecretInvalid { kind, reason })
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

fn parse_http_origin(value: String, field: ConfigurationField) -> Result<Url, ConfigurationError> {
    let url = Url::parse(&value).map_err(|_| ConfigurationError::ValueInvalid {
        field,
        reason: InvalidValueReason::InvalidUrl,
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ConfigurationError::ValueInvalid {
            field,
            reason: InvalidValueReason::UrlSchemeUnsupported,
        });
    }
    if url.host_str().is_none() {
        return Err(ConfigurationError::ValueInvalid {
            field,
            reason: InvalidValueReason::UrlAuthorityMissing,
        });
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigurationError::ValueInvalid {
            field,
            reason: InvalidValueReason::UrlMustBeOrigin,
        });
    }
    Ok(url)
}

fn non_empty_bounded_string(
    value: String,
    field: ConfigurationField,
    maximum_bytes: usize,
) -> Result<String, ConfigurationError> {
    if value.is_empty() {
        return Err(ConfigurationError::ValueInvalid {
            field,
            reason: InvalidValueReason::RequiredNonEmpty,
        });
    }
    bounded_string(value, field, maximum_bytes)
}

fn bounded_string(
    value: String,
    field: ConfigurationField,
    maximum_bytes: usize,
) -> Result<String, ConfigurationError> {
    if value.len() > maximum_bytes {
        return Err(ConfigurationError::ValueInvalid {
            field,
            reason: InvalidValueReason::TooLong,
        });
    }
    Ok(value)
}

fn absolute_path(value: PathBuf, field: ConfigurationField) -> Result<PathBuf, ConfigurationError> {
    if !value.is_absolute() {
        return Err(ConfigurationError::ValueInvalid {
            field,
            reason: InvalidValueReason::PathMustBeAbsolute,
        });
    }
    Ok(value)
}

const fn field(name: &'static str) -> ConfigurationField {
    ConfigurationField::new(name)
}
