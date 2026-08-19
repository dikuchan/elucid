mod environment;
mod error;
mod model;
mod raw;
mod validation;

use std::io::Read;
use std::path::Path;

use toml_edit::DocumentMut;

pub use environment::Environment;
pub use error::{
    ConfigurationError, ConfigurationErrorCode, ConfigurationField, ConfigurationViolation,
    EnvironmentOverrideInvalidReason, InvalidValueReason, SecretInvalidReason, SecretKind,
};
pub use model::{
    Bytes, Connections, IngestionConfiguration, LocalStorageConfiguration, LogFormat,
    MaintenanceConfiguration, MaintenanceMode, MetastoreConfiguration, ObjectStoreConfiguration,
    Queries, QueryConfiguration, Requests, Rows, RuntimeConfiguration, Seconds, SecretString,
    Secrets, ServerConfiguration, TelemetryConfiguration,
};

pub const MAXIMUM_CONFIGURATION_DOCUMENT_BYTES: usize = 1_048_576;
const MAXIMUM_CONFIGURATION_READ_BYTES: u64 = 1_048_577;

impl RuntimeConfiguration {
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigurationError> {
        let environment = Environment::from_current_process();
        Self::load_with_environment(path, &environment)
    }

    pub fn load_with_environment(
        path: Option<&Path>,
        environment: &Environment,
    ) -> Result<Self, ConfigurationError> {
        let Some(path) = path else {
            return Self::from_toml("", environment);
        };
        let file =
            std::fs::File::open(path).map_err(|source| ConfigurationError::FileUnreadable {
                path: path.to_owned(),
                source,
            })?;
        let mut bytes = Vec::with_capacity(MAXIMUM_CONFIGURATION_DOCUMENT_BYTES + 1);
        file.take(MAXIMUM_CONFIGURATION_READ_BYTES)
            .read_to_end(&mut bytes)
            .map_err(|source| ConfigurationError::FileUnreadable {
                path: path.to_owned(),
                source,
            })?;
        reject_oversized_document(bytes.len())?;
        let document = String::from_utf8(bytes).map_err(|_| ConfigurationError::FileNotUtf8 {
            path: path.to_owned(),
        })?;
        Self::from_toml(&document, environment)
    }

    pub fn from_toml(
        document: &str,
        environment: &Environment,
    ) -> Result<Self, ConfigurationError> {
        reject_oversized_document(document.len())?;
        let mut document = document.parse::<DocumentMut>().map_err(|error| {
            ConfigurationError::DocumentMalformed {
                byte_offset: error.span().map(|span| span.start),
            }
        })?;
        environment.apply_configuration_overrides(&mut document)?;
        raw::RawRuntimeConfiguration::from_document(document)?.materialize(environment)
    }
}

fn reject_oversized_document(document_bytes: usize) -> Result<(), ConfigurationError> {
    if document_bytes > MAXIMUM_CONFIGURATION_DOCUMENT_BYTES {
        return Err(ConfigurationError::DocumentTooLarge {
            maximum_bytes: MAXIMUM_CONFIGURATION_DOCUMENT_BYTES,
        });
    }
    Ok(())
}
