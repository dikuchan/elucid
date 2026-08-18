use serde::Serialize;

use crate::arguments::VersionOutput;
use crate::error::{CliErrorCode, Failure};

const STORAGE_FORMAT_VERSION: u32 = 1;
const MINIMUM_METASTORE_MIGRATION_VERSION: u64 = 0;
const MAXIMUM_METASTORE_MIGRATION_VERSION: u64 = 0;

#[derive(Debug, Serialize)]
struct VersionInformation {
    semantic_version: &'static str,
    git_revision: Option<&'static str>,
    build_profile: &'static str,
    frontend_asset_revision: Option<&'static str>,
    storage_format_version: u32,
    supported_metastore_migration_range: MetastoreMigrationRange,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct MetastoreMigrationRange {
    minimum_version: u64,
    maximum_version: u64,
}

pub(crate) fn render(output: VersionOutput) -> Result<Vec<u8>, Failure> {
    let information = VersionInformation::current();
    match output {
        VersionOutput::Human => Ok(information.render_human().into_bytes()),
        VersionOutput::Json => serde_json::to_vec(&information).map_err(|error| {
            Failure::internal(
                CliErrorCode::VersionEncodingFailed,
                anyhow::Error::new(error).context("failed to encode version information"),
            )
        }),
    }
}

impl VersionInformation {
    fn current() -> Self {
        Self {
            semantic_version: env!("CARGO_PKG_VERSION"),
            git_revision: option_env!("ELUCID_GIT_REVISION"),
            build_profile: option_env!("ELUCID_BUILD_PROFILE").unwrap_or(
                if cfg!(debug_assertions) {
                    "debug"
                } else {
                    "release"
                },
            ),
            frontend_asset_revision: option_env!("ELUCID_FRONTEND_ASSET_REVISION"),
            storage_format_version: STORAGE_FORMAT_VERSION,
            supported_metastore_migration_range: MetastoreMigrationRange {
                minimum_version: MINIMUM_METASTORE_MIGRATION_VERSION,
                maximum_version: MAXIMUM_METASTORE_MIGRATION_VERSION,
            },
        }
    }

    fn render_human(&self) -> String {
        format!(
            "elucid {}\ngit revision: {}\nbuild profile: {}\nfrontend asset revision: {}\nstorage format version: {}\nsupported metastore migration range: {}..={}\n",
            self.semantic_version,
            self.git_revision.unwrap_or("unavailable"),
            self.build_profile,
            self.frontend_asset_revision.unwrap_or("unavailable"),
            self.storage_format_version,
            self.supported_metastore_migration_range.minimum_version,
            self.supported_metastore_migration_range.maximum_version
        )
    }
}
