use std::num::NonZeroU64;
use std::path::PathBuf;
use std::str::FromStr;

use clap::{Args, Parser, Subcommand, ValueEnum};
use elucid_catalog::{InputName, SourceName};
use url::Url;

#[derive(Debug, Parser)]
#[command(
    name = "elucid",
    about = "Elucid SIEM",
    disable_version_flag = true,
    arg_required_else_help = true
)]
pub(crate) struct Arguments {
    /// Print build and compatibility information.
    #[arg(long, short = 'V')]
    version: bool,

    /// Select version output representation.
    #[arg(long, value_enum, requires = "version")]
    output: Option<VersionOutput>,

    #[command(subcommand)]
    command: Option<RootCommand>,
}

impl Arguments {
    pub(crate) fn into_action(self) -> Result<Action, String> {
        match (self.version, self.command) {
            (true, None) => Ok(Action::Version(self.output.unwrap_or(VersionOutput::Human))),
            (false, Some(command)) => Ok(Action::Command(command)),
            (true, Some(_)) => Err("--version cannot be combined with a command".to_owned()),
            (false, None) => Err("a command or --version is required".to_owned()),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Action {
    Version(VersionOutput),
    Command(RootCommand),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum VersionOutput {
    Human,
    Json,
}

#[derive(Debug, Subcommand)]
pub(crate) enum RootCommand {
    /// Manage an Elucid server process.
    Server(ServerCommand),
    /// Manage catalog declarations through the product API.
    Catalog(CatalogCommand),
    /// Send events through the product API.
    Ingestion(IngestionCommand),
}

#[derive(Debug, Args)]
pub(crate) struct ServerCommand {
    #[command(subcommand)]
    command: ServerSubcommand,
}

impl ServerCommand {
    pub(crate) fn into_command(self) -> ServerSubcommand {
        self.command
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum ServerSubcommand {
    /// Run an Elucid server in the foreground.
    Run(ServerRunCommand),
}

#[derive(Debug, Args)]
pub(crate) struct ServerRunCommand {
    /// Path to the runtime TOML configuration.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
}

impl ServerRunCommand {
    pub(crate) fn into_config_path(self) -> PathBuf {
        self.config
    }
}

#[derive(Debug, Args)]
pub(crate) struct CatalogCommand {
    #[command(subcommand)]
    command: CatalogSubcommand,
}

impl CatalogCommand {
    pub(crate) fn into_command(self) -> CatalogSubcommand {
        self.command
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum CatalogSubcommand {
    /// Apply one complete source catalog manifest.
    Apply(CatalogApplyCommand),
}

#[derive(Debug, Args)]
pub(crate) struct CatalogApplyCommand {
    /// Elucid HTTP base URL.
    #[arg(long, value_name = "BASE_URL")]
    endpoint: ProductEndpoint,

    /// Manifest path, or - for standard input.
    #[arg(long, value_name = "PATH_OR_DASH")]
    file: PathBuf,

    /// Environment variable containing the operator bearer token.
    #[arg(long, value_name = "NAME")]
    operator_bearer_token_environment_variable: Option<EnvironmentVariableName>,

    /// Maximum local wait for the HTTP operation.
    #[arg(long, default_value = "120", value_name = "SECONDS")]
    timeout_seconds: ClientTimeoutSeconds,
}

impl CatalogApplyCommand {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ProductEndpoint,
        PathBuf,
        Option<EnvironmentVariableName>,
        ClientTimeoutSeconds,
    ) {
        (
            self.endpoint,
            self.file,
            self.operator_bearer_token_environment_variable,
            self.timeout_seconds,
        )
    }
}

#[derive(Debug, Args)]
pub(crate) struct IngestionCommand {
    #[command(subcommand)]
    command: IngestionSubcommand,
}

impl IngestionCommand {
    pub(crate) fn into_command(self) -> IngestionSubcommand {
        self.command
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum IngestionSubcommand {
    /// Submit one complete NDJSON entity body.
    Submit(IngestionSubmitCommand),
}

#[derive(Debug, Args)]
pub(crate) struct IngestionSubmitCommand {
    /// Elucid HTTP base URL.
    #[arg(long, value_name = "BASE_URL")]
    endpoint: ProductEndpoint,

    /// Source receiving the events.
    #[arg(long, value_name = "SOURCE_NAME")]
    source: SourceName,

    /// Input receiving the events.
    #[arg(long, value_name = "INPUT_NAME")]
    input: InputName,

    /// NDJSON path, or - for standard input.
    #[arg(long, value_name = "PATH_OR_DASH")]
    file: PathBuf,

    /// Input-scoped idempotency key.
    #[arg(long, value_name = "KEY")]
    idempotency_key: IdempotencyKey,

    /// Environment variable containing the operator bearer token.
    #[arg(long, value_name = "NAME")]
    operator_bearer_token_environment_variable: Option<EnvironmentVariableName>,

    /// Maximum local wait for the HTTP operation.
    #[arg(long, default_value = "120", value_name = "SECONDS")]
    timeout_seconds: ClientTimeoutSeconds,
}

impl IngestionSubmitCommand {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ProductEndpoint,
        SourceName,
        InputName,
        PathBuf,
        IdempotencyKey,
        Option<EnvironmentVariableName>,
        ClientTimeoutSeconds,
    ) {
        (
            self.endpoint,
            self.source,
            self.input,
            self.file,
            self.idempotency_key,
            self.operator_bearer_token_environment_variable,
            self.timeout_seconds,
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProductEndpoint(Url);

impl ProductEndpoint {
    pub(crate) fn catalog_application_url(&self) -> Url {
        let mut url = self.0.clone();
        url.set_path("/api/v1/catalog-applications");
        url
    }

    pub(crate) fn ingestion_url(
        &self,
        source: &SourceName,
        input: &InputName,
    ) -> Result<Url, String> {
        let mut url = self.0.clone();
        let mut segments = url.path_segments_mut().map_err(|()| {
            "validated product endpoint cannot contain URL path segments".to_owned()
        })?;
        segments.clear();
        segments.extend([
            "api",
            "v1",
            "sources",
            source.as_str(),
            "inputs",
            input.as_str(),
            "events",
        ]);
        drop(segments);
        Ok(url)
    }
}

impl FromStr for ProductEndpoint {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(value).map_err(|_| "endpoint must be an absolute URL".to_owned())?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err("endpoint scheme must be http or https".to_owned());
        }
        if url.host_str().is_none() {
            return Err("endpoint must contain a host".to_owned());
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err("endpoint must not contain credentials".to_owned());
        }
        if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
            return Err("endpoint must be an origin without path, query, or fragment".to_owned());
        }
        Ok(Self(url))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct EnvironmentVariableName(Box<str>);

impl EnvironmentVariableName {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for EnvironmentVariableName {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut bytes = value.bytes();
        let valid = bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
        if !valid {
            return Err("environment variable name must match [A-Za-z_][A-Za-z0-9_]*".to_owned());
        }
        Ok(Self(value.to_owned().into_boxed_str()))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IdempotencyKey(Box<str>);

impl IdempotencyKey {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for IdempotencyKey {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !(1..=128).contains(&value.len())
            || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(
                "idempotency key must contain 1 through 128 visible ASCII bytes".to_owned(),
            );
        }
        Ok(Self(value.to_owned().into_boxed_str()))
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ClientTimeoutSeconds(NonZeroU64);

impl ClientTimeoutSeconds {
    pub(crate) const fn get(self) -> u64 {
        self.0.get()
    }
}

impl FromStr for ClientTimeoutSeconds {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value
            .parse::<u64>()
            .map_err(|_| "timeout seconds must be a positive integer".to_owned())?;
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| "timeout seconds must be positive".to_owned())
    }
}
