use std::future::{Future, IntoFuture as _};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::StreamExt as _;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ClientOptions, ObjectStore};
use serde::Serialize;
use sqlx::Postgres;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use utoipa::ToSchema;

use elucid_engine::QueryObjectStore;
use elucid_metastore::{
    CatalogStore, OperationalStore, PublicationStore, QueryExecutionStore, QuerySnapshotStore,
    install,
};
use elucid_storage::ImmutableObjectStore;

use crate::ingestion::{IngestionAvailability, IngestionBoundary};
use crate::local_storage::LocalStorageBoundary;
use crate::metrics::ServiceMetrics;
use crate::query::{QueryAvailability, QueryBoundary};
use crate::{MaintenanceMode, RuntimeConfiguration, ServiceError};

const DATABASE_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const OBJECT_STORE_REGION: &str = "us-east-1";
const OBJECT_STORE_HEALTH_PREFIX: &str = ".elucid-health-probe";
const MAINTENANCE_LOCK_NAME: &str = "elucid:maintenance";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[non_exhaustive]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ComponentStatus {
    Up,
    Degraded,
    Down,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub(crate) struct ComponentHealth {
    pub(crate) postgresql: ComponentStatus,
    pub(crate) object_store: ComponentStatus,
    pub(crate) spool: ComponentStatus,
    pub(crate) ingestion_worker: ComponentStatus,
    pub(crate) query: ComponentStatus,
    pub(crate) maintenance: ComponentStatus,
}

impl ComponentHealth {
    const fn starting() -> Self {
        Self {
            postgresql: ComponentStatus::Down,
            object_store: ComponentStatus::Down,
            spool: ComponentStatus::Down,
            ingestion_worker: ComponentStatus::Down,
            query: ComponentStatus::Degraded,
            maintenance: ComponentStatus::Down,
        }
    }

    #[must_use]
    pub(crate) const fn permits_admission(self) -> bool {
        matches!(self.postgresql, ComponentStatus::Up)
            && matches!(self.object_store, ComponentStatus::Up)
            && matches!(self.spool, ComponentStatus::Up)
            && matches!(self.ingestion_worker, ComponentStatus::Up)
            && matches!(self.query, ComponentStatus::Up)
    }
}

#[derive(Clone)]
pub(crate) enum RuntimeSnapshot {
    Starting {
        health: ComponentHealth,
    },
    Operational {
        dependencies: Arc<Dependencies>,
        health: ComponentHealth,
    },
    Draining {
        health: ComponentHealth,
    },
}

impl RuntimeSnapshot {
    #[must_use]
    pub(crate) const fn health(&self) -> ComponentHealth {
        match self {
            Self::Starting { health }
            | Self::Operational { health, .. }
            | Self::Draining { health } => *health,
        }
    }

    #[must_use]
    pub(crate) const fn is_ready(&self) -> bool {
        match self {
            Self::Operational { health, .. } => health.permits_admission(),
            Self::Starting { .. } | Self::Draining { .. } => false,
        }
    }

    #[must_use]
    pub(crate) const fn is_draining(&self) -> bool {
        matches!(self, Self::Draining { .. })
    }

    #[must_use]
    pub(crate) fn dependencies(&self) -> Option<&Arc<Dependencies>> {
        match self {
            Self::Operational { dependencies, .. } => Some(dependencies),
            Self::Starting { .. } | Self::Draining { .. } => None,
        }
    }
}

pub(crate) struct ApplicationState {
    configuration: Arc<RuntimeConfiguration>,
    metrics: Arc<ServiceMetrics>,
    runtime: RwLock<RuntimeSnapshot>,
}

impl ApplicationState {
    fn new(configuration: RuntimeConfiguration) -> Self {
        Self {
            configuration: Arc::new(configuration),
            metrics: Arc::new(ServiceMetrics::default()),
            runtime: RwLock::new(RuntimeSnapshot::Starting {
                health: ComponentHealth::starting(),
            }),
        }
    }

    #[must_use]
    pub(crate) fn configuration(&self) -> &RuntimeConfiguration {
        &self.configuration
    }

    #[must_use]
    pub(crate) fn metrics(&self) -> &Arc<ServiceMetrics> {
        &self.metrics
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> RuntimeSnapshot {
        self.read_runtime().clone()
    }

    fn update_starting(&self, update: impl FnOnce(&mut ComponentHealth)) {
        let mut runtime = self.write_runtime();
        if let RuntimeSnapshot::Starting { health } = &mut *runtime {
            update(health);
        }
    }

    fn become_operational(&self, dependencies: Arc<Dependencies>, health: ComponentHealth) {
        *self.write_runtime() = RuntimeSnapshot::Operational {
            dependencies,
            health,
        };
    }

    fn update_operational_health(&self, health: ComponentHealth) {
        let mut runtime = self.write_runtime();
        if let RuntimeSnapshot::Operational {
            health: current, ..
        } = &mut *runtime
        {
            *current = health;
        }
    }

    fn begin_draining(&self) {
        let health = self.read_runtime().health();
        *self.write_runtime() = RuntimeSnapshot::Draining { health };
    }

    fn read_runtime(&self) -> std::sync::RwLockReadGuard<'_, RuntimeSnapshot> {
        self.runtime
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_runtime(&self) -> std::sync::RwLockWriteGuard<'_, RuntimeSnapshot> {
        self.runtime
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) struct Dependencies {
    pub(crate) catalog: CatalogStore,
    pub(crate) ingestion: IngestionBoundary,
    pub(crate) operations: OperationalStore,
    pub(crate) publication: PublicationStore,
    pub(crate) queries: QueryBoundary,
    pub(crate) immutable_objects: ImmutableObjectStore,
    pub(crate) local_storage: LocalStorageBoundary,
    pub(crate) maintenance: MaintenanceBoundary,
    pool: PgPool,
    object_store: Arc<dyn ObjectStore>,
    object_health_prefix: ObjectPath,
    object_request_timeout: Duration,
}

#[derive(Debug)]
pub(crate) enum MaintenanceBoundary {
    Disabled,
    Owned {
        _connection: tokio::sync::Mutex<PoolConnection<Postgres>>,
        lost: AtomicBool,
    },
    Standby,
}

impl MaintenanceBoundary {
    #[must_use]
    pub(crate) fn status(&self) -> ComponentStatus {
        match self {
            Self::Owned { lost, .. } if !lost.load(Ordering::Relaxed) => ComponentStatus::Degraded,
            Self::Owned { .. } => ComponentStatus::Down,
            Self::Disabled | Self::Standby => ComponentStatus::Degraded,
        }
    }

    #[must_use]
    pub(crate) fn ownership(&self) -> MaintenanceOwnership {
        match self {
            Self::Disabled => MaintenanceOwnership::Disabled,
            Self::Owned { lost, .. } if !lost.load(Ordering::Relaxed) => {
                MaintenanceOwnership::Owned
            }
            Self::Owned { .. } => MaintenanceOwnership::Standby,
            Self::Standby => MaintenanceOwnership::Standby,
        }
    }

    fn health(&self, postgresql: ComponentStatus) -> ComponentStatus {
        if postgresql != ComponentStatus::Up {
            if let Self::Owned { lost, .. } = self {
                lost.store(true, Ordering::Relaxed);
            }
            return ComponentStatus::Down;
        }
        self.status()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum MaintenanceOwnership {
    Disabled,
    Owned,
    Standby,
}

#[derive(Debug)]
pub struct RunningServer {
    local_address: SocketAddr,
    cancellation: CancellationToken,
    supervisor: Option<JoinHandle<Result<(), ServiceError>>>,
}

impl RunningServer {
    #[must_use]
    pub const fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    pub async fn shutdown(mut self) -> Result<(), ServiceError> {
        self.cancellation.cancel();
        let supervisor = self.take_supervisor()?;
        finish_supervisor(supervisor.await)
    }

    pub async fn wait_for_signal(mut self) -> Result<(), ServiceError> {
        let outcome = {
            let supervisor = self
                .supervisor
                .as_mut()
                .ok_or_else(supervisor_handle_missing)?;
            tokio::select! {
                result = supervisor => ServerWaitOutcome::Supervisor(result),
                signal = shutdown_signal() => ServerWaitOutcome::Signal(signal),
            }
        };
        match outcome {
            ServerWaitOutcome::Supervisor(result) => {
                self.supervisor.take();
                finish_supervisor(result)
            }
            ServerWaitOutcome::Signal(signal) => {
                if let Err(source) = signal {
                    self.cancellation.cancel();
                    let supervisor = self.take_supervisor()?;
                    return match finish_supervisor(supervisor.await) {
                        Ok(()) => Err(ServiceError::Signal { source }),
                        Err(cleanup_error) => Err(cleanup_error),
                    };
                }
                self.cancellation.cancel();
                let supervisor = self.take_supervisor()?;
                finish_supervisor(supervisor.await)
            }
        }
    }

    fn take_supervisor(&mut self) -> Result<JoinHandle<Result<(), ServiceError>>, ServiceError> {
        self.supervisor.take().ok_or_else(supervisor_handle_missing)
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

enum ServerWaitOutcome {
    Supervisor(Result<Result<(), ServiceError>, tokio::task::JoinError>),
    Signal(Result<(), std::io::Error>),
}

pub async fn start(configuration: RuntimeConfiguration) -> Result<RunningServer, ServiceError> {
    let configured_address = configuration.server().listen_address();
    let listener = TcpListener::bind(configured_address)
        .await
        .map_err(|source| ServiceError::Bind {
            address: configured_address,
            source,
        })?;
    let local_address = listener.local_addr().map_err(|source| ServiceError::Bind {
        address: configured_address,
        source,
    })?;
    let state = Arc::new(ApplicationState::new(configuration));
    let cancellation = CancellationToken::new();
    let supervisor = tokio::spawn(supervise(listener, state, cancellation.clone()));
    Ok(RunningServer {
        local_address,
        cancellation,
        supervisor: Some(supervisor),
    })
}

async fn supervise(
    listener: TcpListener,
    state: Arc<ApplicationState>,
    cancellation: CancellationToken,
) -> Result<(), ServiceError> {
    let shutdown_timeout = Duration::from_secs(
        state
            .configuration()
            .server()
            .shutdown_timeout_seconds()
            .get(),
    );
    let http_cancellation = CancellationToken::new();
    let router = crate::http::router(Arc::clone(&state));
    let mut http = Box::pin(
        axum::serve(listener, router)
            .with_graceful_shutdown(http_cancellation.clone().cancelled_owned())
            .into_future(),
    );
    let mut initialization = Box::pin(initialize(Arc::clone(&state)));

    let dependencies = tokio::select! {
        result = initialization.as_mut() => match result {
            Ok(dependencies) => dependencies,
            Err(error) => {
                state.begin_draining();
                http_cancellation.cancel();
                finish_http(&mut http, shutdown_timeout).await?;
                return Err(error);
            }
        },
        () = cancellation.cancelled() => {
            state.begin_draining();
            http_cancellation.cancel();
            return finish_http(&mut http, shutdown_timeout).await;
        }
        result = http.as_mut() => return unexpected_http_result(result),
    };

    let health = initialized_health(&dependencies);
    let dependencies = Arc::new(dependencies);
    state.become_operational(Arc::clone(&dependencies), health);
    let configuration = state.configuration();
    let mut ingestion_processing = Box::pin(crate::processing::run(
        &dependencies.ingestion,
        crate::processing::ProcessingDependencies {
            catalog: &dependencies.catalog,
            publication: &dependencies.publication,
            operations: &dependencies.operations,
            objects: &dependencies.immutable_objects,
            root: configuration.object_store().managed_root(),
            spool_path: configuration.local_storage().spool_path(),
            scratch_path: dependencies.local_storage.scratch_path(),
            scratch_bytes: configuration.local_storage().scratch_capacity_bytes().get(),
            event_retention_seconds: configuration.maintenance().event_retention_seconds().get(),
            dead_letter_retention_seconds: configuration
                .maintenance()
                .dead_letter_retention_seconds()
                .get(),
        },
    ));
    let mut health_checks = Box::pin(run_health_checks(
        Arc::clone(&state),
        Arc::clone(&dependencies),
    ));

    tokio::select! {
        () = cancellation.cancelled() => {
            state.begin_draining();
            dependencies.ingestion.begin_shutdown();
            dependencies.queries.begin_shutdown();
            http_cancellation.cancel();
            finish_http_and_ingestion(
                &mut http,
                &dependencies.ingestion,
                shutdown_timeout,
            )
            .await
        }
        result = http.as_mut() => unexpected_http_result(result),
        result = ingestion_processing.as_mut() => {
            state.begin_draining();
            dependencies.ingestion.begin_shutdown();
            dependencies.queries.begin_shutdown();
            http_cancellation.cancel();
            finish_http_and_ingestion(
                &mut http,
                &dependencies.ingestion,
                shutdown_timeout,
            )
            .await?;
            match result {
                Ok(()) => Err(ServiceError::IngestionRuntime {
                    reason: "ingestion processing stopped unexpectedly",
                }),
                Err(error) => Err(ServiceError::IngestionRuntime {
                    reason: error.reason(),
                }),
            }
        },
        () = health_checks.as_mut() => Err(ServiceError::HttpRuntime {
            source: std::io::Error::other("health-check loop stopped unexpectedly"),
        }),
    }
}

async fn initialize(state: Arc<ApplicationState>) -> Result<Dependencies, ServiceError> {
    let configuration = state.configuration();
    let maximum_connections = u32::try_from(configuration.metastore().maximum_connections().get())
        .map_err(|_| ServiceError::MetastoreConnection {
            source: sqlx::Error::Configuration(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "configured PostgreSQL pool size exceeds u32",
                )
                .into(),
            ),
        })?;
    let pool = PgPoolOptions::new()
        .max_connections(maximum_connections)
        .acquire_timeout(DATABASE_OPERATION_TIMEOUT)
        .connect(configuration.secrets().postgresql_url().expose_secret())
        .await
        .map_err(|source| ServiceError::MetastoreConnection { source })?;
    install(&pool)
        .await
        .map_err(|source| ServiceError::MetastoreMigration { source })?;
    let catalog = CatalogStore::load(pool.clone())
        .await
        .map_err(|source| ServiceError::CatalogInitialization { source })?;
    state.update_starting(|health| health.postgresql = ComponentStatus::Up);

    let object_request_timeout =
        Duration::from_secs(configuration.object_store().request_timeout_seconds().get());
    let object_store = build_object_store(configuration, object_request_timeout)?;
    let object_health_prefix = object_health_prefix(configuration.object_store().managed_root());
    establish_bucket_access(
        Arc::clone(&object_store),
        &object_health_prefix,
        object_request_timeout,
    )
    .await?;
    state.update_starting(|health| health.object_store = ComponentStatus::Up);

    let local_storage = LocalStorageBoundary::open(configuration.local_storage()).await?;
    let ingestion = IngestionBoundary::open(
        configuration.local_storage(),
        configuration.ingestion(),
        Arc::clone(state.metrics()),
    )
    .await?;
    let (spool, ingestion_worker) = ingestion_component_health(&ingestion);
    state.update_starting(|health| {
        health.spool = spool;
        health.ingestion_worker = ingestion_worker;
    });

    let query_objects = QueryObjectStore::from_url(
        format!("s3://{}", configuration.object_store().bucket()),
        Arc::clone(&object_store),
    )
    .map_err(|source| ServiceError::QueryInitialization {
        source: source.into(),
    })?;
    let queries = QueryBoundary::new(
        configuration.query(),
        configuration.local_storage(),
        QueryExecutionStore::new(pool.clone()),
        QuerySnapshotStore::new(pool.clone()),
        query_objects,
    )
    .map_err(|source| ServiceError::QueryInitialization { source })?;
    state.update_starting(|health| health.query = ComponentStatus::Up);

    let maintenance = initialize_maintenance(configuration, &pool).await?;
    state.update_starting(|health| health.maintenance = maintenance.status());
    let publication = PublicationStore::new(pool.clone());
    let operations = OperationalStore::new(
        pool.clone(),
        configuration.object_store().managed_root().clone(),
    );
    let immutable_objects = ImmutableObjectStore::new(Arc::clone(&object_store));
    Ok(Dependencies {
        catalog,
        ingestion,
        operations,
        publication,
        queries,
        immutable_objects,
        local_storage,
        maintenance,
        pool,
        object_store,
        object_health_prefix,
        object_request_timeout,
    })
}

fn build_object_store(
    configuration: &RuntimeConfiguration,
    request_timeout: Duration,
) -> Result<Arc<dyn ObjectStore>, ServiceError> {
    let object_configuration = configuration.object_store();
    let allow_http = object_configuration.endpoint().scheme() == "http";
    let client_options = ClientOptions::new()
        .with_allow_http(allow_http)
        .with_connect_timeout(request_timeout)
        .with_timeout(request_timeout);
    let mut builder = AmazonS3Builder::new()
        .with_endpoint(object_configuration.endpoint().to_string())
        .with_bucket_name(object_configuration.bucket())
        .with_access_key_id(
            configuration
                .secrets()
                .object_store_access_key_id()
                .expose_secret(),
        )
        .with_secret_access_key(
            configuration
                .secrets()
                .object_store_secret_access_key()
                .expose_secret(),
        )
        .with_region(OBJECT_STORE_REGION)
        .with_allow_http(allow_http)
        .with_client_options(client_options);
    if let Some(token) = configuration
        .secrets()
        .object_store_session_token()
        .map(|token| token.expose_secret())
    {
        builder = builder.with_token(token);
    }
    builder
        .build()
        .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
        .map_err(|source| ServiceError::ObjectStoreInitialization { source })
}

async fn establish_bucket_access(
    object_store: Arc<dyn ObjectStore>,
    health_prefix: &ObjectPath,
    request_timeout: Duration,
) -> Result<(), ServiceError> {
    match tokio::time::timeout(
        request_timeout,
        probe_object_store(&object_store, health_prefix),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(ServiceError::ObjectStoreInitialization { source }),
        Err(_) => Err(ServiceError::ObjectStoreInitialization {
            source: object_store_timeout_error(),
        }),
    }
}

async fn initialize_maintenance(
    configuration: &RuntimeConfiguration,
    pool: &PgPool,
) -> Result<MaintenanceBoundary, ServiceError> {
    if configuration.maintenance().mode() == MaintenanceMode::Disabled {
        return Ok(MaintenanceBoundary::Disabled);
    }
    let mut connection = pool
        .acquire()
        .await
        .map_err(|source| ServiceError::MetastoreConnection { source })?;
    let acquired =
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")
            .bind(MAINTENANCE_LOCK_NAME)
            .fetch_one(&mut *connection)
            .await
            .map_err(|source| ServiceError::MetastoreConnection { source })?;
    if acquired {
        Ok(MaintenanceBoundary::Owned {
            _connection: tokio::sync::Mutex::new(connection),
            lost: AtomicBool::new(false),
        })
    } else {
        Ok(MaintenanceBoundary::Standby)
    }
}

fn initialized_health(dependencies: &Dependencies) -> ComponentHealth {
    let (spool, ingestion_worker) = ingestion_component_health(&dependencies.ingestion);
    ComponentHealth {
        postgresql: ComponentStatus::Up,
        object_store: ComponentStatus::Up,
        spool,
        ingestion_worker,
        query: query_component_health(
            &dependencies.queries,
            ComponentStatus::Up,
            ComponentStatus::Up,
        ),
        maintenance: dependencies.maintenance.status(),
    }
}

async fn run_health_checks(state: Arc<ApplicationState>, dependencies: Arc<Dependencies>) {
    loop {
        tokio::time::sleep(HEALTH_CHECK_INTERVAL).await;
        let (postgresql, object_store, local_storage) = tokio::join!(
            check_postgresql(&dependencies.pool),
            check_object_store(&dependencies),
            dependencies.local_storage.is_accessible(),
        );
        let (spool, ingestion_worker) = if local_storage {
            ingestion_component_health(&dependencies.ingestion)
        } else {
            (ComponentStatus::Down, ComponentStatus::Down)
        };
        let maintenance = dependencies.maintenance.health(postgresql);
        let query = query_component_health(&dependencies.queries, postgresql, object_store);
        state.update_operational_health(ComponentHealth {
            postgresql,
            object_store,
            spool,
            ingestion_worker,
            query,
            maintenance,
        });
    }
}

fn query_component_health(
    queries: &QueryBoundary,
    postgresql: ComponentStatus,
    object_store: ComponentStatus,
) -> ComponentStatus {
    if postgresql != ComponentStatus::Up || object_store != ComponentStatus::Up {
        return ComponentStatus::Down;
    }
    match queries.availability() {
        QueryAvailability::Available => ComponentStatus::Up,
        QueryAvailability::Draining => ComponentStatus::Down,
    }
}

fn ingestion_component_health(ingestion: &IngestionBoundary) -> (ComponentStatus, ComponentStatus) {
    match ingestion.availability() {
        IngestionAvailability::Available => (ComponentStatus::Up, ComponentStatus::Up),
        IngestionAvailability::CapacityExhausted => {
            (ComponentStatus::Degraded, ComponentStatus::Up)
        }
        IngestionAvailability::Unavailable => (ComponentStatus::Down, ComponentStatus::Down),
    }
}

async fn check_postgresql(pool: &PgPool) -> ComponentStatus {
    let probe = sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool);
    match tokio::time::timeout(DATABASE_OPERATION_TIMEOUT, probe).await {
        Ok(Ok(_)) => ComponentStatus::Up,
        Ok(Err(_)) | Err(_) => ComponentStatus::Down,
    }
}

async fn check_object_store(dependencies: &Dependencies) -> ComponentStatus {
    match tokio::time::timeout(
        dependencies.object_request_timeout,
        probe_object_store(
            &dependencies.object_store,
            &dependencies.object_health_prefix,
        ),
    )
    .await
    {
        Ok(Ok(())) => ComponentStatus::Up,
        Ok(Err(_)) | Err(_) => ComponentStatus::Down,
    }
}

async fn probe_object_store(
    object_store: &Arc<dyn ObjectStore>,
    health_prefix: &ObjectPath,
) -> Result<(), object_store::Error> {
    // The reserved prefix stays empty, so this checks bucket access without discovering product objects.
    let mut objects = object_store.list(Some(health_prefix));
    match objects.next().await {
        Some(result) => result.map(|_| ()),
        None => Ok(()),
    }
}

fn managed_root_path(root: &elucid_storage::ManagedRoot) -> Option<ObjectPath> {
    if root.as_str().is_empty() {
        None
    } else {
        Some(ObjectPath::from(root.as_str()))
    }
}

fn object_health_prefix(root: &elucid_storage::ManagedRoot) -> ObjectPath {
    managed_root_path(root).map_or_else(
        || ObjectPath::from(OBJECT_STORE_HEALTH_PREFIX),
        |root| root.child(OBJECT_STORE_HEALTH_PREFIX),
    )
}

fn object_store_timeout_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "S3",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "object-store access probe timed out",
        )),
    }
}

async fn finish_http<F>(
    http: &mut Pin<Box<F>>,
    shutdown_timeout: Duration,
) -> Result<(), ServiceError>
where
    F: Future<Output = Result<(), std::io::Error>>,
{
    match tokio::time::timeout(shutdown_timeout, http.as_mut()).await {
        Ok(result) => result.map_err(|source| ServiceError::HttpRuntime { source }),
        Err(_) => Err(ServiceError::ShutdownTimedOut),
    }
}

async fn finish_http_and_ingestion<F>(
    http: &mut Pin<Box<F>>,
    ingestion: &IngestionBoundary,
    shutdown_timeout: Duration,
) -> Result<(), ServiceError>
where
    F: Future<Output = Result<(), std::io::Error>>,
{
    let finish = async {
        let (http_result, ()) =
            tokio::join!(http.as_mut(), ingestion.wait_for_admitted_requests(),);
        http_result.map_err(|source| ServiceError::HttpRuntime { source })
    };
    match tokio::time::timeout(shutdown_timeout, finish).await {
        Ok(result) => result,
        Err(_) => Err(ServiceError::ShutdownTimedOut),
    }
}

fn unexpected_http_result(result: Result<(), std::io::Error>) -> Result<(), ServiceError> {
    match result {
        Ok(()) => Err(ServiceError::HttpRuntime {
            source: std::io::Error::other("HTTP runtime stopped unexpectedly"),
        }),
        Err(source) => Err(ServiceError::HttpRuntime { source }),
    }
}

fn finish_supervisor(
    result: Result<Result<(), ServiceError>, tokio::task::JoinError>,
) -> Result<(), ServiceError> {
    result.map_err(|source| ServiceError::Supervisor { source })?
}

fn supervisor_handle_missing() -> ServiceError {
    ServiceError::HttpRuntime {
        source: std::io::Error::other("server supervisor handle is missing"),
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        signal = terminate.recv() => signal
            .map(|_| ())
            .ok_or_else(|| std::io::Error::other("SIGTERM listener stopped unexpectedly")),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}
