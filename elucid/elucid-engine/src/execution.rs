use std::sync::Arc;
use std::time::Instant;

use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use elucid_metastore::{MAXIMUM_QUERY_SNAPSHOT_SEGMENTS, QuerySnapshot};
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use crate::pipeline::lower_pipeline;
use crate::result::{QueryBatchStream, encode_query_result};
use crate::runtime::{RuntimeFailureKind, runtime_failure_kind};
use crate::{
    EngineError, HistoricalConversionMetrics, QueryExecutionLimits, QueryObjectStore,
    QueryResourceLimitExceeded, QueryResult, SnapshotTableProvider,
};

const QUERY_EXECUTION_BATCH_ROWS: usize = 256;
const QUERY_EXECUTION_PARTITIONS: usize = 1;

#[derive(Clone, Debug, Default)]
pub struct QueryCancellation {
    token: CancellationToken,
}

impl QueryCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct QueryExecutionGuard<'a> {
    cancellation: &'a QueryCancellation,
    deadline: Instant,
}

impl<'a> QueryExecutionGuard<'a> {
    fn new(
        cancellation: &'a QueryCancellation,
        started_at: Instant,
        timeout: std::time::Duration,
    ) -> Result<Self, EngineError> {
        let deadline = started_at.checked_add(timeout).ok_or_else(|| {
            EngineError::execution_invariant("query timeout exceeds the monotonic clock range")
        })?;
        Ok(Self {
            cancellation,
            deadline,
        })
    }

    pub(crate) fn ensure_active(self) -> Result<(), EngineError> {
        if self.cancellation.is_cancelled() {
            return Err(EngineError::cancelled());
        }
        if Instant::now() >= self.deadline {
            return Err(EngineError::timeout());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct QueryEngine {
    objects: QueryObjectStore,
    historical_conversion_metrics: Arc<HistoricalConversionMetrics>,
    limits: QueryExecutionLimits,
    runtime: Arc<RuntimeEnv>,
}

impl QueryEngine {
    /// Creates one bounded query runtime with a shared memory pool and scratch budget.
    ///
    /// # Errors
    ///
    /// Returns a stable execution error when DataFusion cannot initialize the configured scratch
    /// directory or runtime resources.
    pub fn new(
        objects: QueryObjectStore,
        historical_conversion_metrics: Arc<HistoricalConversionMetrics>,
        limits: QueryExecutionLimits,
    ) -> Result<Self, EngineError> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(limits.memory_bytes())))
            .with_temp_file_path(limits.scratch_path())
            .with_max_temp_directory_size(limits.scratch_capacity_bytes())
            .build_arc()
            .map_err(map_datafusion_error)?;
        Ok(Self {
            objects,
            historical_conversion_metrics,
            limits,
            runtime,
        })
    }

    #[must_use]
    pub const fn limits(&self) -> &QueryExecutionLimits {
        &self.limits
    }

    /// Executes and encodes the already analyzed pipeline against its immutable exact-object
    /// snapshot.
    ///
    /// Cancellation and timeout drop the DataFusion stream, which aborts its tasks and any
    /// in-flight object reads. Successful output remains memory-accounted until the returned
    /// result is dropped.
    ///
    /// # Errors
    ///
    /// Returns a stable object, catalog, cast, evaluation, resource, timeout, cancellation, or
    /// execution error. Output row and byte limits instead return a successful truncated result.
    pub async fn execute(
        &self,
        snapshot: &QuerySnapshot,
        cancellation: &QueryCancellation,
    ) -> Result<QueryResult, EngineError> {
        let started_at = Instant::now();
        let guard = QueryExecutionGuard::new(cancellation, started_at, self.limits.timeout())?;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(EngineError::cancelled()),
            result = tokio::time::timeout(
                self.limits.timeout(),
                self.execute_inner(snapshot, started_at, guard),
            ) => result.map_err(|_| EngineError::timeout())?,
        }
    }

    async fn execute_inner(
        &self,
        snapshot: &QuerySnapshot,
        started_at: Instant,
        guard: QueryExecutionGuard<'_>,
    ) -> Result<QueryResult, EngineError> {
        guard.ensure_active()?;
        enforce_snapshot_limits(snapshot, &self.limits)?;
        let provider = SnapshotTableProvider::open(
            snapshot,
            self.objects.clone(),
            Arc::clone(&self.historical_conversion_metrics),
        )
        .await?;
        guard.ensure_active()?;
        let provider = Arc::new(provider) as Arc<dyn TableProvider>;
        let plan = lower_pipeline(snapshot.analysis().pipeline(), provider)?;
        guard.ensure_active()?;
        let configuration = SessionConfig::new()
            .with_batch_size(QUERY_EXECUTION_BATCH_ROWS)
            .with_target_partitions(QUERY_EXECUTION_PARTITIONS);
        let context = SessionContext::new_with_config_rt(configuration, Arc::clone(&self.runtime));
        let dataframe = context
            .execute_logical_plan(plan)
            .await
            .map_err(map_datafusion_error)?;
        guard.ensure_active()?;
        let stream = dataframe
            .execute_stream()
            .await
            .map_err(map_datafusion_error)?;
        guard.ensure_active()?;
        let stream: QueryBatchStream =
            Box::pin(stream.map(|result| result.map_err(map_datafusion_error)));
        encode_query_result(
            snapshot,
            &self.limits,
            &self.runtime.memory_pool,
            stream,
            started_at,
            guard,
        )
        .await
    }
}

fn enforce_snapshot_limits(
    snapshot: &QuerySnapshot,
    limits: &QueryExecutionLimits,
) -> Result<(), EngineError> {
    let selected_segments = u64::try_from(snapshot.segments().len())
        .map_err(|_| EngineError::execution_invariant("selected segment count exceeds u64"))?;
    if selected_segments > MAXIMUM_QUERY_SNAPSHOT_SEGMENTS {
        return Err(EngineError::resource_limit(
            QueryResourceLimitExceeded::SelectedSegments {
                maximum: MAXIMUM_QUERY_SNAPSHOT_SEGMENTS,
            },
        ));
    }
    if snapshot.selected_parquet_bytes() > limits.maximum_scan_bytes() {
        return Err(EngineError::resource_limit(
            QueryResourceLimitExceeded::ScanBytes {
                maximum: limits.maximum_scan_bytes(),
            },
        ));
    }
    Ok(())
}

fn map_datafusion_error(source: DataFusionError) -> EngineError {
    if contains_resource_exhaustion(&source) {
        return EngineError::resources_exhausted(source);
    }
    match runtime_failure_kind(&source) {
        Some(RuntimeFailureKind::Cast) => EngineError::cast_failed(source),
        Some(RuntimeFailureKind::Evaluation) => EngineError::evaluation_failed(source),
        Some(RuntimeFailureKind::Execution) | None => EngineError::execution(source),
    }
}

fn contains_resource_exhaustion(source: &DataFusionError) -> bool {
    match source {
        DataFusionError::ResourcesExhausted(_) => true,
        DataFusionError::Context(_, source) | DataFusionError::Diagnostic(_, source) => {
            contains_resource_exhaustion(source)
        }
        DataFusionError::Shared(source) => contains_resource_exhaustion(source),
        DataFusionError::Collection(sources) => sources.iter().any(contains_resource_exhaustion),
        _ => false,
    }
}
