use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;

use elucid_engine::{
    EngineError, HistoricalConversionMetrics, QueryCancellation, QueryEngine,
    QueryExecutionLimitConfiguration, QueryExecutionLimits, QueryObjectStore,
    QueryOutputRowLimitError, QueryResult,
};
use elucid_metastore::{
    QueryRequestTimeRange, QuerySnapshot, QuerySnapshotError, QuerySnapshotLimits,
    QuerySnapshotStore,
};

use crate::{LocalStorageConfiguration, QueryConfiguration, QueryInitializationError};

#[derive(Clone, Debug)]
pub(crate) struct QueryBoundary {
    inner: Arc<QueryShared>,
}

#[derive(Debug)]
struct QueryShared {
    snapshots: QuerySnapshotStore,
    snapshot_limits: QuerySnapshotLimits,
    engine: QueryEngine,
    concurrency: Arc<Semaphore>,
    shutdown: CancellationToken,
}

impl QueryBoundary {
    pub(crate) fn new(
        query: &QueryConfiguration,
        local_storage: &LocalStorageConfiguration,
        snapshots: QuerySnapshotStore,
        objects: QueryObjectStore,
    ) -> Result<Self, QueryInitializationError> {
        let maximum_concurrency = query.maximum_concurrent_queries().get();
        let permits = usize::try_from(maximum_concurrency)
            .ok()
            .filter(|permits| *permits <= Semaphore::MAX_PERMITS)
            .ok_or(QueryInitializationError::ConcurrencyUnsupported {
                maximum: maximum_concurrency,
            })?;
        let limits = QueryExecutionLimits::new(QueryExecutionLimitConfiguration {
            timeout: Duration::from_secs(query.timeout_seconds().get()),
            maximum_scan_bytes: query.maximum_scan_bytes().get(),
            memory_bytes: query.memory_bytes().get(),
            scratch_path: local_storage.scratch_path().to_path_buf(),
            scratch_capacity_bytes: local_storage.scratch_capacity_bytes().get(),
            maximum_result_rows: query.maximum_result_rows().get(),
            maximum_result_bytes: query.maximum_result_bytes().get(),
        })?;
        let snapshot_limits = QuerySnapshotLimits::new(query.maximum_scan_bytes().get())?;
        let engine = QueryEngine::new(
            objects,
            Arc::new(HistoricalConversionMetrics::default()),
            limits,
        )?;
        Ok(Self {
            inner: Arc::new(QueryShared {
                snapshots,
                snapshot_limits,
                engine,
                concurrency: Arc::new(Semaphore::new(permits)),
                shutdown: CancellationToken::new(),
            }),
        })
    }

    #[must_use]
    pub(crate) fn availability(&self) -> QueryAvailability {
        if self.inner.concurrency.is_closed() || self.inner.shutdown.is_cancelled() {
            QueryAvailability::Draining
        } else {
            QueryAvailability::Available
        }
    }

    pub(crate) fn try_admit(&self) -> Result<AdmittedQuery, QueryAdmissionFailure> {
        let permit = Arc::clone(&self.inner.concurrency)
            .try_acquire_owned()
            .map_err(|error| match error {
                TryAcquireError::NoPermits => QueryAdmissionFailure::CapacityExhausted,
                TryAcquireError::Closed => QueryAdmissionFailure::Draining,
            })?;
        Ok(AdmittedQuery {
            inner: Arc::clone(&self.inner),
            cancellation: QueryCancellation::new(),
            _permit: permit,
        })
    }

    pub(crate) fn begin_shutdown(&self) {
        self.inner.concurrency.close();
        self.inner.shutdown.cancel();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryAvailability {
    Available,
    Draining,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryAdmissionFailure {
    CapacityExhausted,
    Draining,
}

#[derive(Debug)]
pub(crate) struct AdmittedQuery {
    inner: Arc<QueryShared>,
    cancellation: QueryCancellation,
    _permit: OwnedSemaphorePermit,
}

impl AdmittedQuery {
    pub(crate) async fn execute(
        self,
        query: String,
        request_range: QueryRequestTimeRange,
        output_rows: u64,
    ) -> Result<CompletedQuery, QueryFailure> {
        let output_row_limit = self
            .inner
            .engine
            .limits()
            .output_row_limit(output_rows)
            .map_err(QueryFailure::OutputRowLimit)?;
        let snapshot_shutdown = self.inner.shutdown.clone();
        let snapshot = tokio::select! {
            biased;
            () = snapshot_shutdown.cancelled_owned() => return Err(QueryFailure::Cancelled),
            result = self.inner.snapshots.select(
                &query,
                request_range,
                self.inner.snapshot_limits,
            ) => result.map_err(QueryFailure::Snapshot)?,
        };
        let execution_shutdown = self.inner.shutdown.clone();
        let result = tokio::select! {
            biased;
            () = execution_shutdown.cancelled_owned() => {
                self.cancellation.cancel();
                return Err(QueryFailure::Cancelled);
            }
            result = self.inner.engine.execute(
                &snapshot,
                &self.cancellation,
                output_row_limit,
            ) => result.map_err(QueryFailure::Engine)?,
        };
        Ok(CompletedQuery { snapshot, result })
    }
}

impl Drop for AdmittedQuery {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[derive(Debug)]
pub(crate) struct CompletedQuery {
    snapshot: QuerySnapshot,
    result: QueryResult,
}

impl CompletedQuery {
    #[must_use]
    pub(crate) const fn snapshot(&self) -> &QuerySnapshot {
        &self.snapshot
    }

    #[must_use]
    pub(crate) const fn result(&self) -> &QueryResult {
        &self.result
    }

    #[must_use]
    pub(crate) fn into_result(self) -> QueryResult {
        self.result
    }
}

#[derive(Debug)]
pub(crate) enum QueryFailure {
    Snapshot(QuerySnapshotError),
    Engine(EngineError),
    OutputRowLimit(QueryOutputRowLimitError),
    Cancelled,
}
