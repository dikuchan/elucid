use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

use elucid_ingestion::{
    AppendBodyLimit, BatchMetadata, DurableAppend, MaximumBatchAdmission, RecoveredBatches, Spool,
    SpoolCapacity, SpoolError, SpoolErrorCode, SpoolReservation,
};

use crate::{IngestionConfiguration, LocalStorageConfiguration, ServiceError};

pub(crate) const MAXIMUM_HTTP_BATCH_RECORDS: u64 = 100_000;

#[derive(Clone, Debug)]
pub(crate) struct IngestionBoundary {
    inner: Arc<IngestionShared>,
}

#[derive(Debug)]
struct IngestionShared {
    spool: Spool,
    maximum_body: AppendBodyLimit,
    concurrency: Arc<Semaphore>,
    admitted_requests: TaskTracker,
    shutdown: CancellationToken,
    used_bytes: AtomicU64,
    pending_batches: AtomicU64,
    _recovered_batches: tokio::sync::Mutex<RecoveredBatches>,
}

impl IngestionBoundary {
    pub(crate) async fn open(
        local_storage: &LocalStorageConfiguration,
        ingestion: &IngestionConfiguration,
    ) -> Result<Self, ServiceError> {
        let capacity =
            SpoolCapacity::new(local_storage.spool_capacity_bytes().get()).map_err(|_| {
                ServiceError::IngestionInitialization {
                    reason: "spool capacity is outside the ingestion model",
                }
            })?;
        let maximum_body = AppendBodyLimit::new(ingestion.maximum_http_batch_bytes().get())
            .map_err(|_| ServiceError::IngestionInitialization {
                reason: "HTTP batch limit is outside the ingestion model",
            })?;
        let permits = usize::try_from(ingestion.maximum_concurrent_requests().get())
            .ok()
            .filter(|permits| *permits <= Semaphore::MAX_PERMITS)
            .ok_or(ServiceError::IngestionInitialization {
                reason: "maximum concurrent ingestion requests exceeds the runtime limit",
            })?;
        let recovery = Spool::open(local_storage.spool_path(), capacity, maximum_body)
            .await
            .map_err(|source| ServiceError::SpoolInitialization { source })?;
        let report = recovery.report();
        let (spool, recovered_batches, _) = recovery.into_parts();
        Ok(Self {
            inner: Arc::new(IngestionShared {
                spool,
                maximum_body,
                concurrency: Arc::new(Semaphore::new(permits)),
                admitted_requests: TaskTracker::new(),
                shutdown: CancellationToken::new(),
                used_bytes: AtomicU64::new(report.committed_bytes()),
                pending_batches: AtomicU64::new(report.pending_batches()),
                _recovered_batches: tokio::sync::Mutex::new(recovered_batches),
            }),
        })
    }

    #[must_use]
    pub(crate) fn status(&self) -> IngestionStatus {
        IngestionStatus {
            used_bytes: self.inner.used_bytes.load(Ordering::Relaxed),
            pending_batches: self.inner.pending_batches.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn availability(&self) -> IngestionAvailability {
        if self.inner.concurrency.is_closed() {
            return IngestionAvailability::Unavailable;
        }
        match self
            .inner
            .spool
            .maximum_batch_admission(self.inner.maximum_body)
        {
            Ok(MaximumBatchAdmission::Available) => IngestionAvailability::Available,
            Ok(MaximumBatchAdmission::CapacityExhausted) => {
                IngestionAvailability::CapacityExhausted
            }
            Err(_) => IngestionAvailability::Unavailable,
            Ok(_) => IngestionAvailability::Unavailable,
        }
    }

    pub(crate) fn try_admit(&self) -> Result<AdmittedAppend, AdmissionFailure> {
        // The token must exist before acquiring a permit. Shutdown closes the semaphore before
        // waiting on the tracker, so every request that wins the admission race is accounted for.
        let request = self.inner.admitted_requests.token();
        let permit = Arc::clone(&self.inner.concurrency)
            .try_acquire_owned()
            .map_err(|error| match error {
                TryAcquireError::NoPermits => AdmissionFailure::CapacityExhausted,
                TryAcquireError::Closed => AdmissionFailure::Draining,
            })?;
        let reservation = self
            .inner
            .spool
            .reserve(self.inner.maximum_body)
            .map_err(admission_failure)?;
        Ok(AdmittedAppend {
            inner: Arc::clone(&self.inner),
            reservation,
            _request: request,
            _permit: permit,
        })
    }

    #[must_use]
    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown.clone()
    }

    pub(crate) fn begin_shutdown(&self) {
        self.inner.concurrency.close();
        self.inner.admitted_requests.close();
        self.inner.shutdown.cancel();
    }

    pub(crate) async fn wait_for_admitted_requests(&self) {
        self.inner.admitted_requests.wait().await;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IngestionAvailability {
    Available,
    CapacityExhausted,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IngestionStatus {
    used_bytes: u64,
    pending_batches: u64,
}

impl IngestionStatus {
    #[must_use]
    pub(crate) const fn used_bytes(self) -> u64 {
        self.used_bytes
    }

    #[must_use]
    pub(crate) const fn pending_batches(self) -> u64 {
        self.pending_batches
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionFailure {
    CapacityExhausted,
    Draining,
    Unavailable,
}

#[derive(Debug)]
pub(crate) struct AdmittedAppend {
    inner: Arc<IngestionShared>,
    reservation: SpoolReservation,
    _request: TaskTrackerToken,
    _permit: OwnedSemaphorePermit,
}

impl AdmittedAppend {
    pub(crate) async fn append(
        self,
        metadata: BatchMetadata,
        body: Bytes,
    ) -> Result<DurableAppend, SpoolError> {
        let Self {
            inner,
            reservation,
            _request,
            _permit,
        } = self;
        let durable = reservation.append(metadata, body).await?;
        let usage = inner.spool.usage()?;
        inner
            .used_bytes
            .store(usage.committed_bytes(), Ordering::Relaxed);
        inner.pending_batches.fetch_add(1, Ordering::Relaxed);
        Ok(durable)
    }
}

fn admission_failure(error: SpoolError) -> AdmissionFailure {
    match error.code() {
        SpoolErrorCode::CapacityExhausted => AdmissionFailure::CapacityExhausted,
        SpoolErrorCode::BatchLimitExceeded
        | SpoolErrorCode::Corrupt
        | SpoolErrorCode::Unavailable => AdmissionFailure::Unavailable,
        _ => AdmissionFailure::Unavailable,
    }
}
