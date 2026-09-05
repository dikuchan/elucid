use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

use elucid_ingestion::{
    AppendBodyLimit, BatchMetadata, DurableAppend, IngestionTime, MaximumBatchAdmission,
    RecoveredBatches, Spool, SpoolCapacity, SpoolError, SpoolErrorKind, SpoolReservation,
};

use crate::metrics::ServiceMetrics;
use crate::{IngestionConfiguration, LocalStorageConfiguration, ServiceError};

pub(crate) const MAXIMUM_HTTP_BATCH_RECORDS: u64 = 100_000;
const NO_OLDEST_QUEUED_BATCH: i64 = i64::MIN;

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
    oldest_queued_ingestion_time: AtomicI64,
    metrics: Arc<ServiceMetrics>,
    worker_operational: AtomicBool,
    recovered_batches: tokio::sync::Mutex<Option<RecoveredBatches>>,
}

impl IngestionBoundary {
    pub(crate) async fn open(
        local_storage: &LocalStorageConfiguration,
        ingestion: &IngestionConfiguration,
        metrics: Arc<ServiceMetrics>,
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
                oldest_queued_ingestion_time: AtomicI64::new(NO_OLDEST_QUEUED_BATCH),
                metrics,
                worker_operational: AtomicBool::new(false),
                recovered_batches: tokio::sync::Mutex::new(Some(recovered_batches)),
            }),
        })
    }

    #[must_use]
    pub(crate) fn status(&self) -> IngestionStatus {
        IngestionStatus {
            used_bytes: self.inner.used_bytes.load(Ordering::Relaxed),
            pending_batches: self.inner.pending_batches.load(Ordering::Relaxed),
            oldest_queued_ingestion_time: self
                .inner
                .oldest_queued_ingestion_time
                .load(Ordering::Relaxed),
        }
    }

    pub(crate) fn availability(&self) -> IngestionAvailability {
        if self.inner.concurrency.is_closed()
            || !self.inner.worker_operational.load(Ordering::Acquire)
        {
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

    pub(crate) async fn take_recovered_batches(&self) -> Option<RecoveredBatches> {
        self.inner.recovered_batches.lock().await.take()
    }

    #[must_use]
    pub(crate) fn spool(&self) -> Spool {
        self.inner.spool.clone()
    }

    #[must_use]
    pub(crate) fn metrics(&self) -> &Arc<ServiceMetrics> {
        &self.inner.metrics
    }

    pub(crate) fn set_worker_operational(&self, operational: bool) {
        self.inner
            .worker_operational
            .store(operational, Ordering::Release);
    }

    pub(crate) fn observe_oldest_batch(&self, ingestion_time: IngestionTime) {
        if self
            .inner
            .oldest_queued_ingestion_time
            .load(Ordering::Relaxed)
            == NO_OLDEST_QUEUED_BATCH
        {
            self.inner
                .oldest_queued_ingestion_time
                .store(ingestion_time.unix_milliseconds(), Ordering::Relaxed);
        }
    }

    pub(crate) fn complete_pending_batches(
        &self,
        count: u64,
        next_oldest: Option<IngestionTime>,
    ) -> bool {
        let updated = self
            .inner
            .pending_batches
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |pending| {
                pending.checked_sub(count)
            })
            .is_ok();
        if updated {
            self.inner.oldest_queued_ingestion_time.store(
                next_oldest.map_or(NO_OLDEST_QUEUED_BATCH, IngestionTime::unix_milliseconds),
                Ordering::Relaxed,
            );
        }
        updated
    }

    pub(crate) fn refresh_spool_usage(&self) -> Result<(), SpoolError> {
        let usage = self.inner.spool.usage()?;
        self.inner
            .used_bytes
            .store(usage.committed_bytes(), Ordering::Relaxed);
        Ok(())
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
    oldest_queued_ingestion_time: i64,
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

    #[must_use]
    pub(crate) fn oldest_queued_age_seconds(self) -> Option<u64> {
        if self.oldest_queued_ingestion_time == NO_OLDEST_QUEUED_BATCH {
            return None;
        }
        let age_milliseconds = chrono::Utc::now()
            .timestamp_millis()
            .saturating_sub(self.oldest_queued_ingestion_time)
            .max(0);
        u64::try_from(age_milliseconds / 1_000).ok()
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
        let previous_pending = inner.pending_batches.fetch_add(1, Ordering::Relaxed);
        if previous_pending == 0 {
            inner.oldest_queued_ingestion_time.store(
                durable.metadata().ingestion_time().unix_milliseconds(),
                Ordering::Relaxed,
            );
        }
        inner
            .metrics
            .record_http_accepted(durable.body_bytes().get());
        Ok(durable)
    }
}

fn admission_failure(error: SpoolError) -> AdmissionFailure {
    match error.kind() {
        SpoolErrorKind::CapacityExhausted => AdmissionFailure::CapacityExhausted,
        SpoolErrorKind::BatchLimitExceeded
        | SpoolErrorKind::Corrupt
        | SpoolErrorKind::Unavailable => AdmissionFailure::Unavailable,
        _ => AdmissionFailure::Unavailable,
    }
}
