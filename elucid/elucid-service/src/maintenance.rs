use std::future::pending;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use object_store::ObjectStore;
use sqlx::postgres::PgPool;

use elucid_compaction::{
    CompactionBuildLimitConfiguration, CompactionBuildLimits, CompactionWorker,
};
use elucid_metastore::{
    CompactionClaimLimitConfiguration, CompactionClaimLimits, CompactionFailureCode,
    CompactionRecoveryLimit, CompactionStore, MaintenanceOwner,
    MaintenanceOwnership as MetastoreMaintenanceOwnership, MetadataCleanupLimit,
    ObjectDeletionFailure, ObjectDeletionRetryDelay, ObjectReclamationLimit,
    ObjectReclamationStore, OrphanGracePeriod, PublicationStore, ReclamationGracePeriod,
    RetentionScanLimit, RetentionStore,
};
use elucid_storage::{ImmutableObjectStore, StorageErrorCode};

use crate::runtime::ComponentStatus;
use crate::{MaintenanceError, MaintenanceMode, RuntimeConfiguration};

const MAINTENANCE_SCAN_INTERVAL: Duration = Duration::from_secs(5);
const MAINTENANCE_CYCLE_TIMEOUT: Duration = Duration::from_secs(600);
const COMPACTION_MAXIMUM_DURATION: Duration = Duration::from_secs(300);
const COMPACTION_MINIMUM_RETENTION: Duration = Duration::from_secs(360);
const COMPACTION_CANDIDATE_SEGMENTS: u64 = 1_000;
const COMPACTION_INPUT_SEGMENTS: u64 = 128;
const COMPACTION_INPUT_ROWS: u64 = 4_000_000;
const COMPACTION_INPUT_PARQUET_BYTES: u64 = 512 * 1024 * 1024;
const COMPACTION_INPUT_UNCOMPRESSED_BYTES: u64 = 1024 * 1024 * 1024;
const COMPACTION_READER_BATCH_ROWS: u64 = 8_192;
const COMPACTION_TARGET_OUTPUT_ROWS: u64 = 250_000;
const COMPACTION_TARGET_OUTPUT_UNCOMPRESSED_BYTES: u64 = 64 * 1024 * 1024;
const COMPACTION_MAXIMUM_OUTPUT_PARQUET_BYTES: u64 = 128 * 1024 * 1024;
const COMPACTION_RECOVERY_RUNS: u64 = 1_000;
const COMPACTION_RECOVERY_BATCHES: u64 = 100;
const ORPHAN_GRACE_SECONDS: u64 = 300;
const RECLAMATION_SAFETY_MARGIN_SECONDS: u64 = 60;
const RETENTION_SCAN_ITEMS: u64 = 1_000;
const OBJECT_RECLAMATION_ITEMS: u64 = 100;
const OBJECT_DELETION_RETRY_SECONDS: u64 = 30;
const METADATA_CLEANUP_ROOTS: u64 = 1_000;

#[derive(Clone, Debug)]
pub(crate) struct MaintenanceBoundary {
    state: Arc<RwLock<MaintenanceState>>,
}

impl MaintenanceBoundary {
    fn disabled() -> Self {
        Self::new(MaintenanceState::Disabled)
    }

    fn standby() -> Self {
        Self::new(MaintenanceState::Standby)
    }

    fn new(state: MaintenanceState) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    #[must_use]
    pub(crate) fn status(&self) -> ComponentStatus {
        match self.state() {
            MaintenanceState::OwnedHealthy => ComponentStatus::Up,
            MaintenanceState::Disabled
            | MaintenanceState::Standby
            | MaintenanceState::Recovering
            | MaintenanceState::OwnedDegraded => ComponentStatus::Degraded,
        }
    }

    #[must_use]
    pub(crate) fn ownership(&self) -> MaintenanceOwnership {
        match self.state() {
            MaintenanceState::Disabled => MaintenanceOwnership::Disabled,
            MaintenanceState::Standby => MaintenanceOwnership::Standby,
            MaintenanceState::Recovering
            | MaintenanceState::OwnedHealthy
            | MaintenanceState::OwnedDegraded => MaintenanceOwnership::Owned,
        }
    }

    pub(crate) fn health(
        &self,
        postgresql: ComponentStatus,
        object_store: ComponentStatus,
    ) -> ComponentStatus {
        match self.state() {
            MaintenanceState::Disabled => ComponentStatus::Degraded,
            MaintenanceState::Standby
            | MaintenanceState::Recovering
            | MaintenanceState::OwnedHealthy
            | MaintenanceState::OwnedDegraded
                if postgresql != ComponentStatus::Up || object_store != ComponentStatus::Up =>
            {
                ComponentStatus::Down
            }
            MaintenanceState::Standby
            | MaintenanceState::Recovering
            | MaintenanceState::OwnedHealthy
            | MaintenanceState::OwnedDegraded => self.status(),
        }
    }

    fn transition(&self, state: MaintenanceState) {
        *self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = state;
    }

    fn state(&self) -> MaintenanceState {
        *self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenanceState {
    Disabled,
    Standby,
    Recovering,
    OwnedHealthy,
    OwnedDegraded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceOwnership {
    Disabled,
    Owned,
    Standby,
}

pub(crate) struct MaintenanceRuntime {
    automatic: Option<AutomaticMaintenance>,
}

impl MaintenanceRuntime {
    const fn disabled() -> Self {
        Self { automatic: None }
    }

    fn automatic(runtime: AutomaticMaintenance) -> Self {
        Self {
            automatic: Some(runtime),
        }
    }

    pub(crate) async fn run(self) -> Result<(), MaintenanceError> {
        match self.automatic {
            Some(runtime) => runtime.run().await,
            None => pending().await,
        }
    }
}

pub(crate) async fn initialize(
    configuration: &RuntimeConfiguration,
    pool: PgPool,
    object_store: Arc<dyn ObjectStore>,
    publication: PublicationStore,
) -> Result<(MaintenanceBoundary, MaintenanceRuntime), MaintenanceError> {
    if configuration.maintenance().mode() == MaintenanceMode::Disabled {
        return Ok((
            MaintenanceBoundary::disabled(),
            MaintenanceRuntime::disabled(),
        ));
    }

    let limits = MaintenanceLimits::new(configuration)?;
    let boundary = MaintenanceBoundary::standby();
    let compaction = CompactionStore::new(pool.clone());
    let worker = CompactionWorker::new(
        compaction.clone(),
        publication,
        Arc::clone(&object_store),
        configuration.object_store().managed_root().clone(),
        configuration.local_storage().scratch_path(),
        limits.compaction_build,
    );
    let mut runtime = AutomaticMaintenance {
        boundary: boundary.clone(),
        compaction,
        worker,
        retention: RetentionStore::new(pool.clone()),
        reclamation: ObjectReclamationStore::new(pool),
        objects: ImmutableObjectStore::new(object_store),
        owner: None,
        compaction_scheduling: CompactionScheduling::Enabled,
        limits,
    };
    runtime.acquire_and_recover().await?;
    Ok((boundary, MaintenanceRuntime::automatic(runtime)))
}

struct AutomaticMaintenance {
    boundary: MaintenanceBoundary,
    compaction: CompactionStore,
    worker: CompactionWorker,
    retention: RetentionStore,
    reclamation: ObjectReclamationStore,
    objects: ImmutableObjectStore,
    owner: Option<MaintenanceOwner>,
    compaction_scheduling: CompactionScheduling,
    limits: MaintenanceLimits,
}

impl AutomaticMaintenance {
    async fn run(mut self) -> Result<(), MaintenanceError> {
        loop {
            if self.owner.is_none() {
                match self.acquire_and_recover().await {
                    Ok(()) => {}
                    Err(error) if error.is_retryable() => {
                        self.boundary.transition(MaintenanceState::Standby);
                        tokio::time::sleep(MAINTENANCE_SCAN_INTERVAL).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
                if self.owner.is_none() {
                    tokio::time::sleep(MAINTENANCE_SCAN_INTERVAL).await;
                    continue;
                }
            }

            let cycle = tokio::time::timeout(MAINTENANCE_CYCLE_TIMEOUT, self.run_cycle())
                .await
                .map_err(|_| MaintenanceError::CycleTimedOut)
                .and_then(std::convert::identity);
            match cycle {
                Ok(IterationHealth::Healthy) => {
                    self.boundary.transition(MaintenanceState::OwnedHealthy);
                }
                Ok(IterationHealth::Degraded) => {
                    self.boundary.transition(MaintenanceState::OwnedDegraded);
                }
                Err(error) if error.is_retryable() => {
                    if error.loses_maintenance_owner() {
                        self.owner.take();
                        self.boundary.transition(MaintenanceState::Standby);
                    } else {
                        self.boundary.transition(MaintenanceState::OwnedDegraded);
                    }
                }
                Err(error) => return Err(error),
            }
            tokio::time::sleep(MAINTENANCE_SCAN_INTERVAL).await;
        }
    }

    async fn acquire_and_recover(&mut self) -> Result<(), MaintenanceError> {
        match self.compaction.try_acquire_maintenance().await? {
            MetastoreMaintenanceOwnership::Acquired(mut owner) => {
                self.boundary.transition(MaintenanceState::Recovering);
                recover_unfinished(&mut owner, &self.limits).await?;
                self.owner = Some(owner);
                self.compaction_scheduling = CompactionScheduling::Enabled;
                self.boundary.transition(MaintenanceState::OwnedHealthy);
            }
            MetastoreMaintenanceOwnership::HeldElsewhere => {
                self.owner = None;
                self.boundary.transition(MaintenanceState::Standby);
            }
            _ => return Err(MaintenanceError::OwnershipStateUnsupported),
        }
        Ok(())
    }

    async fn run_cycle(&mut self) -> Result<IterationHealth, MaintenanceError> {
        let owner = self
            .owner
            .as_mut()
            .expect("automatic maintenance cycle requires an acquired owner");
        let mut health = match self.compaction_scheduling {
            CompactionScheduling::Enabled => {
                let health = compact_once(owner, &self.worker, &self.limits).await?;
                if health == IterationHealth::Degraded {
                    self.compaction_scheduling = CompactionScheduling::Suspended;
                }
                health
            }
            CompactionScheduling::Suspended => IterationHealth::Degraded,
        };
        self.retention
            .expire_segments(self.limits.reclamation_grace, self.limits.retention_scan)
            .await?;
        health = health.combine(
            reclaim_objects(
                &self.reclamation,
                &self.objects,
                self.limits.object_deletion_retry,
                self.limits.object_reclamation,
            )
            .await?,
        );
        self.retention
            .clean_terminal_metadata(self.limits.metadata_cleanup)
            .await?;
        Ok(health)
    }
}

#[derive(Clone, Copy)]
struct MaintenanceLimits {
    compaction_claim: CompactionClaimLimits,
    compaction_build: CompactionBuildLimits,
    compaction_recovery: CompactionRecoveryLimit,
    orphan_grace: OrphanGracePeriod,
    reclamation_grace: ReclamationGracePeriod,
    retention_scan: RetentionScanLimit,
    object_deletion_retry: ObjectDeletionRetryDelay,
    object_reclamation: ObjectReclamationLimit,
    metadata_cleanup: MetadataCleanupLimit,
}

impl MaintenanceLimits {
    fn new(configuration: &RuntimeConfiguration) -> Result<Self, MaintenanceError> {
        let scratch_bytes = configuration.local_storage().scratch_capacity_bytes().get();
        let maximum_input_parquet_bytes = scratch_bytes.min(COMPACTION_INPUT_PARQUET_BYTES);
        let maximum_output_parquet_bytes =
            scratch_bytes.min(COMPACTION_MAXIMUM_OUTPUT_PARQUET_BYTES);
        let compaction_claim = CompactionClaimLimits::new(CompactionClaimLimitConfiguration {
            maximum_candidate_segments: COMPACTION_CANDIDATE_SEGMENTS,
            maximum_input_segments: COMPACTION_INPUT_SEGMENTS,
            maximum_input_rows: COMPACTION_INPUT_ROWS,
            maximum_input_parquet_bytes,
            maximum_input_uncompressed_bytes: COMPACTION_INPUT_UNCOMPRESSED_BYTES,
            target_output_rows: COMPACTION_TARGET_OUTPUT_ROWS,
            target_output_uncompressed_bytes: COMPACTION_TARGET_OUTPUT_UNCOMPRESSED_BYTES,
            minimum_retention: COMPACTION_MINIMUM_RETENTION,
        })?;
        let compaction_build = CompactionBuildLimits::new(CompactionBuildLimitConfiguration {
            maximum_input_segments: COMPACTION_INPUT_SEGMENTS,
            maximum_input_rows: COMPACTION_INPUT_ROWS,
            maximum_input_parquet_bytes,
            maximum_input_uncompressed_bytes: COMPACTION_INPUT_UNCOMPRESSED_BYTES,
            reader_batch_rows: COMPACTION_READER_BATCH_ROWS,
            target_output_rows: COMPACTION_TARGET_OUTPUT_ROWS,
            target_output_uncompressed_bytes: COMPACTION_TARGET_OUTPUT_UNCOMPRESSED_BYTES,
            maximum_output_parquet_bytes,
            maximum_staging_bytes: scratch_bytes,
            maximum_duration: COMPACTION_MAXIMUM_DURATION,
        })?;
        Ok(Self {
            compaction_claim,
            compaction_build,
            compaction_recovery: CompactionRecoveryLimit::new(COMPACTION_RECOVERY_RUNS)?,
            orphan_grace: OrphanGracePeriod::new(ORPHAN_GRACE_SECONDS)?,
            reclamation_grace: ReclamationGracePeriod::new(
                configuration.query().timeout_seconds().get(),
                RECLAMATION_SAFETY_MARGIN_SECONDS,
            )?,
            retention_scan: RetentionScanLimit::new(RETENTION_SCAN_ITEMS)?,
            object_deletion_retry: ObjectDeletionRetryDelay::new(OBJECT_DELETION_RETRY_SECONDS)?,
            object_reclamation: ObjectReclamationLimit::new(OBJECT_RECLAMATION_ITEMS)?,
            metadata_cleanup: MetadataCleanupLimit::new(METADATA_CLEANUP_ROOTS)?,
        })
    }
}

async fn recover_unfinished(
    owner: &mut MaintenanceOwner,
    limits: &MaintenanceLimits,
) -> Result<(), MaintenanceError> {
    for _ in 0..COMPACTION_RECOVERY_BATCHES {
        let recovery = owner
            .recover_unfinished(limits.orphan_grace, limits.compaction_recovery)
            .await?;
        let recovered = u64::try_from(recovery.failed_runs().len())
            .map_err(|_| MaintenanceError::RecoveryBoundExceeded)?;
        if recovered < COMPACTION_RECOVERY_RUNS {
            return Ok(());
        }
    }
    Err(MaintenanceError::RecoveryBoundExceeded)
}

async fn compact_once(
    owner: &mut MaintenanceOwner,
    worker: &CompactionWorker,
    limits: &MaintenanceLimits,
) -> Result<IterationHealth, MaintenanceError> {
    let Some(claim) = owner.claim(&limits.compaction_claim).await? else {
        return Ok(IterationHealth::Healthy);
    };
    let run_id = claim.run_id();
    match worker.build_register_and_upload(&claim).await {
        Ok(_) => match owner
            .publish_replacement(run_id, limits.reclamation_grace)
            .await
        {
            Ok(_) => Ok(IterationHealth::Healthy),
            Err(_) => {
                owner
                    .fail_run(
                        run_id,
                        CompactionFailureCode::PublicationFailed,
                        limits.orphan_grace,
                    )
                    .await?;
                Ok(IterationHealth::Degraded)
            }
        },
        Err(error) => {
            owner
                .fail_run(run_id, error.code().into(), limits.orphan_grace)
                .await?;
            Ok(IterationHealth::Degraded)
        }
    }
}

async fn reclaim_objects(
    reclamation: &ObjectReclamationStore,
    objects: &ImmutableObjectStore,
    retry_delay: ObjectDeletionRetryDelay,
    limit: ObjectReclamationLimit,
) -> Result<IterationHealth, MaintenanceError> {
    let claims = reclamation.claim(retry_delay, limit).await?;
    let mut health = IterationHealth::Healthy;
    let mut first_metadata_error = None;
    for claim in claims {
        let result = match objects.delete_exact(claim.descriptor()).await {
            Ok(_) => reclamation.record_deleted(&claim).await.map(|_| ()),
            Err(error) => {
                health = IterationHealth::Degraded;
                let failure = if error.code() == StorageErrorCode::ObjectIntegrityError {
                    ObjectDeletionFailure::Integrity
                } else {
                    ObjectDeletionFailure::Retryable
                };
                reclamation
                    .record_failure(&claim, failure)
                    .await
                    .map(|_| ())
            }
        };
        if let Err(error) = result
            && first_metadata_error.is_none()
        {
            first_metadata_error = Some(error);
        }
    }
    match first_metadata_error {
        Some(error) => Err(error.into()),
        None => Ok(health),
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum IterationHealth {
    Healthy,
    Degraded,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CompactionScheduling {
    Enabled,
    Suspended,
}

impl IterationHealth {
    const fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Healthy, Self::Healthy) => Self::Healthy,
            (Self::Healthy | Self::Degraded, Self::Degraded) | (Self::Degraded, Self::Healthy) => {
                Self::Degraded
            }
        }
    }
}
