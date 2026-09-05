use std::fmt::{Display, Formatter};

use chrono::{DateTime, NaiveDate, Utc};
use elucid_core::ErrorCode;
use sqlx::postgres::PgConnection;
use sqlx::{Connection as _, FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::compaction::{
    CompactionMetadataError, CompactionModelError, CompactionRunId,
    MAXIMUM_COMPACTION_INPUT_SEGMENTS, MAXIMUM_COMPACTION_OUTPUT_SEGMENTS, MaintenanceOwner,
};
use crate::publication::OrphanGracePeriod;
use crate::retention::ReclamationGracePeriod;

pub const MAXIMUM_COMPACTION_RECOVERY_RUNS: u64 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CompactionRecoveryLimit(i64);

impl CompactionRecoveryLimit {
    pub fn new(runs: u64) -> Result<Self, CompactionModelError> {
        if runs == 0 || runs > MAXIMUM_COMPACTION_RECOVERY_RUNS {
            return Err(CompactionModelError::RecoveryLimitOutOfRange {
                maximum: MAXIMUM_COMPACTION_RECOVERY_RUNS,
            });
        }
        i64::try_from(runs)
            .map(Self)
            .map_err(|_| CompactionModelError::RecoveryLimitOutOfRange {
                maximum: MAXIMUM_COMPACTION_RECOVERY_RUNS,
            })
    }

    const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompactionFailureReason {
    InputInvalid,
    BuildFailed,
    NotBeneficial,
    PublicationFailed,
    RecoveryFailed,
}

impl From<CompactionFailureReason> for ErrorCode {
    fn from(value: CompactionFailureReason) -> Self {
        match value {
            CompactionFailureReason::InputInvalid => Self::CompactionInputInvalid,
            CompactionFailureReason::BuildFailed => Self::CompactionBuildFailed,
            CompactionFailureReason::NotBeneficial => Self::CompactionNotBeneficial,
            CompactionFailureReason::PublicationFailed => Self::CompactionPublicationFailed,
            CompactionFailureReason::RecoveryFailed => Self::CompactionRecoveryFailed,
        }
    }
}

impl CompactionFailureReason {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        ErrorCode::from(self).as_str()
    }

    fn parse(value: &str) -> Result<Self, CompactionMetadataError> {
        match value.parse::<ErrorCode>() {
            Ok(ErrorCode::CompactionInputInvalid) => Ok(Self::InputInvalid),
            Ok(ErrorCode::CompactionBuildFailed) => Ok(Self::BuildFailed),
            Ok(ErrorCode::CompactionNotBeneficial) => Ok(Self::NotBeneficial),
            Ok(ErrorCode::CompactionPublicationFailed) => Ok(Self::PublicationFailed),
            Ok(ErrorCode::CompactionRecoveryFailed) => Ok(Self::RecoveryFailed),
            _ => Err(CompactionMetadataError::corrupt(
                "compaction run has an unknown failure code",
            )),
        }
    }
}

impl Display for CompactionFailureReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompactionPublicationOutcome {
    Published,
    AlreadyPublished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompactionFailureOutcome {
    Failed,
    AlreadyFailed,
    AlreadyCommitted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CompactionRecovery {
    failed_runs: Vec<CompactionRunId>,
}

impl CompactionRecovery {
    #[must_use]
    pub fn failed_runs(&self) -> &[CompactionRunId] {
        &self.failed_runs
    }
}

impl MaintenanceOwner {
    /// Publishes every output and supersedes every input in one PostgreSQL transaction.
    ///
    /// A retry resolves an uncertain previous commit from the durable run state.
    ///
    /// # Errors
    ///
    /// Returns a conflict for a non-publishable run, corrupt for inconsistent durable metadata,
    /// or unavailable when PostgreSQL cannot complete or resolve the transaction.
    pub async fn publish_replacement(
        &mut self,
        run_id: CompactionRunId,
        grace: ReclamationGracePeriod,
    ) -> Result<CompactionPublicationOutcome, CompactionMetadataError> {
        match publish_replacement_transaction(&mut self.guard, run_id, grace).await? {
            TransactionOutcome::Committed(outcome) => Ok(outcome),
            TransactionOutcome::CommitFailed { source: commit, .. } => {
                match inspect_run(&self.resolution_pool, run_id).await {
                    Ok(InspectedRun::Committed) => {
                        Ok(CompactionPublicationOutcome::AlreadyPublished)
                    }
                    Ok(InspectedRun::Building | InspectedRun::Uploading) => {
                        Err(CompactionMetadataError::write(commit))
                    }
                    Ok(InspectedRun::Failed) => Err(CompactionMetadataError::conflict(
                        "failed compaction run cannot be published",
                    )),
                    Err(resolution) => Err(CompactionMetadataError::ambiguous_commit(
                        commit, resolution,
                    )),
                }
            }
        }
    }

    /// Fails one pre-publication run, releases its inputs, and abandons its outputs atomically.
    ///
    /// A committed run is reported without being modified, which resolves a publication response
    /// lost after PostgreSQL committed the replacement.
    ///
    /// # Errors
    ///
    /// Returns corrupt for unsafe lifecycle combinations or unavailable when PostgreSQL cannot
    /// complete or resolve the transaction.
    pub async fn fail_run(
        &mut self,
        run_id: CompactionRunId,
        failure_code: CompactionFailureReason,
        grace: OrphanGracePeriod,
    ) -> Result<CompactionFailureOutcome, CompactionMetadataError> {
        match fail_run_transaction(&mut self.guard, run_id, failure_code, grace).await? {
            TransactionOutcome::Committed(outcome) => Ok(outcome),
            TransactionOutcome::CommitFailed { source: commit, .. } => {
                match inspect_run(&self.resolution_pool, run_id).await {
                    Ok(InspectedRun::Committed) => Ok(CompactionFailureOutcome::AlreadyCommitted),
                    Ok(InspectedRun::Failed) => Ok(CompactionFailureOutcome::AlreadyFailed),
                    Ok(InspectedRun::Building | InspectedRun::Uploading) => {
                        Err(CompactionMetadataError::write(commit))
                    }
                    Err(resolution) => Err(CompactionMetadataError::ambiguous_commit(
                        commit, resolution,
                    )),
                }
            }
        }
    }

    /// Fails one bounded batch of unfinished runs left by a former maintenance owner.
    ///
    /// Callers repeat this operation until `failed_runs()` is empty.
    ///
    /// # Errors
    ///
    /// Returns corrupt when an unfinished run cannot be abandoned safely or unavailable when
    /// PostgreSQL cannot complete or resolve the batch transaction.
    pub async fn recover_unfinished(
        &mut self,
        grace: OrphanGracePeriod,
        limit: CompactionRecoveryLimit,
    ) -> Result<CompactionRecovery, CompactionMetadataError> {
        match recover_unfinished_transaction(&mut self.guard, grace, limit).await? {
            TransactionOutcome::Committed(recovery) => Ok(recovery),
            TransactionOutcome::CommitFailed {
                source: commit,
                intended,
            } => match inspect_recovery(&self.resolution_pool, intended.failed_runs()).await {
                Ok(true) => Ok(intended),
                Ok(false) => Err(CompactionMetadataError::write(commit)),
                Err(resolution) => Err(CompactionMetadataError::ambiguous_commit(
                    commit, resolution,
                )),
            },
        }
    }
}

enum TransactionOutcome<Outcome> {
    Committed(Outcome),
    CommitFailed {
        source: sqlx::Error,
        intended: Outcome,
    },
}

async fn publish_replacement_transaction(
    connection: &mut PgConnection,
    run_id: CompactionRunId,
    grace: ReclamationGracePeriod,
) -> Result<TransactionOutcome<CompactionPublicationOutcome>, CompactionMetadataError> {
    let mut transaction = connection
        .begin()
        .await
        .map_err(CompactionMetadataError::unavailable)?;
    let replacement = load_replacement_for_update(&mut transaction, run_id).await?;
    let outcome = match replacement.run.state()? {
        RunState::Uploading => {
            validate_prepublication(&replacement)?;
            publish_locked_replacement(&mut transaction, &replacement, grace).await?;
            CompactionPublicationOutcome::Published
        }
        RunState::Committed => {
            validate_committed(&replacement)?;
            CompactionPublicationOutcome::AlreadyPublished
        }
        RunState::Building => {
            return rollback_with(
                transaction,
                CompactionMetadataError::conflict(
                    "compaction outputs must be uploaded before publication",
                ),
            )
            .await;
        }
        RunState::Failed => {
            return rollback_with(
                transaction,
                CompactionMetadataError::conflict("failed compaction run cannot be published"),
            )
            .await;
        }
    };
    Ok(match transaction.commit().await {
        Ok(()) => TransactionOutcome::Committed(outcome),
        Err(source) => TransactionOutcome::CommitFailed {
            source,
            intended: outcome,
        },
    })
}

async fn fail_run_transaction(
    connection: &mut PgConnection,
    run_id: CompactionRunId,
    failure_code: CompactionFailureReason,
    grace: OrphanGracePeriod,
) -> Result<TransactionOutcome<CompactionFailureOutcome>, CompactionMetadataError> {
    let mut transaction = connection
        .begin()
        .await
        .map_err(CompactionMetadataError::unavailable)?;
    let replacement = load_replacement_for_update(&mut transaction, run_id).await?;
    let outcome = match replacement.run.state()? {
        RunState::Building | RunState::Uploading => {
            validate_failure_safe(&replacement)?;
            fail_locked_run(&mut transaction, &replacement, failure_code, grace).await?;
            CompactionFailureOutcome::Failed
        }
        RunState::Failed => {
            validate_failed(&replacement)?;
            CompactionFailureOutcome::AlreadyFailed
        }
        RunState::Committed => {
            validate_committed(&replacement)?;
            CompactionFailureOutcome::AlreadyCommitted
        }
    };
    Ok(match transaction.commit().await {
        Ok(()) => TransactionOutcome::Committed(outcome),
        Err(source) => TransactionOutcome::CommitFailed {
            source,
            intended: outcome,
        },
    })
}

async fn recover_unfinished_transaction(
    connection: &mut PgConnection,
    grace: OrphanGracePeriod,
    limit: CompactionRecoveryLimit,
) -> Result<TransactionOutcome<CompactionRecovery>, CompactionMetadataError> {
    let mut transaction = connection
        .begin()
        .await
        .map_err(CompactionMetadataError::unavailable)?;
    let run_ids = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT compaction_run_id
        FROM compaction_runs
        WHERE state IN ('BUILDING', 'UPLOADING')
        ORDER BY created_at, compaction_run_id
        LIMIT $1
        FOR UPDATE
        "#,
    )
    .bind(limit.get())
    .fetch_all(&mut *transaction)
    .await
    .map_err(CompactionMetadataError::read)?;
    let mut failed_runs = Vec::with_capacity(run_ids.len());
    for run_uuid in run_ids {
        let run_id = CompactionRunId::from(run_uuid);
        let replacement = load_replacement_for_update(&mut transaction, run_id).await?;
        if !matches!(
            replacement.run.state()?,
            RunState::Building | RunState::Uploading
        ) {
            return rollback_with(
                transaction,
                CompactionMetadataError::corrupt(
                    "locked recovery candidate unexpectedly became terminal",
                ),
            )
            .await;
        }
        validate_failure_safe(&replacement)?;
        fail_locked_run(
            &mut transaction,
            &replacement,
            CompactionFailureReason::RecoveryFailed,
            grace,
        )
        .await?;
        failed_runs.push(run_id);
    }
    let recovery = CompactionRecovery { failed_runs };
    Ok(match transaction.commit().await {
        Ok(()) => TransactionOutcome::Committed(recovery),
        Err(source) => TransactionOutcome::CommitFailed {
            source,
            intended: recovery,
        },
    })
}

async fn load_replacement_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: CompactionRunId,
) -> Result<ReplacementRows, CompactionMetadataError> {
    let run = sqlx::query_as::<_, LifecycleRunRow>(
        r#"
        SELECT
            compaction_run_id,
            source_id,
            schema_id,
            event_day,
            state,
            failure_code,
            completed_at
        FROM compaction_runs
        WHERE compaction_run_id = $1
        FOR UPDATE
        "#,
    )
    .bind(run_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(CompactionMetadataError::read)?
    .ok_or_else(|| CompactionMetadataError::conflict("compaction run is not registered"))?;
    let input_limit = i64::try_from(MAXIMUM_COMPACTION_INPUT_SEGMENTS + 1).map_err(|_| {
        CompactionMetadataError::corrupt("compaction input validation limit overflowed")
    })?;
    let inputs = sqlx::query_as::<_, LifecycleSegmentRow>(
        r#"
        SELECT
            segment_id,
            source_id,
            schema_id,
            event_day,
            minimum_event_time,
            maximum_event_time,
            minimum_ingestion_time,
            maximum_ingestion_time,
            row_count,
            data_expires_at,
            state,
            published_at,
            retired_at,
            reclaim_after
        FROM segments
        WHERE claimed_by_compaction_run_id = $1
        ORDER BY segment_id
        LIMIT $2
        FOR UPDATE
        "#,
    )
    .bind(run_id.as_uuid())
    .bind(input_limit)
    .fetch_all(&mut **transaction)
    .await
    .map_err(CompactionMetadataError::read)?;
    if inputs.len()
        > usize::try_from(MAXIMUM_COMPACTION_INPUT_SEGMENTS).map_err(|_| {
            CompactionMetadataError::corrupt("compaction input segment bound overflowed")
        })?
    {
        return Err(CompactionMetadataError::corrupt(
            "compaction run has too many input segments",
        ));
    }
    let output_limit = i64::try_from(MAXIMUM_COMPACTION_OUTPUT_SEGMENTS + 1).map_err(|_| {
        CompactionMetadataError::corrupt("compaction output validation limit overflowed")
    })?;
    let outputs = sqlx::query_as::<_, LifecycleSegmentRow>(
        r#"
        SELECT
            segment_id,
            source_id,
            schema_id,
            event_day,
            minimum_event_time,
            maximum_event_time,
            minimum_ingestion_time,
            maximum_ingestion_time,
            row_count,
            data_expires_at,
            state,
            published_at,
            retired_at,
            reclaim_after
        FROM segments
        WHERE produced_by_compaction_run_id = $1
        ORDER BY segment_id
        LIMIT $2
        FOR UPDATE
        "#,
    )
    .bind(run_id.as_uuid())
    .bind(output_limit)
    .fetch_all(&mut **transaction)
    .await
    .map_err(CompactionMetadataError::read)?;
    if outputs.len() > MAXIMUM_COMPACTION_OUTPUT_SEGMENTS {
        return Err(CompactionMetadataError::corrupt(
            "compaction run has too many output segments",
        ));
    }
    let output_ids = outputs.iter().map(|row| row.segment_id).collect::<Vec<_>>();
    let objects = if output_ids.is_empty() {
        Vec::new()
    } else {
        sqlx::query_as::<_, LifecycleObjectRow>(
            r#"
            SELECT
                object_id,
                segment_id,
                kind,
                state,
                uploaded_at,
                published_at,
                delete_requested_at,
                deleted_at
            FROM stored_objects
            WHERE segment_id = ANY($1::uuid[])
            ORDER BY segment_id
            FOR UPDATE
            "#,
        )
        .bind(&output_ids)
        .fetch_all(&mut **transaction)
        .await
        .map_err(CompactionMetadataError::read)?
    };
    Ok(ReplacementRows {
        run,
        inputs,
        outputs,
        objects,
    })
}

async fn publish_locked_replacement(
    transaction: &mut Transaction<'_, Postgres>,
    replacement: &ReplacementRows,
    grace: ReclamationGracePeriod,
) -> Result<(), CompactionMetadataError> {
    let output_object_ids = replacement
        .objects
        .iter()
        .map(|row| row.object_id)
        .collect::<Vec<_>>();
    require_exact_rows(
        sqlx::query(
            "UPDATE stored_objects SET state = 'PUBLISHED', published_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE object_id = ANY($1::uuid[]) AND state = 'UPLOADED'",
        )
        .bind(&output_object_ids)
        .execute(&mut **transaction)
        .await
        .map_err(CompactionMetadataError::write)?
        .rows_affected(),
        output_object_ids.len(),
        "locked compaction output objects were not published exactly once",
    )?;
    let output_segment_ids = replacement
        .outputs
        .iter()
        .map(|row| row.segment_id)
        .collect::<Vec<_>>();
    require_exact_rows(
        sqlx::query(
            "UPDATE segments SET state = 'ACTIVE', published_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE segment_id = ANY($1::uuid[]) AND state = 'PREPARED'",
        )
        .bind(&output_segment_ids)
        .execute(&mut **transaction)
        .await
        .map_err(CompactionMetadataError::write)?
        .rows_affected(),
        output_segment_ids.len(),
        "locked compaction output segments were not activated exactly once",
    )?;
    let input_segment_ids = replacement
        .inputs
        .iter()
        .map(|row| row.segment_id)
        .collect::<Vec<_>>();
    require_exact_rows(
        sqlx::query(
            "UPDATE segments SET state = 'SUPERSEDED', retired_at = CURRENT_TIMESTAMP, reclaim_after = CURRENT_TIMESTAMP + make_interval(secs => $2::double precision), updated_at = CURRENT_TIMESTAMP WHERE segment_id = ANY($1::uuid[]) AND state = 'ACTIVE' AND claimed_by_compaction_run_id = $3",
        )
        .bind(&input_segment_ids)
        .bind(grace.seconds())
        .bind(replacement.run.compaction_run_id)
        .execute(&mut **transaction)
        .await
        .map_err(CompactionMetadataError::write)?
        .rows_affected(),
        input_segment_ids.len(),
        "locked compaction inputs were not superseded exactly once",
    )?;
    require_exact_rows(
        sqlx::query(
            "UPDATE compaction_runs SET state = 'COMMITTED', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE compaction_run_id = $1 AND state = 'UPLOADING'",
        )
        .bind(replacement.run.compaction_run_id)
        .execute(&mut **transaction)
        .await
        .map_err(CompactionMetadataError::write)?
        .rows_affected(),
        1,
        "locked compaction run was not committed exactly once",
    )
}

async fn fail_locked_run(
    transaction: &mut Transaction<'_, Postgres>,
    replacement: &ReplacementRows,
    failure_code: CompactionFailureReason,
    grace: OrphanGracePeriod,
) -> Result<(), CompactionMetadataError> {
    let input_segment_ids = replacement
        .inputs
        .iter()
        .map(|row| row.segment_id)
        .collect::<Vec<_>>();
    require_exact_rows(
        sqlx::query(
            "UPDATE segments SET claimed_by_compaction_run_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE segment_id = ANY($1::uuid[]) AND state = 'ACTIVE' AND claimed_by_compaction_run_id = $2",
        )
        .bind(&input_segment_ids)
        .bind(replacement.run.compaction_run_id)
        .execute(&mut **transaction)
        .await
        .map_err(CompactionMetadataError::write)?
        .rows_affected(),
        input_segment_ids.len(),
        "locked compaction input claims were not released exactly once",
    )?;
    let output_segment_ids = replacement
        .outputs
        .iter()
        .map(|row| row.segment_id)
        .collect::<Vec<_>>();
    if !output_segment_ids.is_empty() {
        require_exact_rows(
            sqlx::query(
                "UPDATE segments SET state = 'ABANDONED', retired_at = CURRENT_TIMESTAMP, reclaim_after = CURRENT_TIMESTAMP + make_interval(secs => $2::double precision), updated_at = CURRENT_TIMESTAMP WHERE segment_id = ANY($1::uuid[]) AND state = 'PREPARED'",
            )
            .bind(&output_segment_ids)
            .bind(grace.seconds())
            .execute(&mut **transaction)
            .await
            .map_err(CompactionMetadataError::write)?
            .rows_affected(),
            output_segment_ids.len(),
            "locked compaction outputs were not abandoned exactly once",
        )?;
    }
    require_exact_rows(
        sqlx::query(
            "UPDATE compaction_runs SET state = 'FAILED', failure_code = $2, completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE compaction_run_id = $1 AND state IN ('BUILDING', 'UPLOADING')",
        )
        .bind(replacement.run.compaction_run_id)
        .bind(failure_code.as_str())
        .execute(&mut **transaction)
        .await
        .map_err(CompactionMetadataError::write)?
        .rows_affected(),
        1,
        "locked compaction run was not failed exactly once",
    )
}

fn validate_prepublication(replacement: &ReplacementRows) -> Result<(), CompactionMetadataError> {
    validate_common_replacement(replacement)?;
    if replacement.inputs.iter().any(|input| {
        input.state != "ACTIVE"
            || input.published_at.is_none()
            || input.retired_at.is_some()
            || input.reclaim_after.is_some()
    }) {
        return Err(CompactionMetadataError::conflict(
            "compaction inputs are not active at publication",
        ));
    }
    if replacement.outputs.iter().any(|output| {
        output.state != "PREPARED"
            || output.published_at.is_some()
            || output.retired_at.is_some()
            || output.reclaim_after.is_some()
    }) {
        return Err(CompactionMetadataError::conflict(
            "compaction outputs are not prepared at publication",
        ));
    }
    if replacement.objects.iter().any(|object| {
        object.kind != "PARQUET_DATA"
            || object.state != "UPLOADED"
            || object.uploaded_at.is_none()
            || object.published_at.is_some()
            || object.delete_requested_at.is_some()
            || object.deleted_at.is_some()
    }) {
        return Err(CompactionMetadataError::conflict(
            "every compaction output object must be uploaded before publication",
        ));
    }
    Ok(())
}

fn validate_committed(replacement: &ReplacementRows) -> Result<(), CompactionMetadataError> {
    validate_common_replacement(replacement)?;
    let completed_at = replacement.run.completed_at.ok_or_else(|| {
        CompactionMetadataError::corrupt("committed compaction run has no completion time")
    })?;
    if replacement.inputs.iter().any(|input| {
        input.state != "SUPERSEDED"
            || input.published_at.is_none()
            || input.retired_at != Some(completed_at)
            || input
                .reclaim_after
                .is_none_or(|reclaim_after| reclaim_after <= completed_at)
    }) {
        return Err(CompactionMetadataError::corrupt(
            "committed compaction inputs are not superseded atomically",
        ));
    }
    if replacement.outputs.iter().any(|output| {
        output.state != "ACTIVE"
            || output.published_at != Some(completed_at)
            || output.retired_at.is_some()
            || output.reclaim_after.is_some()
    }) {
        return Err(CompactionMetadataError::corrupt(
            "committed compaction outputs are not active atomically",
        ));
    }
    if replacement.objects.iter().any(|object| {
        object.kind != "PARQUET_DATA"
            || object.state != "PUBLISHED"
            || object.uploaded_at.is_none()
            || object.published_at != Some(completed_at)
            || object.delete_requested_at.is_some()
            || object.deleted_at.is_some()
    }) {
        return Err(CompactionMetadataError::corrupt(
            "committed compaction objects are not published atomically",
        ));
    }
    Ok(())
}

fn validate_failure_safe(replacement: &ReplacementRows) -> Result<(), CompactionMetadataError> {
    if replacement.inputs.len() < 2
        || replacement.inputs.iter().any(|input| {
            !input.matches_run(&replacement.run)
                || input.state != "ACTIVE"
                || input.published_at.is_none()
                || input.retired_at.is_some()
                || input.reclaim_after.is_some()
        })
    {
        return Err(CompactionMetadataError::corrupt(
            "unfinished compaction inputs cannot be released safely",
        ));
    }
    match replacement.run.state()? {
        RunState::Building if replacement.outputs.is_empty() && replacement.objects.is_empty() => {
            Ok(())
        }
        RunState::Uploading => {
            validate_common_replacement(replacement)?;
            if replacement.outputs.iter().any(|output| {
                output.state != "PREPARED"
                    || output.published_at.is_some()
                    || output.retired_at.is_some()
                    || output.reclaim_after.is_some()
            }) || replacement.objects.iter().any(|object| {
                object.kind != "PARQUET_DATA"
                    || !matches!(
                        (object.state.as_str(), object.uploaded_at),
                        ("PLANNED", None) | ("UPLOADED", Some(_))
                    )
                    || object.published_at.is_some()
                    || object.delete_requested_at.is_some()
                    || object.deleted_at.is_some()
            }) {
                return Err(CompactionMetadataError::corrupt(
                    "unfinished compaction outputs cannot be abandoned safely",
                ));
            }
            Ok(())
        }
        RunState::Building => Err(CompactionMetadataError::corrupt(
            "building compaction run already has registered outputs",
        )),
        RunState::Committed | RunState::Failed => Err(CompactionMetadataError::corrupt(
            "terminal compaction run reached pre-publication failure cleanup",
        )),
    }
}

fn validate_failed(replacement: &ReplacementRows) -> Result<(), CompactionMetadataError> {
    replacement.run.failure_code()?;
    if !replacement.inputs.is_empty()
        || replacement.objects.len() != replacement.outputs.len()
        || replacement.outputs.iter().any(|output| {
            !output.matches_run(&replacement.run)
                || output.state != "ABANDONED"
                || output.published_at.is_some()
                || output.retired_at.is_none()
                || output
                    .reclaim_after
                    .zip(output.retired_at)
                    .is_none_or(|(reclaim_after, retired_at)| reclaim_after <= retired_at)
        })
        || replacement
            .outputs
            .iter()
            .zip(&replacement.objects)
            .any(|(output, object)| {
                object.segment_id != output.segment_id
                    || object.kind != "PARQUET_DATA"
                    || !matches!(
                        object.state.as_str(),
                        "PLANNED" | "UPLOADED" | "DELETE_PENDING" | "DELETED"
                    )
                    || object.published_at.is_some()
            })
    {
        return Err(CompactionMetadataError::corrupt(
            "failed compaction run was not cleaned up atomically",
        ));
    }
    Ok(())
}

fn validate_common_replacement(
    replacement: &ReplacementRows,
) -> Result<(), CompactionMetadataError> {
    if replacement.inputs.len() < 2
        || replacement.outputs.is_empty()
        || replacement.outputs.len() >= replacement.inputs.len()
        || replacement.objects.len() != replacement.outputs.len()
    {
        return Err(CompactionMetadataError::corrupt(
            "compaction replacement has invalid input or output cardinality",
        ));
    }
    for input in &replacement.inputs {
        if !input.matches_run(&replacement.run) {
            return Err(CompactionMetadataError::corrupt(
                "compaction input ownership differs from its run",
            ));
        }
    }
    for (output, object) in replacement.outputs.iter().zip(&replacement.objects) {
        if !output.matches_run(&replacement.run) || object.segment_id != output.segment_id {
            return Err(CompactionMetadataError::corrupt(
                "compaction output ownership differs from its run or object",
            ));
        }
    }
    let input = ReplacementTotals::from_segments(&replacement.inputs)?;
    let output = ReplacementTotals::from_segments(&replacement.outputs)?;
    if input.rows != output.rows
        || input.minimum_event_time != output.minimum_event_time
        || input.maximum_event_time != output.maximum_event_time
        || input.minimum_ingestion_time != output.minimum_ingestion_time
        || input.maximum_ingestion_time != output.maximum_ingestion_time
        || replacement
            .outputs
            .iter()
            .any(|row| row.data_expires_at != Some(input.maximum_data_expires_at))
    {
        return Err(CompactionMetadataError::corrupt(
            "compaction replacement totals, bounds, or retention differ",
        ));
    }
    Ok(())
}

async fn inspect_run(
    pool: &sqlx::PgPool,
    run_id: CompactionRunId,
) -> Result<InspectedRun, CompactionMetadataError> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(CompactionMetadataError::unavailable)?;
    let replacement = load_replacement_for_update(&mut transaction, run_id).await?;
    let inspected = match replacement.run.state()? {
        RunState::Building => InspectedRun::Building,
        RunState::Uploading => InspectedRun::Uploading,
        RunState::Committed => {
            validate_committed(&replacement)?;
            InspectedRun::Committed
        }
        RunState::Failed => {
            validate_failed(&replacement)?;
            InspectedRun::Failed
        }
    };
    transaction
        .commit()
        .await
        .map_err(CompactionMetadataError::write)?;
    Ok(inspected)
}

async fn inspect_recovery(
    pool: &sqlx::PgPool,
    run_ids: &[CompactionRunId],
) -> Result<bool, CompactionMetadataError> {
    if run_ids.is_empty() {
        let unfinished = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM compaction_runs WHERE state IN ('BUILDING', 'UPLOADING'))",
        )
        .fetch_one(pool)
        .await
        .map_err(CompactionMetadataError::read)?;
        return Ok(!unfinished);
    }
    let identities = run_ids
        .iter()
        .map(|run_id| run_id.as_uuid())
        .collect::<Vec<_>>();
    let failed = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM compaction_runs
        WHERE compaction_run_id = ANY($1::uuid[])
          AND state = 'FAILED'
          AND failure_code = 'COMPACTION_RECOVERY_FAILED'
          AND completed_at IS NOT NULL
        "#,
    )
    .bind(&identities)
    .fetch_one(pool)
    .await
    .map_err(CompactionMetadataError::read)?;
    Ok(usize::try_from(failed).ok() == Some(run_ids.len()))
}

async fn rollback_with<Outcome>(
    transaction: Transaction<'_, Postgres>,
    error: CompactionMetadataError,
) -> Result<Outcome, CompactionMetadataError> {
    match transaction.rollback().await {
        Ok(()) => Err(error),
        Err(source) => Err(CompactionMetadataError::write(source)),
    }
}

fn require_exact_rows(
    actual: u64,
    expected: usize,
    message: &'static str,
) -> Result<(), CompactionMetadataError> {
    if actual
        == u64::try_from(expected)
            .map_err(|_| CompactionMetadataError::corrupt("affected-row bound overflowed"))?
    {
        Ok(())
    } else {
        Err(CompactionMetadataError::corrupt(message))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunState {
    Building,
    Uploading,
    Committed,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InspectedRun {
    Building,
    Uploading,
    Committed,
    Failed,
}

#[derive(Debug)]
struct ReplacementRows {
    run: LifecycleRunRow,
    inputs: Vec<LifecycleSegmentRow>,
    outputs: Vec<LifecycleSegmentRow>,
    objects: Vec<LifecycleObjectRow>,
}

#[derive(Debug, FromRow)]
struct LifecycleRunRow {
    compaction_run_id: Uuid,
    source_id: Uuid,
    schema_id: Uuid,
    event_day: NaiveDate,
    state: String,
    failure_code: Option<String>,
    completed_at: Option<DateTime<Utc>>,
}

impl LifecycleRunRow {
    fn state(&self) -> Result<RunState, CompactionMetadataError> {
        match self.state.as_str() {
            "BUILDING" if self.failure_code.is_none() && self.completed_at.is_none() => {
                Ok(RunState::Building)
            }
            "UPLOADING" if self.failure_code.is_none() && self.completed_at.is_none() => {
                Ok(RunState::Uploading)
            }
            "COMMITTED" if self.failure_code.is_none() && self.completed_at.is_some() => {
                Ok(RunState::Committed)
            }
            "FAILED" if self.failure_code.is_some() && self.completed_at.is_some() => {
                Ok(RunState::Failed)
            }
            "BUILDING" | "UPLOADING" | "COMMITTED" | "FAILED" => {
                Err(CompactionMetadataError::corrupt(
                    "compaction run timestamps or failure code contradict its state",
                ))
            }
            _ => Err(CompactionMetadataError::corrupt(
                "compaction run has an unknown lifecycle state",
            )),
        }
    }

    fn failure_code(&self) -> Result<CompactionFailureReason, CompactionMetadataError> {
        self.failure_code
            .as_deref()
            .ok_or_else(|| {
                CompactionMetadataError::corrupt("failed compaction run has no failure code")
            })
            .and_then(CompactionFailureReason::parse)
    }
}

#[derive(Debug, FromRow)]
struct LifecycleSegmentRow {
    segment_id: Uuid,
    source_id: Uuid,
    schema_id: Uuid,
    event_day: NaiveDate,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    minimum_ingestion_time: DateTime<Utc>,
    maximum_ingestion_time: DateTime<Utc>,
    row_count: i64,
    data_expires_at: Option<DateTime<Utc>>,
    state: String,
    published_at: Option<DateTime<Utc>>,
    retired_at: Option<DateTime<Utc>>,
    reclaim_after: Option<DateTime<Utc>>,
}

impl LifecycleSegmentRow {
    fn matches_run(&self, run: &LifecycleRunRow) -> bool {
        self.source_id == run.source_id
            && self.schema_id == run.schema_id
            && self.event_day == run.event_day
    }
}

#[derive(Debug, FromRow)]
struct LifecycleObjectRow {
    object_id: Uuid,
    segment_id: Uuid,
    kind: String,
    state: String,
    uploaded_at: Option<DateTime<Utc>>,
    published_at: Option<DateTime<Utc>>,
    delete_requested_at: Option<DateTime<Utc>>,
    deleted_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReplacementTotals {
    rows: u64,
    minimum_event_time: DateTime<Utc>,
    maximum_event_time: DateTime<Utc>,
    minimum_ingestion_time: DateTime<Utc>,
    maximum_ingestion_time: DateTime<Utc>,
    maximum_data_expires_at: DateTime<Utc>,
}

impl ReplacementTotals {
    fn from_segments(segments: &[LifecycleSegmentRow]) -> Result<Self, CompactionMetadataError> {
        let first = segments.first().ok_or_else(|| {
            CompactionMetadataError::corrupt("compaction replacement has no segments")
        })?;
        let mut totals = Self {
            rows: positive_rows(first.row_count)?,
            minimum_event_time: first.minimum_event_time,
            maximum_event_time: first.maximum_event_time,
            minimum_ingestion_time: first.minimum_ingestion_time,
            maximum_ingestion_time: first.maximum_ingestion_time,
            maximum_data_expires_at: first.data_expires_at.ok_or_else(|| {
                CompactionMetadataError::corrupt("compaction segment has no retention deadline")
            })?,
        };
        for segment in &segments[1..] {
            totals.rows = totals
                .rows
                .checked_add(positive_rows(segment.row_count)?)
                .ok_or_else(|| {
                    CompactionMetadataError::corrupt("compaction row total overflowed")
                })?;
            totals.minimum_event_time = totals.minimum_event_time.min(segment.minimum_event_time);
            totals.maximum_event_time = totals.maximum_event_time.max(segment.maximum_event_time);
            totals.minimum_ingestion_time = totals
                .minimum_ingestion_time
                .min(segment.minimum_ingestion_time);
            totals.maximum_ingestion_time = totals
                .maximum_ingestion_time
                .max(segment.maximum_ingestion_time);
            totals.maximum_data_expires_at =
                totals
                    .maximum_data_expires_at
                    .max(segment.data_expires_at.ok_or_else(|| {
                        CompactionMetadataError::corrupt(
                            "compaction segment has no retention deadline",
                        )
                    })?);
        }
        Ok(totals)
    }
}

fn positive_rows(rows: i64) -> Result<u64, CompactionMetadataError> {
    u64::try_from(rows)
        .ok()
        .filter(|rows| *rows > 0)
        .ok_or_else(|| CompactionMetadataError::corrupt("compaction segment row count is invalid"))
}
