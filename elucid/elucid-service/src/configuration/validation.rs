use url::Host;

use super::error::{ConfigurationError, ConfigurationExpression, ConfigurationViolation};
use super::model::NetworkTrust;
use super::raw::RuntimeConfigurationCandidate;

const MAXIMUM_QUERY_SNAPSHOT_LIFETIME_SECONDS: u64 = 30;

pub(super) fn validate(
    configuration: &RuntimeConfigurationCandidate,
) -> Result<(), ConfigurationError> {
    validate_browser_origin(configuration)?;
    validate_network_trust(configuration)?;

    if configuration.server.default_page_items.get() > configuration.server.maximum_page_items.get()
    {
        return violation(ConfigurationViolation::DefaultPageItemsExceedMaximum);
    }
    if configuration.query.default_output_rows.get() > configuration.query.maximum_output_rows.get()
    {
        return violation(ConfigurationViolation::DefaultOutputRowsExceedMaximum);
    }

    let minimum_ingestion_stale_seconds = checked_multiply(
        configuration
            .ingestion
            .attempt_heartbeat_interval_seconds
            .get(),
        3,
        ConfigurationExpression::IngestionStaleThreshold,
    )?;
    if configuration.ingestion.attempt_stale_after_seconds.get() < minimum_ingestion_stale_seconds {
        return violation(ConfigurationViolation::IngestionStaleThresholdTooSmall);
    }

    let minimum_compaction_stale_seconds = checked_multiply(
        configuration
            .compaction
            .run_heartbeat_interval_seconds
            .get(),
        3,
        ConfigurationExpression::CompactionStaleThreshold,
    )?;
    if configuration.compaction.run_stale_after_seconds.get() < minimum_compaction_stale_seconds {
        return violation(ConfigurationViolation::CompactionStaleThresholdTooSmall);
    }

    if configuration.ingestion.maximum_record_bytes.get()
        > configuration.ingestion.maximum_request_body_bytes.get()
    {
        return violation(ConfigurationViolation::MaximumRecordBytesExceedRequestBody);
    }
    if configuration.ingestion.maximum_dead_letter_page_bytes.get()
        <= configuration
            .ingestion
            .dead_letter_complete_raw_maximum_bytes
            .get()
    {
        return violation(ConfigurationViolation::DeadLetterPageBytesDoNotExceedCompleteRaw);
    }
    if configuration.query.execution_timeout_seconds.get() > MAXIMUM_QUERY_SNAPSHOT_LIFETIME_SECONDS
    {
        return violation(ConfigurationViolation::QueryTimeoutExceedsSnapshotLifetime);
    }
    if configuration
        .garbage_collection
        .retired_object_grace_period_seconds
        .get()
        <= MAXIMUM_QUERY_SNAPSHOT_LIFETIME_SECONDS
    {
        return violation(
            ConfigurationViolation::RetiredObjectGracePeriodDoesNotExceedSnapshotLifetime,
        );
    }

    let maximum_upload_attempt_seconds = checked_multiply(
        configuration.object_store.request_timeout_seconds.get(),
        configuration.object_store.maximum_request_attempts.get(),
        ConfigurationExpression::OrphanGracePeriodMinimum,
    )?;
    let minimum_orphan_grace_period_seconds = configuration
        .compaction
        .run_timeout_seconds
        .get()
        .checked_add(maximum_upload_attempt_seconds)
        .ok_or(ConfigurationError::ArithmeticOverflow {
            expression: ConfigurationExpression::OrphanGracePeriodMinimum,
        })?;
    if configuration
        .garbage_collection
        .orphan_grace_period_seconds
        .get()
        <= minimum_orphan_grace_period_seconds
    {
        return violation(ConfigurationViolation::OrphanGracePeriodTooShort);
    }

    if configuration.compaction.minimum_input_segments.get() < 2 {
        return violation(ConfigurationViolation::MinimumInputSegmentsBelowTwo);
    }
    if configuration.compaction.minimum_input_segments.get()
        > configuration.compaction.maximum_input_segments.get()
    {
        return violation(ConfigurationViolation::MinimumInputSegmentsExceedMaximum);
    }
    if configuration.compaction.maximum_output_segments.get()
        >= configuration.compaction.maximum_input_segments.get()
    {
        return violation(
            ConfigurationViolation::MaximumOutputSegmentsNotBelowMaximumInputSegments,
        );
    }
    if configuration
        .compaction
        .maximum_output_parquet_object_bytes
        .get()
        > configuration.compaction.maximum_output_parquet_bytes.get()
    {
        return violation(ConfigurationViolation::MaximumOutputObjectBytesExceedTotal);
    }
    if configuration.compaction.maximum_concurrent_runs.get()
        > configuration
            .compaction
            .maximum_cluster_concurrent_runs
            .get()
    {
        return violation(ConfigurationViolation::LocalCompactionConcurrencyExceedsCluster);
    }
    if configuration.retention.maximum_task_duration_seconds.get()
        >= configuration.retention.scan_interval_seconds.get()
    {
        return violation(ConfigurationViolation::RetentionTaskDurationNotBelowScanInterval);
    }
    if configuration.ingestion.attempt_timeout_seconds.get()
        >= configuration.retention.idempotency_retention_seconds.get()
    {
        return violation(ConfigurationViolation::AttemptTimeoutNotBelowIdempotencyRetention);
    }
    if configuration.retention.idempotency_retention_seconds.get()
        <= configuration.ingestion.attempt_stale_after_seconds.get()
    {
        return violation(ConfigurationViolation::IdempotencyRetentionDoesNotExceedAttemptStale);
    }
    if configuration.retention.idempotency_retention_seconds.get()
        > configuration
            .retention
            .ingestion_provenance_retention_seconds
            .get()
    {
        return violation(ConfigurationViolation::IdempotencyRetentionExceedsIngestionProvenance);
    }
    if configuration.retention.event_data_retention_seconds.get()
        > configuration
            .retention
            .ingestion_provenance_retention_seconds
            .get()
    {
        return violation(ConfigurationViolation::EventDataRetentionExceedsIngestionProvenance);
    }
    if configuration.retention.dead_letter_retention_seconds.get()
        > configuration
            .retention
            .ingestion_provenance_retention_seconds
            .get()
    {
        return violation(ConfigurationViolation::DeadLetterRetentionExceedsIngestionProvenance);
    }
    if configuration.ingestion.staging_capacity_bytes.get()
        < configuration.ingestion.maximum_request_body_bytes.get()
    {
        return violation(ConfigurationViolation::IngestionStagingCapacityBelowMaximumRequest);
    }
    if configuration.query.memory_pool_bytes.get() < configuration.query.maximum_result_bytes.get()
    {
        return violation(ConfigurationViolation::QueryMemoryCapacityBelowMaximumResult);
    }
    if configuration.query.spill_capacity_bytes.get()
        < configuration.query.maximum_result_bytes.get()
    {
        return violation(ConfigurationViolation::QuerySpillCapacityBelowMaximumResult);
    }

    let concurrent_compaction_output_bytes = checked_multiply(
        configuration.compaction.maximum_concurrent_runs.get(),
        configuration.compaction.maximum_output_parquet_bytes.get(),
        ConfigurationExpression::ConcurrentCompactionOutputCapacity,
    )?;
    if configuration.compaction.working_capacity_bytes.get() < concurrent_compaction_output_bytes {
        return violation(ConfigurationViolation::CompactionWorkingCapacityBelowConcurrentOutput);
    }

    let output_row_capacity = checked_multiply(
        configuration.compaction.maximum_output_segments.get(),
        configuration.compaction.maximum_output_segment_rows.get(),
        ConfigurationExpression::InputRowOutputCapacity,
    )?;
    if configuration.compaction.maximum_input_rows.get() > output_row_capacity {
        return violation(ConfigurationViolation::MaximumInputRowsExceedOutputCapacity);
    }

    let output_uncompressed_byte_capacity = checked_multiply(
        configuration.compaction.maximum_output_segments.get(),
        configuration
            .compaction
            .target_output_segment_uncompressed_bytes
            .get(),
        ConfigurationExpression::InputUncompressedOutputCapacity,
    )?;
    if configuration
        .compaction
        .maximum_input_uncompressed_bytes
        .get()
        > output_uncompressed_byte_capacity
    {
        return violation(
            ConfigurationViolation::MaximumInputUncompressedBytesExceedOutputCapacity,
        );
    }

    Ok(())
}

fn validate_browser_origin(
    configuration: &RuntimeConfigurationCandidate,
) -> Result<(), ConfigurationError> {
    let origin = &configuration.server.browser_origin;
    if !matches!(origin.scheme(), "http" | "https") {
        return violation(ConfigurationViolation::BrowserOriginSchemeUnsupported);
    }
    if !origin.username().is_empty()
        || origin.password().is_some()
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
    {
        return violation(ConfigurationViolation::BrowserOriginIsNotAnOrigin);
    }
    Ok(())
}

fn validate_network_trust(
    configuration: &RuntimeConfigurationCandidate,
) -> Result<(), ConfigurationError> {
    let server = &configuration.server;
    match server.network_trust {
        NetworkTrust::LoopbackOnly => {
            if !server.bind.ip().is_loopback() {
                return violation(ConfigurationViolation::LoopbackTrustRequiresLoopbackBind);
            }
            if server.operator_bearer_token_environment_variable.is_some() {
                return violation(
                    ConfigurationViolation::OperatorSecretReferenceRequiresTrustedNetwork,
                );
            }
        }
        NetworkTrust::LocalContainer => {
            if server.browser_origin.scheme() != "http"
                || !browser_origin_host_is_loopback(&server.browser_origin)
            {
                return violation(ConfigurationViolation::LocalContainerRequiresHttpLoopbackOrigin);
            }
            if server.operator_bearer_token_environment_variable.is_some() {
                return violation(
                    ConfigurationViolation::OperatorSecretReferenceRequiresTrustedNetwork,
                );
            }
        }
        NetworkTrust::TrustedNetwork => {
            if server.operator_bearer_token_environment_variable.is_none() {
                return violation(
                    ConfigurationViolation::TrustedNetworkRequiresOperatorSecretReference,
                );
            }
            if server.browser_origin.scheme() != "https" {
                return violation(ConfigurationViolation::TrustedNetworkRequiresHttpsOrigin);
            }
        }
    }
    Ok(())
}

fn browser_origin_host_is_loopback(origin: &url::Url) -> bool {
    match origin.host() {
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

fn checked_multiply(
    left: u64,
    right: u64,
    expression: ConfigurationExpression,
) -> Result<u64, ConfigurationError> {
    left.checked_mul(right)
        .ok_or(ConfigurationError::ArithmeticOverflow { expression })
}

fn violation<T>(violation: ConfigurationViolation) -> Result<T, ConfigurationError> {
    Err(ConfigurationError::ConstraintViolation { violation })
}
