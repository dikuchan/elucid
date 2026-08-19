use super::error::{ConfigurationError, ConfigurationViolation};
use super::raw::RuntimeConfigurationCandidate;

pub(super) fn validate(
    configuration: &RuntimeConfigurationCandidate,
) -> Result<(), ConfigurationError> {
    let batch_bytes = configuration.ingestion.maximum_http_batch_bytes.get();
    let spool_bytes = configuration.local_storage.spool_capacity_bytes.get();
    let scratch_bytes = configuration.local_storage.scratch_capacity_bytes.get();
    let result_bytes = configuration.query.maximum_result_bytes.get();

    if batch_bytes > spool_bytes {
        return violation(ConfigurationViolation::MaximumHttpBatchExceedsSpoolCapacity);
    }
    if batch_bytes > scratch_bytes {
        return violation(ConfigurationViolation::MaximumHttpBatchExceedsScratchCapacity);
    }
    if result_bytes > configuration.query.memory_bytes.get() {
        return violation(ConfigurationViolation::MaximumResultExceedsQueryMemory);
    }
    if result_bytes > scratch_bytes {
        return violation(ConfigurationViolation::MaximumResultExceedsScratchCapacity);
    }
    if configuration.local_storage.spool_path == configuration.local_storage.scratch_path {
        return violation(ConfigurationViolation::SpoolAndScratchPathsMustDiffer);
    }
    Ok(())
}

fn violation(violation: ConfigurationViolation) -> Result<(), ConfigurationError> {
    Err(ConfigurationError::ConstraintViolation { violation })
}
