//! Durable local admission spool.

mod checkpoint;
mod error;
mod frame;
mod model;
mod recovery;
mod spool;

pub use elucid_storage::BatchId;
pub use error::{SpoolError, SpoolErrorCode, SpoolModelError};
pub use model::{
    AppendBodyLimit, BatchByteSize, BatchMetadata, BodyDigest, DurableAppend, IngestionTime,
    MaximumBatchAdmission, PinnedCatalogIdentities, RecoveredBatch, RecoveryReport, SpoolCapacity,
    SpoolCheckpoint, SpoolUsage,
};
pub use recovery::{RecoveredBatches, SpoolRecovery};
pub use spool::{Spool, SpoolReservation};
