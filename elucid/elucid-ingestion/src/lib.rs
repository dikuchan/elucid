//! Durable local admission spool.

mod error;
mod frame;
mod model;
mod spool;

pub use elucid_storage::BatchId;
pub use error::{SpoolError, SpoolErrorCode, SpoolModelError};
pub use model::{
    AppendBodyLimit, BatchByteSize, BatchMetadata, BodyDigest, DurableAppend, IngestionTime,
    PinnedCatalogIdentities, SpoolCapacity, SpoolUsage,
};
pub use spool::{Spool, SpoolReservation};
