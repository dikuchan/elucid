use std::future::Future;
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub u64);

/// Write-ahead log.
pub trait Wal: Send {
    /// Persist a raw event. Returns a monotonically increasing offset.
    fn append(&mut self, raw: &str) -> impl Future<Output = io::Result<Offset>> + Send;

    /// Mark all events up to (and including) `offset` as committed.
    fn checkpoint(&mut self, offset: Offset) -> impl Future<Output = io::Result<()>> + Send;
}

/// No-op WAL. Tracks offsets but does not persist.
pub struct NoopWal {
    next_offset: u64,
    checkpointed: u64,
}

impl NoopWal {
    pub fn new() -> Self {
        Self {
            next_offset: 0,
            checkpointed: 0,
        }
    }

    /// Returns the highest offset that has been checkpointed.
    pub fn checkpointed(&self) -> Offset {
        Offset(self.checkpointed)
    }
}

impl Default for NoopWal {
    fn default() -> Self {
        Self::new()
    }
}

impl Wal for NoopWal {
    async fn append(&mut self, _raw: &str) -> std::io::Result<Offset> {
        let offset = Offset(self.next_offset);
        self.next_offset += 1;
        Ok(offset)
    }

    async fn checkpoint(&mut self, offset: Offset) -> std::io::Result<()> {
        self.checkpointed = self.checkpointed.max(offset.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn append_returns_strictly_increasing_offsets() {
        let mut wal = NoopWal::new();
        let o0 = wal.append("").await.unwrap();
        let o1 = wal.append("").await.unwrap();
        let o2 = wal.append("").await.unwrap();
        assert!(o0 < o1);
        assert!(o1 < o2);
        assert_eq!(o0, Offset(0));
        assert_eq!(o1, Offset(1));
        assert_eq!(o2, Offset(2));
    }

    #[tokio::test]
    async fn checkpoint_updates_checkpointed_offset() {
        let mut wal = NoopWal::new();
        let o = wal.append("").await.unwrap();
        assert_eq!(wal.checkpointed(), Offset(0));
        wal.checkpoint(o).await.unwrap();
        assert_eq!(wal.checkpointed(), o);
    }

    #[tokio::test]
    async fn multiple_checkpoints_offset_only_goes_up() {
        let mut wal = NoopWal::new();
        let o0 = wal.append("").await.unwrap();
        let o1 = wal.append("").await.unwrap();
        let o2 = wal.append("").await.unwrap();

        wal.checkpoint(o2).await.unwrap();
        assert_eq!(wal.checkpointed(), Offset(2));

        wal.checkpoint(o0).await.unwrap();
        assert_eq!(
            wal.checkpointed(),
            Offset(2),
            "checkpoint should not go down"
        );

        wal.checkpoint(o1).await.unwrap();
        assert_eq!(
            wal.checkpointed(),
            Offset(2),
            "checkpoint should stay at max"
        );
    }

    #[tokio::test]
    async fn checkpoint_with_zero_offset_no_events() {
        let mut wal = NoopWal::new();
        assert_eq!(wal.checkpointed(), Offset(0));
        wal.checkpoint(Offset(0)).await.unwrap();
        assert_eq!(wal.checkpointed(), Offset(0));
    }
}
