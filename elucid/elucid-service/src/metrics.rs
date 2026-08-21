use std::sync::atomic::AtomicU64;

use elucid_metastore::OperationalBacklog;
use prometheus_client::{
    encoding::text::encode,
    metrics::{counter::Counter, gauge::Gauge},
    registry::Registry,
};

type UnsignedGauge = Gauge<u64, AtomicU64>;

#[derive(Debug)]
pub(crate) struct ServiceMetrics {
    registry: Registry,
    http_batches_accepted: Counter,
    http_bytes_accepted: Counter,
    http_batches_rejected: Counter,
    records_accepted: Counter,
    records_rejected: Counter,
    records_ignored: Counter,
    segments_published: Counter,
    dead_letter_objects_published: Counter,
    publication_retries: Counter,
    spool_used_bytes: UnsignedGauge,
    spool_pending_batches: UnsignedGauge,
    prepared_segments: UnsignedGauge,
    planned_objects: UnsignedGauge,
    uploaded_objects: UnsignedGauge,
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        let http_batches_accepted = Counter::default();
        let http_bytes_accepted = Counter::default();
        let http_batches_rejected = Counter::default();
        let records_accepted = Counter::default();
        let records_rejected = Counter::default();
        let records_ignored = Counter::default();
        let segments_published = Counter::default();
        let dead_letter_objects_published = Counter::default();
        let publication_retries = Counter::default();
        let spool_used_bytes = UnsignedGauge::default();
        let spool_pending_batches = UnsignedGauge::default();
        let prepared_segments = UnsignedGauge::default();
        let planned_objects = UnsignedGauge::default();
        let uploaded_objects = UnsignedGauge::default();

        let mut registry = Registry::default();
        registry.register(
            "elucid_ingestion_http_batches_accepted",
            "HTTP ingestion batches accepted",
            http_batches_accepted.clone(),
        );
        registry.register(
            "elucid_ingestion_http_bytes_accepted",
            "HTTP ingestion bytes accepted",
            http_bytes_accepted.clone(),
        );
        registry.register(
            "elucid_ingestion_http_batches_rejected",
            "HTTP ingestion batches rejected",
            http_batches_rejected.clone(),
        );
        registry.register(
            "elucid_ingestion_records_accepted",
            "Ingestion records accepted",
            records_accepted.clone(),
        );
        registry.register(
            "elucid_ingestion_records_rejected",
            "Ingestion records rejected",
            records_rejected.clone(),
        );
        registry.register(
            "elucid_ingestion_records_ignored",
            "Ingestion records ignored",
            records_ignored.clone(),
        );
        registry.register(
            "elucid_ingestion_segments_published",
            "Ingestion segments published",
            segments_published.clone(),
        );
        registry.register(
            "elucid_ingestion_dead_letter_objects_published",
            "Ingestion dead-letter objects published",
            dead_letter_objects_published.clone(),
        );
        registry.register(
            "elucid_ingestion_publication_retries",
            "Ingestion publication retries",
            publication_retries.clone(),
        );
        registry.register(
            "elucid_spool_used_bytes",
            "Bytes used by the local ingestion spool",
            spool_used_bytes.clone(),
        );
        registry.register(
            "elucid_spool_pending_batches",
            "Batches pending in the local ingestion spool",
            spool_pending_batches.clone(),
        );
        registry.register(
            "elucid_publication_prepared_segments",
            "Segments prepared for publication",
            prepared_segments.clone(),
        );
        registry.register(
            "elucid_publication_planned_objects",
            "Objects with planned publication",
            planned_objects.clone(),
        );
        registry.register(
            "elucid_publication_uploaded_objects",
            "Objects uploaded but not committed",
            uploaded_objects.clone(),
        );

        Self {
            registry,
            http_batches_accepted,
            http_bytes_accepted,
            http_batches_rejected,
            records_accepted,
            records_rejected,
            records_ignored,
            segments_published,
            dead_letter_objects_published,
            publication_retries,
            spool_used_bytes,
            spool_pending_batches,
            prepared_segments,
            planned_objects,
            uploaded_objects,
        }
    }
}

impl ServiceMetrics {
    pub(crate) fn record_http_accepted(&self, bytes: u64) {
        self.http_batches_accepted.inc();
        self.http_bytes_accepted.inc_by(bytes);
    }

    pub(crate) fn record_http_rejected(&self) {
        self.http_batches_rejected.inc();
    }

    pub(crate) fn record_normalized(&self, accepted: u64, rejected: u64, ignored: u64) {
        self.records_accepted.inc_by(accepted);
        self.records_rejected.inc_by(rejected);
        self.records_ignored.inc_by(ignored);
    }

    pub(crate) fn record_segment_published(&self) {
        self.segments_published.inc();
    }

    pub(crate) fn record_dead_letter_published(&self) {
        self.dead_letter_objects_published.inc();
    }

    pub(crate) fn record_publication_retry(&self) {
        self.publication_retries.inc();
    }

    pub(crate) fn update_spool(&self, used_bytes: u64, pending_batches: u64) {
        self.spool_used_bytes.set(used_bytes);
        self.spool_pending_batches.set(pending_batches);
    }

    pub(crate) fn update_publication_backlog(&self, backlog: OperationalBacklog) {
        self.prepared_segments.set(backlog.prepared_segments());
        self.planned_objects.set(backlog.planned_objects());
        self.uploaded_objects.set(backlog.uploaded_objects());
    }

    #[must_use]
    pub(crate) fn publication_backlog(&self) -> OperationalBacklogSnapshot {
        OperationalBacklogSnapshot {
            prepared_segments: self.prepared_segments.get(),
            planned_objects: self.planned_objects.get(),
            uploaded_objects: self.uploaded_objects.get(),
        }
    }

    pub(crate) fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut output = String::new();
        encode(&mut output, &self.registry)?;
        Ok(output)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct OperationalBacklogSnapshot {
    pub(crate) prepared_segments: u64,
    pub(crate) planned_objects: u64,
    pub(crate) uploaded_objects: u64,
}

#[cfg(test)]
mod tests {
    use super::ServiceMetrics;

    #[test]
    fn encodes_registered_metrics_as_openmetrics() {
        let metrics = ServiceMetrics::default();
        metrics.record_http_accepted(42);
        metrics.update_spool(128, 1);

        let exposition = metrics.encode().expect("encode metrics");

        assert!(exposition.contains("# HELP elucid_ingestion_http_batches_accepted "));
        assert!(exposition.contains("# TYPE elucid_ingestion_http_batches_accepted counter"));
        assert!(exposition.contains("elucid_ingestion_http_batches_accepted_total 1"));
        assert!(exposition.contains("elucid_ingestion_http_bytes_accepted_total 42"));
        assert!(exposition.contains("elucid_spool_used_bytes 128"));
        assert!(exposition.contains("elucid_spool_pending_batches 1"));
        assert!(exposition.ends_with("# EOF\n"));
    }
}
