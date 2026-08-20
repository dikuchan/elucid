use std::sync::atomic::{AtomicU64, Ordering};

use elucid_metastore::OperationalBacklog;

#[derive(Debug, Default)]
pub(crate) struct ServiceMetrics {
    http_batches_accepted: AtomicU64,
    http_bytes_accepted: AtomicU64,
    http_batches_rejected: AtomicU64,
    records_accepted: AtomicU64,
    records_rejected: AtomicU64,
    records_ignored: AtomicU64,
    segments_published: AtomicU64,
    dead_letter_objects_published: AtomicU64,
    publication_retries: AtomicU64,
    prepared_segments: AtomicU64,
    planned_objects: AtomicU64,
    uploaded_objects: AtomicU64,
}

impl ServiceMetrics {
    pub(crate) fn record_http_accepted(&self, bytes: u64) {
        self.http_batches_accepted.fetch_add(1, Ordering::Relaxed);
        self.http_bytes_accepted.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_http_rejected(&self) {
        self.http_batches_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_normalized(&self, accepted: u64, rejected: u64, ignored: u64) {
        self.records_accepted.fetch_add(accepted, Ordering::Relaxed);
        self.records_rejected.fetch_add(rejected, Ordering::Relaxed);
        self.records_ignored.fetch_add(ignored, Ordering::Relaxed);
    }

    pub(crate) fn record_segment_published(&self) {
        self.segments_published.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dead_letter_published(&self) {
        self.dead_letter_objects_published
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_publication_retry(&self) {
        self.publication_retries.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn update_publication_backlog(&self, backlog: OperationalBacklog) {
        self.prepared_segments
            .store(backlog.prepared_segments(), Ordering::Relaxed);
        self.planned_objects
            .store(backlog.planned_objects(), Ordering::Relaxed);
        self.uploaded_objects
            .store(backlog.uploaded_objects(), Ordering::Relaxed);
    }

    #[must_use]
    pub(crate) fn publication_backlog(&self) -> OperationalBacklogSnapshot {
        OperationalBacklogSnapshot {
            prepared_segments: self.prepared_segments.load(Ordering::Relaxed),
            planned_objects: self.planned_objects.load(Ordering::Relaxed),
            uploaded_objects: self.uploaded_objects.load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub(crate) fn render(&self, spool_used_bytes: u64, spool_pending_batches: u64) -> String {
        let values = [
            (
                "elucid_ingestion_http_batches_accepted_total",
                self.http_batches_accepted.load(Ordering::Relaxed),
            ),
            (
                "elucid_ingestion_http_bytes_accepted_total",
                self.http_bytes_accepted.load(Ordering::Relaxed),
            ),
            (
                "elucid_ingestion_http_batches_rejected_total",
                self.http_batches_rejected.load(Ordering::Relaxed),
            ),
            (
                "elucid_ingestion_records_accepted_total",
                self.records_accepted.load(Ordering::Relaxed),
            ),
            (
                "elucid_ingestion_records_rejected_total",
                self.records_rejected.load(Ordering::Relaxed),
            ),
            (
                "elucid_ingestion_records_ignored_total",
                self.records_ignored.load(Ordering::Relaxed),
            ),
            (
                "elucid_ingestion_segments_published_total",
                self.segments_published.load(Ordering::Relaxed),
            ),
            (
                "elucid_ingestion_dead_letter_objects_published_total",
                self.dead_letter_objects_published.load(Ordering::Relaxed),
            ),
            (
                "elucid_ingestion_publication_retries_total",
                self.publication_retries.load(Ordering::Relaxed),
            ),
            ("elucid_spool_used_bytes", spool_used_bytes),
            ("elucid_spool_pending_batches", spool_pending_batches),
            (
                "elucid_publication_prepared_segments",
                self.prepared_segments.load(Ordering::Relaxed),
            ),
            (
                "elucid_publication_planned_objects",
                self.planned_objects.load(Ordering::Relaxed),
            ),
            (
                "elucid_publication_uploaded_objects",
                self.uploaded_objects.load(Ordering::Relaxed),
            ),
        ];
        let mut output = String::with_capacity(values.len() * 64);
        for (name, value) in values {
            output.push_str("# TYPE ");
            output.push_str(name);
            output.push_str(if name.ends_with("_total") {
                " counter\n"
            } else {
                " gauge\n"
            });
            output.push_str(name);
            output.push(' ');
            output.push_str(&value.to_string());
            output.push('\n');
        }
        output
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct OperationalBacklogSnapshot {
    pub(crate) prepared_segments: u64,
    pub(crate) planned_objects: u64,
    pub(crate) uploaded_objects: u64,
}
