use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use elucid_metastore::QuerySnapshot;
use futures::{Stream, StreamExt as _};

use crate::pipeline::lower_pipeline;
use crate::runtime::{RuntimeFailureKind, runtime_failure_kind};
use crate::{EngineError, HistoricalConversionMetrics, QueryObjectStore, SnapshotTableProvider};

pub type QueryBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, EngineError>> + Send + 'static>>;

#[derive(Clone, Debug)]
pub struct QueryEngine {
    objects: QueryObjectStore,
    historical_conversion_metrics: Arc<HistoricalConversionMetrics>,
}

impl QueryEngine {
    #[must_use]
    pub fn new(
        objects: QueryObjectStore,
        historical_conversion_metrics: Arc<HistoricalConversionMetrics>,
    ) -> Self {
        Self {
            objects,
            historical_conversion_metrics,
        }
    }

    /// Executes the already analyzed pipeline against its immutable exact-object snapshot.
    ///
    /// The returned stream owns the DataFusion execution. Runtime cast and arithmetic failures
    /// retain their stable Elucid error codes when the stream is polled.
    ///
    /// # Errors
    ///
    /// Returns a stable object, catalog, cast, evaluation, or execution error when opening,
    /// planning, or starting the snapshot fails.
    pub async fn execute(&self, snapshot: &QuerySnapshot) -> Result<QueryBatchStream, EngineError> {
        let provider = SnapshotTableProvider::open(
            snapshot,
            self.objects.clone(),
            Arc::clone(&self.historical_conversion_metrics),
        )
        .await?;
        let provider = Arc::new(provider) as Arc<dyn TableProvider>;
        let plan = lower_pipeline(snapshot.analysis().pipeline(), provider)?;
        let context = SessionContext::new();
        let dataframe = context
            .execute_logical_plan(plan)
            .await
            .map_err(map_datafusion_error)?;
        let stream = dataframe
            .execute_stream()
            .await
            .map_err(map_datafusion_error)?;
        Ok(Box::pin(
            stream.map(|result| result.map_err(map_datafusion_error)),
        ))
    }
}

fn map_datafusion_error(source: DataFusionError) -> EngineError {
    match runtime_failure_kind(&source) {
        Some(RuntimeFailureKind::Cast) => EngineError::cast_failed(source),
        Some(RuntimeFailureKind::Evaluation) => EngineError::evaluation_failed(source),
        Some(RuntimeFailureKind::Execution) | None => EngineError::execution(source),
    }
}
