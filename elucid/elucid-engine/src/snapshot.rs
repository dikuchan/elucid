use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::error::Error as _;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Column, ScalarValue, ToDFSchema as _};
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{col, lit};
use elucid_catalog::{Schema, SchemaId};
use elucid_language::ir::TimeRange;
use elucid_metastore::QuerySnapshot;
use elucid_storage::{
    ImmutableObjectStore, ObjectDescriptor, ObjectVerificationOutcome, PARQUET_FORMAT_VERSION,
    ParquetSegmentExpectation, SegmentId, StorageError, StorageErrorKind,
    validate_parquet_segment_metadata,
};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use object_store::path::Path as ObjectPath;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use crate::schema_adapter::{ElucidSchemaAdapterFactory, StoredSchemaPlan};
use crate::{EngineError, HistoricalConversionMetrics, QueryObjectStore};

const MAXIMUM_CONCURRENT_FOOTER_READS: usize = 16;

#[derive(Debug)]
pub struct SnapshotTableProvider {
    schema: SchemaRef,
    time_range: TimeRange,
    segments: Arc<[ValidatedSegment]>,
    schema_plans: Arc<[StoredSchemaPlan]>,
    objects: QueryObjectStore,
    metrics: Arc<HistoricalConversionMetrics>,
}

impl SnapshotTableProvider {
    /// Opens one immutable query snapshot and validates every selected exact object before scan.
    ///
    /// # Errors
    ///
    /// Returns a stable missing-object, corrupt-object, catalog-corrupt, or execution error. No
    /// rows can be yielded until all selected descriptors and Parquet footers pass validation.
    pub async fn open(
        snapshot: &QuerySnapshot,
        objects: QueryObjectStore,
        metrics: Arc<HistoricalConversionMetrics>,
    ) -> Result<Self, EngineError> {
        let PreparedSnapshot {
            active_schema,
            time_range,
            stored_schemas,
            segments,
            schema_plans,
        } = PreparedSnapshot::from_query_snapshot(snapshot)?;
        let segments = validate_objects(segments, stored_schemas, objects.clone()).await?;
        Ok(Self {
            schema: Arc::new(active_schema.arrow_schema().clone()),
            time_range,
            segments: segments.into(),
            schema_plans,
            objects,
            metrics,
        })
    }
}

#[async_trait]
impl TableProvider for SnapshotTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        state.runtime_env().register_object_store(
            self.objects.url().as_ref(),
            Arc::clone(self.objects.store()),
        );
        let predicate = mandatory_time_predicate(self.time_range);
        let datafusion_schema = Arc::clone(&self.schema).to_dfschema_ref()?;
        let physical_predicate = state.create_physical_expr(predicate, &datafusion_schema)?;
        let parquet_source: Arc<dyn FileSource> = Arc::new(
            ParquetSource::default()
                .with_predicate(physical_predicate)
                .with_pushdown_filters(true)
                .with_enable_page_index(true),
        );
        let parquet_source = parquet_source.with_schema_adapter_factory(Arc::new(
            ElucidSchemaAdapterFactory::new(
                Arc::clone(&self.schema),
                Arc::clone(&self.schema_plans),
                Arc::clone(&self.metrics),
            ),
        ))?;
        let file_groups = self
            .segments
            .iter()
            .map(|segment| {
                FileGroup::new(vec![PartitionedFile::new(
                    segment.object.key().as_str(),
                    segment.object.expected_byte_size().get(),
                )])
            })
            .collect();
        let scan = FileScanConfigBuilder::new(
            self.objects.url().clone(),
            Arc::clone(&self.schema),
            parquet_source,
        )
        .with_file_groups(file_groups)
        .with_projection_indices(projection.cloned())
        .with_limit(limit)
        .build();
        Ok(DataSourceExec::from_data_source(scan))
    }
}

fn mandatory_time_predicate(time_range: TimeRange) -> Expr {
    let timezone = Some(Arc::<str>::from("UTC"));
    let start = ScalarValue::TimestampMillisecond(
        Some(time_range.start_inclusive().unix_milliseconds()),
        timezone.clone(),
    );
    let end = ScalarValue::TimestampMillisecond(
        Some(time_range.end_exclusive().unix_milliseconds()),
        timezone,
    );
    col(Column::from_name("@event_time"))
        .gt_eq(lit(start))
        .and(col(Column::from_name("@event_time")).lt(lit(end)))
}

#[derive(Debug)]
struct PreparedSnapshot {
    active_schema: Schema,
    time_range: TimeRange,
    stored_schemas: HashMap<SchemaId, Arc<Schema>>,
    segments: Vec<PreparedSegment>,
    schema_plans: Arc<[StoredSchemaPlan]>,
}

impl PreparedSnapshot {
    fn from_query_snapshot(snapshot: &QuerySnapshot) -> Result<Self, EngineError> {
        let active_schema = snapshot.active_schema().clone();
        if active_schema.source_id() != snapshot.source_id() {
            return Err(EngineError::catalog_corrupt(
                "active schema does not belong to the selected source",
            ));
        }
        let mut stored_schemas = HashMap::with_capacity(snapshot.stored_schemas().len());
        let mut schema_plans = Vec::with_capacity(snapshot.stored_schemas().len());
        for stored_schema in snapshot.stored_schemas() {
            if stored_schema.source_id() != snapshot.source_id() {
                return Err(EngineError::catalog_corrupt(
                    "stored schema does not belong to the selected source",
                ));
            }
            if stored_schemas
                .insert(stored_schema.id(), Arc::new(stored_schema.clone()))
                .is_some()
            {
                return Err(EngineError::catalog_corrupt(
                    "query snapshot contains one stored schema more than once",
                ));
            }
            schema_plans.push(StoredSchemaPlan::build(&active_schema, stored_schema)?);
        }
        let mut segment_ids = HashSet::with_capacity(snapshot.segments().len());
        let mut segments = Vec::with_capacity(snapshot.segments().len());
        for segment in snapshot.segments() {
            if !segment_ids.insert(segment.segment_id()) {
                return Err(EngineError::catalog_corrupt(
                    "query snapshot contains one segment more than once",
                ));
            }
            if !stored_schemas.contains_key(&segment.schema_id()) {
                return Err(EngineError::catalog_corrupt(
                    "selected segment references a missing stored schema",
                ));
            }
            if segment.object().format_version().get() != PARQUET_FORMAT_VERSION {
                return Err(EngineError::corrupt_object_invariant(
                    "published object has an unsupported Parquet format version",
                ));
            }
            segments.push(PreparedSegment {
                segment_id: segment.segment_id(),
                schema_id: segment.schema_id(),
                row_count: segment.row_count(),
                object: segment.object().clone(),
            });
        }
        Ok(Self {
            active_schema,
            time_range: snapshot.time_range(),
            stored_schemas,
            segments,
            schema_plans: schema_plans.into(),
        })
    }
}

#[derive(Debug)]
struct PreparedSegment {
    segment_id: SegmentId,
    schema_id: SchemaId,
    row_count: u64,
    object: ObjectDescriptor,
}

#[derive(Debug)]
struct ValidatedSegment {
    object: ObjectDescriptor,
}

async fn validate_objects(
    segments: Vec<PreparedSegment>,
    stored_schemas: HashMap<SchemaId, Arc<Schema>>,
    objects: QueryObjectStore,
) -> Result<Vec<ValidatedSegment>, EngineError> {
    stream::iter(segments)
        .map(move |segment| {
            let stored_schema = stored_schemas.get(&segment.schema_id).cloned();
            let objects = objects.clone();
            async move {
                let stored_schema = stored_schema.ok_or_else(|| {
                    EngineError::catalog_corrupt(
                        "selected segment references a missing stored schema",
                    )
                })?;
                validate_object(&segment, &stored_schema, &objects).await?;
                let validated = ValidatedSegment {
                    object: segment.object,
                };
                tokio::task::yield_now().await;
                Ok(validated)
            }
        })
        .buffered(MAXIMUM_CONCURRENT_FOOTER_READS)
        .try_collect()
        .await
}

async fn validate_object(
    segment: &PreparedSegment,
    stored_schema: &Schema,
    objects: &QueryObjectStore,
) -> Result<(), EngineError> {
    let exact_store = ImmutableObjectStore::new(Arc::clone(objects.store()));
    match exact_store.verify(&segment.object).await {
        Ok(ObjectVerificationOutcome::Verified) => {}
        Ok(ObjectVerificationOutcome::Absent) => return Err(EngineError::missing_object()),
        Err(source) => return Err(map_storage_error(source)),
        _ => {
            return Err(EngineError::execution_invariant(
                "object verification returned an unsupported outcome",
            ));
        }
    }
    let expectation = ParquetSegmentExpectation::new(
        segment.object.key().clone(),
        stored_schema,
        segment.row_count,
    )
    .map_err(|_| {
        EngineError::catalog_corrupt("selected segment cannot form a Parquet expectation")
    })?;
    if expectation.segment_id() != segment.segment_id {
        return Err(EngineError::catalog_corrupt(
            "selected segment identity contradicts its exact object key",
        ));
    }
    let reader = ParquetObjectReader::new(
        Arc::clone(objects.store()),
        ObjectPath::from(segment.object.key().as_str()),
    )
    .with_file_size(segment.object.expected_byte_size().get());
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(map_parquet_error)?;
    validate_parquet_segment_metadata(
        builder.schema().as_ref(),
        builder.metadata().as_ref(),
        &expectation,
    )
    .map_err(map_storage_error)?;
    Ok(())
}

fn map_storage_error(source: StorageError) -> EngineError {
    match source.kind() {
        StorageErrorKind::ObjectIntegrityError | StorageErrorKind::ParquetInvalid => {
            EngineError::corrupt_object(source)
        }
        StorageErrorKind::ParquetBuildFailed
        | StorageErrorKind::ObjectStoreUnavailable
        | StorageErrorKind::ObjectUploadFailed
        | StorageErrorKind::ObjectVerificationFailed
        | StorageErrorKind::ObjectDeleteFailed
        | StorageErrorKind::LocalCapacityExhausted => EngineError::execution(source),
        _ => EngineError::execution(source),
    }
}

fn map_parquet_error(source: parquet::errors::ParquetError) -> EngineError {
    let mut cause = source.source();
    while let Some(error) = cause {
        if let Some(object_store_error) = error.downcast_ref::<object_store::Error>() {
            return match object_store_error {
                object_store::Error::NotFound { .. } => EngineError::missing_object(),
                _ => EngineError::execution(source),
            };
        }
        cause = error.source();
    }
    EngineError::corrupt_object(source)
}
