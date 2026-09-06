use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array as _, FixedSizeBinaryArray, MutableArrayData, TimestampMillisecondArray, make_array,
};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};
use elucid_catalog::Schema;
use elucid_core::EventId;
use elucid_metastore::{
    CompactionInputSegment, CompactionOutputRegistration, CompactionRunClaim, CompactionRunId,
    CompactionStore, PublicationStore,
};
use elucid_storage::{
    ImmutableObjectStore, ManagedObjectKey, ManagedRoot, ObjectVerificationOutcome,
    PARQUET_FORMAT_VERSION, ParquetSegmentExpectation, ParquetSegmentInput, ParquetWriteLimit,
    RowCount, SegmentDescriptor, SegmentId, SegmentTimes, StagedParquetSegment, StoredObjectId,
    TransferLimit, UncompressedByteSize, read_staged_object, validate_parquet_segment_metadata,
    write_parquet_segment,
};
use futures::StreamExt as _;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use uuid::Uuid;

use crate::{CompactionBuildLimits, CompactionError};

const EVENT_TIME_ORDINAL: usize = 0;
const INGESTION_TIME_ORDINAL: usize = 1;
const EVENT_ID_ORDINAL: usize = 2;

#[derive(Debug)]
pub struct CompactionWorker {
    compaction: CompactionStore,
    publication: PublicationStore,
    object_store: Arc<dyn ObjectStore>,
    objects: ImmutableObjectStore,
    root: ManagedRoot,
    staging_root: PathBuf,
    limits: CompactionBuildLimits,
}

impl CompactionWorker {
    #[must_use]
    pub fn new(
        compaction: CompactionStore,
        publication: PublicationStore,
        object_store: Arc<dyn ObjectStore>,
        root: ManagedRoot,
        staging_root: impl AsRef<Path>,
        limits: CompactionBuildLimits,
    ) -> Self {
        Self {
            compaction,
            publication,
            objects: ImmutableObjectStore::new(Arc::clone(&object_store)),
            object_store,
            root,
            staging_root: staging_root.as_ref().to_owned(),
            limits,
        }
    }

    /// Builds all outputs locally, registers them atomically, and uploads their exact objects.
    ///
    /// The claimed inputs remain active. Atomic visibility replacement belongs to the publication
    /// stage and is deliberately not performed here.
    ///
    /// # Errors
    ///
    /// Returns a stable invalid-input, build-failed, or not-beneficial compaction error. Failed
    /// runs remain durable for the recovery stage to release or abandon.
    pub async fn build_register_and_upload(
        &self,
        claim: &CompactionRunClaim,
    ) -> Result<CompactionRunBuild, CompactionError> {
        let deadline = Deadline::new(self.limits)?;
        let outputs = self.build_outputs(claim, deadline).await?;
        let registrations = outputs
            .iter()
            .map(|output| output.registration.clone())
            .collect::<Vec<_>>();
        let operation = self
            .register_and_upload(claim, &outputs, &registrations, deadline)
            .await;
        if let Err(error) = operation {
            return match cleanup_outputs(&outputs).await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.with_cleanup_failure(cleanup)),
            };
        }
        cleanup_outputs(&outputs).await.map_err(|source| {
            CompactionError::io("removing uploaded compaction staging", source)
        })?;
        let output_parquet_bytes = outputs.iter().try_fold(0_u64, |total, output| {
            total
                .checked_add(output.staged.object_descriptor().expected_byte_size().get())
                .ok_or_else(|| CompactionError::build("compaction output byte total overflowed"))
        })?;
        Ok(CompactionRunBuild {
            run_id: claim.run_id(),
            input_segments: claim.inputs().len(),
            output_segments: outputs.len(),
            rows: claim.input_rows(),
            input_parquet_bytes: claim.input_parquet_bytes(),
            output_parquet_bytes,
        })
    }

    async fn build_outputs(
        &self,
        claim: &CompactionRunClaim,
        deadline: Deadline,
    ) -> Result<Vec<LocalOutput>, CompactionError> {
        validate_claim(claim, self.limits)?;
        let mut outputs = Vec::new();
        let result = self
            .build_outputs_inner(claim, deadline, &mut outputs)
            .await;
        match result {
            Ok(()) => Ok(outputs),
            Err(error) => match cleanup_outputs(&outputs).await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.with_cleanup_failure(cleanup)),
            },
        }
    }

    async fn build_outputs_inner(
        &self,
        claim: &CompactionRunClaim,
        deadline: Deadline,
        outputs: &mut Vec<LocalOutput>,
    ) -> Result<(), CompactionError> {
        let schema = claim.schema();
        let mut cursors = Vec::with_capacity(claim.inputs().len());
        for input in claim.inputs() {
            cursors.push(
                InputCursor::open(
                    Arc::clone(&self.object_store),
                    &self.objects,
                    input.clone(),
                    schema,
                    claim.event_day(),
                    self.limits.reader_batch_rows(),
                    deadline,
                )
                .await?,
            );
        }
        let mut heap = BinaryHeap::with_capacity(cursors.len());
        for (input_index, cursor) in cursors.iter().enumerate() {
            heap.push(Reverse(HeapEntry {
                key: cursor.current_key()?,
                input_index,
            }));
        }

        let mut total_rows = 0_u64;
        let mut total_staging_bytes = 0_u64;
        while !heap.is_empty() {
            deadline.ensure_active()?;
            let mut accumulator = OutputAccumulator::new(
                claim.event_day(),
                self.limits.target_output_rows(),
                self.limits.target_output_uncompressed_bytes(),
            );
            while let Some(Reverse(entry)) = heap.peek().copied() {
                let selected = cursors[entry.input_index].selected_row(entry.input_index)?;
                if selected.key != entry.key {
                    return Err(CompactionError::input(
                        "compaction merge heap differs from its input cursor",
                    ));
                }
                if !accumulator.can_accept(selected.estimated_uncompressed_bytes)? {
                    break;
                }
                heap.pop();
                accumulator.append(selected)?;
                total_rows = total_rows
                    .checked_add(1)
                    .ok_or_else(|| CompactionError::build("compaction row total overflowed"))?;
                if cursors[entry.input_index]
                    .advance(schema, claim.event_day(), deadline)
                    .await?
                {
                    heap.push(Reverse(HeapEntry {
                        key: cursors[entry.input_index].current_key()?,
                        input_index: entry.input_index,
                    }));
                }
                deadline.ensure_active()?;
            }
            let output = self
                .write_output(claim, schema, accumulator, deadline)
                .await?;
            total_staging_bytes = total_staging_bytes
                .checked_add(output.staged.object_descriptor().expected_byte_size().get())
                .ok_or_else(|| {
                    CompactionError::build("compaction staging byte total overflowed")
                })?;
            outputs.push(output);
            if total_staging_bytes > self.limits.maximum_staging_bytes() {
                return Err(CompactionError::build(
                    "compaction outputs exceed their total staging limit",
                ));
            }
            if outputs.len() >= claim.inputs().len() {
                return Err(CompactionError::not_beneficial(
                    "compaction did not reduce its segment count",
                ));
            }
        }
        if total_rows != claim.input_rows() {
            return Err(CompactionError::input(
                "compaction merge row count differs from its claim",
            ));
        }
        if outputs.is_empty() || outputs.len() >= claim.inputs().len() {
            return Err(CompactionError::not_beneficial(
                "compaction did not produce fewer non-empty segments",
            ));
        }
        Ok(())
    }

    async fn write_output(
        &self,
        claim: &CompactionRunClaim,
        schema: &Schema,
        accumulator: OutputAccumulator,
        deadline: Deadline,
    ) -> Result<LocalOutput, CompactionError> {
        let OutputBatch {
            batch,
            times,
            estimated_uncompressed_bytes,
        } = accumulator.finish(schema)?;
        let segment_id = SegmentId::from(Uuid::now_v7());
        let object_id = StoredObjectId::from(Uuid::now_v7());
        let key = ManagedObjectKey::parquet(&self.root, segment_id, object_id);
        let input = ParquetSegmentInput::new(key, schema, batch)
            .map_err(CompactionError::output_storage_model)?;
        let write_limit = ParquetWriteLimit::new(self.limits.maximum_output_parquet_bytes())
            .map_err(CompactionError::output_storage_model)?;
        let staged = deadline
            .run(write_parquet_segment(
                &self.staging_root,
                input,
                write_limit,
            ))
            .await?
            .map_err(CompactionError::output_storage)?;
        let row_count = RowCount::new(staged.row_count())
            .map_err(|_| CompactionError::build("compaction output is empty"))?;
        let uncompressed_bytes = UncompressedByteSize::new(estimated_uncompressed_bytes)
            .map_err(|_| CompactionError::build("compaction output has no estimated bytes"))?;
        let registration = SegmentDescriptor::new(
            segment_id,
            claim.source_id(),
            schema.id(),
            times,
            row_count,
            uncompressed_bytes,
            staged.object_descriptor().clone(),
        )
        .map_err(CompactionError::output_storage_model)
        .and_then(|descriptor| {
            CompactionOutputRegistration::new(claim.run_id(), descriptor, claim.data_expires_at())
                .map_err(CompactionError::metadata_model)
        });
        let registration = match registration {
            Ok(registration) => registration,
            Err(error) => {
                return match tokio::fs::remove_file(staged.path()).await {
                    Ok(()) => Err(error),
                    Err(cleanup) if cleanup.kind() == io::ErrorKind::NotFound => Err(error),
                    Err(cleanup) => Err(error.with_cleanup_failure(cleanup)),
                };
            }
        };
        Ok(LocalOutput {
            registration,
            staged,
        })
    }

    async fn register_and_upload(
        &self,
        claim: &CompactionRunClaim,
        outputs: &[LocalOutput],
        registrations: &[CompactionOutputRegistration],
        deadline: Deadline,
    ) -> Result<(), CompactionError> {
        deadline
            .run(
                self.compaction
                    .register_outputs(claim.run_id(), registrations),
            )
            .await?
            .map_err(CompactionError::metadata)?;
        let transfer_limit = TransferLimit::new(self.limits.maximum_output_parquet_bytes())
            .map_err(CompactionError::output_storage_model)?;
        for output in outputs {
            let descriptor = output.staged.object_descriptor();
            let bytes = deadline
                .run(read_staged_object(
                    output.staged.path(),
                    descriptor,
                    transfer_limit,
                ))
                .await?
                .map_err(CompactionError::staged_read)?;
            deadline
                .run(self.objects.upload(descriptor, bytes, transfer_limit))
                .await?
                .map_err(CompactionError::output_storage)?;
            deadline
                .run(self.publication.record_verified_upload(descriptor))
                .await?
                .map_err(CompactionError::publication)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionRunBuild {
    run_id: CompactionRunId,
    input_segments: usize,
    output_segments: usize,
    rows: u64,
    input_parquet_bytes: u64,
    output_parquet_bytes: u64,
}

impl CompactionRunBuild {
    #[must_use]
    pub const fn run_id(self) -> CompactionRunId {
        self.run_id
    }

    #[must_use]
    pub const fn input_segments(self) -> usize {
        self.input_segments
    }

    #[must_use]
    pub const fn output_segments(self) -> usize {
        self.output_segments
    }

    #[must_use]
    pub const fn rows(self) -> u64 {
        self.rows
    }

    #[must_use]
    pub const fn input_parquet_bytes(self) -> u64 {
        self.input_parquet_bytes
    }

    #[must_use]
    pub const fn output_parquet_bytes(self) -> u64 {
        self.output_parquet_bytes
    }
}

#[derive(Debug)]
struct LocalOutput {
    registration: CompactionOutputRegistration,
    staged: StagedParquetSegment,
}

type InputBatchStream = ParquetRecordBatchStream<ParquetObjectReader>;

#[derive(Debug)]
struct InputCursor {
    input: CompactionInputSegment,
    stream: InputBatchStream,
    batch: Option<RecordBatch>,
    row_index: usize,
    batch_generation: u64,
    rows_loaded: u64,
    rows_emitted: u64,
    minimum_event_time: Option<i64>,
    maximum_event_time: Option<i64>,
    minimum_ingestion_time: Option<i64>,
    maximum_ingestion_time: Option<i64>,
    last_loaded_key: Option<RowKey>,
}

impl InputCursor {
    async fn open(
        object_store: Arc<dyn ObjectStore>,
        objects: &ImmutableObjectStore,
        input: CompactionInputSegment,
        schema: &Schema,
        event_day: NaiveDate,
        reader_batch_rows: usize,
        deadline: Deadline,
    ) -> Result<Self, CompactionError> {
        if input.descriptor().object().format_version().get() != PARQUET_FORMAT_VERSION {
            return Err(CompactionError::input(
                "compaction input has an unsupported Parquet format version",
            ));
        }
        match deadline
            .run(objects.verify(input.descriptor().object()))
            .await?
            .map_err(CompactionError::storage)?
        {
            ObjectVerificationOutcome::Verified => {}
            ObjectVerificationOutcome::Absent => {
                return Err(CompactionError::input(
                    "registered compaction input object is absent",
                ));
            }
            _ => {
                return Err(CompactionError::input(
                    "object verification returned an unsupported outcome",
                ));
            }
        }
        let expectation = ParquetSegmentExpectation::new(
            input.descriptor().object().key().clone(),
            schema,
            input.descriptor().row_count().get(),
        )
        .map_err(CompactionError::input_storage_model)?;
        if expectation.segment_id() != input.descriptor().segment_id() {
            return Err(CompactionError::input(
                "compaction input identity contradicts its object key",
            ));
        }
        let reader = ParquetObjectReader::new(
            object_store,
            ObjectPath::from(input.descriptor().object().key().as_str()),
        )
        .with_file_size(input.descriptor().object().expected_byte_size().get());
        let builder = deadline
            .run(ParquetRecordBatchStreamBuilder::new(reader))
            .await?
            .map_err(CompactionError::parquet_input)?;
        validate_parquet_segment_metadata(
            builder.schema().as_ref(),
            builder.metadata().as_ref(),
            &expectation,
        )
        .map_err(CompactionError::storage)?;
        let stream = builder
            .with_batch_size(reader_batch_rows)
            .build()
            .map_err(CompactionError::parquet_input)?;
        let mut cursor = Self {
            input,
            stream,
            batch: None,
            row_index: 0,
            batch_generation: 0,
            rows_loaded: 0,
            rows_emitted: 0,
            minimum_event_time: None,
            maximum_event_time: None,
            minimum_ingestion_time: None,
            maximum_ingestion_time: None,
            last_loaded_key: None,
        };
        if !cursor.load_next_batch(schema, event_day, deadline).await? {
            return Err(CompactionError::input(
                "compaction input Parquet stream is empty",
            ));
        }
        Ok(cursor)
    }

    fn current_key(&self) -> Result<RowKey, CompactionError> {
        let batch = self
            .batch
            .as_ref()
            .ok_or_else(|| CompactionError::input("compaction input cursor is exhausted"))?;
        row_key(batch, self.row_index)
    }

    fn selected_row(&self, input_index: usize) -> Result<SelectedRow, CompactionError> {
        let batch = self
            .batch
            .as_ref()
            .ok_or_else(|| CompactionError::input("compaction input cursor is exhausted"))?;
        let event_times = event_times(batch)?;
        let ingestion_times = ingestion_times(batch)?;
        let estimated_uncompressed_bytes = self.estimated_row_bytes()?;
        Ok(SelectedRow {
            input_index,
            batch_generation: self.batch_generation,
            batch: batch.clone(),
            row_index: self.row_index,
            key: row_key(batch, self.row_index)?,
            event_time: event_times.value(self.row_index),
            ingestion_time: ingestion_times.value(self.row_index),
            estimated_uncompressed_bytes,
        })
    }

    async fn advance(
        &mut self,
        schema: &Schema,
        event_day: NaiveDate,
        deadline: Deadline,
    ) -> Result<bool, CompactionError> {
        self.rows_emitted = self
            .rows_emitted
            .checked_add(1)
            .ok_or_else(|| CompactionError::input("compaction input row count overflowed"))?;
        self.row_index = self
            .row_index
            .checked_add(1)
            .ok_or_else(|| CompactionError::input("compaction cursor index overflowed"))?;
        let batch_rows = self
            .batch
            .as_ref()
            .ok_or_else(|| CompactionError::input("compaction input cursor is exhausted"))?
            .num_rows();
        if self.row_index < batch_rows {
            return Ok(true);
        }
        self.load_next_batch(schema, event_day, deadline).await
    }

    async fn load_next_batch(
        &mut self,
        schema: &Schema,
        event_day: NaiveDate,
        deadline: Deadline,
    ) -> Result<bool, CompactionError> {
        match deadline.run(self.stream.next()).await? {
            Some(Ok(batch)) => {
                self.validate_batch(&batch, schema, event_day)?;
                self.batch_generation = self.batch_generation.checked_add(1).ok_or_else(|| {
                    CompactionError::input("compaction input batch generation overflowed")
                })?;
                self.batch = Some(batch);
                self.row_index = 0;
                Ok(true)
            }
            Some(Err(source)) => Err(CompactionError::parquet_input(source)),
            None => {
                self.batch = None;
                self.validate_complete()?;
                Ok(false)
            }
        }
    }

    fn validate_batch(
        &mut self,
        batch: &RecordBatch,
        schema: &Schema,
        event_day: NaiveDate,
    ) -> Result<(), CompactionError> {
        if batch.num_rows() == 0 || batch.schema().fields() != schema.arrow_schema().fields() {
            return Err(CompactionError::input(
                "compaction input batch has an invalid schema or row count",
            ));
        }
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if !field.is_nullable() && column.null_count() != 0 {
                return Err(CompactionError::input(
                    "compaction input has nulls in a non-null field",
                ));
            }
        }
        let event_times = event_times(batch)?;
        let ingestion_times = ingestion_times(batch)?;
        let event_ids = event_ids(batch)?;
        if event_times.len() != batch.num_rows()
            || ingestion_times.len() != batch.num_rows()
            || event_ids.len() != batch.num_rows()
        {
            return Err(CompactionError::input(
                "compaction system columns have inconsistent lengths",
            ));
        }
        let (day_start, day_end) = event_day_bounds(event_day)?;
        for index in 0..batch.num_rows() {
            let event_time = event_times.value(index);
            if event_time < day_start || event_time >= day_end {
                return Err(CompactionError::input(
                    "compaction input rows cross their UTC event day",
                ));
            }
            let key = RowKey {
                event_time,
                event_id: event_ids
                    .value(index)
                    .try_into()
                    .map_err(|_| CompactionError::input("event identity is not 16 bytes"))?,
            };
            if self.last_loaded_key.is_some_and(|previous| previous > key) {
                return Err(CompactionError::input(
                    "compaction input rows are not ordered",
                ));
            }
            self.last_loaded_key = Some(key);
            self.minimum_event_time = Some(
                self.minimum_event_time
                    .map_or(event_time, |minimum| minimum.min(event_time)),
            );
            self.maximum_event_time = Some(
                self.maximum_event_time
                    .map_or(event_time, |maximum| maximum.max(event_time)),
            );
            let ingestion_time = ingestion_times.value(index);
            self.minimum_ingestion_time = Some(
                self.minimum_ingestion_time
                    .map_or(ingestion_time, |minimum| minimum.min(ingestion_time)),
            );
            self.maximum_ingestion_time = Some(
                self.maximum_ingestion_time
                    .map_or(ingestion_time, |maximum| maximum.max(ingestion_time)),
            );
        }
        self.rows_loaded = self
            .rows_loaded
            .checked_add(
                u64::try_from(batch.num_rows())
                    .map_err(|_| CompactionError::input("input batch row count overflowed"))?,
            )
            .ok_or_else(|| CompactionError::input("compaction input row count overflowed"))?;
        if self.rows_loaded > self.input.descriptor().row_count().get() {
            return Err(CompactionError::input(
                "compaction input contains more rows than registered",
            ));
        }
        Ok(())
    }

    fn validate_complete(&self) -> Result<(), CompactionError> {
        let times = self.input.descriptor().times();
        if self.rows_loaded != self.input.descriptor().row_count().get()
            || self.rows_emitted != self.input.descriptor().row_count().get()
            || self.minimum_event_time != Some(times.minimum_event_time().timestamp_millis())
            || self.maximum_event_time != Some(times.maximum_event_time().timestamp_millis())
            || self.minimum_ingestion_time
                != Some(times.minimum_ingestion_time().timestamp_millis())
            || self.maximum_ingestion_time
                != Some(times.maximum_ingestion_time().timestamp_millis())
        {
            return Err(CompactionError::input(
                "compaction input rows contradict registered counts or time bounds",
            ));
        }
        Ok(())
    }

    fn estimated_row_bytes(&self) -> Result<u64, CompactionError> {
        if self.rows_emitted >= self.input.descriptor().row_count().get() {
            return Err(CompactionError::input(
                "compaction input row estimate is past the registered count",
            ));
        }
        let total = self
            .input
            .descriptor()
            .uncompressed_bytes()
            .get()
            .max(self.input.descriptor().row_count().get());
        let base = total / self.input.descriptor().row_count().get();
        let remainder = total % self.input.descriptor().row_count().get();
        Ok(base + u64::from(self.rows_emitted < remainder))
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RowKey {
    event_time: i64,
    event_id: EventId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct HeapEntry {
    key: RowKey,
    input_index: usize,
}

#[derive(Debug)]
struct SelectedRow {
    input_index: usize,
    batch_generation: u64,
    batch: RecordBatch,
    row_index: usize,
    key: RowKey,
    event_time: i64,
    ingestion_time: i64,
    estimated_uncompressed_bytes: u64,
}

#[derive(Debug)]
struct OutputAccumulator {
    event_day: NaiveDate,
    target_rows: usize,
    target_uncompressed_bytes: u64,
    source_batches: Vec<RecordBatch>,
    source_indexes: HashMap<(usize, u64), usize>,
    selections: Vec<(usize, usize)>,
    estimated_uncompressed_bytes: u64,
    minimum_event_time: Option<i64>,
    maximum_event_time: Option<i64>,
    minimum_ingestion_time: Option<i64>,
    maximum_ingestion_time: Option<i64>,
}

impl OutputAccumulator {
    fn new(event_day: NaiveDate, target_rows: usize, target_uncompressed_bytes: u64) -> Self {
        Self {
            event_day,
            target_rows,
            target_uncompressed_bytes,
            source_batches: Vec::new(),
            source_indexes: HashMap::new(),
            selections: Vec::with_capacity(target_rows),
            estimated_uncompressed_bytes: 0,
            minimum_event_time: None,
            maximum_event_time: None,
            minimum_ingestion_time: None,
            maximum_ingestion_time: None,
        }
    }

    fn can_accept(&self, estimated_bytes: u64) -> Result<bool, CompactionError> {
        if estimated_bytes == 0 || estimated_bytes > self.target_uncompressed_bytes {
            return Err(CompactionError::input(
                "one compaction row exceeds the output byte target",
            ));
        }
        if self.selections.is_empty() {
            return Ok(true);
        }
        let projected = self
            .estimated_uncompressed_bytes
            .checked_add(estimated_bytes)
            .ok_or_else(|| CompactionError::build("compaction output byte estimate overflowed"))?;
        Ok(self.selections.len() < self.target_rows && projected <= self.target_uncompressed_bytes)
    }

    fn append(&mut self, row: SelectedRow) -> Result<(), CompactionError> {
        let source_index = match self
            .source_indexes
            .get(&(row.input_index, row.batch_generation))
            .copied()
        {
            Some(index) => index,
            None => {
                let index = self.source_batches.len();
                self.source_batches.push(row.batch);
                self.source_indexes
                    .insert((row.input_index, row.batch_generation), index);
                index
            }
        };
        self.selections.push((source_index, row.row_index));
        self.estimated_uncompressed_bytes = self
            .estimated_uncompressed_bytes
            .checked_add(row.estimated_uncompressed_bytes)
            .ok_or_else(|| CompactionError::build("compaction output byte estimate overflowed"))?;
        self.minimum_event_time = Some(
            self.minimum_event_time
                .map_or(row.event_time, |minimum| minimum.min(row.event_time)),
        );
        self.maximum_event_time = Some(
            self.maximum_event_time
                .map_or(row.event_time, |maximum| maximum.max(row.event_time)),
        );
        self.minimum_ingestion_time = Some(
            self.minimum_ingestion_time
                .map_or(row.ingestion_time, |minimum| {
                    minimum.min(row.ingestion_time)
                }),
        );
        self.maximum_ingestion_time = Some(
            self.maximum_ingestion_time
                .map_or(row.ingestion_time, |maximum| {
                    maximum.max(row.ingestion_time)
                }),
        );
        Ok(())
    }

    fn finish(self, schema: &Schema) -> Result<OutputBatch, CompactionError> {
        if self.selections.is_empty() || self.source_batches.is_empty() {
            return Err(CompactionError::build(
                "compaction attempted to materialize an empty output",
            ));
        }
        let arrow_schema = Arc::new(schema.arrow_schema().clone());
        let mut columns = Vec::with_capacity(arrow_schema.fields().len());
        for column_index in 0..arrow_schema.fields().len() {
            let source_data = self
                .source_batches
                .iter()
                .map(|batch| batch.column(column_index).to_data())
                .collect::<Vec<_>>();
            let source_refs = source_data.iter().collect::<Vec<_>>();
            let mut output = MutableArrayData::new(source_refs, false, self.selections.len());
            for &(source_index, row_index) in &self.selections {
                let row_end = row_index
                    .checked_add(1)
                    .ok_or_else(|| CompactionError::build("compaction row selection overflowed"))?;
                output.extend(source_index, row_index, row_end);
            }
            columns.push(make_array(output.freeze()));
        }
        let batch = RecordBatch::try_new(arrow_schema, columns).map_err(CompactionError::arrow)?;
        let times = SegmentTimes::new(
            self.event_day,
            timestamp(self.minimum_event_time)?,
            timestamp(self.maximum_event_time)?,
            timestamp(self.minimum_ingestion_time)?,
            timestamp(self.maximum_ingestion_time)?,
        )
        .map_err(|_| CompactionError::build("compaction output time bounds are invalid"))?;
        Ok(OutputBatch {
            batch,
            times,
            estimated_uncompressed_bytes: self.estimated_uncompressed_bytes,
        })
    }
}

#[derive(Debug)]
struct OutputBatch {
    batch: RecordBatch,
    times: SegmentTimes,
    estimated_uncompressed_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
struct Deadline(Instant);

impl Deadline {
    fn new(limits: CompactionBuildLimits) -> Result<Self, CompactionError> {
        Instant::now()
            .checked_add(limits.maximum_duration())
            .map(Self)
            .ok_or_else(|| CompactionError::build("compaction duration deadline overflowed"))
    }

    fn ensure_active(self) -> Result<(), CompactionError> {
        if Instant::now() < self.0 {
            Ok(())
        } else {
            Err(CompactionError::timeout())
        }
    }

    async fn run<F>(self, future: F) -> Result<F::Output, CompactionError>
    where
        F: Future,
    {
        tokio::time::timeout_at(tokio::time::Instant::from_std(self.0), future)
            .await
            .map_err(|_| CompactionError::timeout())
    }
}

fn validate_claim(
    claim: &CompactionRunClaim,
    limits: CompactionBuildLimits,
) -> Result<(), CompactionError> {
    if claim.inputs().len() < 2 || claim.inputs().len() > limits.maximum_input_segments() {
        return Err(CompactionError::input(
            "compaction claim has an invalid input segment count",
        ));
    }
    if claim.schema().source_id() != claim.source_id()
        || claim.inputs().iter().any(|input| {
            input.descriptor().times().event_day() != claim.event_day()
                || input.descriptor().object().key().owner()
                    != elucid_storage::ObjectOwner::Segment(input.descriptor().segment_id())
        })
    {
        return Err(CompactionError::input(
            "compaction claim is not homogeneous or has invalid object ownership",
        ));
    }
    let input_segments = u64::try_from(claim.inputs().len())
        .map_err(|_| CompactionError::input("compaction input count overflowed"))?;
    let recomputed_rows = claim.inputs().iter().try_fold(0_u64, |total, input| {
        total
            .checked_add(input.descriptor().row_count().get())
            .ok_or_else(|| CompactionError::input("compaction input row total overflowed"))
    })?;
    let recomputed_parquet_bytes = claim.inputs().iter().try_fold(0_u64, |total, input| {
        total
            .checked_add(input.descriptor().object().expected_byte_size().get())
            .ok_or_else(|| CompactionError::input("compaction input byte total overflowed"))
    })?;
    let recomputed_uncompressed_bytes = claim.inputs().iter().try_fold(0_u64, |total, input| {
        total
            .checked_add(input.descriptor().uncompressed_bytes().get())
            .ok_or_else(|| CompactionError::input("compaction input uncompressed total overflowed"))
    })?;
    let maximum_deadline = claim
        .inputs()
        .iter()
        .map(CompactionInputSegment::data_expires_at)
        .max()
        .ok_or_else(|| CompactionError::input("compaction claim has no retention deadline"))?;
    let target_output_rows = u64::try_from(limits.target_output_rows())
        .map_err(|_| CompactionError::build("compaction output row target overflowed"))?;
    if claim.inputs().iter().any(|input| {
        input.descriptor().row_count().get() >= target_output_rows
            || input.descriptor().uncompressed_bytes().get()
                >= limits.target_output_uncompressed_bytes()
    }) {
        return Err(CompactionError::input(
            "compaction claim contains a segment at or above the output target",
        ));
    }
    let expected_outputs =
        ceiling_dividend(recomputed_rows, target_output_rows).max(ceiling_dividend(
            recomputed_uncompressed_bytes,
            limits.target_output_uncompressed_bytes(),
        ));
    if input_segments == 0
        || recomputed_rows != claim.input_rows()
        || recomputed_parquet_bytes != claim.input_parquet_bytes()
        || recomputed_uncompressed_bytes != claim.input_uncompressed_bytes()
        || recomputed_rows > limits.maximum_input_rows()
        || recomputed_parquet_bytes > limits.maximum_input_parquet_bytes()
        || recomputed_uncompressed_bytes > limits.maximum_input_uncompressed_bytes()
        || maximum_deadline != claim.data_expires_at()
    {
        return Err(CompactionError::input(
            "compaction claim totals exceed limits or contradict its inputs",
        ));
    }
    if expected_outputs >= input_segments {
        return Err(CompactionError::not_beneficial(
            "compaction claim cannot produce fewer target-sized outputs",
        ));
    }
    Ok(())
}

const fn ceiling_dividend(value: u64, divisor: u64) -> u64 {
    value / divisor + if value.is_multiple_of(divisor) { 0 } else { 1 }
}

fn event_times(batch: &RecordBatch) -> Result<&TimestampMillisecondArray, CompactionError> {
    batch
        .column(EVENT_TIME_ORDINAL)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| CompactionError::input("compaction event-time column has an invalid type"))
}

fn ingestion_times(batch: &RecordBatch) -> Result<&TimestampMillisecondArray, CompactionError> {
    batch
        .column(INGESTION_TIME_ORDINAL)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| {
            CompactionError::input("compaction ingestion-time column has an invalid type")
        })
}

fn event_ids(batch: &RecordBatch) -> Result<&FixedSizeBinaryArray, CompactionError> {
    batch
        .column(EVENT_ID_ORDINAL)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| CompactionError::input("compaction event-id column has an invalid type"))
}

fn row_key(batch: &RecordBatch, index: usize) -> Result<RowKey, CompactionError> {
    let event_times = event_times(batch)?;
    let event_ids = event_ids(batch)?;
    if index >= batch.num_rows() {
        return Err(CompactionError::input(
            "compaction input cursor is outside its batch",
        ));
    }
    Ok(RowKey {
        event_time: event_times.value(index),
        event_id: event_ids
            .value(index)
            .try_into()
            .map_err(|_| CompactionError::input("event identity is not 16 bytes"))?,
    })
}

fn event_day_bounds(event_day: NaiveDate) -> Result<(i64, i64), CompactionError> {
    let start = event_day
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| CompactionError::input("compaction event day is invalid"))?
        .and_utc()
        .timestamp_millis();
    let end = start
        .checked_add(86_400_000)
        .ok_or_else(|| CompactionError::input("compaction event day overflows UTC"))?;
    Ok((start, end))
}

fn timestamp(value: Option<i64>) -> Result<DateTime<Utc>, CompactionError> {
    value
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .ok_or_else(|| CompactionError::build("compaction output timestamp is invalid"))
}

async fn cleanup_outputs(outputs: &[LocalOutput]) -> Result<(), io::Error> {
    for output in outputs {
        match tokio::fs::remove_file(output.staged.path()).await {
            Ok(()) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(source),
        }
    }
    Ok(())
}
