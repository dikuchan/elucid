use std::fs::{File, OpenOptions};
use std::io::{self, BufReader};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array as _, FixedSizeBinaryArray, TimestampMillisecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use elucid_catalog::{Schema, SchemaId, SourceId};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterVersion};

use crate::{
    ManagedObjectKey, ManagedObjectKind, ObjectByteSize, ObjectDescriptor, ObjectDigest,
    ObjectFormatVersion, ObjectMediaType, ObjectOwner, SegmentId, StorageError, StorageModelError,
};

pub const PARQUET_FORMAT_VERSION: u64 = 1;
pub const PARQUET_MAX_ROW_GROUP_ROWS: usize = 8_192;

const EVENT_TIME_ORDINAL: usize = 0;
const EVENT_ID_ORDINAL: usize = 2;
const MILLISECONDS_PER_DAY: i64 = 86_400_000;
const STAGING_NAMESPACE: &str = "parquet";
const SEGMENT_ID_FOOTER_KEY: &str = "elucid.segment_id";
const SOURCE_ID_FOOTER_KEY: &str = "elucid.source_id";
const SCHEMA_ID_FOOTER_KEY: &str = "elucid.schema_id";
const ROW_COUNT_FOOTER_KEY: &str = "elucid.row_count";
const FIELD_IDS_FOOTER_KEY: &str = "elucid.field_ids";
const FORMAT_VERSION_FOOTER_KEY: &str = "elucid.format_version";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ParquetWriteLimit(NonZeroU64);

impl ParquetWriteLimit {
    pub fn new(value: u64) -> Result<Self, StorageModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(StorageModelError::ParquetWriteLimitMustBePositive)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ParquetSegmentExpectation {
    key: ManagedObjectKey,
    segment_id: SegmentId,
    source_id: SourceId,
    schema_id: SchemaId,
    row_count: u64,
    arrow_schema: SchemaRef,
    field_ids: Arc<str>,
}

impl ParquetSegmentExpectation {
    pub fn new(
        key: ManagedObjectKey,
        stored_schema: &Schema,
        row_count: u64,
    ) -> Result<Self, StorageModelError> {
        let segment_id = parquet_segment_id(&key)?;
        if row_count == 0 {
            return Err(StorageModelError::ParquetRowCountMustBePositive);
        }
        i64::try_from(row_count).map_err(|_| StorageModelError::ParquetRowCountOutOfRange)?;
        let field_ids = stored_schema
            .fields()
            .iter()
            .map(|field| field.id().to_string())
            .collect::<Vec<_>>()
            .join(",");
        Ok(Self {
            key,
            segment_id,
            source_id: stored_schema.source_id(),
            schema_id: stored_schema.id(),
            row_count,
            arrow_schema: Arc::new(stored_schema.arrow_schema().clone()),
            field_ids: field_ids.into(),
        })
    }

    #[must_use]
    pub const fn key(&self) -> &ManagedObjectKey {
        &self.key
    }

    #[must_use]
    pub const fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    #[must_use]
    pub fn staging_path(&self, root: impl AsRef<Path>) -> PathBuf {
        staging_path(root.as_ref(), self)
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct ParquetSegmentInput {
    expectation: ParquetSegmentExpectation,
    batch: RecordBatch,
}

impl ParquetSegmentInput {
    pub fn new(
        key: ManagedObjectKey,
        stored_schema: &Schema,
        batch: RecordBatch,
    ) -> Result<Self, StorageModelError> {
        let row_count = u64::try_from(batch.num_rows())
            .map_err(|_| StorageModelError::ParquetRowCountOutOfRange)?;
        let expectation = ParquetSegmentExpectation::new(key, stored_schema, row_count)?;
        if batch.schema().fields() != expectation.arrow_schema.fields() {
            return Err(StorageModelError::ParquetSchemaMismatch);
        }
        validate_non_null_columns(&batch)?;
        validate_segment_order_and_day(&batch)?;
        Ok(Self { expectation, batch })
    }

    #[must_use]
    pub const fn expectation(&self) -> &ParquetSegmentExpectation {
        &self.expectation
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct StagedParquetSegment {
    path: PathBuf,
    expectation: ParquetSegmentExpectation,
    object_descriptor: ObjectDescriptor,
    row_group_count: usize,
}

impl StagedParquetSegment {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub const fn expectation(&self) -> &ParquetSegmentExpectation {
        &self.expectation
    }

    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.expectation.row_count()
    }

    #[must_use]
    pub const fn row_group_count(&self) -> usize {
        self.row_group_count
    }

    #[must_use]
    pub const fn object_descriptor(&self) -> &ObjectDescriptor {
        &self.object_descriptor
    }
}

/// Writes a create-only local Parquet file within the supplied exact byte limit.
///
/// The returned artifact has been closed, reopened, validated, and hashed from disk.
///
/// # Errors
///
/// Returns a typed local-capacity, Parquet-build, or Parquet-validation failure. An incomplete or
/// invalid file created by this call is removed before the error is returned.
pub async fn write_parquet_segment(
    staging_root: impl AsRef<Path>,
    input: ParquetSegmentInput,
    limit: ParquetWriteLimit,
) -> Result<StagedParquetSegment, StorageError> {
    let staging_root = staging_root.as_ref().to_owned();
    tokio::task::spawn_blocking(move || write_parquet_segment_blocking(&staging_root, input, limit))
        .await
        .map_err(StorageError::parquet_build_task)?
}

/// Reopens and validates a local Parquet segment and hashes its exact bytes.
///
/// # Errors
///
/// Returns `PARQUET_INVALID` when the local bytes, schema, footer identities, row count, row-group
/// bounds, or compression contradict the expected immutable segment.
pub async fn validate_parquet_segment(
    path: impl AsRef<Path>,
    expectation: ParquetSegmentExpectation,
) -> Result<StagedParquetSegment, StorageError> {
    let path = path.as_ref().to_owned();
    tokio::task::spawn_blocking(move || validate_parquet_segment_blocking(path, expectation))
        .await
        .map_err(StorageError::parquet_invalid_task)?
}

fn write_parquet_segment_blocking(
    staging_root: &Path,
    input: ParquetSegmentInput,
    limit: ParquetWriteLimit,
) -> Result<StagedParquetSegment, StorageError> {
    let path = staging_path(staging_root, &input.expectation);
    let parent = path.parent().ok_or_else(|| {
        StorageError::parquet_build_invariant("Parquet staging path has no parent directory")
    })?;
    std::fs::create_dir_all(parent).map_err(StorageError::parquet_build_io)?;
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(StorageError::parquet_build_io)?;
    let exhausted = Arc::new(AtomicBool::new(false));
    let writer = LimitedWriter::new(file, limit, Arc::clone(&exhausted));
    let result = write_batch(writer, &input);
    if let Err(source) = result {
        let error = if exhausted.load(Ordering::Relaxed) {
            StorageError::capacity("Parquet file exceeds the caller's local write limit")
        } else {
            StorageError::parquet_build(source)
        };
        return Err(remove_incomplete(path, error));
    }

    match validate_parquet_segment_blocking(path.clone(), input.expectation) {
        Ok(staged) => Ok(staged),
        Err(error) => Err(remove_incomplete(path, error)),
    }
}

fn write_batch(
    writer: LimitedWriter,
    input: &ParquetSegmentInput,
) -> Result<(), parquet::errors::ParquetError> {
    let mut parquet = ArrowWriter::try_new(
        writer,
        Arc::clone(&input.expectation.arrow_schema),
        Some(writer_properties(&input.expectation)),
    )?;
    parquet.write(&input.batch)?;
    parquet.close()?;
    Ok(())
}

fn writer_properties(expectation: &ParquetSegmentExpectation) -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_size(PARQUET_MAX_ROW_GROUP_ROWS)
        .set_compression(Compression::ZSTD(Default::default()))
        .set_created_by(format!("elucid {}", env!("CARGO_PKG_VERSION")))
        .set_key_value_metadata(Some(footer_metadata(expectation)))
        .build()
}

fn footer_metadata(expectation: &ParquetSegmentExpectation) -> Vec<KeyValue> {
    vec![
        footer_entry(SEGMENT_ID_FOOTER_KEY, expectation.segment_id.to_string()),
        footer_entry(SOURCE_ID_FOOTER_KEY, expectation.source_id.to_string()),
        footer_entry(SCHEMA_ID_FOOTER_KEY, expectation.schema_id.to_string()),
        footer_entry(ROW_COUNT_FOOTER_KEY, expectation.row_count.to_string()),
        footer_entry(FIELD_IDS_FOOTER_KEY, expectation.field_ids.to_string()),
        footer_entry(
            FORMAT_VERSION_FOOTER_KEY,
            PARQUET_FORMAT_VERSION.to_string(),
        ),
    ]
}

fn footer_entry(key: &str, value: String) -> KeyValue {
    KeyValue::new(key.to_owned(), value)
}

fn validate_parquet_segment_blocking(
    path: PathBuf,
    expectation: ParquetSegmentExpectation,
) -> Result<StagedParquetSegment, StorageError> {
    let file = File::open(&path).map_err(StorageError::parquet_invalid_io)?;
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(StorageError::parquet_invalid)?;
    if reader.schema().fields() != expectation.arrow_schema.fields() {
        return Err(StorageError::parquet_invalid_invariant(
            "Parquet Arrow schema does not match the stored schema",
        ));
    }
    let metadata = reader.metadata();
    let expected_rows = i64::try_from(expectation.row_count).map_err(|_| {
        StorageError::parquet_invalid_invariant("expected Parquet row count is out of range")
    })?;
    if metadata.file_metadata().num_rows() != expected_rows {
        return Err(StorageError::parquet_invalid_invariant(
            "Parquet footer row count does not match the segment",
        ));
    }
    validate_footer(metadata.file_metadata().key_value_metadata(), &expectation)?;
    validate_row_groups(metadata.row_groups(), expected_rows)?;
    let row_group_count = metadata.num_row_groups();
    drop(reader);

    let (byte_size, digest) = hash_exact_file(&path)?;
    let format_version = ObjectFormatVersion::new(PARQUET_FORMAT_VERSION).map_err(|_| {
        StorageError::parquet_invalid_invariant("Parquet format version is not positive")
    })?;
    let object_descriptor = ObjectDescriptor::new(
        expectation.key.clone(),
        byte_size,
        digest,
        ObjectMediaType::ParquetData,
        format_version,
    )
    .map_err(|_| {
        StorageError::parquet_invalid_invariant(
            "validated Parquet key does not match its media type",
        )
    })?;
    Ok(StagedParquetSegment {
        path,
        expectation,
        object_descriptor,
        row_group_count,
    })
}

fn validate_footer(
    metadata: Option<&Vec<KeyValue>>,
    expectation: &ParquetSegmentExpectation,
) -> Result<(), StorageError> {
    let metadata = metadata.ok_or_else(|| {
        StorageError::parquet_invalid_invariant("Parquet footer has no key-value metadata")
    })?;
    let expected = [
        (SEGMENT_ID_FOOTER_KEY, expectation.segment_id.to_string()),
        (SOURCE_ID_FOOTER_KEY, expectation.source_id.to_string()),
        (SCHEMA_ID_FOOTER_KEY, expectation.schema_id.to_string()),
        (ROW_COUNT_FOOTER_KEY, expectation.row_count.to_string()),
        (FIELD_IDS_FOOTER_KEY, expectation.field_ids.to_string()),
        (
            FORMAT_VERSION_FOOTER_KEY,
            PARQUET_FORMAT_VERSION.to_string(),
        ),
    ];
    for (key, value) in expected {
        let mut matches = metadata
            .iter()
            .filter(|entry| entry.key == key)
            .map(|entry| entry.value.as_deref());
        if matches.next() != Some(Some(value.as_str())) || matches.next().is_some() {
            return Err(StorageError::parquet_invalid_invariant(
                "Parquet footer identity metadata is missing, duplicated, or mismatched",
            ));
        }
    }
    Ok(())
}

fn validate_row_groups(
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
    expected_rows: i64,
) -> Result<(), StorageError> {
    if row_groups.is_empty() {
        return Err(StorageError::parquet_invalid_invariant(
            "Parquet segment has no row groups",
        ));
    }
    let maximum_rows = i64::try_from(PARQUET_MAX_ROW_GROUP_ROWS).map_err(|_| {
        StorageError::parquet_invalid_invariant("Parquet row-group limit is out of range")
    })?;
    let mut total_rows = 0_i64;
    for row_group in row_groups {
        if row_group.num_rows() <= 0 || row_group.num_rows() > maximum_rows {
            return Err(StorageError::parquet_invalid_invariant(
                "Parquet row group exceeds the implementation row bound",
            ));
        }
        if row_group
            .columns()
            .iter()
            .any(|column| column.compression() == Compression::UNCOMPRESSED)
        {
            return Err(StorageError::parquet_invalid_invariant(
                "Parquet column is not compressed",
            ));
        }
        total_rows = total_rows
            .checked_add(row_group.num_rows())
            .ok_or_else(|| {
                StorageError::parquet_invalid_invariant("Parquet row-group count overflow")
            })?;
    }
    if total_rows != expected_rows {
        return Err(StorageError::parquet_invalid_invariant(
            "Parquet row-group counts do not match the segment",
        ));
    }
    Ok(())
}

fn hash_exact_file(path: &Path) -> Result<(ObjectByteSize, ObjectDigest), StorageError> {
    let file = File::open(path).map_err(StorageError::parquet_invalid_io)?;
    let expected_size = file
        .metadata()
        .map_err(StorageError::parquet_invalid_io)?
        .len();
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    let hashed_size =
        io::copy(&mut reader, &mut hasher).map_err(StorageError::parquet_invalid_io)?;
    if hashed_size != expected_size {
        return Err(StorageError::parquet_invalid_invariant(
            "Parquet file changed while it was being hashed",
        ));
    }
    Ok((
        ObjectByteSize::new(expected_size),
        ObjectDigest::new(*hasher.finalize().as_bytes()),
    ))
}

fn validate_non_null_columns(batch: &RecordBatch) -> Result<(), StorageModelError> {
    for (ordinal, (field, column)) in batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .enumerate()
    {
        if !field.is_nullable() && column.null_count() != 0 {
            return Err(StorageModelError::ParquetNonNullFieldContainsNull {
                field_ordinal: ordinal,
            });
        }
    }
    Ok(())
}

fn validate_segment_order_and_day(batch: &RecordBatch) -> Result<(), StorageModelError> {
    let event_times = batch
        .column(EVENT_TIME_ORDINAL)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or(StorageModelError::ParquetSystemColumnsInvalid)?;
    let event_ids = batch
        .column(EVENT_ID_ORDINAL)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or(StorageModelError::ParquetSystemColumnsInvalid)?;
    if event_times.len() != event_ids.len() || event_times.is_empty() {
        return Err(StorageModelError::ParquetSystemColumnsInvalid);
    }
    let event_day = event_times.value(0).div_euclid(MILLISECONDS_PER_DAY);
    for index in 0..event_times.len() {
        if event_times.value(index).div_euclid(MILLISECONDS_PER_DAY) != event_day {
            return Err(StorageModelError::ParquetRowsSpanEventDays);
        }
        if index > 0 {
            let previous_time = event_times.value(index - 1);
            let current_time = event_times.value(index);
            if previous_time > current_time
                || (previous_time == current_time
                    && event_ids.value(index - 1) > event_ids.value(index))
            {
                return Err(StorageModelError::ParquetRowsNotOrdered);
            }
        }
    }
    Ok(())
}

fn parquet_segment_id(key: &ManagedObjectKey) -> Result<SegmentId, StorageModelError> {
    if key.kind() != ManagedObjectKind::ParquetData {
        return Err(StorageModelError::ParquetManagedKeyRequired);
    }
    match key.owner() {
        ObjectOwner::Segment(segment_id) => Ok(segment_id),
        _ => Err(StorageModelError::ParquetManagedKeyRequired),
    }
}

fn staging_path(root: &Path, expectation: &ParquetSegmentExpectation) -> PathBuf {
    root.join(STAGING_NAMESPACE)
        .join(expectation.segment_id.to_string())
        .join(format!("{}.parquet", expectation.key.object_id()))
}

fn remove_incomplete(path: PathBuf, error: StorageError) -> StorageError {
    match std::fs::remove_file(path) {
        Ok(()) => error,
        Err(source) if source.kind() == io::ErrorKind::NotFound => error,
        Err(cleanup) => error.with_cleanup_failure(cleanup),
    }
}

#[derive(Debug)]
struct LimitedWriter {
    file: File,
    limit: u64,
    written: u64,
    exhausted: Arc<AtomicBool>,
}

impl LimitedWriter {
    fn new(file: File, limit: ParquetWriteLimit, exhausted: Arc<AtomicBool>) -> Self {
        Self {
            file,
            limit: limit.get(),
            written: 0,
            exhausted,
        }
    }
}

impl io::Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let requested = u64::try_from(bytes.len())
            .map_err(|_| io::Error::other("Parquet write size cannot be represented"))?;
        let projected = self
            .written
            .checked_add(requested)
            .ok_or_else(|| io::Error::other("Parquet write size overflow"))?;
        if projected > self.limit {
            self.exhausted.store(true, Ordering::Relaxed);
            return Err(io::Error::other("Parquet local write limit exhausted"));
        }
        let written = io::Write::write(&mut self.file, bytes)?;
        self.written = self
            .written
            .checked_add(u64::try_from(written).map_err(|_| {
                io::Error::other("Parquet written byte count cannot be represented")
            })?)
            .ok_or_else(|| io::Error::other("Parquet written byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        io::Write::flush(&mut self.file)
    }
}
