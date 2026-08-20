use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    Array as _, ArrayRef, BooleanArray, FixedSizeBinaryArray, StringArray,
    TimestampMillisecondArray,
};
use arrow::record_batch::RecordBatch;
use elucid_catalog::{
    DeclarationDigest, DefinitionDigests, FieldId, MaterializedDigest, Nullability, Schema,
    SchemaId, SchemaVersion, SourceId, UserField, UserFieldName, UserLogicalType,
};
use elucid_storage::{
    ManagedObjectKey, ManagedRoot, ObjectDigest, PARQUET_FORMAT_VERSION,
    PARQUET_MAX_ROW_GROUP_ROWS, ParquetSegmentExpectation, ParquetSegmentInput, ParquetWriteLimit,
    SegmentId, StorageErrorCode, StoredObjectId, validate_parquet_segment, write_parquet_segment,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn parquet_segment_round_trip_preserves_identity_schema_rows_and_exact_bytes() {
    let schema = stored_schema();
    let key = parquet_key(10, 11);
    let row_count = PARQUET_MAX_ROW_GROUP_ROWS + 1;
    let batch = event_batch(&schema, row_count);
    let input = ParquetSegmentInput::new(key.clone(), &schema, batch).expect("segment input");
    let staging = tempdir().expect("staging directory");

    let staged = write_parquet_segment(
        staging.path(),
        input,
        ParquetWriteLimit::new(16 * 1024 * 1024).expect("write limit"),
    )
    .await
    .expect("write and validate Parquet segment");

    assert_eq!(
        staged.row_count(),
        u64::try_from(row_count).expect("row count")
    );
    assert!(staged.row_group_count() >= 2);
    assert_eq!(staged.object_descriptor().key(), &key);
    assert_eq!(
        staged.object_descriptor().format_version().get(),
        PARQUET_FORMAT_VERSION
    );
    let exact_bytes = std::fs::read(staged.path()).expect("read exact staged bytes");
    assert_eq!(
        staged.object_descriptor().expected_byte_size().get(),
        u64::try_from(exact_bytes.len()).expect("file length")
    );
    assert_eq!(
        staged.object_descriptor().digest(),
        ObjectDigest::calculate(&exact_bytes)
    );

    let reopened = ParquetRecordBatchReaderBuilder::try_new(
        File::open(staged.path()).expect("reopen staged Parquet file"),
    )
    .expect("read Parquet metadata");
    assert_eq!(reopened.schema().fields(), schema.arrow_schema().fields());
    let metadata = reopened.metadata();
    assert_eq!(
        metadata.file_metadata().num_rows(),
        i64::try_from(row_count).expect("row count")
    );
    assert_eq!(metadata.num_row_groups(), staged.row_group_count());
    assert!(metadata.row_groups().iter().all(|group| {
        group.num_rows() > 0
            && group.num_rows()
                <= i64::try_from(PARQUET_MAX_ROW_GROUP_ROWS).expect("row-group limit")
            && group
                .columns()
                .iter()
                .all(|column| column.compression() != Compression::UNCOMPRESSED)
    }));
    let footer = metadata
        .file_metadata()
        .key_value_metadata()
        .expect("Elucid footer metadata");
    assert_footer_value(footer, "elucid.segment_id", segment_id(10).to_string());
    assert_footer_value(footer, "elucid.source_id", schema.source_id().to_string());
    assert_footer_value(footer, "elucid.schema_id", schema.id().to_string());
    assert_footer_value(footer, "elucid.row_count", row_count.to_string());
    assert_footer_value(
        footer,
        "elucid.format_version",
        PARQUET_FORMAT_VERSION.to_string(),
    );
    assert_footer_value(
        footer,
        "elucid.field_ids",
        schema
            .fields()
            .iter()
            .map(|field| field.id().to_string())
            .collect::<Vec<_>>()
            .join(","),
    );

    let mut reader = reopened.build().expect("build Parquet reader");
    let first = reader
        .next()
        .expect("first Arrow batch")
        .expect("read first Arrow batch");
    let event_time = first
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("event-time column");
    let message = first
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("message column");
    let remainder = first
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("remainder column");
    assert_eq!(event_time.value(0), 1_750_000_000_000);
    assert_eq!(message.value(0), "event-0");
    assert!(remainder.is_null(0));

    let validated = validate_parquet_segment(staged.path(), staged.expectation().clone())
        .await
        .expect("validate reopened segment");
    assert_eq!(validated.object_descriptor(), staged.object_descriptor());

    let wrong_identity = ParquetSegmentExpectation::new(
        parquet_key(12, 13),
        &schema,
        u64::try_from(row_count).expect("row count"),
    )
    .expect("different expected identity");
    let error = validate_parquet_segment(staged.path(), wrong_identity)
        .await
        .expect_err("footer identity mismatch must reject the complete segment");
    assert_eq!(error.code(), StorageErrorCode::ParquetInvalid);
}

#[tokio::test]
async fn parquet_segment_write_stops_at_the_exact_local_limit_without_leaving_a_file() {
    let schema = stored_schema();
    let staging = tempdir().expect("staging directory");
    let input = ParquetSegmentInput::new(parquet_key(20, 21), &schema, event_batch(&schema, 1))
        .expect("segment input");

    let error = write_parquet_segment(
        staging.path(),
        input,
        ParquetWriteLimit::new(1).expect("write limit"),
    )
    .await
    .expect_err("one byte cannot contain a Parquet segment");

    assert_eq!(error.code(), StorageErrorCode::LocalCapacityExhausted);
    assert_eq!(regular_file_count(staging.path()), 0);
}

fn event_batch(schema: &Schema, row_count: usize) -> RecordBatch {
    let event_times = (0..row_count)
        .map(|index| 1_750_000_000_000 + i64::try_from(index).expect("event index"))
        .collect::<Vec<_>>();
    let ingestion_times = vec![1_750_000_100_000; row_count];
    let event_ids = (0..row_count)
        .map(|index| u128::try_from(index).expect("event index").to_be_bytes())
        .collect::<Vec<_>>();
    let messages = (0..row_count)
        .map(|index| format!("event-{index}"))
        .collect::<Vec<_>>();
    let flags = (0..row_count)
        .map(|index| (index % 2 == 0).then_some(true))
        .collect::<Vec<_>>();
    let remainders = (0..row_count)
        .map(|index| (index == row_count.saturating_sub(1)).then_some("{\"tail\":true}"))
        .collect::<Vec<_>>();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampMillisecondArray::from(event_times).with_timezone("UTC")),
        Arc::new(TimestampMillisecondArray::from(ingestion_times).with_timezone("UTC")),
        Arc::new(
            FixedSizeBinaryArray::try_from_iter(event_ids.iter())
                .expect("fixed-size event identities"),
        ),
        Arc::new(StringArray::from(messages)),
        Arc::new(BooleanArray::from(flags)),
        Arc::new(StringArray::from(remainders)),
    ];
    RecordBatch::try_new(Arc::new(schema.arrow_schema().clone()), arrays).expect("record batch")
}

fn stored_schema() -> Schema {
    Schema::new(
        schema_id(2),
        source_id(1),
        SchemaVersion::new(1).expect("schema version"),
        DefinitionDigests::new(
            DeclarationDigest::new([1; 32]),
            MaterializedDigest::new([2; 32]),
        ),
        vec![
            UserField::new(
                field_id(3),
                UserFieldName::try_from("message").expect("field name"),
                UserLogicalType::Utf8,
                Nullability::NonNull,
            )
            .expect("message field"),
            UserField::new(
                field_id(4),
                UserFieldName::try_from("flag").expect("field name"),
                UserLogicalType::Bool,
                Nullability::Nullable,
            )
            .expect("flag field"),
        ],
    )
    .expect("stored schema")
}

fn parquet_key(segment: u128, object: u128) -> ManagedObjectKey {
    ManagedObjectKey::parquet(
        &ManagedRoot::parse("test").expect("managed root"),
        segment_id(segment),
        StoredObjectId::from(uuid(object)),
    )
}

fn segment_id(value: u128) -> SegmentId {
    SegmentId::from(uuid(value))
}

fn source_id(value: u128) -> SourceId {
    SourceId::try_from(uuid(value)).expect("source identity")
}

fn schema_id(value: u128) -> SchemaId {
    SchemaId::try_from(uuid(value)).expect("schema identity")
}

fn field_id(value: u128) -> FieldId {
    FieldId::try_from(uuid(value)).expect("field identity")
}

const fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(0x019d_0000_0000_7000_8000_0000_0000_0000 | value)
}

fn assert_footer_value(footer: &[parquet::file::metadata::KeyValue], key: &str, expected: String) {
    let values = footer
        .iter()
        .filter(|entry| entry.key == key)
        .map(|entry| entry.value.as_deref())
        .collect::<Vec<_>>();
    assert_eq!(values, [Some(expected.as_str())]);
}

fn regular_file_count(path: &std::path::Path) -> usize {
    std::fs::read_dir(path)
        .expect("read staging directory")
        .map(|entry| entry.expect("directory entry").path())
        .map(|entry| {
            if entry.is_dir() {
                regular_file_count(&entry)
            } else {
                usize::from(entry.is_file())
            }
        })
        .sum()
}
