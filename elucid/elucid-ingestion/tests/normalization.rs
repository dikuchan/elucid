use elucid_catalog::{
    DeclarationDigest, DefinitionDigests, EventTimeFormat, EventTimeMapping, FieldId, FieldMapping,
    IngestionProfile, IngestionProfileRevision, IngestionProfileRevisionId, Input, InputId,
    InputName, JsonPointer, MaterializedDigest, MaximumRecordBytes, Nullability, ProfileRevision,
    Schema, SchemaId, SchemaVersion, Source, SourceId, SourceName, UserField, UserFieldName,
    UserLogicalType,
};
use elucid_ingestion::{
    BatchId, BatchMetadata, DEAD_LETTER_PAYLOAD_PREFIX_BYTES, DeadLetterCode, IngestionTime,
    MAXIMUM_BATCH_EVENT_DAYS, NormalizedRecord, NormalizedValue, PayloadEncoding, PayloadExtent,
    PinnedCatalogIdentities, RecordLocation, normalize_records,
};
use serde_json::Value;
use uuid::Uuid;

#[test]
fn normalization_frames_occurrences_and_constructs_complete_typed_rows() {
    let message_id = field_id(10);
    let nested_id = field_id(11);
    let fixture = fixture(
        1_024,
        EventTimeFormat::Rfc3339,
        vec![
            field(
                message_id,
                "message",
                UserLogicalType::Utf8,
                Nullability::NonNull,
            ),
            field(
                nested_id,
                "nested_value",
                UserLogicalType::Utf8,
                Nullability::Nullable,
            ),
        ],
        vec![
            mapping(message_id, "/message"),
            mapping(nested_id, "/nested/value"),
        ],
    );
    let first = br#"{"timestamp":"2026-08-20T12:00:00.123Z","message":"first","nested":{"value":"kept","sibling":7},"extra":true}"#;
    let second = br#"{"timestamp":"2026-08-20T13:00:00.000+01:00","message":"second"}"#;
    let mut body = b" \r\n".to_vec();
    let first_position = body.len();
    body.extend_from_slice(first);
    body.extend_from_slice(b"\r\n\n");
    let second_position = body.len();
    body.extend_from_slice(second);

    let normalized = normalize_records(fixture.metadata, &body, &fixture.source)
        .expect("the pinned catalog exists");

    assert_eq!(normalized.metadata(), fixture.metadata);
    assert_eq!(normalized.ignored_records(), 2);
    assert_eq!(normalized.records().len(), 2);

    let first_row = accepted(&normalized.records()[0]);
    assert_location(first_row.location(), 2, first_position as u64);
    assert_eq!(
        first_row.event_time().unix_milliseconds(),
        1_787_227_200_123
    );
    assert_eq!(
        first_row.ingestion_time(),
        fixture.metadata.ingestion_time()
    );
    assert_eq!(
        *first_row.event_id().as_bytes(),
        expected_event_id(fixture.metadata.batch_id(), first_position as u64)
    );
    assert_eq!(
        first_row
            .fields()
            .iter()
            .map(|field| (field.field_id(), field.value()))
            .collect::<Vec<_>>(),
        vec![
            (message_id, &NormalizedValue::Utf8("first".to_owned())),
            (nested_id, &NormalizedValue::Utf8("kept".to_owned())),
        ]
    );
    let remainder = first_row.remainder().expect("the row has remainder data");
    assert_eq!(remainder.get("extra"), Some(&Value::Bool(true)));
    assert_eq!(
        remainder.get("nested"),
        Some(&serde_json::json!({"value": "kept", "sibling": 7}))
    );
    assert_eq!(remainder.get("timestamp"), None);
    assert_eq!(remainder.get("message"), None);

    let second_row = accepted(&normalized.records()[1]);
    assert_location(second_row.location(), 4, second_position as u64);
    assert_eq!(
        second_row.event_time().unix_milliseconds(),
        1_787_227_200_000
    );
    assert_eq!(second_row.fields()[1].value(), &NormalizedValue::Null);
    assert!(second_row.remainder().is_none());
    assert_ne!(first_row.event_id(), second_row.event_id());
}

#[test]
fn invalid_occurrences_become_bounded_dead_letters_without_stopping_the_batch() {
    let message_id = field_id(20);
    let fixture = fixture(
        256,
        EventTimeFormat::Rfc3339,
        vec![field(
            message_id,
            "message",
            UserLogicalType::Utf8,
            Nullability::NonNull,
        )],
        vec![mapping(message_id, "/message")],
    );
    let invalid_utf8 = b"{\"timestamp\":\"2026-08-20T12:00:00Z\",\"message\":\"\xff\"}";
    let duplicate =
        br#"{"timestamp":"2026-08-20T12:00:00Z","message":"x","nested":{"key":1,"key":2}}"#;
    let oversized = format!(
        "{{\"timestamp\":\"2026-08-20T12:00:00Z\",\"message\":\"{}\"}}",
        "x".repeat(DEAD_LETTER_PAYLOAD_PREFIX_BYTES + 64)
    );
    let accepted_record = br#"{"timestamp":"2026-08-20T12:00:00Z","message":"survives"}"#;
    let mut body = b"\n".to_vec();
    let invalid_position = body.len();
    body.extend_from_slice(invalid_utf8);
    body.push(b'\n');
    let duplicate_position = body.len();
    body.extend_from_slice(duplicate);
    body.push(b'\n');
    let oversized_position = body.len();
    body.extend_from_slice(oversized.as_bytes());
    body.push(b'\n');
    let accepted_position = body.len();
    body.extend_from_slice(accepted_record);

    let normalized = normalize_records(fixture.metadata, &body, &fixture.source)
        .expect("the pinned catalog exists");

    assert_eq!(normalized.ignored_records(), 1);
    assert_eq!(normalized.records().len(), 4);

    let invalid = rejected(&normalized.records()[0]);
    assert_location(invalid.location(), 2, invalid_position as u64);
    assert_eq!(invalid.code(), DeadLetterCode::InvalidUtf8);
    assert_eq!(invalid.payload_byte_count(), invalid_utf8.len() as u64);
    assert_eq!(
        invalid.payload_digest().as_bytes(),
        blake3::hash(invalid_utf8).as_bytes()
    );
    assert_eq!(invalid.payload().encoding(), PayloadEncoding::Base64);
    assert_eq!(invalid.payload().extent(), PayloadExtent::Complete);

    let duplicate_entry = rejected(&normalized.records()[1]);
    assert_location(duplicate_entry.location(), 3, duplicate_position as u64);
    assert_eq!(duplicate_entry.code(), DeadLetterCode::ParseFailed);
    assert_eq!(duplicate_entry.payload().encoding(), PayloadEncoding::Utf8);

    let oversized_entry = rejected(&normalized.records()[2]);
    assert_location(oversized_entry.location(), 4, oversized_position as u64);
    assert_eq!(oversized_entry.code(), DeadLetterCode::TooLarge);
    assert_eq!(oversized_entry.payload().extent(), PayloadExtent::Prefix);
    assert_eq!(oversized_entry.payload().encoding(), PayloadEncoding::Utf8);
    assert!(oversized_entry.payload().content().len() <= DEAD_LETTER_PAYLOAD_PREFIX_BYTES);
    assert_eq!(oversized_entry.payload_byte_count(), oversized.len() as u64);

    let row = accepted(&normalized.records()[3]);
    assert_location(row.location(), 5, accepted_position as u64);
    assert_eq!(
        row.fields()[0].value(),
        &NormalizedValue::Utf8("survives".to_owned())
    );
}

#[test]
fn strict_mappings_report_missing_null_conversion_and_event_time_separately() {
    let required_id = field_id(30);
    let optional_id = field_id(31);
    let fixture = fixture(
        1_024,
        EventTimeFormat::Rfc3339,
        vec![
            field(
                required_id,
                "required",
                UserLogicalType::Int32,
                Nullability::NonNull,
            ),
            field(
                optional_id,
                "optional",
                UserLogicalType::Bool,
                Nullability::Nullable,
            ),
        ],
        vec![
            mapping(required_id, "/required"),
            mapping(optional_id, "/optional"),
        ],
    );
    let body = br#"{"timestamp":"2026-08-20T12:00:00Z"}
{"timestamp":"2026-08-20T12:00:00Z","required":null}
{"timestamp":"2026-08-20T12:00:00Z","required":"1"}
{"timestamp":"not-a-datetime","required":1}
{"timestamp":"2026-08-20T12:00:00Z","required":1}"#;

    let normalized = normalize_records(fixture.metadata, body, &fixture.source)
        .expect("the pinned catalog exists");

    assert_eq!(
        normalized
            .records()
            .iter()
            .map(|record| match record {
                NormalizedRecord::DeadLetter(entry) => Some(entry.code()),
                NormalizedRecord::Accepted(_) => None,
                _ => unreachable!("the normalization result is known"),
            })
            .collect::<Vec<_>>(),
        vec![
            Some(DeadLetterCode::FieldMissing),
            Some(DeadLetterCode::FieldNull),
            Some(DeadLetterCode::ConversionFailed),
            Some(DeadLetterCode::EventTimeInvalid),
            None,
        ]
    );
    assert_eq!(
        accepted(&normalized.records()[4]).fields()[1].value(),
        &NormalizedValue::Null
    );
}

#[test]
fn every_declared_scalar_type_is_converted_directly_to_its_target_type() {
    let declarations = [
        ("boolean", UserLogicalType::Bool),
        ("i32", UserLogicalType::Int32),
        ("i64", UserLogicalType::Int64),
        ("u32", UserLogicalType::UInt32),
        ("u64", UserLogicalType::UInt64),
        ("f32", UserLogicalType::Float32),
        ("f64", UserLogicalType::Float64),
        ("text", UserLogicalType::Utf8),
        ("datetime", UserLogicalType::Datetime),
        ("unmapped", UserLogicalType::Utf8),
    ];
    let fields = declarations
        .iter()
        .enumerate()
        .map(|(index, (name, logical_type))| {
            field(
                field_id(40 + index as u128),
                name,
                *logical_type,
                if *name == "unmapped" {
                    Nullability::Nullable
                } else {
                    Nullability::NonNull
                },
            )
        })
        .collect::<Vec<_>>();
    let mappings = declarations
        .iter()
        .enumerate()
        .filter(|(_, (name, _))| *name != "unmapped")
        .map(|(index, (name, _))| mapping(field_id(40 + index as u128), &format!("/{name}")))
        .collect::<Vec<_>>();
    let fixture = fixture(4_096, EventTimeFormat::Rfc3339, fields, mappings);
    let body = br#"{"timestamp":"2026-08-20T12:00:00Z","boolean":true,"i32":-2147483648,"i64":-9223372036854775808,"u32":4294967295,"u64":18446744073709551615,"f32":1.0000000596046447753906250000000000000000000000000000001,"f64":1.7976931348623157e308,"text":"value","datetime":"1969-12-31T23:59:59.999Z"}"#;

    let normalized = normalize_records(fixture.metadata, body, &fixture.source)
        .expect("the pinned catalog exists");
    let values = accepted(&normalized.records()[0])
        .fields()
        .iter()
        .map(|field| field.value())
        .collect::<Vec<_>>();

    assert_eq!(values[0], &NormalizedValue::Bool(true));
    assert_eq!(values[1], &NormalizedValue::Int32(i32::MIN));
    assert_eq!(values[2], &NormalizedValue::Int64(i64::MIN));
    assert_eq!(values[3], &NormalizedValue::UInt32(u32::MAX));
    assert_eq!(values[4], &NormalizedValue::UInt64(u64::MAX));
    let NormalizedValue::Float32(float32) = values[5] else {
        panic!("f32 field has the declared type");
    };
    assert_eq!(float32.to_bits(), 1.0_f32.to_bits() + 1);
    assert_eq!(values[6], &NormalizedValue::Float64(f64::MAX));
    assert_eq!(values[7], &NormalizedValue::Utf8("value".to_owned()));
    assert_eq!(values[8], &NormalizedValue::Datetime(-1));
    assert_eq!(values[9], &NormalizedValue::Null);
}

#[test]
fn event_day_fan_out_rejects_only_days_not_admitted_in_first_occurrence_order() {
    let message_id = field_id(60);
    let fixture = fixture(
        256,
        EventTimeFormat::Rfc3339,
        vec![field(
            message_id,
            "message",
            UserLogicalType::Utf8,
            Nullability::NonNull,
        )],
        vec![mapping(message_id, "/message")],
    );
    let first_day = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).expect("first event day");
    let mut records = (0..=MAXIMUM_BATCH_EVENT_DAYS)
        .map(|offset| {
            let day = first_day
                .checked_add_days(chrono::Days::new(
                    u64::try_from(offset).expect("event-day test offset fits u64"),
                ))
                .expect("bounded event day");
            format!("{{\"timestamp\":\"{day}T00:00:00Z\",\"message\":\"day-{offset}\"}}")
        })
        .collect::<Vec<_>>();
    records.push(format!(
        "{{\"timestamp\":\"{first_day}T12:00:00Z\",\"message\":\"known-day\"}}"
    ));
    let rejected_day = first_day
        .checked_add_days(chrono::Days::new(
            u64::try_from(MAXIMUM_BATCH_EVENT_DAYS).expect("event-day limit fits u64"),
        ))
        .expect("rejected event day");
    records.push(format!(
        "{{\"timestamp\":\"{rejected_day}T12:00:00Z\",\"message\":\"still-unseen\"}}"
    ));
    let body = records.join("\n");

    let normalized = normalize_records(fixture.metadata, body.as_bytes(), &fixture.source)
        .expect("the pinned catalog exists");

    assert_eq!(normalized.records().len(), MAXIMUM_BATCH_EVENT_DAYS + 3);
    assert!(matches!(
        normalized.records()[MAXIMUM_BATCH_EVENT_DAYS],
        NormalizedRecord::DeadLetter(ref entry)
            if entry.code() == DeadLetterCode::EventDayLimitExceeded
    ));
    assert!(matches!(
        normalized.records()[MAXIMUM_BATCH_EVENT_DAYS + 1],
        NormalizedRecord::Accepted(_)
    ));
    assert!(matches!(
        normalized.records()[MAXIMUM_BATCH_EVENT_DAYS + 2],
        NormalizedRecord::DeadLetter(ref entry)
            if entry.code() == DeadLetterCode::EventDayLimitExceeded
    ));
}

fn fixture(
    maximum_record_bytes: u64,
    event_time_format: EventTimeFormat,
    fields: Vec<UserField>,
    mappings: Vec<FieldMapping>,
) -> Fixture {
    let source_id = source_id(1);
    let schema_id = schema_id(2);
    let input_id = input_id(3);
    let profile_revision_id = profile_revision_id(4);
    let schema = Schema::new(
        schema_id,
        source_id,
        SchemaVersion::new(1).expect("schema version"),
        digests(1),
        fields,
    )
    .expect("schema");
    let profile = IngestionProfile::new(
        MaximumRecordBytes::new(maximum_record_bytes).expect("maximum record bytes"),
        EventTimeMapping::new(
            JsonPointer::parse("/timestamp").expect("event-time pointer"),
            event_time_format,
        ),
        mappings,
    )
    .expect("profile");
    let revision = IngestionProfileRevision::new(
        profile_revision_id,
        input_id,
        ProfileRevision::new(1).expect("profile revision"),
        schema_id,
        digests(2),
        profile,
    );
    let input = Input::new(
        input_id,
        source_id,
        InputName::try_from("vector").expect("input name"),
        digests(3),
        profile_revision_id,
        vec![revision],
    )
    .expect("input");
    let source = Source::new(
        source_id,
        SourceName::try_from("logs").expect("source name"),
        "Logs",
        DeclarationDigest::new([4; 32]),
        schema_id,
        vec![schema],
        vec![input],
    )
    .expect("source");
    let metadata = BatchMetadata::new(
        BatchId::try_from(identity(5)).expect("batch identity"),
        PinnedCatalogIdentities::new(source_id, input_id, profile_revision_id, schema_id),
        IngestionTime::from_unix_milliseconds(1_776_945_600_000).expect("ingestion time"),
    );
    Fixture { source, metadata }
}

struct Fixture {
    source: Source,
    metadata: BatchMetadata,
}

fn field(
    id: FieldId,
    name: &str,
    logical_type: UserLogicalType,
    nullability: Nullability,
) -> UserField {
    UserField::new(
        id,
        UserFieldName::try_from(name).expect("field name"),
        logical_type,
        nullability,
    )
    .expect("field")
}

fn mapping(id: FieldId, pointer: &str) -> FieldMapping {
    FieldMapping::new(id, JsonPointer::parse(pointer).expect("JSON pointer")).expect("mapping")
}

fn expected_event_id(batch_id: BatchId, input_position: u64) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"elucid:event\0");
    hasher.update(batch_id.as_uuid().as_bytes());
    hasher.update(&input_position.to_be_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    bytes
}

fn accepted(record: &NormalizedRecord) -> &elucid_ingestion::AcceptedRow {
    match record {
        NormalizedRecord::Accepted(row) => row,
        NormalizedRecord::DeadLetter(entry) => {
            panic!("expected an accepted row, got {}", entry.code())
        }
        _ => panic!("unexpected normalization result"),
    }
}

fn rejected(record: &NormalizedRecord) -> &elucid_ingestion::DeadLetterEntry {
    match record {
        NormalizedRecord::DeadLetter(entry) => entry,
        NormalizedRecord::Accepted(_) => panic!("expected a dead-letter entry"),
        _ => panic!("unexpected normalization result"),
    }
}

fn assert_location(location: RecordLocation, line_number: u64, input_position: u64) {
    assert_eq!(location.line_number(), line_number);
    assert_eq!(location.input_position(), input_position);
}

fn digests(byte: u8) -> DefinitionDigests {
    DefinitionDigests::new(
        DeclarationDigest::new([byte; 32]),
        MaterializedDigest::new([byte.wrapping_add(64); 32]),
    )
}

fn source_id(value: u128) -> SourceId {
    SourceId::try_from(identity(value)).expect("source identity")
}

fn schema_id(value: u128) -> SchemaId {
    SchemaId::try_from(identity(value)).expect("schema identity")
}

fn field_id(value: u128) -> FieldId {
    FieldId::try_from(identity(value)).expect("field identity")
}

fn input_id(value: u128) -> InputId {
    InputId::try_from(identity(value)).expect("input identity")
}

fn profile_revision_id(value: u128) -> IngestionProfileRevisionId {
    IngestionProfileRevisionId::try_from(identity(value)).expect("profile revision identity")
}

fn identity(value: u128) -> Uuid {
    Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 | value)
}
