use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use elucid_catalog::{
    CatalogModelError, DeclarationDigest, DefinitionDigests, EventTimeFormat, EventTimeMapping,
    FieldId, FieldMapping, FieldRole, IngestionProfile, IngestionProfileRevision,
    IngestionProfileRevisionId, Input, InputId, InputName, JsonPointer, LogicalType,
    MaterializedDigest, MaximumRecordBytes, Nullability, ProfileRevision, Schema, SchemaId,
    SchemaVersion, Source, SourceId, SourceName, UserField, UserFieldName, UserLogicalType,
};
use uuid::Uuid;

#[test]
fn scalar_boundaries_reject_invalid_external_values() {
    let version_four = Uuid::from_u128(0x018f_0000_0000_4000_8000_0000_0000_0001);

    assert!(matches!(
        SourceId::try_from(version_four),
        Err(CatalogModelError::IdentityMustBeUuidV7 { .. })
    ));
    assert!(matches!(
        SourceName::try_from("access-logs"),
        Err(CatalogModelError::InvalidName { .. })
    ));
    assert!(matches!(
        SchemaVersion::new(0),
        Err(CatalogModelError::VersionMustBePositive { .. })
    ));
    assert!(matches!(
        ProfileRevision::new(0),
        Err(CatalogModelError::VersionMustBePositive { .. })
    ));
    assert!(matches!(
        MaximumRecordBytes::new(0),
        Err(CatalogModelError::MaximumRecordBytesMustBePositive)
    ));

    let name = SourceName::try_from("access_logs_2").expect("the source name is valid");
    assert_eq!(name.as_str(), "access_logs_2");
}

#[test]
fn json_pointer_materializes_rfc_6901_tokens() {
    let pointer = JsonPointer::parse("/a~1b/m~0n/").expect("the JSON Pointer is valid");
    let tokens = pointer
        .tokens()
        .iter()
        .map(|token| token.as_str())
        .collect::<Vec<_>>();

    assert_eq!(tokens, ["a/b", "m~n", ""]);
    assert_eq!(pointer.to_string(), "/a~1b/m~0n/");
    assert!(
        JsonPointer::parse("")
            .expect("the root pointer is valid")
            .is_root()
    );
    assert!(matches!(
        JsonPointer::parse("timestamp"),
        Err(CatalogModelError::JsonPointerMustStartWithSlash)
    ));
    assert!(matches!(
        JsonPointer::parse("/~2"),
        Err(CatalogModelError::InvalidJsonPointerEscape { .. })
    ));
}

#[test]
fn schema_materializes_exact_system_and_user_arrow_fields() {
    let source_id = source_id(1);
    let message_id = field_id(2);
    let latency_id = field_id(3);
    let message = UserField::new(
        message_id,
        user_field_name("message"),
        UserLogicalType::Utf8,
        Nullability::NonNull,
    )
    .expect("the user field is valid")
    .with_description("Rendered log message");
    let latency = UserField::new(
        latency_id,
        user_field_name("latency"),
        UserLogicalType::Float64,
        Nullability::Nullable,
    )
    .expect("the user field is valid");

    let schema = Schema::new(
        schema_id(4),
        source_id,
        schema_version(1),
        definition_digests(1, 2),
        vec![message, latency],
    )
    .expect("the schema is valid");

    let fields = schema.fields();
    assert_eq!(fields.len(), 6);
    assert_eq!(fields[0].id(), FieldId::EVENT_TIME);
    assert_eq!(fields[0].name(), "@event_time");
    assert_eq!(fields[0].logical_type(), LogicalType::Datetime);
    assert_eq!(fields[0].nullability(), Nullability::NonNull);
    assert_eq!(fields[0].role(), FieldRole::EventTime);
    assert_eq!(fields[0].ordinal().get(), 0);
    assert_eq!(fields[3].id(), message_id);
    assert_eq!(fields[3].role(), FieldRole::Data);
    assert_eq!(fields[3].description(), Some("Rendered log message"));
    assert_eq!(fields[5].id(), FieldId::REMAINDER);
    assert_eq!(fields[5].ordinal().get(), 5);

    let arrow = schema.arrow_schema();
    assert_eq!(
        arrow.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC")))
    );
    assert!(!arrow.field(0).is_nullable());
    assert_eq!(
        arrow.field(0).metadata().get("elucid.field_id"),
        Some(&FieldId::EVENT_TIME.to_string())
    );
    assert_eq!(arrow.field(2).data_type(), &DataType::FixedSizeBinary(16));
    assert_eq!(
        arrow.field(2).metadata().get("elucid.logical_type"),
        Some(&"eid".to_owned())
    );
    assert_eq!(arrow.field(3).data_type(), &DataType::Utf8);
    assert_eq!(
        arrow.field(3).metadata().get("elucid.field_id"),
        Some(&message_id.to_string())
    );
    assert!(
        !arrow
            .field(3)
            .metadata()
            .contains_key("elucid.logical_type")
    );
    assert!(arrow.field(4).is_nullable());
    assert_eq!(arrow.field(5).data_type(), &DataType::Utf8);
    assert_eq!(
        arrow.field(5).metadata().get("elucid.logical_type"),
        Some(&"json".to_owned())
    );
}

#[test]
fn schema_rejects_reserved_and_duplicate_user_fields() {
    assert!(matches!(
        UserField::new(
            FieldId::EVENT_TIME,
            user_field_name("timestamp"),
            UserLogicalType::Datetime,
            Nullability::NonNull,
        ),
        Err(CatalogModelError::SystemFieldIdentityIsReserved { .. })
    ));

    let duplicate_name = vec![
        UserField::new(
            field_id(10),
            user_field_name("message"),
            UserLogicalType::Utf8,
            Nullability::NonNull,
        )
        .expect("the user field is valid"),
        UserField::new(
            field_id(11),
            user_field_name("message"),
            UserLogicalType::Utf8,
            Nullability::Nullable,
        )
        .expect("the user field is valid"),
    ];

    assert!(matches!(
        Schema::new(
            schema_id(12),
            source_id(13),
            schema_version(1),
            definition_digests(3, 4),
            duplicate_name,
        ),
        Err(CatalogModelError::DuplicateFieldName { .. })
    ));
}

#[test]
fn profile_rejects_system_and_duplicate_mapping_targets() {
    let pointer = JsonPointer::parse("/message").expect("the JSON Pointer is valid");
    assert!(matches!(
        FieldMapping::new(FieldId::EVENT_ID, pointer.clone()),
        Err(CatalogModelError::SystemFieldCannotBeMapped { field_id })
            if field_id == FieldId::EVENT_ID
    ));

    let target_field_id = field_id(18);
    let mapping = FieldMapping::new(target_field_id, pointer)
        .expect("the user field mapping target is valid");
    let profile = IngestionProfile::new(
        MaximumRecordBytes::new(1024).expect("the limit is positive"),
        EventTimeMapping::new(
            JsonPointer::parse("/timestamp").expect("the JSON Pointer is valid"),
            EventTimeFormat::Rfc3339,
        ),
        vec![mapping.clone(), mapping],
    );

    assert!(matches!(
        profile,
        Err(CatalogModelError::DuplicateProfileMappingTarget { field_id })
            if field_id == target_field_id
    ));
}

#[test]
fn versioned_source_accepts_a_profile_targeting_a_historical_schema() {
    let source_id = source_id(20);
    let message_id = field_id(21);
    let region_id = field_id(22);
    let schema_one = schema(
        source_id,
        schema_id(23),
        1,
        vec![required_utf8(message_id, "message")],
    );
    let schema_two = schema(
        source_id,
        schema_id(24),
        2,
        vec![
            required_utf8(message_id, "message"),
            UserField::new(
                region_id,
                user_field_name("region"),
                UserLogicalType::Utf8,
                Nullability::Nullable,
            )
            .expect("the user field is valid")
            .with_historical_remainder_pointer(
                JsonPointer::parse("/region").expect("the JSON Pointer is valid"),
            )
            .expect("a nullable field may define a historical remainder pointer"),
        ],
    );
    let input_id = input_id(25);
    let revision_one_id = profile_revision_id(26);
    let revision_two_id = profile_revision_id(27);
    let revision_one = profile_revision(
        input_id,
        revision_one_id,
        1,
        schema_one.id(),
        vec![mapping(message_id, "/message")],
    );
    let revision_two = profile_revision(
        input_id,
        revision_two_id,
        2,
        schema_two.id(),
        vec![
            mapping(message_id, "/message"),
            mapping(region_id, "/region"),
        ],
    );
    let input = Input::new(
        input_id,
        source_id,
        input_name("http"),
        definition_digests(5, 6),
        revision_one_id,
        vec![revision_one, revision_two],
    )
    .expect("the input history is valid");

    let source = Source::new(
        source_id,
        SourceName::try_from("logs").expect("the source name is valid"),
        "Access logs",
        declaration_digest(7),
        schema_two.id(),
        vec![schema_one, schema_two],
        vec![input],
    )
    .expect("the versioned source is valid");

    assert_eq!(source.active_schema().version(), schema_version(2));
    let active_input = &source.inputs()[0];
    assert_eq!(active_input.active_profile_revision().id(), revision_one_id);
    let profile = active_input.active_profile_revision().profile();
    assert_eq!(
        profile.event_time().json_pointer(),
        &JsonPointer::parse("/timestamp").expect("the JSON Pointer is valid")
    );
    assert_eq!(
        source.active_schema().fields()[4].historical_remainder_pointer(),
        Some(&JsonPointer::parse("/region").expect("the JSON Pointer is valid"))
    );
}

#[test]
fn aggregate_construction_rejects_incomplete_histories_and_mappings() {
    let source_id = source_id(30);
    let target_field_id = field_id(33);
    let target_schema = schema(
        source_id,
        schema_id(34),
        1,
        vec![required_utf8(target_field_id, "message")],
    );
    let input_id = input_id(35);
    let revision_id = profile_revision_id(36);
    let incomplete_revision =
        profile_revision(input_id, revision_id, 1, target_schema.id(), Vec::new());
    let input = Input::new(
        input_id,
        source_id,
        input_name("http"),
        definition_digests(9, 10),
        revision_id,
        vec![incomplete_revision],
    )
    .expect("the revision history is structurally valid");

    assert!(matches!(
        Source::new(
            source_id,
            SourceName::try_from("logs").expect("the source name is valid"),
            "Logs",
            declaration_digest(11),
            target_schema.id(),
            vec![target_schema],
            vec![input],
        ),
        Err(CatalogModelError::ProfileMappingMissing { field_id, .. })
            if field_id == target_field_id
    ));
}

fn source_id(value: u64) -> SourceId {
    SourceId::try_from(uuid_v7(value)).expect("the source identity is UUIDv7")
}

fn schema_id(value: u64) -> SchemaId {
    SchemaId::try_from(uuid_v7(value)).expect("the schema identity is UUIDv7")
}

fn field_id(value: u64) -> FieldId {
    FieldId::try_from(uuid_v7(value)).expect("the field identity is UUIDv7")
}

fn input_id(value: u64) -> InputId {
    InputId::try_from(uuid_v7(value)).expect("the input identity is UUIDv7")
}

fn profile_revision_id(value: u64) -> IngestionProfileRevisionId {
    IngestionProfileRevisionId::try_from(uuid_v7(value))
        .expect("the profile revision identity is UUIDv7")
}

fn uuid_v7(value: u64) -> Uuid {
    Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 | u128::from(value))
}

fn schema(
    source_id: SourceId,
    schema_id: SchemaId,
    version: u64,
    fields: Vec<UserField>,
) -> Schema {
    Schema::new(
        schema_id,
        source_id,
        schema_version(version),
        definition_digests(version as u8, (version + 32) as u8),
        fields,
    )
    .expect("the schema is valid")
}

fn required_utf8(field_id: FieldId, name: &str) -> UserField {
    UserField::new(
        field_id,
        user_field_name(name),
        UserLogicalType::Utf8,
        Nullability::NonNull,
    )
    .expect("the user field is valid")
}

fn profile_revision(
    input_id: InputId,
    id: IngestionProfileRevisionId,
    revision: u64,
    target_schema_id: SchemaId,
    mappings: Vec<FieldMapping>,
) -> IngestionProfileRevision {
    let profile = IngestionProfile::new(
        MaximumRecordBytes::new(10 * 1024 * 1024).expect("the limit is positive"),
        EventTimeMapping::new(
            JsonPointer::parse("/timestamp").expect("the JSON Pointer is valid"),
            EventTimeFormat::Rfc3339,
        ),
        mappings,
    )
    .expect("the profile is valid");

    IngestionProfileRevision::new(
        id,
        input_id,
        ProfileRevision::new(revision).expect("the revision is positive"),
        target_schema_id,
        definition_digests(revision as u8, (revision + 64) as u8),
        profile,
    )
}

fn mapping(field_id: FieldId, pointer: &str) -> FieldMapping {
    FieldMapping::new(
        field_id,
        JsonPointer::parse(pointer).expect("the JSON Pointer is valid"),
    )
    .expect("the user field mapping target is valid")
}

fn user_field_name(name: &str) -> UserFieldName {
    UserFieldName::try_from(name).expect("the user field name is valid")
}

fn input_name(name: &str) -> InputName {
    InputName::try_from(name).expect("the input name is valid")
}

fn schema_version(version: u64) -> SchemaVersion {
    SchemaVersion::new(version).expect("the schema version is positive")
}

fn declaration_digest(byte: u8) -> DeclarationDigest {
    DeclarationDigest::new([byte; 32])
}

fn materialized_digest(byte: u8) -> MaterializedDigest {
    MaterializedDigest::new([byte; 32])
}

fn definition_digests(declaration: u8, materialized: u8) -> DefinitionDigests {
    DefinitionDigests::new(
        declaration_digest(declaration),
        materialized_digest(materialized),
    )
}
