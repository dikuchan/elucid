use elucid_catalog::{
    DeclarationDigest, DefinitionDigests, FieldId, MaterializedDigest, Nullability, Schema,
    SchemaId, SchemaVersion, Source, SourceId, SourceName, UserField, UserFieldName,
    UserLogicalType,
};
use uuid::Uuid;

pub fn test_logs_source() -> Source {
    let source_id = SourceId::try_from(catalog_uuid(1)).expect("source identity");
    let schema_id = SchemaId::try_from(catalog_uuid(2)).expect("schema identity");
    let schema = Schema::new(
        schema_id,
        source_id,
        SchemaVersion::new(1).expect("schema version"),
        DefinitionDigests::new(
            DeclarationDigest::new([1; 32]),
            MaterializedDigest::new([2; 32]),
        ),
        vec![
            user_field(3, "source", UserLogicalType::Utf8),
            user_field(4, "status", UserLogicalType::Int64),
            user_field(5, "path", UserLogicalType::Utf8),
        ],
    )
    .expect("catalog schema");
    Source::new(
        source_id,
        SourceName::try_from("test_logs").expect("source name"),
        "Test logs",
        DeclarationDigest::new([3; 32]),
        schema_id,
        vec![schema],
        Vec::new(),
    )
    .expect("catalog source")
}

fn user_field(identity: u128, name: &str, logical_type: UserLogicalType) -> UserField {
    UserField::new(
        FieldId::try_from(catalog_uuid(identity)).expect("field identity"),
        UserFieldName::try_from(name).expect("field name"),
        logical_type,
        Nullability::Nullable,
    )
    .expect("user field")
}

fn catalog_uuid(suffix: u128) -> Uuid {
    Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 | suffix)
}
