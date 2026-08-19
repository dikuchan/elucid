use elucid_catalog::{
    DeclarationDigest, DefinitionDigests, FieldId, MaterializedDigest, Nullability, Schema,
    SchemaId, SchemaVersion, Source, SourceId, SourceName, UserField, UserFieldName,
    UserLogicalType,
};
use uuid::Uuid;

use crate::{AnalyzeError, CatalogSnapshot, SemanticError, Span, analyze, ir};

#[test]
fn canonical_relations_resolve_catalog_and_derived_fields() {
    let pipeline = analyze_query(
        "source logs | filter status >= 400 | project @event_time, service_name = service, status | summarize events = count(), maximum_status = max(status) by service_name | sort by -events | take 10",
    )
    .expect("catalog-backed query is valid");

    insta::assert_debug_snapshot!(pipeline);
}

#[test]
fn every_stage_exposes_the_relation_seen_by_the_next_stage() {
    let pipeline = analyze_query(
        "source logs | project service_name = service, status | summarize events = count(), maximum_status = max(status) by service_name | sort by -events",
    )
    .expect("catalog-backed query is valid");

    assert_eq!(
        field_names(pipeline.source_relation()),
        [
            "@event_time",
            "@ingestion_time",
            "@event_id",
            "service",
            "status",
            "bytes",
            "@rest",
        ]
    );
    assert_eq!(
        field_names(pipeline.stages()[0].output_relation()),
        ["service_name", "status"]
    );
    assert_eq!(
        field_names(pipeline.stages()[1].output_relation()),
        ["service_name", "events", "maximum_status"]
    );
    assert_eq!(
        field_names(pipeline.stages()[2].output_relation()),
        ["service_name", "events", "maximum_status"]
    );
    assert_eq!(
        field_names(pipeline.output_relation()),
        ["service_name", "events", "maximum_status"]
    );
}

#[test]
fn names_are_resolved_only_from_the_catalog_or_preceding_relation() {
    let cases = [
        ("source absent", "absent", 7..13, ResolutionFailure::Source),
        (
            "source logs | project service | sort by status",
            "status",
            40..46,
            ResolutionFailure::Field,
        ),
        (
            "source logs | project @unknown",
            "@unknown",
            22..30,
            ResolutionFailure::Field,
        ),
    ];

    for (query, expected_name, expected_span, failure) in cases {
        let error = analyze_query(query).expect_err("name must not resolve");
        let errors = semantic_errors(error);
        assert_eq!(errors.len(), 1, "{query}: {errors:?}");
        match (&errors[0], failure) {
            (SemanticError::SourceNotFound { name, span }, ResolutionFailure::Source) => {
                assert_eq!(name, expected_name);
                assert_eq!(*span, Span::new(expected_span));
            }
            (SemanticError::FieldNotFound { name, span }, ResolutionFailure::Field) => {
                assert_eq!(name, expected_name);
                assert_eq!(*span, Span::new(expected_span));
            }
            (actual, _) => panic!("{query}: unexpected semantic error {actual:?}"),
        }
    }
}

#[test]
fn transformed_relations_reject_duplicate_output_names() {
    for query in [
        "source logs | project service, service",
        "source logs | summarize service = count() by service",
    ] {
        let error = analyze_query(query).expect_err("duplicate output must be rejected");
        let errors = semantic_errors(error);
        assert!(matches!(
            errors.as_slice(),
            [SemanticError::DuplicateOutputField { name, .. }] if name == "service"
        ));
    }
}

#[test]
fn only_sort_and_take_can_follow_summarize() {
    let query = "source logs | summarize events = count() | filter status == 500";
    let error = analyze_query(query).expect_err("filter after summarize must be rejected");
    let errors = semantic_errors(error);

    assert!(matches!(
        errors.as_slice(),
        [SemanticError::StageOrderInvalid { span }] if *span == Span::new(43..63)
    ));
}

#[derive(Clone, Copy)]
enum ResolutionFailure {
    Source,
    Field,
}

pub(super) fn analyze_query(query: &str) -> Result<ir::Pipeline, AnalyzeError> {
    let source = catalog_source();
    analyze(query, &CatalogSnapshot::new(&source))
}

pub(super) fn semantic_errors(error: AnalyzeError) -> Vec<SemanticError> {
    match error {
        AnalyzeError::Semantic(errors) => errors,
        AnalyzeError::Parse(error) => panic!("query unexpectedly failed to parse: {error}"),
    }
}

fn field_names(relation: &ir::Relation) -> Vec<&str> {
    relation.fields().iter().map(ir::Field::name).collect()
}

fn catalog_source() -> Source {
    let source_id = SourceId::try_from(uuid(1)).expect("source identity is valid");
    let schema_id = SchemaId::try_from(uuid(2)).expect("schema identity is valid");
    let schema = Schema::new(
        schema_id,
        source_id,
        SchemaVersion::new(1).expect("schema version is valid"),
        DefinitionDigests::new(
            DeclarationDigest::new([1; 32]),
            MaterializedDigest::new([2; 32]),
        ),
        vec![
            user_field(3, "service", UserLogicalType::Utf8, Nullability::NonNull),
            user_field(4, "status", UserLogicalType::Int32, Nullability::Nullable),
            user_field(5, "bytes", UserLogicalType::Int64, Nullability::Nullable),
        ],
    )
    .expect("schema is valid");

    Source::new(
        source_id,
        SourceName::try_from("logs").expect("source name is valid"),
        "Logs",
        DeclarationDigest::new([3; 32]),
        schema_id,
        vec![schema],
        Vec::new(),
    )
    .expect("source is valid")
}

fn user_field(
    identity_suffix: u128,
    name: &str,
    logical_type: UserLogicalType,
    nullability: Nullability,
) -> UserField {
    UserField::new(
        FieldId::try_from(uuid(identity_suffix)).expect("field identity is valid"),
        UserFieldName::try_from(name).expect("field name is valid"),
        logical_type,
        nullability,
    )
    .expect("user field is valid")
}

fn uuid(suffix: u128) -> Uuid {
    Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 | suffix)
}
