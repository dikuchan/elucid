use elucid_catalog::{
    DeclarationDigest, DefinitionDigests, FieldId, MaterializedDigest, Nullability, Schema,
    SchemaId, SchemaVersion, Source, SourceId, SourceName, UserField, UserFieldName,
    UserLogicalType,
};
use uuid::Uuid;

use crate::{
    AnalyzeErrorCode, CatalogSnapshot, DiagnosticCode, DiagnosticSeverity, QueryTimeContext,
    analyze, ir,
};

#[test]
fn typed_expression_analysis_is_canonical() {
    let query = r#"source logs start_inclusive=-1d@h end_exclusive=datetime("1970-01-02T03:00:00+03:00") | filter status >= 400 and active | project @event_time, widened = status + ratio, dynamic_status = try_cast(experimental_status as int32), explicit_value = rest("explicit_null"), explicit_present = rest_exists("explicit_null"), stamp = datetime("1970-01-01T00:00:00.123Z"), event = eid("00112233445566778899aabbccddeeff"), constant = 1 + 2 * 3 | sort by -dynamic_status"#;
    let source = typed_catalog_source();
    let analysis = analyze(
        query,
        &CatalogSnapshot::new(&source),
        &bounded_time_context(),
    )
    .expect("canonical typed query is valid");

    assert_eq!(analysis.diagnostics().len(), 1);
    assert_eq!(
        analysis.diagnostics()[0].code(),
        DiagnosticCode::FieldResolvedFromRemainder
    );
    assert_eq!(
        analysis.diagnostics()[0].severity(),
        DiagnosticSeverity::Warning
    );
    insta::assert_debug_snapshot!(analysis);
}

#[test]
fn exact_literals_widening_and_constant_evaluation_preserve_types() {
    let source = typed_catalog_source();
    let analysis = analyze(
        "source logs | project default_integer = 12, minimum = int32(-2147483648), maximum = uint64(18446744073709551615), contextual = status + 1, widened = status + ratio, folded = 1 + 2 * 3, null_sum = 1 + null, missing = active == null",
        &CatalogSnapshot::new(&source),
        &bounded_time_context(),
    )
    .expect("typed numeric expressions are valid");

    let fields = analysis.pipeline().output_relation().fields();
    assert_eq!(fields[0].logical_type(), elucid_catalog::LogicalType::Int64);
    assert_eq!(fields[1].logical_type(), elucid_catalog::LogicalType::Int32);
    assert_eq!(
        fields[2].logical_type(),
        elucid_catalog::LogicalType::UInt64
    );
    assert_eq!(fields[3].logical_type(), elucid_catalog::LogicalType::Int32);
    assert_eq!(fields[3].nullability(), Nullability::Nullable);
    assert_eq!(
        fields[4].logical_type(),
        elucid_catalog::LogicalType::Float64
    );
    assert_eq!(fields[5].logical_type(), elucid_catalog::LogicalType::Int64);
    assert_eq!(fields[6].logical_type(), elucid_catalog::LogicalType::Int64);
    assert_eq!(fields[6].nullability(), Nullability::Nullable);
    assert_eq!(fields[7].logical_type(), elucid_catalog::LogicalType::Bool);
    assert_eq!(fields[7].nullability(), Nullability::NonNull);

    let ir::StageKind::Project(projections) = analysis.pipeline().stages()[0].kind() else {
        panic!("expected project stage");
    };
    assert!(matches!(
        projections[5].expression().kind(),
        ir::ExpressionKind::Literal(ir::Literal::Int64(7))
    ));
    assert!(matches!(
        projections[6].expression().kind(),
        ir::ExpressionKind::Literal(ir::Literal::Null(elucid_catalog::LogicalType::Int64))
    ));
    assert!(matches!(
        projections[7].expression().kind(),
        ir::ExpressionKind::NullPredicate {
            predicate: ir::NullPredicate::IsNull,
            ..
        }
    ));
}

#[test]
fn aggregates_expose_their_exact_result_types_and_nullability() {
    let source = typed_catalog_source();
    let analysis = analyze(
        "source logs | summarize rows = count(), present = count(active), signed = sum(status), unsigned = sum(requests), mean = avg(ratio), lowest = min(service), latest = max(@event_time) by active",
        &CatalogSnapshot::new(&source),
        &bounded_time_context(),
    )
    .expect("aggregate signatures are valid");

    let actual = analysis
        .pipeline()
        .output_relation()
        .fields()
        .iter()
        .map(|field| (field.logical_type(), field.nullability()))
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        [
            (elucid_catalog::LogicalType::Bool, Nullability::Nullable),
            (elucid_catalog::LogicalType::Int64, Nullability::NonNull),
            (elucid_catalog::LogicalType::Int64, Nullability::NonNull),
            (elucid_catalog::LogicalType::Int64, Nullability::Nullable),
            (elucid_catalog::LogicalType::UInt64, Nullability::Nullable),
            (elucid_catalog::LogicalType::Float64, Nullability::Nullable),
            (elucid_catalog::LogicalType::Utf8, Nullability::Nullable),
            (elucid_catalog::LogicalType::Datetime, Nullability::Nullable,),
        ]
    );
}

#[test]
fn diagnostic_registry_classifies_semantic_failures() {
    let cases = [
        (
            "source absent",
            bounded_time_context(),
            DiagnosticCode::SourceNotFound,
            "absent",
        ),
        (
            "source logs | project @unknown",
            bounded_time_context(),
            DiagnosticCode::FieldNotFound,
            "@unknown",
        ),
        (
            "source logs start_inclusive=datetime(\"not-a-time\")",
            bounded_time_context(),
            DiagnosticCode::TimeExpressionInvalid,
            "datetime(\"not-a-time\")",
        ),
        (
            "source logs",
            QueryTimeContext::new(ir::UtcInstant::UNIX_EPOCH, None, None),
            DiagnosticCode::TimeBoundUnresolved,
            "source logs",
        ),
        (
            "source logs start_inclusive=now end_exclusive=now",
            bounded_time_context(),
            DiagnosticCode::TimeRangeInvalid,
            "source logs start_inclusive=now end_exclusive=now",
        ),
        (
            "source logs | project value = 9223372036854775808",
            bounded_time_context(),
            DiagnosticCode::LiteralInvalid,
            "9223372036854775808",
        ),
        (
            "source logs | summarize total = sum()",
            bounded_time_context(),
            DiagnosticCode::FunctionArityInvalid,
            "sum()",
        ),
        (
            "source logs | summarize total = sum(active)",
            bounded_time_context(),
            DiagnosticCode::FunctionArgumentTypeInvalid,
            "sum(active)",
        ),
        (
            "source logs | project value = cast(active as int32)",
            bounded_time_context(),
            DiagnosticCode::CastInvalid,
            "cast(active as int32)",
        ),
        (
            "source logs | project active + 1",
            bounded_time_context(),
            DiagnosticCode::ProjectionAliasRequired,
            "active + 1",
        ),
        (
            "source logs | project (active)",
            bounded_time_context(),
            DiagnosticCode::ProjectionAliasRequired,
            "(active)",
        ),
        (
            "source logs | summarize count()",
            bounded_time_context(),
            DiagnosticCode::AggregateAliasRequired,
            "count()",
        ),
        (
            "source logs | project status, status",
            bounded_time_context(),
            DiagnosticCode::DuplicateOutputField,
            "status",
        ),
        (
            "source logs | summarize events = count() | filter active",
            bounded_time_context(),
            DiagnosticCode::StageOrderInvalid,
            "filter active",
        ),
        (
            "source logs | take -1",
            bounded_time_context(),
            DiagnosticCode::TakeInvalid,
            "-1",
        ),
        (
            "source logs | filter bytes > 1000.5",
            bounded_time_context(),
            DiagnosticCode::TypeMismatch,
            "bytes > 1000.5",
        ),
        (
            "source logs | project value = int32(2147483647) + int32(1)",
            bounded_time_context(),
            DiagnosticCode::ConstantEvaluationFailed,
            "int32(2147483647) + int32(1)",
        ),
    ];

    for (query, time_context, expected_code, expected_text) in cases {
        let source = typed_catalog_source();
        let error = analyze(query, &CatalogSnapshot::new(&source), &time_context)
            .expect_err("query must fail semantic analysis");
        assert_eq!(error.code(), AnalyzeErrorCode::Semantic, "{query}");
        assert_eq!(error.diagnostics().len(), 1, "{query}");
        let diagnostic = &error.diagnostics()[0];
        assert_eq!(diagnostic.code(), expected_code, "{query}");
        assert_eq!(diagnostic.severity(), DiagnosticSeverity::Error, "{query}");
        let span = diagnostic.span().expect("semantic diagnostic has a span");
        assert_eq!(&query[span.start()..span.end()], expected_text, "{query}");
    }
}

#[test]
fn syntax_errors_and_successful_warnings_use_stable_ordering() {
    let source = typed_catalog_source();
    let syntax = analyze(
        "source logs | filter )",
        &CatalogSnapshot::new(&source),
        &bounded_time_context(),
    )
    .expect_err("query must fail parsing");
    assert_eq!(syntax.code(), AnalyzeErrorCode::Syntax);
    assert_eq!(syntax.diagnostics()[0].code(), DiagnosticCode::SyntaxError);

    let query = "source logs | filter service == \"ошибка\" | project later, earlier";
    let analysis = analyze(
        query,
        &CatalogSnapshot::new(&source),
        &bounded_time_context(),
    )
    .expect("implicit remainder fields are valid");
    let spans = analysis
        .diagnostics()
        .iter()
        .map(|diagnostic| diagnostic.span().expect("warning has a span"))
        .collect::<Vec<_>>();
    assert_eq!(spans.len(), 2);
    assert!(spans[0].start() < spans[1].start());
    assert_eq!(&query[spans[0].start()..spans[0].end()], "later");
    assert_eq!(&query[spans[1].start()..spans[1].end()], "earlier");
}

fn bounded_time_context() -> QueryTimeContext {
    QueryTimeContext::new(
        ir::UtcInstant::UNIX_EPOCH,
        Some(ir::UtcInstant::from_unix_milliseconds(-172_800_000)),
        Some(ir::UtcInstant::from_unix_milliseconds(172_800_000)),
    )
}

fn typed_catalog_source() -> Source {
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
            user_field(4, "active", UserLogicalType::Bool, Nullability::Nullable),
            user_field(5, "status", UserLogicalType::Int32, Nullability::Nullable),
            user_field(6, "bytes", UserLogicalType::Int64, Nullability::Nullable),
            user_field(7, "requests", UserLogicalType::UInt32, Nullability::NonNull),
            user_field(8, "ratio", UserLogicalType::Float32, Nullability::NonNull),
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
