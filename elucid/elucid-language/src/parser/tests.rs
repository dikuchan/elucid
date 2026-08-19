use crate::ast::{
    BinaryOperator, CastKind, ConstructorKind, ExpressionKind, LogicalType, NumericLiteralKind,
    NumericSign, NumericType, Projection, RemainderFunction, StageKind, TimeDirection,
    TimeExpressionKind, TimeOperationKind, TimeUnit,
};

use super::parse;

#[test]
fn complete_query_has_structured_bounds_ast_and_utf8_byte_spans() {
    let source = r#"source `source` start_inclusive=-1d@h end_exclusive=datetime("2026-08-18T00:00:00Z") | filter message == "ошибка" or status >= uint32(400)"#;
    let query = parse(source).expect("query is valid");

    assert_eq!(query.span().start(), 0);
    assert_eq!(query.span().end(), source.len());
    assert_eq!(query.source().name().as_str(), "source");
    assert_eq!(
        query.source().name().span().start(),
        source.find("`source`").expect("quoted source is present")
    );

    let start = query
        .source()
        .start_inclusive()
        .expect("start bound is present");
    let TimeExpressionKind::Relative(operations) = start.kind() else {
        panic!("expected relative start bound");
    };
    assert_eq!(operations.len(), 2);
    assert!(matches!(
        operations[0].kind(),
        TimeOperationKind::Shift {
            direction: TimeDirection::Backward,
            magnitude,
            unit: TimeUnit::Day,
        } if magnitude.get() == 1
    ));
    assert!(matches!(
        operations[1].kind(),
        TimeOperationKind::Truncate(TimeUnit::Hour)
    ));

    let end = query
        .source()
        .end_exclusive()
        .expect("end bound is present");
    let TimeExpressionKind::Datetime(value) = end.kind() else {
        panic!("expected datetime end bound");
    };
    assert_eq!(value.value(), "2026-08-18T00:00:00Z");

    let StageKind::Filter(expression) = query.stages()[0].kind() else {
        panic!("expected filter stage");
    };
    let ExpressionKind::Binary {
        operator: BinaryOperator::Or,
        left,
        right,
    } = expression.kind()
    else {
        panic!("expected logical OR at the expression root");
    };
    let ExpressionKind::Binary {
        operator: BinaryOperator::Equal,
        left: message,
        right: text,
    } = left.kind()
    else {
        panic!("expected message comparison");
    };
    let ExpressionKind::Field(message) = message.kind() else {
        panic!("expected message field");
    };
    assert_eq!(message.as_str(), "message");
    let ExpressionKind::Literal(text) = text.kind() else {
        panic!("expected string literal");
    };
    assert_eq!(text.as_string(), Some("ошибка"));

    let ExpressionKind::Binary {
        operator: BinaryOperator::GreaterThanOrEqual,
        left: status,
        right: threshold,
    } = right.kind()
    else {
        panic!("expected status comparison");
    };
    let ExpressionKind::Field(status) = status.kind() else {
        panic!("expected status field");
    };
    assert_eq!(status.as_str(), "status");
    assert_eq!(
        status.span().start(),
        source.rfind("status").expect("status is present")
    );
    let ExpressionKind::Constructor(threshold) = threshold.kind() else {
        panic!("expected uint32 constructor");
    };
    let ConstructorKind::Numeric { target, literal } = threshold.kind() else {
        panic!("expected numeric constructor");
    };
    assert_eq!(*target, NumericType::UInt32);
    assert_eq!(literal.sign(), NumericSign::NonNegative);
    assert_eq!(literal.kind(), &NumericLiteralKind::Integer(400));
}

#[test]
fn numeric_tokens_are_exact_and_constructors_accept_only_literals() {
    let source = r#"source logs | project maximum = uint64(18446744073709551615), ratio = float32(-0.1), parsed = try_cast(rest("status") as uint32)"#;
    let query = parse(source).expect("query is valid");
    let StageKind::Project(projections) = query.stages()[0].kind() else {
        panic!("expected project stage");
    };
    assert_eq!(projections.len(), 3);

    let Projection::Computed(maximum) = &projections[0] else {
        panic!("expected computed projection");
    };
    assert_eq!(maximum.alias().as_str(), "maximum");
    let ExpressionKind::Constructor(maximum) = maximum.expression().kind() else {
        panic!("expected uint64 constructor");
    };
    let ConstructorKind::Numeric { target, literal } = maximum.kind() else {
        panic!("expected numeric constructor");
    };
    assert_eq!(*target, NumericType::UInt64);
    assert_eq!(literal.kind(), &NumericLiteralKind::Integer(u64::MAX));

    let Projection::Computed(ratio) = &projections[1] else {
        panic!("expected computed projection");
    };
    let ExpressionKind::Constructor(ratio) = ratio.expression().kind() else {
        panic!("expected float32 constructor");
    };
    let ConstructorKind::Numeric { target, literal } = ratio.kind() else {
        panic!("expected numeric constructor");
    };
    assert_eq!(*target, NumericType::Float32);
    assert_eq!(literal.sign(), NumericSign::Negative);
    assert_eq!(
        literal.kind(),
        &NumericLiteralKind::FloatingPoint("0.1".into())
    );

    let Projection::Computed(parsed) = &projections[2] else {
        panic!("expected computed projection");
    };
    let ExpressionKind::Cast(cast) = parsed.expression().kind() else {
        panic!("expected cast expression");
    };
    assert_eq!(cast.kind(), CastKind::NullOnFailure);
    assert_eq!(cast.target(), LogicalType::UInt32);
    let ExpressionKind::Remainder(access) = cast.expression().kind() else {
        panic!("expected remainder access");
    };
    assert_eq!(access.function(), RemainderFunction::Value);
    assert_eq!(access.key().value(), "status");

    for invalid in [
        "source logs | project value = uint32(field)",
        "source logs | project value = uint32(1 + 2)",
        "source logs | project value = uint32(1.0)",
        "source logs | filter value == 18446744073709551616",
        "source logs | filter value == 1e400",
        "source logs | filter value == 01.2",
    ] {
        assert!(
            parse(invalid).is_err(),
            "query unexpectedly parsed: {invalid}"
        );
    }
}

#[test]
fn contextual_keywords_and_alias_rules_are_exact() {
    let query = parse(
        "source source | filter filter == 1 | project project, answer = project + 1 | summarize event_count = count(), total = sum(project) by source",
    )
    .expect("contextual command keywords are identifiers in identifier positions");
    let StageKind::Summarize { measures, group_by } = query.stages()[2].kind() else {
        panic!("expected summarize stage");
    };
    assert_eq!(
        measures[0].alias().expect("measure is aliased").as_str(),
        "event_count"
    );
    assert_eq!(
        measures[1].alias().expect("measure is aliased").as_str(),
        "total"
    );
    assert_eq!(group_by[0].as_str(), "source");

    let quoted = parse("source `as` | project `count`").expect("quoted reserved names are valid");
    assert_eq!(quoted.source().name().as_str(), "as");

    let system_time_unit =
        parse("source logs | project @h").expect("time units remain valid system fields");
    let StageKind::Project(projections) = system_time_unit.stages()[0].kind() else {
        panic!("expected project stage");
    };
    let Projection::Field(field) = &projections[0] else {
        panic!("expected system-field projection");
    };
    assert_eq!(field.as_str(), "@h");

    for invalid in [
        "source as",
        "source logs | summarize total = sum(value + 1)",
        "source logs | filter arbitrary(1)",
        "dataset logs",
        "source logs | where status == 200",
        "source logs | fields status",
        "source logs | head 10",
        "source logs | stats event_count = count()",
    ] {
        let error = parse(invalid).expect_err("query must be rejected");
        assert!(
            error.span().start() <= error.span().end(),
            "{invalid}: {error}"
        );
    }
}

#[test]
fn canonical_query_ast() {
    let query = parse(
        r#"source `source` start_inclusive=-1d@h end_exclusive=datetime("2026-08-18T00:00:00Z") | filter not active or status >= uint32(400) and rest_exists("threat") | project @event_time, status, parsed = try_cast(rest("status") as uint32), ratio = float32(-0.1), id = eid("00112233445566778899aabbccddeeff") | summarize event_count = count(), total = sum(parsed) by status | sort by -event_count, +status | take 10"#,
    )
    .expect("canonical query is valid");

    insta::assert_debug_snapshot!(query);
}

#[test]
fn canonical_expression_precedence_ast() {
    let query =
        parse("source logs | filter a + b * c >= int64(-10) or not (active and deleted == false)")
            .expect("canonical expression is valid");

    insta::assert_debug_snapshot!(query);
}

#[test]
fn source_bounds_require_fixed_order_and_positive_relative_magnitudes() {
    for valid in [
        "source logs start_inclusive=now",
        "source logs end_exclusive=@d",
        "source logs start_inclusive=+m end_exclusive=+2h@h",
    ] {
        assert!(parse(valid).is_ok(), "query unexpectedly rejected: {valid}");
    }

    for invalid in [
        "source logs end_exclusive=now start_inclusive=-1h",
        "source logs start_inclusive=now start_inclusive=-1h",
        "source logs start_inclusive=+0h",
    ] {
        assert!(
            parse(invalid).is_err(),
            "query unexpectedly parsed: {invalid}"
        );
    }
}
