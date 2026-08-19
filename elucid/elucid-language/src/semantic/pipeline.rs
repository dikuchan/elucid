use elucid_catalog::{FieldRole, Schema};

use crate::ast::{
    Query, StageKind, TimeDirection, TimeExpression, TimeExpressionKind, TimeOperationKind,
    TimeUnit,
};
use crate::ir;
use crate::{
    Analysis, AnalyzeError, CatalogSnapshot, Diagnostic, DiagnosticCode, QueryTimeContext, Span,
};

use super::command::convert_stage;
use super::expression::parse_datetime;

pub(crate) fn convert_query(
    query: &Query,
    catalog: &CatalogSnapshot<'_>,
    time_context: &QueryTimeContext,
) -> Result<Analysis, AnalyzeError> {
    let source_name = query.source().name();
    let catalog_source = catalog.source(source_name.as_str()).ok_or_else(|| {
        AnalyzeError::semantic(vec![Diagnostic::error(
            DiagnosticCode::SourceNotFound,
            format!(
                "source {:?} was not found in the catalog snapshot",
                source_name.as_str()
            ),
            source_name.span(),
        )])
    })?;
    let active_schema = catalog_source.active_schema();
    let source_relation = relation_from_schema(active_schema);
    let source = ir::Source::new(
        catalog_source.id(),
        catalog_source.name().as_str(),
        active_schema.id(),
    );
    let time_range = resolve_time_range(query, *time_context).map_err(semantic_failure)?;

    let mut position = PipelinePosition::Rows;
    let mut relation = source_relation.clone();
    let mut stages = Vec::with_capacity(query.stages().len());
    for stage in query.stages() {
        if position == PipelinePosition::Summarized
            && !matches!(stage.kind(), StageKind::Sort(_) | StageKind::Take(_))
        {
            return Err(semantic_failure(Diagnostic::error(
                DiagnosticCode::StageOrderInvalid,
                "only sort and take may follow summarize",
                stage.span(),
            )));
        }

        let converted = convert_stage(stage, &relation).map_err(semantic_failure)?;
        relation = converted.output_relation().clone();
        if matches!(stage.kind(), StageKind::Summarize { .. }) {
            position = PipelinePosition::Summarized;
        }
        stages.push(converted);
    }

    Ok(Analysis::new(ir::Pipeline::new(
        source,
        time_range,
        source_relation,
        stages,
    )))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PipelinePosition {
    Rows,
    Summarized,
}

fn resolve_time_range(
    query: &Query,
    context: QueryTimeContext,
) -> Result<ir::TimeRange, Diagnostic> {
    let source = query.source();
    let start_inclusive = match source.start_inclusive() {
        Some(expression) => evaluate_time_expression(expression, context.reference_time())?,
        None => context.request_start_inclusive().ok_or_else(|| {
            Diagnostic::error(
                DiagnosticCode::TimeBoundUnresolved,
                "source start_inclusive cannot inherit a request bound",
                source.span(),
            )
        })?,
    };
    let end_exclusive = match source.end_exclusive() {
        Some(expression) => evaluate_time_expression(expression, context.reference_time())?,
        None => context.request_end_exclusive().ok_or_else(|| {
            Diagnostic::error(
                DiagnosticCode::TimeBoundUnresolved,
                "source end_exclusive cannot inherit a request bound",
                source.span(),
            )
        })?,
    };

    ir::TimeRange::new(start_inclusive, end_exclusive).ok_or_else(|| {
        Diagnostic::error(
            DiagnosticCode::TimeRangeInvalid,
            "source time range must satisfy start_inclusive < end_exclusive",
            source.span(),
        )
    })
}

fn evaluate_time_expression(
    expression: &TimeExpression,
    reference_time: ir::UtcInstant,
) -> Result<ir::UtcInstant, Diagnostic> {
    match expression.kind() {
        TimeExpressionKind::Datetime(value) => parse_datetime(value.value(), expression.span()),
        TimeExpressionKind::Now => Ok(reference_time),
        TimeExpressionKind::Relative(operations) => {
            let mut value = reference_time.unix_milliseconds();
            for operation in operations {
                value = match operation.kind() {
                    TimeOperationKind::Truncate(unit) => {
                        truncate_time(value, *unit, expression.span())?
                    }
                    TimeOperationKind::Shift {
                        direction,
                        magnitude,
                        unit,
                    } => shift_time(value, *direction, magnitude.get(), *unit, expression.span())?,
                };
            }
            Ok(ir::UtcInstant::from_unix_milliseconds(value))
        }
    }
}

fn truncate_time(value: i64, unit: TimeUnit, span: Span) -> Result<i64, Diagnostic> {
    let unit = i128::from(unit_milliseconds(unit));
    let truncated = i128::from(value).div_euclid(unit) * unit;
    i64::try_from(truncated).map_err(|_| {
        Diagnostic::error(
            DiagnosticCode::TimeExpressionInvalid,
            "time truncation is outside the UTC millisecond domain",
            span,
        )
    })
}

fn shift_time(
    value: i64,
    direction: TimeDirection,
    magnitude: u64,
    unit: TimeUnit,
    span: Span,
) -> Result<i64, Diagnostic> {
    let delta = i128::from(magnitude) * i128::from(unit_milliseconds(unit));
    let result = match direction {
        TimeDirection::Forward => i128::from(value) + delta,
        TimeDirection::Backward => i128::from(value) - delta,
    };
    i64::try_from(result).map_err(|_| {
        Diagnostic::error(
            DiagnosticCode::TimeExpressionInvalid,
            "relative time expression is outside the UTC millisecond domain",
            span,
        )
    })
}

const fn unit_milliseconds(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1_000,
        TimeUnit::Minute => 60_000,
        TimeUnit::Hour => 3_600_000,
        TimeUnit::Day => 86_400_000,
    }
}

fn relation_from_schema(schema: &Schema) -> ir::Relation {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let origin = match field.role() {
            FieldRole::Data => ir::FieldOrigin::Schema {
                field_id: field.id(),
            },
            FieldRole::EventTime
            | FieldRole::IngestionTime
            | FieldRole::EventId
            | FieldRole::Remainder => ir::FieldOrigin::System {
                field_id: field.id(),
            },
            _ => unreachable!("catalog schema contains an unknown field role"),
        };
        fields.push(ir::Field::new(
            field.name(),
            field.logical_type(),
            field.nullability(),
            origin,
        ));
    }
    ir::Relation::new(fields)
}

fn semantic_failure(error: Diagnostic) -> AnalyzeError {
    AnalyzeError::semantic(vec![error])
}
