use std::collections::HashSet;

use elucid_catalog::{LogicalType, Nullability};

use crate::ast::{self, NumericSign, Projection, SortDirection, StageKind};
use crate::ir;
use crate::{Diagnostic, DiagnosticCode, Span};

use super::expression::{convert_expression, resolve_field};

pub(crate) fn convert_stage(
    stage: &ast::Stage,
    input: &ir::Relation,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<ir::Stage, Diagnostic> {
    let (kind, output) = match stage.kind() {
        StageKind::Filter(expression) => {
            let span = expression.span();
            let expression = convert_expression(expression, input, diagnostics)?;
            if expression.logical_type() != LogicalType::Bool {
                return Err(type_mismatch("filter expression must have type bool", span));
            }
            (ir::StageKind::Filter(expression), input.clone())
        }
        StageKind::Project(projections) => convert_project(projections, input, diagnostics)?,
        StageKind::Sort(items) => {
            let mut specs = Vec::with_capacity(items.len());
            for item in items {
                let field = resolve_field(item.field(), input, diagnostics)?;
                if field.logical_type() == LogicalType::Json {
                    return Err(type_mismatch(
                        "sort does not support json fields",
                        item.span(),
                    ));
                }
                let direction = match item.direction() {
                    SortDirection::Ascending => ir::SortOrder::Ascending,
                    SortDirection::Descending => ir::SortOrder::Descending,
                };
                specs.push(ir::SortSpec::new(field, direction));
            }
            (ir::StageKind::Sort(specs), input.clone())
        }
        StageKind::Take(value) => (convert_take(value)?, input.clone()),
        StageKind::Summarize { measures, group_by } => {
            convert_summarize(measures, group_by, input, diagnostics)?
        }
    };
    Ok(ir::Stage::new(kind, output))
}

fn convert_project(
    projections: &[Projection],
    input: &ir::Relation,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<(ir::StageKind, ir::Relation), Diagnostic> {
    let mut names = HashSet::with_capacity(projections.len());
    let mut converted = Vec::with_capacity(projections.len());
    let mut output_fields = Vec::with_capacity(projections.len());

    for projection in projections {
        let (expression, output_field, declaration_span) = match projection {
            Projection::Field(reference) => {
                let field = resolve_field(reference, input, diagnostics)?;
                (
                    ir::Expression::field(field.clone()),
                    field,
                    reference.span(),
                )
            }
            Projection::Computed(projection) => {
                let expression = convert_expression(projection.expression(), input, diagnostics)?;
                let output = ir::Field::new(
                    projection.alias().as_str(),
                    expression.logical_type(),
                    expression.nullability(),
                    ir::FieldOrigin::Derived {
                        declaration_span: projection.alias().span(),
                    },
                );
                (expression, output, projection.alias().span())
            }
            Projection::Unaliased(expression) => {
                return Err(Diagnostic::error(
                    DiagnosticCode::ProjectionAliasRequired,
                    "computed projection requires an alias",
                    expression.span(),
                ));
            }
        };
        ensure_unique_output(&mut names, &output_field, declaration_span)?;
        output_fields.push(output_field.clone());
        converted.push(ir::Projection::new(expression, output_field));
    }

    Ok((
        ir::StageKind::Project(converted),
        ir::Relation::new(output_fields),
    ))
}

fn convert_summarize(
    measures: &[ast::Measure],
    group_by: &[ast::FieldReference],
    input: &ir::Relation,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<(ir::StageKind, ir::Relation), Diagnostic> {
    let mut names = HashSet::new();
    let mut resolved_groups = Vec::with_capacity(group_by.len());
    let mut output_fields = Vec::new();
    for reference in group_by {
        let field = resolve_field(reference, input, diagnostics)?;
        if field.logical_type() == LogicalType::Json {
            return Err(type_mismatch(
                "summarize group keys do not support json",
                reference.span(),
            ));
        }
        ensure_unique_output(&mut names, &field, reference.span())?;
        output_fields.push(field.clone());
        resolved_groups.push(field);
    }

    let mut resolved_measures = Vec::with_capacity(measures.len());
    for measure in measures {
        let aggregate = measure.aggregate();
        let alias = measure.alias().ok_or_else(|| {
            Diagnostic::error(
                DiagnosticCode::AggregateAliasRequired,
                "aggregate measure requires an alias",
                aggregate.span(),
            )
        })?;
        let argument = aggregate
            .argument()
            .map(|reference| resolve_field(reference, input, diagnostics))
            .transpose()?;
        let function = convert_aggregate_function(aggregate.function());
        let (logical_type, nullability) = aggregate_output(function, argument.as_ref(), aggregate)?;
        let output = ir::Field::new(
            alias.as_str(),
            logical_type,
            nullability,
            ir::FieldOrigin::Derived {
                declaration_span: alias.span(),
            },
        );
        ensure_unique_output(&mut names, &output, alias.span())?;
        output_fields.push(output.clone());
        resolved_measures.push(ir::AggregateExpression::new(function, argument, output));
    }

    Ok((
        ir::StageKind::Aggregate {
            measures: resolved_measures,
            group_by: resolved_groups,
        },
        ir::Relation::new(output_fields),
    ))
}

fn aggregate_output(
    function: ir::AggregateFunction,
    argument: Option<&ir::Field>,
    aggregate: &ast::AggregateCall,
) -> Result<(LogicalType, Nullability), Diagnostic> {
    match (function, argument) {
        (ir::AggregateFunction::Count, _) => Ok((LogicalType::Int64, Nullability::NonNull)),
        (
            ir::AggregateFunction::Sum
            | ir::AggregateFunction::Min
            | ir::AggregateFunction::Max
            | ir::AggregateFunction::Average,
            None,
        ) => Err(Diagnostic::error(
            DiagnosticCode::FunctionArityInvalid,
            format!("{} requires one field argument", function),
            aggregate.span(),
        )),
        (ir::AggregateFunction::Sum, Some(field)) => match field.logical_type() {
            LogicalType::Int32 | LogicalType::Int64 => {
                Ok((LogicalType::Int64, Nullability::Nullable))
            }
            LogicalType::UInt32 | LogicalType::UInt64 => {
                Ok((LogicalType::UInt64, Nullability::Nullable))
            }
            LogicalType::Float32 | LogicalType::Float64 => {
                Ok((LogicalType::Float64, Nullability::Nullable))
            }
            _ => Err(invalid_aggregate_argument(
                function,
                field,
                aggregate.span(),
            )),
        },
        (ir::AggregateFunction::Average, Some(field)) if is_numeric(field.logical_type()) => {
            Ok((LogicalType::Float64, Nullability::Nullable))
        }
        (ir::AggregateFunction::Min | ir::AggregateFunction::Max, Some(field))
            if is_orderable_aggregate_type(field.logical_type()) =>
        {
            Ok((field.logical_type(), Nullability::Nullable))
        }
        (function, Some(field)) => Err(invalid_aggregate_argument(
            function,
            field,
            aggregate.span(),
        )),
    }
}

fn invalid_aggregate_argument(
    function: ir::AggregateFunction,
    field: &ir::Field,
    span: Span,
) -> Diagnostic {
    Diagnostic::error(
        DiagnosticCode::FunctionArgumentTypeInvalid,
        format!(
            "{} does not accept an argument of type {}",
            function,
            field.logical_type()
        ),
        span,
    )
}

fn convert_aggregate_function(function: ast::AggregateFunction) -> ir::AggregateFunction {
    match function {
        ast::AggregateFunction::Count => ir::AggregateFunction::Count,
        ast::AggregateFunction::Sum => ir::AggregateFunction::Sum,
        ast::AggregateFunction::Min => ir::AggregateFunction::Min,
        ast::AggregateFunction::Max => ir::AggregateFunction::Max,
        ast::AggregateFunction::Avg => ir::AggregateFunction::Average,
    }
}

fn ensure_unique_output(
    names: &mut HashSet<String>,
    field: &ir::Field,
    span: Span,
) -> Result<(), Diagnostic> {
    if names.insert(field.name().to_owned()) {
        Ok(())
    } else {
        Err(Diagnostic::error(
            DiagnosticCode::DuplicateOutputField,
            format!("output field {:?} occurs more than once", field.name()),
            span,
        ))
    }
}

fn convert_take(value: &ast::SignedIntegerLiteral) -> Result<ir::StageKind, Diagnostic> {
    if value.sign() == NumericSign::Negative && value.magnitude() != 0 {
        return Err(Diagnostic::error(
            DiagnosticCode::TakeInvalid,
            "take requires a non-negative signed 64-bit integer",
            value.span(),
        ));
    }
    if value.magnitude() > i64::MAX as u64 {
        return Err(Diagnostic::error(
            DiagnosticCode::TakeInvalid,
            "take value is outside the signed 64-bit range",
            value.span(),
        ));
    }
    Ok(ir::StageKind::Take(value.magnitude()))
}

fn is_numeric(logical_type: LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Int32
            | LogicalType::Int64
            | LogicalType::UInt32
            | LogicalType::UInt64
            | LogicalType::Float32
            | LogicalType::Float64
    )
}

fn is_orderable_aggregate_type(logical_type: LogicalType) -> bool {
    is_numeric(logical_type)
        || matches!(
            logical_type,
            LogicalType::Utf8 | LogicalType::Datetime | LogicalType::Eid
        )
}

fn type_mismatch(message: impl Into<String>, span: Span) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::TypeMismatch, message, span)
}

#[cfg(test)]
mod tests {
    use super::super::relation_tests::analyze_query;

    #[test]
    fn take_accepts_zero_with_or_without_a_minus_sign() {
        for query in ["source logs | take 0", "source logs | take -0"] {
            let pipeline = analyze_query(query).expect("zero is a valid take value");
            assert!(matches!(
                pipeline.stages()[0].kind(),
                crate::ir::StageKind::Take(0)
            ));
        }
    }
}
