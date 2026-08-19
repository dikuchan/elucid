use std::collections::HashSet;

use elucid_catalog::{LogicalType, Nullability};

use crate::ast::{self, NumericSign, Projection, SortDirection, StageKind};
use crate::ir;

use super::error::SemanticError;
use super::expression::{convert_expression, resolve_field};

pub(crate) fn convert_stage(
    stage: &ast::Stage,
    input: &ir::Relation,
) -> Result<ir::Stage, SemanticError> {
    let (kind, output) = match stage.kind() {
        StageKind::Filter(expression) => (
            ir::StageKind::Filter(convert_expression(expression, input)?),
            input.clone(),
        ),
        StageKind::Project(projections) => convert_project(projections, input)?,
        StageKind::Sort(items) => {
            let mut specs = Vec::with_capacity(items.len());
            for item in items {
                let direction = match item.direction() {
                    SortDirection::Ascending => ir::SortOrder::Ascending,
                    SortDirection::Descending => ir::SortOrder::Descending,
                };
                specs.push(ir::SortSpec::new(
                    resolve_field(item.field(), input)?,
                    direction,
                ));
            }
            (ir::StageKind::Sort(specs), input.clone())
        }
        StageKind::Take(value) => (convert_take(value)?, input.clone()),
        StageKind::Summarize { measures, group_by } => {
            convert_summarize(measures, group_by, input)?
        }
    };
    Ok(ir::Stage::new(kind, output))
}

fn convert_project(
    projections: &[Projection],
    input: &ir::Relation,
) -> Result<(ir::StageKind, ir::Relation), SemanticError> {
    let mut names = HashSet::with_capacity(projections.len());
    let mut converted = Vec::with_capacity(projections.len());
    let mut output_fields = Vec::with_capacity(projections.len());

    for projection in projections {
        let (expression, output_field, span) = match projection {
            Projection::Field(reference) => {
                let field = resolve_field(reference, input)?;
                (
                    ir::Expression::Field(field.clone()),
                    field,
                    reference.span(),
                )
            }
            Projection::Computed(projection) => {
                let expression = convert_expression(projection.expression(), input)?;
                let source_field = match &expression {
                    ir::Expression::Field(field) => field,
                    _ => {
                        return Err(unsupported_syntax(
                            "typed computed projection",
                            projection.span(),
                        ));
                    }
                };
                let output = ir::Field::new(
                    projection.alias().as_str(),
                    source_field.logical_type(),
                    source_field.nullability(),
                    ir::FieldOrigin::Derived {
                        declaration_span: projection.alias().span(),
                    },
                );
                (expression, output, projection.alias().span())
            }
        };
        ensure_unique_output(&mut names, &output_field, span)?;
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
) -> Result<(ir::StageKind, ir::Relation), SemanticError> {
    let output_count = group_by.len().checked_add(measures.len()).ok_or_else(|| {
        SemanticError::ConversionError("summarize output field count overflowed".to_owned())
    })?;
    let mut names = HashSet::with_capacity(output_count);
    let mut resolved_groups = Vec::with_capacity(group_by.len());
    let mut output_fields = Vec::with_capacity(output_count);
    for reference in group_by {
        let field = resolve_field(reference, input)?;
        ensure_unique_output(&mut names, &field, reference.span())?;
        output_fields.push(field.clone());
        resolved_groups.push(field);
    }

    let mut resolved_measures = Vec::with_capacity(measures.len());
    for measure in measures {
        let aggregate = measure.aggregate();
        let argument = aggregate
            .argument()
            .map(|reference| resolve_field(reference, input))
            .transpose()?;
        let function = convert_aggregate_function(aggregate.function());
        let (logical_type, nullability) = aggregate_output(function, argument.as_ref(), aggregate)?;
        let output = ir::Field::new(
            measure.alias().as_str(),
            logical_type,
            nullability,
            ir::FieldOrigin::Derived {
                declaration_span: measure.alias().span(),
            },
        );
        ensure_unique_output(&mut names, &output, measure.alias().span())?;
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
) -> Result<(LogicalType, Nullability), SemanticError> {
    match (function, argument) {
        (ir::AggregateFunction::Count, _) => Ok((LogicalType::Int64, Nullability::NonNull)),
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
            _ => Err(unsupported_syntax(
                "aggregate argument type",
                aggregate.span(),
            )),
        },
        (ir::AggregateFunction::Average, Some(field))
            if matches!(
                field.logical_type(),
                LogicalType::Int32
                    | LogicalType::Int64
                    | LogicalType::UInt32
                    | LogicalType::UInt64
                    | LogicalType::Float32
                    | LogicalType::Float64
            ) =>
        {
            Ok((LogicalType::Float64, Nullability::Nullable))
        }
        (ir::AggregateFunction::Min | ir::AggregateFunction::Max, Some(field))
            if matches!(
                field.logical_type(),
                LogicalType::Int32
                    | LogicalType::Int64
                    | LogicalType::UInt32
                    | LogicalType::UInt64
                    | LogicalType::Float32
                    | LogicalType::Float64
                    | LogicalType::Utf8
                    | LogicalType::Datetime
                    | LogicalType::Eid
            ) =>
        {
            Ok((field.logical_type(), Nullability::Nullable))
        }
        (
            ir::AggregateFunction::Sum
            | ir::AggregateFunction::Min
            | ir::AggregateFunction::Max
            | ir::AggregateFunction::Average,
            None,
        )
        | (
            ir::AggregateFunction::Min
            | ir::AggregateFunction::Max
            | ir::AggregateFunction::Average,
            Some(_),
        ) => Err(unsupported_syntax("aggregate argument", aggregate.span())),
    }
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
    span: crate::Span,
) -> Result<(), SemanticError> {
    if names.insert(field.name().to_owned()) {
        Ok(())
    } else {
        Err(SemanticError::DuplicateOutputField {
            name: field.name().to_owned(),
            span,
        })
    }
}

fn convert_take(value: &ast::SignedIntegerLiteral) -> Result<ir::StageKind, SemanticError> {
    if value.sign() == NumericSign::Negative && value.magnitude() != 0 {
        let invalid = if value.magnitude() == i64::MAX as u64 + 1 {
            i64::MIN
        } else {
            let magnitude = i64::try_from(value.magnitude()).map_err(|_| {
                SemanticError::ConversionError(
                    "take literal is outside the signed 64-bit range".to_owned(),
                )
            })?;
            -magnitude
        };
        return Err(SemanticError::InvalidLimitValue { value: invalid });
    }

    let value = i64::try_from(value.magnitude()).map_err(|_| {
        SemanticError::ConversionError("take literal is outside the signed 64-bit range".to_owned())
    })?;
    let value = usize::try_from(value).map_err(|_| {
        SemanticError::ConversionError("take literal exceeds the platform limit".to_owned())
    })?;
    Ok(ir::StageKind::Take(value))
}

fn unsupported_syntax(feature: &str, span: crate::Span) -> SemanticError {
    SemanticError::ConversionError(format!(
        "{feature} at bytes {span} is not supported by semantic analysis"
    ))
}
