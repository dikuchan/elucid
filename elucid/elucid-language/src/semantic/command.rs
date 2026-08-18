use crate::ast::{self, NumericSign, Projection, SortDirection, StageKind};
use crate::ir;

use super::error::SemanticError;
use super::expression::convert_expression;

pub(crate) fn convert_stage(stage: &ast::Stage) -> Result<ir::PipelineStage, SemanticError> {
    match stage.kind() {
        StageKind::Filter(expression) => {
            Ok(ir::PipelineStage::Filter(convert_expression(expression)?))
        }
        StageKind::Project(projections) => {
            let fields = projections
                .iter()
                .map(|projection| match projection {
                    Projection::Field(field) => Ok(ir::FieldRef::new(field.as_str().to_owned())),
                    Projection::Computed(_) => Err(unsupported_syntax("computed projection")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ir::PipelineStage::Project(fields))
        }
        StageKind::Sort(items) => Ok(ir::PipelineStage::Sort(
            items
                .iter()
                .map(|item| {
                    let direction = match item.direction() {
                        SortDirection::Ascending => ir::SortOrder::Ascending,
                        SortDirection::Descending => ir::SortOrder::Descending,
                    };
                    ir::SortSpec::new(
                        ir::Expression::Field(ir::FieldRef::new(item.field().as_str().to_owned())),
                        direction,
                    )
                })
                .collect(),
        )),
        StageKind::Take(value) => convert_take(value),
        StageKind::Summarize { measures, group_by } => {
            let measures = measures
                .iter()
                .map(|measure| {
                    let aggregate = measure.aggregate();
                    let argument = aggregate.argument().map(|field| {
                        ir::Expression::Field(ir::FieldRef::new(field.as_str().to_owned()))
                    });
                    ir::AggregateExpr::new(
                        aggregate.function().as_str().to_owned(),
                        argument,
                        Some(measure.alias().as_str().to_owned()),
                    )
                })
                .collect();
            let group_by = group_by
                .iter()
                .map(|field| ir::FieldRef::new(field.as_str().to_owned()))
                .collect();
            Ok(ir::PipelineStage::Aggregate { measures, group_by })
        }
    }
}

fn convert_take(value: &ast::SignedIntegerLiteral) -> Result<ir::PipelineStage, SemanticError> {
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
    Ok(ir::PipelineStage::Limit(value))
}

fn unsupported_syntax(feature: &str) -> SemanticError {
    SemanticError::ConversionError(format!("{feature} is not supported by semantic analysis"))
}
