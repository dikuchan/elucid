use elucid_catalog::{FieldRole, Schema};

use crate::CatalogSnapshot;
use crate::ast::{Query, StageKind};
use crate::ir;

use super::command::convert_stage;
use super::error::SemanticError;

pub(crate) fn convert_query(
    query: &Query,
    catalog: &CatalogSnapshot<'_>,
) -> Result<ir::Pipeline, Vec<SemanticError>> {
    if query.source().start_inclusive().is_some() || query.source().end_exclusive().is_some() {
        return Err(vec![SemanticError::ConversionError(
            "source bounds are not supported by semantic analysis".to_owned(),
        )]);
    }

    let source_name = query.source().name();
    let catalog_source = catalog.source(source_name.as_str()).ok_or_else(|| {
        vec![SemanticError::SourceNotFound {
            name: source_name.as_str().to_owned(),
            span: source_name.span(),
        }]
    })?;
    let active_schema = catalog_source.active_schema();
    let source_relation = relation_from_schema(active_schema)?;
    let source = ir::Source::new(
        catalog_source.id(),
        catalog_source.name().as_str(),
        active_schema.id(),
    );

    let mut position = PipelinePosition::Rows;
    let mut relation = source_relation.clone();
    let mut stages = Vec::with_capacity(query.stages().len());
    for stage in query.stages() {
        if position == PipelinePosition::Summarized
            && !matches!(stage.kind(), StageKind::Sort(_) | StageKind::Take(_))
        {
            return Err(vec![SemanticError::StageOrderInvalid {
                span: stage.span(),
            }]);
        }

        let converted = convert_stage(stage, &relation).map_err(|error| vec![error])?;
        relation = converted.output_relation().clone();
        if matches!(stage.kind(), StageKind::Summarize { .. }) {
            position = PipelinePosition::Summarized;
        }
        stages.push(converted);
    }

    Ok(ir::Pipeline::new(
        source,
        ir::TimeRange::default(),
        source_relation,
        stages,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PipelinePosition {
    Rows,
    Summarized,
}

fn relation_from_schema(schema: &Schema) -> Result<ir::Relation, Vec<SemanticError>> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let origin = match field.role() {
            FieldRole::Data => ir::FieldOrigin::Schema {
                field_id: field.id(),
            },
            FieldRole::EventTime
            | FieldRole::IngestTime
            | FieldRole::EventId
            | FieldRole::Remainder => ir::FieldOrigin::System {
                field_id: field.id(),
            },
            _ => {
                return Err(vec![SemanticError::ConversionError(format!(
                    "catalog field {:?} has an unsupported role",
                    field.name()
                ))]);
            }
        };
        fields.push(ir::Field::new(
            field.name(),
            field.logical_type(),
            field.nullability(),
            origin,
        ));
    }
    Ok(ir::Relation::new(fields))
}

#[cfg(test)]
mod tests {
    use crate::SemanticError;

    use super::super::relation_tests::{analyze_query, semantic_errors};

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

    #[test]
    fn take_rejects_a_negative_value() {
        let error =
            analyze_query("source logs | take -1").expect_err("negative take must be rejected");
        assert!(matches!(
            semantic_errors(error).as_slice(),
            [SemanticError::InvalidLimitValue { value: -1 }]
        ));
    }
}
