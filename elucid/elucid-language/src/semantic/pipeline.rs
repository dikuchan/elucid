use crate::ast::Query;
use crate::ir;

use super::command::convert_stage;
use super::error::SemanticError;
use super::validate::validate_pipeline;

pub(crate) fn convert_query(query: &Query) -> Result<ir::Pipeline, Vec<SemanticError>> {
    if query.source().start_inclusive().is_some() || query.source().end_exclusive().is_some() {
        return Err(vec![SemanticError::ConversionError(
            "source bounds are not supported by semantic analysis".to_owned(),
        )]);
    }

    let source = ir::SourceSpec::new(query.source().name().as_str().to_owned());
    let mut errors = Vec::new();
    let mut stages = Vec::with_capacity(query.stages().len());
    for stage in query.stages() {
        match convert_stage(stage) {
            Ok(stage) => stages.push(stage),
            Err(error) => errors.push(error),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }

    let pipeline = ir::Pipeline::new(source, ir::TimeRange::default(), stages);
    validate_pipeline(&pipeline)?;
    Ok(pipeline)
}

#[cfg(test)]
mod tests {
    use crate::analyze;

    #[test]
    fn canonical_filter_project_sort_take_ir() {
        let pipeline = analyze(
            r#"source logs | filter status >= 400 and source == "nginx" | project source, status | sort by -status | take 5"#,
        )
        .expect("canonical query is valid");

        insta::assert_debug_snapshot!(pipeline);
    }

    #[test]
    fn canonical_aggregate_sort_take_ir() {
        let pipeline = analyze(
            "source logs | summarize event_count = count(), total = sum(status) by source | sort by -event_count | take 10",
        )
        .expect("canonical query is valid");

        insta::assert_debug_snapshot!(pipeline);
    }

    #[test]
    fn take_accepts_zero_with_or_without_a_minus_sign() {
        for query in ["source logs | take 0", "source logs | take -0"] {
            let pipeline = analyze(query).expect("zero is a valid take value");
            assert!(matches!(
                pipeline.stages(),
                [crate::ir::PipelineStage::Limit(0)]
            ));
        }
    }

    #[test]
    fn take_rejects_a_negative_value() {
        let error = analyze("source logs | take -1").expect_err("negative take must be rejected");
        assert!(matches!(
            error,
            crate::AnalyzeError::Semantic(errors)
                if errors == vec![crate::SemanticError::InvalidLimitValue { value: -1 }]
        ));
    }
}
