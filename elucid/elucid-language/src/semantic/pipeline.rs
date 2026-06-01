use super::error::SemanticError;
use crate::ast::Query;
use crate::ir;

use super::command::convert_command;
use super::validate::validate_pipeline;

/// Converts a parsed [`Query`] AST into an [`ir::Pipeline`].
///
/// This is the top-level entry point for semantic analysis. It extracts the
/// dataset name, validates the source, and converts each command into a
/// pipeline stage.
///
/// # Errors
///
/// Returns a `Vec<SemanticError>` if:
/// - The dataset name is empty or whitespace-only.
/// - Any command fails structural validation (see [`convert_command`]).
pub(crate) fn convert_query(query: &Query) -> Result<ir::Pipeline, Vec<SemanticError>> {
    if query.source().trim().is_empty() {
        return Err(vec![SemanticError::ConversionError(
            "dataset name must not be empty".to_owned(),
        )]);
    }

    let source = ir::SourceSpec::new(query.source().to_owned());
    let time_range = ir::TimeRange::default();

    let mut errors: Vec<SemanticError> = Vec::new();
    let mut stages: Vec<ir::PipelineStage> = Vec::with_capacity(query.commands().len());
    for cmd in query.commands().iter().cloned() {
        match convert_command(cmd) {
            Ok(stage) => stages.push(stage),
            Err(e) => errors.push(e),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }

    let pipeline = ir::Pipeline::new(source, time_range, stages);
    validate_pipeline(&pipeline)?;

    Ok(pipeline)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Command;
    use crate::ir;
    use crate::parser;

    /// Test helper: parse a query string and convert it to an [`ir::Pipeline`].
    ///
    /// Panics if parsing fails.
    fn parse_and_convert(input: &str) -> Result<ir::Pipeline, Vec<SemanticError>> {
        let query = parser::parse(input).unwrap_or_else(|e| {
            e.eprint(input).unwrap();
            panic!("parse failed for input: '{input}'");
        });
        convert_query(&query)
    }

    #[test]
    fn source_only_no_stages() {
        let pipeline = parse_and_convert("dataset test").expect("should convert");
        assert_eq!(pipeline.source().dataset(), "test");
        assert!(pipeline.stages().is_empty());
    }

    #[test]
    fn single_filter_stage() {
        let pipeline =
            parse_and_convert("dataset test | where status == 200").expect("should convert");
        assert_eq!(pipeline.source().dataset(), "test");
        assert_eq!(pipeline.stages().len(), 1);
        assert_eq!(
            &pipeline.stages()[0],
            &ir::PipelineStage::Filter(ir::Expression::Binary(
                ir::BinaryOperator::Equal,
                Box::new(ir::Expression::Field(ir::FieldRef::new(
                    "status".to_owned()
                ))),
                Box::new(ir::Expression::Literal(ir::Literal::Number(200.0))),
            ))
        );
    }

    #[test]
    fn single_sort_stage() {
        let pipeline = parse_and_convert("dataset test | sort by -count, +status, time")
            .expect("should convert");
        assert_eq!(pipeline.stages().len(), 1);

        let ir::PipelineStage::Sort(specs) = &pipeline.stages()[0] else {
            panic!("expected Sort stage");
        };
        assert_eq!(specs.len(), 3);

        // -count → descending
        assert_eq!(specs[0].order(), ir::SortOrder::Descending);
        // +status → ascending
        assert_eq!(specs[1].order(), ir::SortOrder::Ascending);
        // time (no prefix) → ascending
        assert_eq!(specs[2].order(), ir::SortOrder::Ascending);
    }

    #[test]
    fn single_limit_stage() {
        let pipeline = parse_and_convert("dataset test | head 10").expect("should convert");
        assert_eq!(pipeline.stages().len(), 1);
        assert_eq!(&pipeline.stages()[0], &ir::PipelineStage::Limit(10));
    }

    #[test]
    fn single_project_stage() {
        let pipeline =
            parse_and_convert("dataset test | fields name, age, active").expect("should convert");
        assert_eq!(pipeline.stages().len(), 1);

        let ir::PipelineStage::Project(fields) = &pipeline.stages()[0] else {
            panic!("expected Project stage");
        };
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].as_str(), "name");
        assert_eq!(fields[1].as_str(), "age");
        assert_eq!(fields[2].as_str(), "active");
    }

    #[test]
    fn single_aggregate_stage() {
        let pipeline =
            parse_and_convert("dataset test | stats total = sum(bytes), count() by method")
                .expect("should convert");
        assert_eq!(pipeline.stages().len(), 1);

        let ir::PipelineStage::Aggregate { measures, group_by } = &pipeline.stages()[0] else {
            panic!("expected Aggregate stage");
        };
        assert_eq!(measures.len(), 2);
        assert_eq!(measures[0].function(), "sum");
        assert_eq!(measures[0].alias(), Some("total"));
        assert_eq!(measures[1].function(), "count");
        assert_eq!(group_by.len(), 1);
        assert_eq!(group_by[0].as_str(), "method");
    }

    #[test]
    fn multi_stage_pipeline() {
        let pipeline =
            parse_and_convert("dataset test | where status == 200 | sort by -count | head 5")
                .expect("should convert");

        assert_eq!(pipeline.source().dataset(), "test");
        assert_eq!(pipeline.stages().len(), 3);

        // Stage 0: Filter
        assert!(matches!(
            &pipeline.stages()[0],
            ir::PipelineStage::Filter(_)
        ));

        // Stage 1: Sort
        let ir::PipelineStage::Sort(specs) = &pipeline.stages()[1] else {
            panic!("expected Sort stage at index 1");
        };
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].order(), ir::SortOrder::Descending);

        // Stage 2: Limit
        assert_eq!(&pipeline.stages()[2], &ir::PipelineStage::Limit(5));
    }

    // Error cases.

    #[test]
    fn empty_source_returns_error() {
        // Construct a Query with an empty source directly, since the parser
        // would reject it.
        let query = Query::new(String::new(), vec![]);
        let result = convert_query(&query);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            SemanticError::ConversionError(msg) if msg.contains("dataset name must not be empty")
        ));
    }

    #[test]
    fn whitespace_only_source_returns_error() {
        let query = Query::new("   ".to_owned(), vec![]);
        let result = convert_query(&query);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            SemanticError::ConversionError(msg) if msg.contains("dataset name must not be empty")
        ));
    }

    #[test]
    fn multiple_invalid_commands_collect_all_errors() {
        let query = Query::new(
            "test".to_owned(),
            vec![
                Command::Head(0),        // InvalidLimitValue
                Command::Fields(vec![]), // EmptyFieldList
            ],
        );
        let result = convert_query(&query);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(
            errors.len(),
            2,
            "should collect both errors, got {errors:?}"
        );
    }

    #[test]
    fn snapshot_multi_stage_pipeline() {
        let pipeline =
            parse_and_convert("dataset test | where status == 200 | sort by -count | head 5")
                .expect("should convert");
        insta::assert_debug_snapshot!("multi_stage_pipeline", pipeline);
    }

    #[test]
    fn snapshot_aggregate_pipeline() {
        let pipeline =
            parse_and_convert("dataset test | stats total = sum(bytes), count() by method")
                .expect("should convert");
        insta::assert_debug_snapshot!("aggregate_pipeline", pipeline);
    }

    #[test]
    fn validation_rejects_two_stats_commands() {
        let result = parse_and_convert("dataset logs | stats count() | stats sum(count)");
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0], SemanticError::MultipleAggregates);
    }

    #[test]
    fn validation_rejects_filter_after_stats() {
        let result = parse_and_convert("dataset logs | stats count() | where x > 1");
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0], SemanticError::AggregateAfterAggregate);
    }

    #[test]
    fn validation_allows_sort_limit_after_stats() {
        let result =
            parse_and_convert("dataset logs | stats count() by host | sort -count | head 10");
        assert!(result.is_ok());
        let pipeline = result.expect("should convert");
        assert_eq!(pipeline.stages().len(), 3);
        assert!(matches!(
            &pipeline.stages()[0],
            ir::PipelineStage::Aggregate { .. }
        ));
        assert!(matches!(&pipeline.stages()[1], ir::PipelineStage::Sort(_)));
        assert!(matches!(&pipeline.stages()[2], ir::PipelineStage::Limit(_)));
    }

    // Public API integration tests.

    use crate::analyze;
    use crate::semantic::error::AnalyzeError;

    #[test]
    fn analyze_snapshot_filter() {
        let pipeline = analyze("dataset test | where status == 200").expect("should analyze");
        insta::assert_debug_snapshot!("analyze_filter", pipeline);
    }

    #[test]
    fn analyze_snapshot_stats_sort_head() {
        let pipeline = analyze("dataset test | stats count() by method | sort by -count | head 10")
            .expect("should analyze");
        insta::assert_debug_snapshot!("analyze_stats_sort_head", pipeline);
    }

    #[test]
    fn analyze_snapshot_fields_sort_head() {
        let pipeline = analyze("dataset test | fields name, age | sort by name | head 5")
            .expect("should analyze");
        insta::assert_debug_snapshot!("analyze_fields_sort_head", pipeline);
    }

    #[test]
    fn analyze_snapshot_multiple_filters() {
        let pipeline = analyze("dataset test | where a > 1 | where b < 2 | sort by a")
            .expect("should analyze");
        insta::assert_debug_snapshot!("analyze_multiple_filters", pipeline);
    }

    #[test]
    fn analyze_snapshot_source_only() {
        let pipeline = analyze("dataset test").expect("should analyze");
        insta::assert_debug_snapshot!("analyze_source_only", pipeline);
    }

    #[test]
    fn analyze_error_multiple_aggregates() {
        let result = analyze("dataset logs | stats count() | stats sum(count)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match &err {
            AnalyzeError::Semantic(errors) => {
                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0], SemanticError::MultipleAggregates);
            }
            other => panic!("expected AnalyzeError::Semantic, got {other:?}"),
        }
        insta::assert_snapshot!("analyze_error_multiple_aggregates", err.to_string());
    }

    #[test]
    fn analyze_error_aggregate_after_aggregate() {
        let result = analyze("dataset logs | stats count() | where x > 1");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match &err {
            AnalyzeError::Semantic(errors) => {
                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0], SemanticError::AggregateAfterAggregate);
            }
            other => panic!("expected AnalyzeError::Semantic, got {other:?}"),
        }
        insta::assert_snapshot!("analyze_error_aggregate_after_aggregate", err.to_string());
    }

    #[test]
    fn analyze_error_invalid_limit() {
        let result = analyze("dataset logs | head 0");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match &err {
            AnalyzeError::Semantic(errors) => {
                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0], SemanticError::InvalidLimitValue { value: 0 });
            }
            other => panic!("expected AnalyzeError::Semantic, got {other:?}"),
        }
        insta::assert_snapshot!("analyze_error_invalid_limit", err.to_string());
    }

    #[test]
    fn analyze_error_parse_failure_empty_input() {
        let result = analyze("");
        assert!(result.is_err(), "empty input should fail");
        assert!(
            matches!(result.unwrap_err(), AnalyzeError::Parse(_)),
            "empty input should produce a Parse error"
        );
    }

    #[test]
    fn analyze_error_parse_failure_no_dataset() {
        let result = analyze("| where x > 1");
        assert!(result.is_err(), "missing dataset should fail");
        assert!(
            matches!(result.unwrap_err(), AnalyzeError::Parse(_)),
            "missing dataset should produce a Parse error"
        );
    }
}
