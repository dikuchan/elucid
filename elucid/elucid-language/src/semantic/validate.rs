//! Pipeline-level validation rules that run after stage assembly.
//!
//! These checks enforce structural constraints on the pipeline as a whole —
//! for example, "at most one aggregate" — that cannot be expressed during
//! per-command conversion.

use crate::ir;
use super::error::SemanticError;

/// Validates structural rules on a fully-assembled [`ir::Pipeline`].
///
/// # Errors
///
/// Returns a `Vec<SemanticError>` containing every rule violation found:
///
/// - [`SemanticError::MultipleAggregates`] if more than one `Aggregate` stage
///   appears in the pipeline.
/// - [`SemanticError::AggregateAfterAggregate`] if any stage other than `Sort`
///   or `Limit` appears after an `Aggregate` stage.
pub(crate) fn validate_pipeline(pipeline: &ir::Pipeline) -> Result<(), Vec<SemanticError>> {
    let mut errors: Vec<SemanticError> = Vec::new();
    let stages = pipeline.stages();

    // Rule 1: at most one Aggregate stage.
    let aggregate_count = stages
        .iter()
        .filter(|s| matches!(s, ir::PipelineStage::Aggregate { .. }))
        .count();

    if aggregate_count > 1 {
        errors.push(SemanticError::MultipleAggregates);
    }

    // Rule 2: after the first Aggregate, only Sort and Limit are allowed.
    // Only check this when there is exactly one Aggregate — if there are
    // multiple, Rule 1 already covers it.
    if aggregate_count == 1 {
        let mut seen_aggregate = false;
        for stage in stages {
            if seen_aggregate {
                let allowed = matches!(
                    stage,
                    ir::PipelineStage::Sort(_) | ir::PipelineStage::Limit(_)
                );
                if !allowed {
                    errors.push(SemanticError::AggregateAfterAggregate);
                    // Only report once — break to avoid duplicate errors for
                    // the same root cause.
                    break;
                }
            }
            if matches!(stage, ir::PipelineStage::Aggregate { .. }) {
                seen_aggregate = true;
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir;

    /// Helper: build a pipeline from the given stages.
    fn make_pipeline(stages: Vec<ir::PipelineStage>) -> ir::Pipeline {
        ir::Pipeline::new(ir::SourceSpec::new("test".to_owned()), ir::TimeRange::default(), stages)
    }

    /// Helper: a minimal Aggregate stage with `count()`, no group-by.
    fn aggregate_stage() -> ir::PipelineStage {
        ir::PipelineStage::Aggregate {
            measures: vec![ir::AggregateExpr::new("count".to_owned(), None, None)],
            group_by: vec![],
        }
    }

    /// Helper: a sort-by-count-descending stage.
    fn sort_stage() -> ir::PipelineStage {
        ir::PipelineStage::Sort(vec![ir::SortSpec::new(
            ir::Expr::Field(ir::FieldRef::new("count".to_owned())),
            ir::SortOrder::Descending,
        )])
    }

    /// Helper: a limit stage.
    fn limit_stage(n: usize) -> ir::PipelineStage {
        ir::PipelineStage::Limit(n)
    }

    /// Helper: a filter stage (`where x > 5`).
    fn filter_stage() -> ir::PipelineStage {
        ir::PipelineStage::Filter(ir::Expr::Field(ir::FieldRef::new("x".to_owned())))
    }

    // ── Rule 1: at most one Aggregate ────────────────────────────────────

    #[test]
    fn two_aggregates_produces_multiple_aggregates_error() {
        let pipeline = make_pipeline(vec![aggregate_stage(), aggregate_stage()]);
        let errors = validate_pipeline(&pipeline).expect_err("should fail");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0], SemanticError::MultipleAggregates);
    }

    #[test]
    fn single_aggregate_is_valid() {
        let pipeline = make_pipeline(vec![aggregate_stage()]);
        assert!(validate_pipeline(&pipeline).is_ok());
    }

    #[test]
    fn zero_aggregates_is_valid() {
        let pipeline = make_pipeline(vec![filter_stage(), sort_stage()]);
        assert!(validate_pipeline(&pipeline).is_ok());
    }

    // ── Rule 2: no non-sort/limit after aggregate ────────────────────────

    #[test]
    fn filter_after_aggregate_produces_error() {
        // stats count() by host | where x > 5
        let pipeline = make_pipeline(vec![aggregate_stage(), filter_stage()]);
        let errors = validate_pipeline(&pipeline).expect_err("should fail");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0], SemanticError::AggregateAfterAggregate);
    }

    #[test]
    fn sort_and_limit_after_aggregate_is_valid() {
        // stats count() by host | sort -count | head 10
        let pipeline = make_pipeline(vec![aggregate_stage(), sort_stage(), limit_stage(10)]);
        assert!(validate_pipeline(&pipeline).is_ok());
    }

    #[test]
    fn sort_limit_then_filter_after_aggregate_produces_error() {
        // stats count() by host | sort -count | head 10 | where x > 5
        let pipeline = make_pipeline(vec![
            aggregate_stage(),
            sort_stage(),
            limit_stage(10),
            filter_stage(),
        ]);
        let errors = validate_pipeline(&pipeline).expect_err("should fail");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0], SemanticError::AggregateAfterAggregate);
    }

    // ── Combined rules ───────────────────────────────────────────────────

    #[test]
    fn two_aggregates_reports_only_multiple_aggregates() {
        // When there are multiple aggregates, Rule 1 fires but Rule 2 is
        // skipped (it is only relevant for single-aggregate pipelines).
        let pipeline = make_pipeline(vec![aggregate_stage(), aggregate_stage()]);
        let errors = validate_pipeline(&pipeline).expect_err("should fail");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0], SemanticError::MultipleAggregates);
    }

    // ── Edge cases ───────────────────────────────────────────────────────

    #[test]
    fn empty_stages_is_valid() {
        let pipeline = make_pipeline(vec![]);
        assert!(validate_pipeline(&pipeline).is_ok());
    }

    #[test]
    fn filter_sort_without_aggregate_is_valid() {
        let pipeline = make_pipeline(vec![filter_stage(), sort_stage()]);
        assert!(validate_pipeline(&pipeline).is_ok());
    }

    #[test]
    fn aggregate_with_only_sort_after_is_valid() {
        let pipeline = make_pipeline(vec![aggregate_stage(), sort_stage()]);
        assert!(validate_pipeline(&pipeline).is_ok());
    }

    #[test]
    fn aggregate_with_only_limit_after_is_valid() {
        let pipeline = make_pipeline(vec![aggregate_stage(), limit_stage(5)]);
        assert!(validate_pipeline(&pipeline).is_ok());
    }
}
