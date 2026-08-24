CREATE INDEX compaction_runs_terminal_cleanup
    ON compaction_runs (completed_at, compaction_run_id)
    WHERE state IN ('COMMITTED', 'FAILED');
