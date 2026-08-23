CREATE INDEX segments_retention_candidates
    ON segments (data_expires_at, segment_id)
    WHERE state = 'ACTIVE' AND claimed_by_compaction_run_id IS NULL;
