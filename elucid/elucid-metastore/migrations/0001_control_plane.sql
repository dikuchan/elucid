CREATE TABLE sources (
    source_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    active_schema_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sources_name_key UNIQUE (name),
    CONSTRAINT sources_name_check CHECK (
        name ~ '^[A-Za-z_][A-Za-z0-9_]*$'
        AND name !~ '^@'
    ),
    CONSTRAINT sources_timestamps_check CHECK (created_at <= updated_at)
);

CREATE TABLE schema_versions (
    schema_id UUID PRIMARY KEY,
    source_id UUID NOT NULL,
    version BIGINT NOT NULL,
    definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT schema_versions_source_version_key UNIQUE (source_id, version),
    CONSTRAINT schema_versions_source_identity_key UNIQUE (source_id, schema_id),
    CONSTRAINT schema_versions_version_check CHECK (version > 0),
    CONSTRAINT schema_versions_definition_check CHECK (jsonb_typeof(definition) = 'object'),
    CONSTRAINT schema_versions_source_fkey FOREIGN KEY (source_id)
        REFERENCES sources (source_id)
        DEFERRABLE INITIALLY DEFERRED
);

ALTER TABLE sources
    ADD CONSTRAINT sources_active_schema_owner_fkey
    FOREIGN KEY (source_id, active_schema_id)
    REFERENCES schema_versions (source_id, schema_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE inputs (
    input_id UUID PRIMARY KEY,
    source_id UUID NOT NULL,
    name TEXT NOT NULL,
    active_profile_revision_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT inputs_source_name_key UNIQUE (source_id, name),
    CONSTRAINT inputs_identity_source_key UNIQUE (input_id, source_id),
    CONSTRAINT inputs_name_check CHECK (
        name ~ '^[A-Za-z_][A-Za-z0-9_]*$'
        AND name !~ '^@'
    ),
    CONSTRAINT inputs_timestamps_check CHECK (created_at <= updated_at),
    CONSTRAINT inputs_source_fkey FOREIGN KEY (source_id)
        REFERENCES sources (source_id)
);

CREATE TABLE ingestion_profile_revisions (
    profile_revision_id UUID PRIMARY KEY,
    input_id UUID NOT NULL,
    source_id UUID NOT NULL,
    revision BIGINT NOT NULL,
    target_schema_id UUID NOT NULL,
    definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ingestion_profile_revisions_input_revision_key UNIQUE (input_id, revision),
    CONSTRAINT ingestion_profile_revisions_owner_identity_key
        UNIQUE (input_id, source_id, profile_revision_id),
    CONSTRAINT ingestion_profile_revisions_revision_check CHECK (revision > 0),
    CONSTRAINT ingestion_profile_revisions_definition_check
        CHECK (jsonb_typeof(definition) = 'object'),
    CONSTRAINT ingestion_profile_revisions_input_owner_fkey
        FOREIGN KEY (input_id, source_id)
        REFERENCES inputs (input_id, source_id)
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT ingestion_profile_revisions_target_schema_owner_fkey
        FOREIGN KEY (source_id, target_schema_id)
        REFERENCES schema_versions (source_id, schema_id)
        DEFERRABLE INITIALLY DEFERRED
);

ALTER TABLE inputs
    ADD CONSTRAINT inputs_active_profile_owner_fkey
    FOREIGN KEY (input_id, source_id, active_profile_revision_id)
    REFERENCES ingestion_profile_revisions (input_id, source_id, profile_revision_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE compaction_runs (
    compaction_run_id UUID PRIMARY KEY,
    source_id UUID NOT NULL,
    schema_id UUID NOT NULL,
    event_day DATE NOT NULL,
    state TEXT NOT NULL,
    failure_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    CONSTRAINT compaction_runs_owner_identity_key
        UNIQUE (compaction_run_id, source_id, schema_id, event_day),
    CONSTRAINT compaction_runs_schema_owner_fkey
        FOREIGN KEY (source_id, schema_id)
        REFERENCES schema_versions (source_id, schema_id),
    CONSTRAINT compaction_runs_state_check CHECK (
        state IN ('BUILDING', 'UPLOADING', 'COMMITTED', 'FAILED')
    ),
    CONSTRAINT compaction_runs_failure_code_check CHECK (
        failure_code IS NULL
        OR (octet_length(failure_code) BETWEEN 1 AND 128)
    ),
    CONSTRAINT compaction_runs_lifecycle_check CHECK (
        (state IN ('BUILDING', 'UPLOADING') AND failure_code IS NULL AND completed_at IS NULL)
        OR (state = 'COMMITTED' AND failure_code IS NULL AND completed_at IS NOT NULL)
        OR (state = 'FAILED' AND failure_code IS NOT NULL AND completed_at IS NOT NULL)
    ),
    CONSTRAINT compaction_runs_timestamps_check CHECK (
        created_at <= updated_at
        AND (completed_at IS NULL OR created_at <= completed_at)
    )
);

CREATE TABLE segments (
    segment_id UUID PRIMARY KEY,
    source_id UUID NOT NULL,
    schema_id UUID NOT NULL,
    origin TEXT NOT NULL,
    produced_by_compaction_run_id UUID,
    claimed_by_compaction_run_id UUID,
    event_day DATE NOT NULL,
    minimum_event_time TIMESTAMPTZ NOT NULL,
    maximum_event_time TIMESTAMPTZ NOT NULL,
    minimum_ingestion_time TIMESTAMPTZ NOT NULL,
    maximum_ingestion_time TIMESTAMPTZ NOT NULL,
    row_count BIGINT NOT NULL,
    uncompressed_bytes BIGINT NOT NULL,
    data_expires_at TIMESTAMPTZ,
    state TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ,
    reclaim_after TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT segments_schema_owner_fkey
        FOREIGN KEY (source_id, schema_id)
        REFERENCES schema_versions (source_id, schema_id),
    CONSTRAINT segments_producing_run_owner_fkey
        FOREIGN KEY (
            produced_by_compaction_run_id, source_id, schema_id, event_day
        ) REFERENCES compaction_runs (
            compaction_run_id, source_id, schema_id, event_day
        ),
    CONSTRAINT segments_claimed_run_owner_fkey
        FOREIGN KEY (
            claimed_by_compaction_run_id, source_id, schema_id, event_day
        ) REFERENCES compaction_runs (
            compaction_run_id, source_id, schema_id, event_day
        ),
    CONSTRAINT segments_origin_check CHECK (
        (origin = 'INGESTION' AND produced_by_compaction_run_id IS NULL)
        OR (origin = 'COMPACTION' AND produced_by_compaction_run_id IS NOT NULL)
    ),
    CONSTRAINT segments_state_check CHECK (
        state IN ('PREPARED', 'ACTIVE', 'SUPERSEDED', 'EXPIRED', 'ABANDONED')
    ),
    CONSTRAINT segments_positive_counts_check CHECK (
        row_count > 0 AND uncompressed_bytes > 0
    ),
    CONSTRAINT segments_ordered_bounds_check CHECK (
        minimum_event_time <= maximum_event_time
        AND minimum_ingestion_time <= maximum_ingestion_time
    ),
    CONSTRAINT segments_event_day_check CHECK (
        (minimum_event_time AT TIME ZONE 'UTC')::DATE = event_day
        AND (maximum_event_time AT TIME ZONE 'UTC')::DATE = event_day
    ),
    CONSTRAINT segments_claim_state_check CHECK (
        claimed_by_compaction_run_id IS NULL
        OR state IN ('ACTIVE', 'SUPERSEDED')
    ),
    CONSTRAINT segments_lifecycle_check CHECK (
        (
            state = 'PREPARED'
            AND published_at IS NULL
            AND retired_at IS NULL
            AND reclaim_after IS NULL
        )
        OR (
            state = 'ACTIVE'
            AND data_expires_at IS NOT NULL
            AND published_at IS NOT NULL
            AND retired_at IS NULL
            AND reclaim_after IS NULL
        )
        OR (
            state = 'SUPERSEDED'
            AND data_expires_at IS NOT NULL
            AND published_at IS NOT NULL
            AND retired_at IS NOT NULL
            AND reclaim_after IS NOT NULL
            AND claimed_by_compaction_run_id IS NOT NULL
        )
        OR (
            state = 'EXPIRED'
            AND data_expires_at IS NOT NULL
            AND published_at IS NOT NULL
            AND retired_at IS NOT NULL
            AND reclaim_after IS NOT NULL
            AND claimed_by_compaction_run_id IS NULL
        )
        OR (
            state = 'ABANDONED'
            AND published_at IS NULL
            AND retired_at IS NOT NULL
            AND reclaim_after IS NOT NULL
            AND claimed_by_compaction_run_id IS NULL
        )
    ),
    CONSTRAINT segments_timestamps_check CHECK (
        created_at <= updated_at
        AND (published_at IS NULL OR created_at <= published_at)
        AND (retired_at IS NULL OR COALESCE(published_at, created_at) <= retired_at)
        AND (reclaim_after IS NULL OR retired_at <= reclaim_after)
        AND (data_expires_at IS NULL OR published_at IS NULL OR published_at <= data_expires_at)
    )
);

CREATE TABLE stored_objects (
    object_id UUID PRIMARY KEY,
    kind TEXT NOT NULL,
    segment_id UUID,
    input_id UUID,
    batch_id UUID,
    object_key TEXT NOT NULL,
    expected_byte_size BIGINT NOT NULL,
    blake3_digest BYTEA NOT NULL,
    media_type TEXT NOT NULL,
    format_version BIGINT NOT NULL,
    state TEXT NOT NULL,
    uploaded_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    retention_deadline TIMESTAMPTZ,
    delete_requested_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT stored_objects_object_key_key UNIQUE (object_key),
    CONSTRAINT stored_objects_segment_fkey FOREIGN KEY (segment_id)
        REFERENCES segments (segment_id),
    CONSTRAINT stored_objects_input_fkey FOREIGN KEY (input_id)
        REFERENCES inputs (input_id),
    CONSTRAINT stored_objects_kind_check CHECK (
        kind IN ('PARQUET_DATA', 'DEAD_LETTER')
    ),
    CONSTRAINT stored_objects_owner_check CHECK (
        (
            kind = 'PARQUET_DATA'
            AND segment_id IS NOT NULL
            AND input_id IS NULL
            AND batch_id IS NULL
        )
        OR (
            kind = 'DEAD_LETTER'
            AND segment_id IS NULL
            AND input_id IS NOT NULL
            AND batch_id IS NOT NULL
        )
    ),
    CONSTRAINT stored_objects_key_check CHECK (
        octet_length(object_key) BETWEEN 1 AND 2048
    ),
    CONSTRAINT stored_objects_size_check CHECK (expected_byte_size >= 0),
    CONSTRAINT stored_objects_digest_check CHECK (octet_length(blake3_digest) = 32),
    CONSTRAINT stored_objects_media_type_check CHECK (
        octet_length(media_type) BETWEEN 1 AND 255
    ),
    CONSTRAINT stored_objects_format_version_check CHECK (format_version > 0),
    CONSTRAINT stored_objects_state_check CHECK (
        state IN ('PLANNED', 'UPLOADED', 'PUBLISHED', 'DELETE_PENDING', 'DELETED')
    ),
    CONSTRAINT stored_objects_error_code_check CHECK (
        last_error_code IS NULL
        OR octet_length(last_error_code) BETWEEN 1 AND 128
    ),
    CONSTRAINT stored_objects_retention_check CHECK (
        (retention_deadline IS NOT NULL) = (
            kind = 'DEAD_LETTER' AND published_at IS NOT NULL
        )
    ),
    CONSTRAINT stored_objects_lifecycle_check CHECK (
        (
            state = 'PLANNED'
            AND uploaded_at IS NULL
            AND published_at IS NULL
            AND delete_requested_at IS NULL
            AND deleted_at IS NULL
        )
        OR (
            state = 'UPLOADED'
            AND uploaded_at IS NOT NULL
            AND published_at IS NULL
            AND delete_requested_at IS NULL
            AND deleted_at IS NULL
        )
        OR (
            state = 'PUBLISHED'
            AND uploaded_at IS NOT NULL
            AND published_at IS NOT NULL
            AND delete_requested_at IS NULL
            AND deleted_at IS NULL
        )
        OR (
            state = 'DELETE_PENDING'
            AND delete_requested_at IS NOT NULL
            AND deleted_at IS NULL
        )
        OR (
            state = 'DELETED'
            AND delete_requested_at IS NOT NULL
            AND deleted_at IS NOT NULL
        )
    ),
    CONSTRAINT stored_objects_timestamps_check CHECK (
        created_at <= updated_at
        AND (uploaded_at IS NULL OR created_at <= uploaded_at)
        AND (published_at IS NULL OR uploaded_at <= published_at)
        AND (
            delete_requested_at IS NULL
            OR COALESCE(published_at, uploaded_at, created_at) <= delete_requested_at
        )
        AND (deleted_at IS NULL OR delete_requested_at <= deleted_at)
    )
);

CREATE UNIQUE INDEX stored_objects_segment_owner_key
    ON stored_objects (segment_id)
    WHERE segment_id IS NOT NULL;

CREATE UNIQUE INDEX stored_objects_dead_letter_owner_key
    ON stored_objects (input_id, batch_id)
    WHERE kind = 'DEAD_LETTER';

CREATE INDEX segments_active_query
    ON segments (
        source_id,
        event_day,
        minimum_event_time,
        maximum_event_time,
        segment_id
    )
    WHERE state = 'ACTIVE';

CREATE INDEX segments_compaction_candidates
    ON segments (
        source_id,
        schema_id,
        event_day,
        uncompressed_bytes,
        published_at,
        segment_id
    )
    WHERE state = 'ACTIVE' AND claimed_by_compaction_run_id IS NULL;

CREATE INDEX segments_by_producing_run
    ON segments (produced_by_compaction_run_id, segment_id)
    WHERE produced_by_compaction_run_id IS NOT NULL;

CREATE INDEX segments_by_claimed_run
    ON segments (claimed_by_compaction_run_id, segment_id)
    WHERE claimed_by_compaction_run_id IS NOT NULL;

CREATE INDEX segments_terminal_reclamation
    ON segments (reclaim_after, segment_id)
    WHERE state IN ('SUPERSEDED', 'EXPIRED', 'ABANDONED');

CREATE INDEX stored_objects_by_state
    ON stored_objects (state, updated_at, object_id);

CREATE INDEX stored_objects_by_retention_deadline
    ON stored_objects (retention_deadline, object_id)
    WHERE state = 'PUBLISHED' AND retention_deadline IS NOT NULL;

CREATE INDEX stored_objects_by_delete_request
    ON stored_objects (delete_requested_at, object_id)
    WHERE state = 'DELETE_PENDING';

CREATE INDEX compaction_runs_by_state_and_update
    ON compaction_runs (state, updated_at, compaction_run_id)
    WHERE state IN ('BUILDING', 'UPLOADING');
