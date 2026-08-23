CREATE TABLE query_executions (
    query_id UUID PRIMARY KEY,
    recorded_sequence BIGINT GENERATED ALWAYS AS IDENTITY,
    query_text TEXT NOT NULL,
    start_inclusive TIMESTAMPTZ NOT NULL,
    end_exclusive TIMESTAMPTZ NOT NULL,
    output_rows NUMERIC(20, 0) NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', CURRENT_TIMESTAMP),
    CONSTRAINT query_executions_query_size_check CHECK (
        octet_length(query_text) <= 1048576
    ),
    CONSTRAINT query_executions_time_range_check CHECK (
        start_inclusive < end_exclusive
    ),
    CONSTRAINT query_executions_output_rows_check CHECK (
        output_rows BETWEEN 1 AND 18446744073709551615
    ),
    CONSTRAINT query_executions_timestamp_precision_check CHECK (
        start_inclusive = date_trunc('milliseconds', start_inclusive)
        AND end_exclusive = date_trunc('milliseconds', end_exclusive)
        AND submitted_at = date_trunc('milliseconds', submitted_at)
    )
);

CREATE INDEX query_executions_recent
    ON query_executions (recorded_sequence DESC);
