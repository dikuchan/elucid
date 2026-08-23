use std::collections::BTreeSet;

use elucid_metastore::{MetastoreErrorCode, install};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Executor as _, Row as _};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner as _;

#[tokio::test]
#[ignore = "requires Docker"]
async fn embedded_migration_installs_the_complete_contract_and_restarts() {
    let container = Postgres::default()
        .start()
        .await
        .expect("start PostgreSQL container");
    let host = container.get_host().await.expect("PostgreSQL host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("PostgreSQL port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

    let first_pool = connect(&url).await;
    install(&first_pool).await.expect("install fresh metastore");
    first_pool.close().await;

    let pool = connect(&url).await;
    install(&pool)
        .await
        .expect("restart against an unchanged metastore");

    assert_table_contract(&pool).await;
    assert_index_contract(&pool).await;
    install_valid_catalog(&pool).await;
    install_valid_storage_state(&pool).await;
    assert_constraints_reject_corrupt_state(&pool).await;
    assert_checksum_mismatch_has_stable_error(&pool).await;
}

async fn connect(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .connect(url)
        .await
        .expect("connect to PostgreSQL")
}

async fn assert_table_contract(pool: &PgPool) {
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() ORDER BY tablename",
    )
    .fetch_all(pool)
    .await
    .expect("load public tables");
    assert_eq!(
        tables,
        [
            "_sqlx_migrations",
            "compaction_runs",
            "ingestion_profile_revisions",
            "inputs",
            "query_executions",
            "schema_versions",
            "segments",
            "sources",
            "stored_objects",
        ]
    );

    let applied = sqlx::query("SELECT version, description, success FROM _sqlx_migrations")
        .fetch_all(pool)
        .await
        .expect("load SQLx migration ledger");
    assert_eq!(applied.len(), 3);
    assert_eq!(applied[0].get::<i64, _>("version"), 1);
    assert_eq!(applied[0].get::<String, _>("description"), "control plane");
    assert!(applied[0].get::<bool, _>("success"));
    assert_eq!(applied[1].get::<i64, _>("version"), 2);
    assert_eq!(
        applied[1].get::<String, _>("description"),
        "query executions"
    );
    assert!(applied[1].get::<bool, _>("success"));
    assert_eq!(applied[2].get::<i64, _>("version"), 3);
    assert_eq!(
        applied[2].get::<String, _>("description"),
        "retention expiration"
    );
    assert!(applied[2].get::<bool, _>("success"));
}

async fn assert_index_contract(pool: &PgPool) {
    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()",
    )
    .fetch_all(pool)
    .await
    .expect("load public indexes")
    .into_iter()
    .collect::<BTreeSet<_>>();
    let required = [
        "compaction_runs_by_state_and_update",
        "query_executions_recent",
        "segments_active_query",
        "segments_by_claimed_run",
        "segments_by_producing_run",
        "segments_compaction_candidates",
        "segments_retention_candidates",
        "segments_terminal_reclamation",
        "stored_objects_by_delete_request",
        "stored_objects_by_retention_deadline",
        "stored_objects_by_state",
        "stored_objects_dead_letter_owner_key",
        "stored_objects_segment_owner_key",
    ];
    for name in required {
        assert!(indexes.contains(name), "missing access index {name}");
    }
}

async fn install_valid_catalog(pool: &PgPool) {
    let mut transaction = pool.begin().await.expect("begin catalog transaction");
    transaction
        .execute(
            r#"
            INSERT INTO sources (
                source_id, name, display_name, active_schema_id
            ) VALUES (
                '01980000-0000-7000-8000-000000000001',
                'security_events',
                'Security events',
                '01980000-0000-7000-8000-000000000002'
            )
            "#,
        )
        .await
        .expect("insert source before its deferred active schema");
    transaction
        .execute(
            r#"
            INSERT INTO schema_versions (
                schema_id, source_id, version, definition
            ) VALUES (
                '01980000-0000-7000-8000-000000000002',
                '01980000-0000-7000-8000-000000000001',
                1,
                '{"fields":[]}'::jsonb
            )
            "#,
        )
        .await
        .expect("insert active schema");
    transaction
        .execute(
            r#"
            INSERT INTO inputs (
                input_id, source_id, name, active_profile_revision_id
            ) VALUES (
                '01980000-0000-7000-8000-000000000003',
                '01980000-0000-7000-8000-000000000001',
                'vector',
                '01980000-0000-7000-8000-000000000004'
            )
            "#,
        )
        .await
        .expect("insert input before its deferred active profile");
    transaction
        .execute(
            r#"
            INSERT INTO ingestion_profile_revisions (
                profile_revision_id, input_id, source_id, revision,
                target_schema_id, definition
            ) VALUES (
                '01980000-0000-7000-8000-000000000004',
                '01980000-0000-7000-8000-000000000003',
                '01980000-0000-7000-8000-000000000001',
                1,
                '01980000-0000-7000-8000-000000000002',
                '{"mappings":[]}'::jsonb
            )
            "#,
        )
        .await
        .expect("insert active profile");
    transaction.commit().await.expect("commit valid catalog");

    let mut second_source = pool.begin().await.expect("begin second source");
    second_source
        .execute(
            r#"
            INSERT INTO sources (
                source_id, name, display_name, active_schema_id
            ) VALUES (
                '01980000-0000-7000-8000-000000000011',
                'audit_events',
                'Audit events',
                '01980000-0000-7000-8000-000000000012'
            )
            "#,
        )
        .await
        .expect("insert second source");
    second_source
        .execute(
            r#"
            INSERT INTO schema_versions (
                schema_id, source_id, version, definition
            ) VALUES (
                '01980000-0000-7000-8000-000000000012',
                '01980000-0000-7000-8000-000000000011',
                1,
                '{"fields":[]}'::jsonb
            )
            "#,
        )
        .await
        .expect("insert second schema");
    second_source.commit().await.expect("commit second source");
}

async fn install_valid_storage_state(pool: &PgPool) {
    pool.execute(
        r#"
        INSERT INTO segments (
            segment_id, source_id, schema_id, origin, event_day,
            minimum_event_time, maximum_event_time,
            minimum_ingestion_time, maximum_ingestion_time,
            row_count, uncompressed_bytes, state
        ) VALUES (
            '01980000-0000-7000-8000-000000000021',
            '01980000-0000-7000-8000-000000000001',
            '01980000-0000-7000-8000-000000000002',
            'INGESTION',
            DATE '2026-08-19',
            TIMESTAMPTZ '2026-08-19 00:00:00Z',
            TIMESTAMPTZ '2026-08-19 23:59:59Z',
            TIMESTAMPTZ '2026-08-19 00:00:01Z',
            TIMESTAMPTZ '2026-08-19 23:59:59Z',
            2,
            128,
            'PREPARED'
        )
        "#,
    )
    .await
    .expect("insert prepared ingestion segment");
    pool.execute(
        r#"
        INSERT INTO stored_objects (
            object_id, kind, segment_id, object_key, expected_byte_size,
            blake3_digest, media_type, format_version, state
        ) VALUES (
            '01980000-0000-7000-8000-000000000022',
            'PARQUET_DATA',
            '01980000-0000-7000-8000-000000000021',
            'segments/21/22.parquet',
            64,
            decode(repeat('12', 32), 'hex'),
            'application/vnd.apache.parquet',
            1,
            'PLANNED'
        )
        "#,
    )
    .await
    .expect("insert planned Parquet object");
    pool.execute(
        r#"
        INSERT INTO stored_objects (
            object_id, kind, input_id, batch_id, object_key,
            expected_byte_size, blake3_digest, media_type,
            format_version, state
        ) VALUES (
            '01980000-0000-7000-8000-000000000023',
            'DEAD_LETTER',
            '01980000-0000-7000-8000-000000000003',
            '01980000-0000-7000-8000-000000000024',
            'dead-letters/24/23.ndjson',
            32,
            decode(repeat('34', 32), 'hex'),
            'application/x-ndjson',
            1,
            'PLANNED'
        )
        "#,
    )
    .await
    .expect("insert planned dead-letter object");
}

async fn assert_constraints_reject_corrupt_state(pool: &PgPool) {
    let cases = [
        RejectedCase {
            name: "active schema from another source",
            constraint: "sources_active_schema_owner_fkey",
            statement: r#"
                UPDATE sources
                SET active_schema_id = '01980000-0000-7000-8000-000000000012'
                WHERE source_id = '01980000-0000-7000-8000-000000000001'
            "#,
        },
        RejectedCase {
            name: "profile targeting another source schema",
            constraint: "ingestion_profile_revisions_target_schema_owner_fkey",
            statement: r#"
                INSERT INTO ingestion_profile_revisions (
                    profile_revision_id, input_id, source_id, revision,
                    target_schema_id, definition
                ) VALUES (
                    '01980000-0000-7000-8000-000000000031',
                    '01980000-0000-7000-8000-000000000003',
                    '01980000-0000-7000-8000-000000000001',
                    2,
                    '01980000-0000-7000-8000-000000000012',
                    '{}'::jsonb
                )
            "#,
        },
        RejectedCase {
            name: "empty segment",
            constraint: "segments_positive_counts_check",
            statement: r#"
                INSERT INTO segments (
                    segment_id, source_id, schema_id, origin, event_day,
                    minimum_event_time, maximum_event_time,
                    minimum_ingestion_time, maximum_ingestion_time,
                    row_count, uncompressed_bytes, state
                ) VALUES (
                    '01980000-0000-7000-8000-000000000032',
                    '01980000-0000-7000-8000-000000000001',
                    '01980000-0000-7000-8000-000000000002',
                    'INGESTION', DATE '2026-08-19',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    0, 1, 'PREPARED'
                )
            "#,
        },
        RejectedCase {
            name: "active segment without publication state",
            constraint: "segments_lifecycle_check",
            statement: r#"
                INSERT INTO segments (
                    segment_id, source_id, schema_id, origin, event_day,
                    minimum_event_time, maximum_event_time,
                    minimum_ingestion_time, maximum_ingestion_time,
                    row_count, uncompressed_bytes, state
                ) VALUES (
                    '01980000-0000-7000-8000-000000000033',
                    '01980000-0000-7000-8000-000000000001',
                    '01980000-0000-7000-8000-000000000002',
                    'INGESTION', DATE '2026-08-19',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    1, 1, 'ACTIVE'
                )
            "#,
        },
        RejectedCase {
            name: "compaction output without producer",
            constraint: "segments_origin_check",
            statement: r#"
                INSERT INTO segments (
                    segment_id, source_id, schema_id, origin, event_day,
                    minimum_event_time, maximum_event_time,
                    minimum_ingestion_time, maximum_ingestion_time,
                    row_count, uncompressed_bytes, state
                ) VALUES (
                    '01980000-0000-7000-8000-000000000034',
                    '01980000-0000-7000-8000-000000000001',
                    '01980000-0000-7000-8000-000000000002',
                    'COMPACTION', DATE '2026-08-19',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    TIMESTAMPTZ '2026-08-19 00:00:00Z',
                    1, 1, 'PREPARED'
                )
            "#,
        },
        RejectedCase {
            name: "object with mixed owners",
            constraint: "stored_objects_owner_check",
            statement: r#"
                INSERT INTO stored_objects (
                    object_id, kind, segment_id, input_id, batch_id,
                    object_key, expected_byte_size, blake3_digest,
                    media_type, format_version, state
                ) VALUES (
                    '01980000-0000-7000-8000-000000000035',
                    'PARQUET_DATA',
                    '01980000-0000-7000-8000-000000000021',
                    '01980000-0000-7000-8000-000000000003',
                    '01980000-0000-7000-8000-000000000036',
                    'invalid/mixed-owner', 1,
                    decode(repeat('56', 32), 'hex'),
                    'application/octet-stream', 1, 'PLANNED'
                )
            "#,
        },
        RejectedCase {
            name: "invalid BLAKE3 digest",
            constraint: "stored_objects_digest_check",
            statement: r#"
                INSERT INTO stored_objects (
                    object_id, kind, segment_id, object_key,
                    expected_byte_size, blake3_digest, media_type,
                    format_version, state
                ) VALUES (
                    '01980000-0000-7000-8000-000000000037',
                    'PARQUET_DATA',
                    '01980000-0000-7000-8000-000000000021',
                    'invalid/digest', 1, '\x00'::bytea,
                    'application/octet-stream', 1, 'PLANNED'
                )
            "#,
        },
        RejectedCase {
            name: "duplicate dead-letter owner",
            constraint: "stored_objects_dead_letter_owner_key",
            statement: r#"
                INSERT INTO stored_objects (
                    object_id, kind, input_id, batch_id, object_key,
                    expected_byte_size, blake3_digest, media_type,
                    format_version, state
                ) VALUES (
                    '01980000-0000-7000-8000-000000000038',
                    'DEAD_LETTER',
                    '01980000-0000-7000-8000-000000000003',
                    '01980000-0000-7000-8000-000000000024',
                    'invalid/duplicate-dead-letter', 1,
                    decode(repeat('78', 32), 'hex'),
                    'application/x-ndjson', 1, 'PLANNED'
                )
            "#,
        },
    ];

    for case in cases {
        let error = pool.execute(case.statement).await.expect_err(case.name);
        let database = error
            .as_database_error()
            .unwrap_or_else(|| panic!("{} did not return a database error: {error}", case.name));
        assert_eq!(
            database.constraint(),
            Some(case.constraint),
            "wrong constraint rejected {}",
            case.name
        );
    }
}

async fn assert_checksum_mismatch_has_stable_error(pool: &PgPool) {
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 1")
        .bind(vec![0_u8; 32])
        .execute(pool)
        .await
        .expect("corrupt migration checksum for the failure-path check");

    let error = install(pool)
        .await
        .expect_err("checksum mismatch must fail installation");
    assert_eq!(error.code(), MetastoreErrorCode::MigrationFailed);
    assert_eq!(error.code().as_str(), "METASTORE_MIGRATION_FAILED");
    assert_eq!(error.to_string(), "metastore migration failed");
}

struct RejectedCase {
    name: &'static str,
    constraint: &'static str,
    statement: &'static str,
}
