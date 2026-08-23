use std::time::Duration;

use chrono::{TimeZone as _, Utc};
use elucid_metastore::{
    MAXIMUM_RETAINED_QUERY_EXECUTIONS, NewQueryExecution, QueryExecutionId,
    QueryExecutionListLimit, QueryExecutionStore, QueryRequestTimeRange, install,
};
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ImageExt as _, runners::AsyncRunner as _};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires Docker"]
async fn recent_query_executions_are_durable_ordered_and_bounded() {
    let container = Postgres::default()
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("start PostgreSQL container");
    let host = container.get_host().await.expect("PostgreSQL host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("PostgreSQL port");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&format!(
            "postgresql://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("connect to PostgreSQL");
    install(&pool).await.expect("install metastore");

    let store = QueryExecutionStore::new(pool.clone());
    let time_range = QueryRequestTimeRange::new(
        Utc.with_ymd_and_hms(2026, 8, 23, 12, 0, 0)
            .single()
            .expect("valid start"),
        Utc.with_ymd_and_hms(2026, 8, 23, 13, 0, 0)
            .single()
            .expect("valid end"),
    )
    .expect("ordered query range");
    let inserted = MAXIMUM_RETAINED_QUERY_EXECUTIONS + 2;
    for sequence in 0..inserted {
        let query_id = query_id(sequence);
        let output_rows = if sequence + 1 == inserted {
            u64::MAX
        } else {
            sequence + 1
        };
        let execution = NewQueryExecution::new(
            query_id,
            format!("source logs | take {}", sequence + 1),
            time_range,
            output_rows,
        )
        .expect("valid query execution");
        let recorded = store
            .record(execution)
            .await
            .expect("record query execution");
        assert_eq!(recorded.query_id(), query_id);
        assert!(
            recorded
                .submitted_at()
                .timestamp_subsec_nanos()
                .is_multiple_of(1_000_000)
        );
    }

    let row_count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_executions")
        .fetch_one(&pool)
        .await
        .expect("count retained query executions");
    assert_eq!(
        row_count,
        i64::try_from(MAXIMUM_RETAINED_QUERY_EXECUTIONS).expect("retention fits BIGINT")
    );

    let recent = store
        .recent(QueryExecutionListLimit::new(3).expect("valid list limit"))
        .await
        .expect("list recent query executions");
    assert!(recent.is_truncated());
    assert_eq!(recent.limit(), 3);
    assert_eq!(
        recent
            .items()
            .iter()
            .map(|execution| execution.query_id())
            .collect::<Vec<_>>(),
        [
            query_id(inserted - 1),
            query_id(inserted - 2),
            query_id(inserted - 3)
        ]
    );

    let restarted = QueryExecutionStore::new(pool.clone());
    let retained = restarted
        .recent(
            QueryExecutionListLimit::new(MAXIMUM_RETAINED_QUERY_EXECUTIONS)
                .expect("retention is a valid list limit"),
        )
        .await
        .expect("list query executions after restart");
    assert!(!retained.is_truncated());
    assert_eq!(
        retained.items().first().expect("newest execution").query(),
        format!("source logs | take {inserted}")
    );
    assert_eq!(
        retained
            .items()
            .first()
            .expect("newest execution")
            .output_rows(),
        u64::MAX
    );
    assert_eq!(
        retained
            .items()
            .last()
            .expect("oldest execution")
            .query_id(),
        query_id(2)
    );
    assert_eq!(
        retained
            .items()
            .first()
            .expect("newest execution")
            .time_range(),
        time_range
    );
}

fn query_id(sequence: u64) -> QueryExecutionId {
    QueryExecutionId::from(Uuid::from_u128(u128::from(sequence) + 1))
}
