use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use elucid_service::{
    AddressingStyle, ConfigurationErrorCode, ConfigurationViolation, Environment, LogFormat,
    MAXIMUM_CONFIGURATION_DOCUMENT_BYTES, NetworkTrust, RuntimeConfiguration, RuntimeRole,
    SecretKind,
};

const ACCEPTANCE_PROFILE: &str = include_str!("fixtures/runtime.toml");
const CURSOR_HMAC_KEY: &str = "cursor-hmac-key-with-at-least-32-bytes";
const POSTGRESQL_DSN: &str = "postgresql://elucid:postgresql-secret@postgres.example/elucid";
const S3_ACCESS_KEY_ID: &str = "elucid-access-key";
const S3_SECRET_ACCESS_KEY: &str = "elucid-s3-secret";

#[test]
fn acceptance_profile_materializes_typed_configuration_and_redacts_secrets() {
    let configuration = RuntimeConfiguration::from_toml(ACCEPTANCE_PROFILE, &environment())
        .expect("the specification acceptance profile is valid");

    assert_eq!(
        configuration.server().bind(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080)
    );
    assert_eq!(
        configuration.server().network_trust(),
        NetworkTrust::LoopbackOnly
    );
    assert!(
        configuration
            .server()
            .roles()
            .contains(RuntimeRole::Serving)
    );
    assert!(
        configuration
            .server()
            .roles()
            .contains(RuntimeRole::Maintenance)
    );
    assert_eq!(
        configuration.server().maximum_request_header_bytes().get(),
        32_768
    );
    assert_eq!(configuration.metastore().maximum_connections().get(), 10);
    assert_eq!(
        configuration.object_store().addressing_style(),
        AddressingStyle::Path
    );
    assert_eq!(
        configuration.ingestion().staging_directory().to_str(),
        Some("/var/lib/elucid/staging")
    );
    assert_eq!(configuration.query().default_output_rows().get(), 1_000);
    assert_eq!(configuration.telemetry().log_format(), LogFormat::Json);
    assert_eq!(
        configuration.secrets().postgresql_dsn().expose_secret(),
        POSTGRESQL_DSN
    );

    let rendered = format!("{configuration:?}");
    for secret in [
        CURSOR_HMAC_KEY,
        POSTGRESQL_DSN,
        S3_ACCESS_KEY_ID,
        S3_SECRET_ACCESS_KEY,
    ] {
        assert!(!rendered.contains(secret));
    }
    assert!(rendered.contains("[REDACTED]"));
}

#[test]
fn environment_overrides_file_values_and_direct_secrets() {
    let direct_postgresql_dsn = "postgresql://elucid:direct-secret@postgres.example/overridden";
    let mut environment = environment();
    environment.set("ELUCID_SERVER__NETWORK_TRUST", "TRUSTED_NETWORK");
    environment.set("ELUCID_SERVER__ROLES", "[\"SERVING\"]");
    environment.set("ELUCID_SERVER__BROWSER_ORIGIN", "https://elucid.example");
    environment.set(
        "ELUCID_SERVER__OPERATOR_BEARER_TOKEN_ENVIRONMENT_VARIABLE",
        "ELUCID_OPERATOR_BEARER_TOKEN",
    );
    environment.set(
        "ELUCID_OPERATOR_BEARER_TOKEN",
        "operator-bearer-token-with-32-visible-bytes",
    );
    environment.set("ELUCID_QUERY__DEFAULT_OUTPUT_ROWS", "42");
    environment.set("ELUCID_METASTORE__POSTGRESQL_DSN", direct_postgresql_dsn);

    let configuration = RuntimeConfiguration::from_toml(ACCEPTANCE_PROFILE, &environment)
        .expect("environment overrides are valid");

    assert_eq!(
        configuration.server().network_trust(),
        NetworkTrust::TrustedNetwork
    );
    assert_eq!(
        configuration.server().roles().as_slice(),
        &[RuntimeRole::Serving]
    );
    assert_eq!(configuration.query().default_output_rows().get(), 42);
    assert_eq!(
        configuration.secrets().postgresql_dsn().expose_secret(),
        direct_postgresql_dsn
    );
    assert_eq!(
        configuration
            .secrets()
            .operator_bearer_token()
            .expect("trusted network has an operator token")
            .expose_secret(),
        "operator-bearer-token-with-32-visible-bytes"
    );
    assert!(!format!("{configuration:?}").contains(direct_postgresql_dsn));
}

#[test]
fn failures_distinguish_sources_and_never_echo_secret_values() {
    let temporary_directory = tempfile::tempdir().expect("temporary directory is available");
    let missing_file = temporary_directory.path().join("missing.toml");
    let unreadable =
        RuntimeConfiguration::load_with_environment(Some(missing_file.as_path()), &environment())
            .expect_err("a supplied missing file is unreadable");
    assert_eq!(unreadable.code(), ConfigurationErrorCode::FileUnreadable);

    let malformed = RuntimeConfiguration::from_toml("[server", &environment())
        .expect_err("invalid TOML syntax is malformed");
    assert_eq!(malformed.code(), ConfigurationErrorCode::DocumentMalformed);

    let mut missing_secret_environment = environment();
    missing_secret_environment.remove("ELUCID_CURSOR_HMAC_KEY");
    let missing_secret =
        RuntimeConfiguration::from_toml(ACCEPTANCE_PROFILE, &missing_secret_environment)
            .expect_err("a referenced secret must exist");
    assert_eq!(missing_secret.code(), ConfigurationErrorCode::SecretMissing);
    assert_eq!(
        missing_secret.secret_kind(),
        Some(SecretKind::CursorHmacKey)
    );

    let invalid_secret_value = "far-too-short";
    let mut invalid_secret_environment = environment();
    invalid_secret_environment.set("ELUCID_CURSOR_HMAC_KEY", invalid_secret_value);
    let invalid_secret =
        RuntimeConfiguration::from_toml(ACCEPTANCE_PROFILE, &invalid_secret_environment)
            .expect_err("a short cursor key is invalid");
    assert_eq!(invalid_secret.code(), ConfigurationErrorCode::SecretInvalid);
    assert!(!format!("{invalid_secret:?}").contains(invalid_secret_value));
    assert!(!invalid_secret.to_string().contains(invalid_secret_value));
}

#[test]
fn cross_field_validation_enforces_the_normative_profile() {
    let cases = [
        violation_case(
            "empty roles",
            &[("roles = [\"SERVING\", \"MAINTENANCE\"]", "roles = []")],
            ConfigurationViolation::EmptyRuntimeRoles,
        ),
        violation_case(
            "duplicate roles",
            &[(
                "roles = [\"SERVING\", \"MAINTENANCE\"]",
                "roles = [\"SERVING\", \"SERVING\"]",
            )],
            ConfigurationViolation::DuplicateRuntimeRole {
                role: RuntimeRole::Serving,
            },
        ),
        violation_case(
            "page default",
            &[("default_page_items = 50", "default_page_items = 201")],
            ConfigurationViolation::DefaultPageItemsExceedMaximum,
        ),
        violation_case(
            "query row default",
            &[("default_output_rows = 1000", "default_output_rows = 10001")],
            ConfigurationViolation::DefaultOutputRowsExceedMaximum,
        ),
        violation_case(
            "ingestion stale threshold",
            &[(
                "attempt_stale_after_seconds = 30",
                "attempt_stale_after_seconds = 14",
            )],
            ConfigurationViolation::IngestionStaleThresholdTooSmall,
        ),
        violation_case(
            "compaction stale threshold",
            &[(
                "run_stale_after_seconds = 30",
                "run_stale_after_seconds = 14",
            )],
            ConfigurationViolation::CompactionStaleThresholdTooSmall,
        ),
        violation_case(
            "browser origin scheme",
            &[(
                "browser_origin = \"http://127.0.0.1:8080\"",
                "browser_origin = \"ftp://127.0.0.1:8080\"",
            )],
            ConfigurationViolation::BrowserOriginSchemeUnsupported,
        ),
        violation_case(
            "record limit",
            &[(
                "maximum_record_bytes = 10485760",
                "maximum_record_bytes = 16777217",
            )],
            ConfigurationViolation::MaximumRecordBytesExceedRequestBody,
        ),
        violation_case(
            "dead letter page",
            &[(
                "maximum_dead_letter_page_bytes = 4194304",
                "maximum_dead_letter_page_bytes = 65536",
            )],
            ConfigurationViolation::DeadLetterPageBytesDoNotExceedCompleteRaw,
        ),
        violation_case(
            "query lifetime",
            &[(
                "execution_timeout_seconds = 30",
                "execution_timeout_seconds = 31",
            )],
            ConfigurationViolation::QueryTimeoutExceedsSnapshotLifetime,
        ),
        violation_case(
            "retired object grace period",
            &[(
                "retired_object_grace_period_seconds = 60",
                "retired_object_grace_period_seconds = 30",
            )],
            ConfigurationViolation::RetiredObjectGracePeriodDoesNotExceedSnapshotLifetime,
        ),
        violation_case(
            "orphan grace period",
            &[(
                "orphan_grace_period_seconds = 3600",
                "orphan_grace_period_seconds = 990",
            )],
            ConfigurationViolation::OrphanGracePeriodTooShort,
        ),
        violation_case(
            "minimum input segments below two",
            &[("minimum_input_segments = 2", "minimum_input_segments = 1")],
            ConfigurationViolation::MinimumInputSegmentsBelowTwo,
        ),
        violation_case(
            "minimum input segments above maximum",
            &[("minimum_input_segments = 2", "minimum_input_segments = 33")],
            ConfigurationViolation::MinimumInputSegmentsExceedMaximum,
        ),
        violation_case(
            "output segments not below inputs",
            &[(
                "maximum_output_segments = 16",
                "maximum_output_segments = 32",
            )],
            ConfigurationViolation::MaximumOutputSegmentsNotBelowMaximumInputSegments,
        ),
        violation_case(
            "output object exceeds total",
            &[(
                "maximum_output_parquet_object_bytes = 536870912",
                "maximum_output_parquet_object_bytes = 2147483649",
            )],
            ConfigurationViolation::MaximumOutputObjectBytesExceedTotal,
        ),
        violation_case(
            "local compaction concurrency",
            &[("maximum_concurrent_runs = 1", "maximum_concurrent_runs = 5")],
            ConfigurationViolation::LocalCompactionConcurrencyExceedsCluster,
        ),
        violation_case(
            "retention task duration",
            &[(
                "maximum_task_duration_seconds = 30",
                "maximum_task_duration_seconds = 60",
            )],
            ConfigurationViolation::RetentionTaskDurationNotBelowScanInterval,
        ),
        violation_case(
            "attempt timeout",
            &[(
                "attempt_timeout_seconds = 900",
                "attempt_timeout_seconds = 86400",
            )],
            ConfigurationViolation::AttemptTimeoutNotBelowIdempotencyRetention,
        ),
        violation_case(
            "idempotency stale threshold",
            &[
                (
                    "idempotency_retention_seconds = 86400",
                    "idempotency_retention_seconds = 30",
                ),
                (
                    "attempt_timeout_seconds = 900",
                    "attempt_timeout_seconds = 29",
                ),
            ],
            ConfigurationViolation::IdempotencyRetentionDoesNotExceedAttemptStale,
        ),
        violation_case(
            "idempotency provenance",
            &[(
                "idempotency_retention_seconds = 86400",
                "idempotency_retention_seconds = 2592001",
            )],
            ConfigurationViolation::IdempotencyRetentionExceedsIngestProvenance,
        ),
        violation_case(
            "event provenance",
            &[(
                "event_data_retention_seconds = 2592000",
                "event_data_retention_seconds = 2592001",
            )],
            ConfigurationViolation::EventDataRetentionExceedsIngestProvenance,
        ),
        violation_case(
            "dead letter provenance",
            &[(
                "dead_letter_retention_seconds = 604800",
                "dead_letter_retention_seconds = 2592001",
            )],
            ConfigurationViolation::DeadLetterRetentionExceedsIngestProvenance,
        ),
        violation_case(
            "ingestion staging capacity",
            &[(
                "staging_capacity_bytes = 2147483648",
                "staging_capacity_bytes = 16777215",
            )],
            ConfigurationViolation::IngestionStagingCapacityBelowMaximumRequest,
        ),
        violation_case(
            "query memory capacity",
            &[(
                "memory_pool_bytes = 536870912",
                "memory_pool_bytes = 16777215",
            )],
            ConfigurationViolation::QueryMemoryCapacityBelowMaximumResult,
        ),
        violation_case(
            "query spill capacity",
            &[(
                "spill_capacity_bytes = 2147483648",
                "spill_capacity_bytes = 16777215",
            )],
            ConfigurationViolation::QuerySpillCapacityBelowMaximumResult,
        ),
        violation_case(
            "compaction working capacity",
            &[(
                "working_capacity_bytes = 2147483648",
                "working_capacity_bytes = 2147483647",
            )],
            ConfigurationViolation::CompactionWorkingCapacityBelowConcurrentOutput,
        ),
        violation_case(
            "input row capacity",
            &[(
                "maximum_input_rows = 16000000",
                "maximum_input_rows = 16000001",
            )],
            ConfigurationViolation::MaximumInputRowsExceedOutputCapacity,
        ),
        violation_case(
            "input uncompressed capacity",
            &[(
                "maximum_input_uncompressed_bytes = 4294967296",
                "maximum_input_uncompressed_bytes = 4294967297",
            )],
            ConfigurationViolation::MaximumInputUncompressedBytesExceedOutputCapacity,
        ),
        violation_case(
            "loopback bind",
            &[("bind = \"127.0.0.1:8080\"", "bind = \"0.0.0.0:8080\"")],
            ConfigurationViolation::LoopbackTrustRequiresLoopbackBind,
        ),
        violation_case(
            "local container origin",
            &[
                (
                    "network_trust = \"LOOPBACK_ONLY\"",
                    "network_trust = \"LOCAL_CONTAINER\"",
                ),
                (
                    "browser_origin = \"http://127.0.0.1:8080\"",
                    "browser_origin = \"https://127.0.0.1:8080\"",
                ),
            ],
            ConfigurationViolation::LocalContainerRequiresHttpLoopbackOrigin,
        ),
        violation_case(
            "trusted network token reference",
            &[
                (
                    "network_trust = \"LOOPBACK_ONLY\"",
                    "network_trust = \"TRUSTED_NETWORK\"",
                ),
                (
                    "browser_origin = \"http://127.0.0.1:8080\"",
                    "browser_origin = \"https://elucid.example\"",
                ),
            ],
            ConfigurationViolation::TrustedNetworkRequiresOperatorSecretReference,
        ),
        violation_case(
            "unexpected token reference",
            &[(
                "cursor_hmac_key_environment_variable = \"ELUCID_CURSOR_HMAC_KEY\"",
                "cursor_hmac_key_environment_variable = \"ELUCID_CURSOR_HMAC_KEY\"\noperator_bearer_token_environment_variable = \"ELUCID_OPERATOR_BEARER_TOKEN\"",
            )],
            ConfigurationViolation::OperatorSecretReferenceRequiresTrustedNetwork,
        ),
        violation_case(
            "trusted network origin",
            &[
                (
                    "network_trust = \"LOOPBACK_ONLY\"",
                    "network_trust = \"TRUSTED_NETWORK\"",
                ),
                (
                    "cursor_hmac_key_environment_variable = \"ELUCID_CURSOR_HMAC_KEY\"",
                    "cursor_hmac_key_environment_variable = \"ELUCID_CURSOR_HMAC_KEY\"\noperator_bearer_token_environment_variable = \"ELUCID_OPERATOR_BEARER_TOKEN\"",
                ),
            ],
            ConfigurationViolation::TrustedNetworkRequiresHttpsOrigin,
        ),
    ];

    for case in cases {
        let error =
            RuntimeConfiguration::from_toml(&case.document, &environment()).expect_err(case.name);
        assert_eq!(
            error.violation(),
            Some(&case.expected),
            "unexpected violation for {}: {error}",
            case.name
        );
    }
}

#[test]
fn required_positive_values_and_checked_arithmetic_fail_deterministically() {
    let zero_connections =
        changed_profile(&[("maximum_connections = 10", "maximum_connections = 0")]);
    let zero_error = RuntimeConfiguration::from_toml(&zero_connections, &environment())
        .expect_err("required counts reject zero");
    assert_eq!(zero_error.code(), ConfigurationErrorCode::ValueInvalid);

    let overflow = changed_profile(&[
        (
            "maximum_concurrent_runs = 1",
            "maximum_concurrent_runs = 9223372036854775807",
        ),
        (
            "maximum_cluster_concurrent_runs = 4",
            "maximum_cluster_concurrent_runs = 9223372036854775807",
        ),
    ]);
    let overflow_error = RuntimeConfiguration::from_toml(&overflow, &environment())
        .expect_err("capacity arithmetic is checked");
    assert_eq!(
        overflow_error.code(),
        ConfigurationErrorCode::ArithmeticOverflow
    );
}

#[test]
fn oversized_configuration_documents_are_rejected_before_parsing() {
    let document = " ".repeat(MAXIMUM_CONFIGURATION_DOCUMENT_BYTES + 1);

    let error = RuntimeConfiguration::from_toml(&document, &environment())
        .expect_err("an oversized configuration document is rejected");

    assert_eq!(error.code(), ConfigurationErrorCode::DocumentTooLarge);
}

fn environment() -> Environment {
    Environment::from_pairs([
        ("ELUCID_CURSOR_HMAC_KEY", CURSOR_HMAC_KEY),
        ("ELUCID_POSTGRES_DSN", POSTGRESQL_DSN),
        ("ELUCID_S3_ACCESS_KEY_ID", S3_ACCESS_KEY_ID),
        ("ELUCID_S3_SECRET_ACCESS_KEY", S3_SECRET_ACCESS_KEY),
    ])
}

struct ViolationCase {
    name: &'static str,
    document: String,
    expected: ConfigurationViolation,
}

fn violation_case(
    name: &'static str,
    replacements: &[(&str, &str)],
    expected: ConfigurationViolation,
) -> ViolationCase {
    ViolationCase {
        name,
        document: changed_profile(replacements),
        expected,
    }
}

fn changed_profile(replacements: &[(&str, &str)]) -> String {
    replacements
        .iter()
        .fold(ACCEPTANCE_PROFILE.to_owned(), |document, (from, to)| {
            assert_eq!(
                document.matches(from).count(),
                1,
                "fixture replacement must identify exactly one value: {from}"
            );
            document.replacen(from, to, 1)
        })
}
