use std::net::SocketAddr;

use elucid_service::{
    ConfigurationErrorCode, ConfigurationViolation, Environment, LogFormat,
    MAXIMUM_CONFIGURATION_DOCUMENT_BYTES, MaintenanceMode, RuntimeConfiguration, SecretKind,
};

const ACCEPTANCE_PROFILE: &str = include_str!("fixtures/runtime.toml");
const POSTGRESQL_URL: &str = "postgresql://elucid:postgresql-secret@postgres.example/elucid";
const OBJECT_STORE_ACCESS_KEY_ID: &str = "elucid-access-key";
const OBJECT_STORE_SECRET_ACCESS_KEY: &str = "elucid-object-store-secret";

#[test]
fn acceptance_profile_materializes_the_exact_surface_and_redacts_secrets() {
    let configuration = RuntimeConfiguration::from_toml(ACCEPTANCE_PROFILE, &environment())
        .expect("the acceptance profile is valid");

    assert_eq!(
        configuration.server().listen_address(),
        "127.0.0.1:8080"
            .parse::<SocketAddr>()
            .expect("valid address")
    );
    assert_eq!(configuration.server().request_timeout_seconds().get(), 60);
    assert_eq!(configuration.server().shutdown_timeout_seconds().get(), 15);
    assert_eq!(configuration.metastore().maximum_connections().get(), 10);
    assert_eq!(
        configuration.object_store().endpoint().as_str(),
        "http://minio:9000/"
    );
    assert_eq!(configuration.object_store().bucket(), "elucid");
    assert_eq!(configuration.object_store().root_prefix(), "showcase");
    assert_eq!(
        configuration.local_storage().spool_path().to_str(),
        Some("/var/lib/elucid/spool")
    );
    assert_eq!(
        configuration.local_storage().scratch_path().to_str(),
        Some("/var/lib/elucid/scratch")
    );
    assert_eq!(
        configuration.ingestion().maximum_http_batch_bytes().get(),
        16_777_216
    );
    assert_eq!(
        configuration
            .ingestion()
            .maximum_concurrent_requests()
            .get(),
        4
    );
    assert_eq!(configuration.query().maximum_concurrent_queries().get(), 2);
    assert_eq!(configuration.query().timeout_seconds().get(), 30);
    assert_eq!(
        configuration.query().maximum_scan_bytes().get(),
        107_374_182_400
    );
    assert_eq!(configuration.query().memory_bytes().get(), 536_870_912);
    assert_eq!(configuration.query().maximum_result_rows().get(), 10_000);
    assert_eq!(
        configuration.query().maximum_result_bytes().get(),
        16_777_216
    );
    assert_eq!(
        configuration.maintenance().mode(),
        MaintenanceMode::Automatic
    );
    assert_eq!(
        configuration.maintenance().event_retention_seconds().get(),
        2_592_000
    );
    assert_eq!(
        configuration
            .maintenance()
            .dead_letter_retention_seconds()
            .get(),
        604_800
    );
    assert_eq!(configuration.telemetry().log_format(), LogFormat::Json);
    assert_eq!(
        configuration.secrets().postgresql_url().expose_secret(),
        POSTGRESQL_URL
    );

    let rendered = format!("{configuration:?}");
    for secret in [
        POSTGRESQL_URL,
        OBJECT_STORE_ACCESS_KEY_ID,
        OBJECT_STORE_SECRET_ACCESS_KEY,
    ] {
        assert!(!rendered.contains(secret));
    }
    assert!(rendered.contains("[REDACTED]"));
}

#[test]
fn optional_file_uses_bounded_defaults_and_direct_secret_environment() {
    let configuration = RuntimeConfiguration::from_toml("", &environment())
        .expect("an omitted file uses bounded defaults");

    assert_eq!(
        configuration.server().listen_address(),
        "127.0.0.1:8080"
            .parse::<SocketAddr>()
            .expect("valid address")
    );
    assert_eq!(
        configuration.maintenance().mode(),
        MaintenanceMode::Automatic
    );
    assert_eq!(
        configuration.secrets().postgresql_url().expose_secret(),
        POSTGRESQL_URL
    );
}

#[test]
fn environment_overrides_file_values_and_direct_secrets() {
    let direct_postgresql_url = "postgresql://elucid:direct-secret@postgres.example/overridden";
    let mut environment = environment();
    environment.set("ELUCID_SERVER__REQUEST_TIMEOUT_SECONDS", "42");
    environment.set("ELUCID_QUERY__MAXIMUM_RESULT_ROWS", "41");
    environment.set("ELUCID_METASTORE__POSTGRESQL_URL", direct_postgresql_url);

    let configuration = RuntimeConfiguration::from_toml(ACCEPTANCE_PROFILE, &environment)
        .expect("environment overrides are valid");

    assert_eq!(configuration.server().request_timeout_seconds().get(), 42);
    assert_eq!(configuration.query().maximum_result_rows().get(), 41);
    assert_eq!(
        configuration.secrets().postgresql_url().expose_secret(),
        direct_postgresql_url
    );
    assert!(!format!("{configuration:?}").contains(direct_postgresql_url));
}

#[test]
fn retired_runtime_modes_and_attempt_controls_are_rejected() {
    let documents = [
        changed_profile(&[(
            "listen_address = \"127.0.0.1:8080\"",
            "listen_address = \"127.0.0.1:8080\"\nnetwork_trust = \"LOOPBACK_ONLY\"",
        )]),
        changed_profile(&[(
            "maximum_concurrent_requests = 4",
            "maximum_concurrent_requests = 4\nattempt_timeout_seconds = 900",
        )]),
        changed_profile(&[(
            "request_timeout_seconds = 30",
            "request_timeout_seconds = 30\nmaximum_request_attempts = 3",
        )]),
        format!("{ACCEPTANCE_PROFILE}\n[retention]\nidempotency_retention_seconds = 86400\n"),
    ];

    for document in documents {
        let error = RuntimeConfiguration::from_toml(&document, &environment())
            .expect_err("retired configuration must not be accepted");
        assert_eq!(error.code(), ConfigurationErrorCode::DocumentInvalid);
    }

    let mut environment = environment();
    environment.set("ELUCID_SERVER__ENABLED_SERVICES", "[\"QUERY\"]");
    let error = RuntimeConfiguration::from_toml(ACCEPTANCE_PROFILE, &environment)
        .expect_err("retired environment overrides must not be accepted");
    assert_eq!(error.code(), ConfigurationErrorCode::DocumentInvalid);
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
    missing_secret_environment.remove("ELUCID_METASTORE__POSTGRESQL_URL");
    let missing_secret =
        RuntimeConfiguration::from_toml(ACCEPTANCE_PROFILE, &missing_secret_environment)
            .expect_err("the PostgreSQL URL is required");
    assert_eq!(missing_secret.code(), ConfigurationErrorCode::SecretMissing);
    assert_eq!(
        missing_secret.secret_kind(),
        Some(SecretKind::PostgreSqlUrl)
    );

    let invalid_secret_value = "contains a space";
    let mut invalid_secret_environment = environment();
    invalid_secret_environment.set("ELUCID_OBJECT_STORE__ACCESS_KEY_ID", invalid_secret_value);
    let invalid_secret =
        RuntimeConfiguration::from_toml(ACCEPTANCE_PROFILE, &invalid_secret_environment)
            .expect_err("object-store credentials reject whitespace");
    assert_eq!(invalid_secret.code(), ConfigurationErrorCode::SecretInvalid);
    assert_eq!(
        invalid_secret.secret_kind(),
        Some(SecretKind::ObjectStoreAccessKeyId)
    );
    assert!(!format!("{invalid_secret:?}").contains(invalid_secret_value));
    assert!(!invalid_secret.to_string().contains(invalid_secret_value));
}

#[test]
fn capacity_relationships_protect_spool_scratch_and_query_limits() {
    let cases = [
        violation_case(
            "HTTP batch exceeds spool",
            &[(
                "maximum_http_batch_bytes = 16777216",
                "maximum_http_batch_bytes = 2147483649",
            )],
            ConfigurationViolation::MaximumHttpBatchExceedsSpoolCapacity,
        ),
        violation_case(
            "HTTP batch exceeds scratch",
            &[(
                "scratch_capacity_bytes = 2147483648",
                "scratch_capacity_bytes = 16777215",
            )],
            ConfigurationViolation::MaximumHttpBatchExceedsScratchCapacity,
        ),
        violation_case(
            "result exceeds query memory",
            &[("memory_bytes = 536870912", "memory_bytes = 16777215")],
            ConfigurationViolation::MaximumResultExceedsQueryMemory,
        ),
        violation_case(
            "result exceeds scratch",
            &[
                (
                    "maximum_http_batch_bytes = 16777216",
                    "maximum_http_batch_bytes = 1",
                ),
                (
                    "scratch_capacity_bytes = 2147483648",
                    "scratch_capacity_bytes = 16777215",
                ),
            ],
            ConfigurationViolation::MaximumResultExceedsScratchCapacity,
        ),
        violation_case(
            "spool and scratch overlap",
            &[(
                "scratch_path = \"/var/lib/elucid/scratch\"",
                "scratch_path = \"/var/lib/elucid/spool\"",
            )],
            ConfigurationViolation::SpoolAndScratchPathsMustDiffer,
        ),
    ];

    for case in cases {
        let error =
            RuntimeConfiguration::from_toml(&case.document, &environment()).expect_err(case.name);
        assert_eq!(error.violation(), Some(&case.expected), "{}", case.name);
    }
}

#[test]
fn required_positive_values_and_absolute_paths_are_enforced() {
    let zero_connections =
        changed_profile(&[("maximum_connections = 10", "maximum_connections = 0")]);
    let zero_error = RuntimeConfiguration::from_toml(&zero_connections, &environment())
        .expect_err("required counts reject zero");
    assert_eq!(zero_error.code(), ConfigurationErrorCode::ValueInvalid);

    let relative_spool = changed_profile(&[(
        "spool_path = \"/var/lib/elucid/spool\"",
        "spool_path = \"relative/spool\"",
    )]);
    let path_error = RuntimeConfiguration::from_toml(&relative_spool, &environment())
        .expect_err("local state paths must be absolute");
    assert_eq!(path_error.code(), ConfigurationErrorCode::ValueInvalid);
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
        ("ELUCID_METASTORE__POSTGRESQL_URL", POSTGRESQL_URL),
        (
            "ELUCID_OBJECT_STORE__ACCESS_KEY_ID",
            OBJECT_STORE_ACCESS_KEY_ID,
        ),
        (
            "ELUCID_OBJECT_STORE__SECRET_ACCESS_KEY",
            OBJECT_STORE_SECRET_ACCESS_KEY,
        ),
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
