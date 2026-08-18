use elucid_catalog::{
    CatalogApplicationOutcome, CatalogEntityDisposition, CatalogErrorCode,
    CatalogIdentityGenerator, CatalogManifest, FieldId, IngestProfileRevisionId, InputId, SchemaId,
    SourceId, plan_catalog_application,
};
use uuid::Uuid;

const BASE_MANIFEST: &str = r#"
format_version: 1
source:
  name: logs
  display_name: Access logs
  active_schema_version: 1
  schemas:
    - version: 1
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
          description: Rendered log message
  inputs:
    - name: http
      kind: HTTP_NDJSON
      active_ingest_profile_revision: 1
      ingest_profile_revisions:
        - revision: 1
          target_schema_version: 1
          parser_kind: NDJSON
          encoding: UTF8
          line_boundary_policy: LF_WITH_OPTIONAL_CR
          maximum_record_bytes: 10485760
          conversion_policy: STRICT
          unknown_field_policy: CAPTURE_TOP_LEVEL_REMAINDER
          event_time_mapping:
            json_pointer: /timestamp
            format: RFC3339
          mappings:
            - target_field: message
              json_pointer: /message
"#;

const EXTENDED_MANIFEST: &str = r#"
format_version: 1
source:
  name: logs
  display_name: Access logs
  active_schema_version: 2
  schemas:
    - version: 1
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
          description: Rendered log message
    - version: 2
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
          description: Rendered log message
        - name: status
          logical_type: int32
          nullability: NULLABLE
  inputs:
    - name: http
      kind: HTTP_NDJSON
      active_ingest_profile_revision: 1
      ingest_profile_revisions:
        - revision: 1
          target_schema_version: 1
          parser_kind: NDJSON
          encoding: UTF8
          line_boundary_policy: LF_WITH_OPTIONAL_CR
          maximum_record_bytes: 10485760
          conversion_policy: STRICT
          unknown_field_policy: CAPTURE_TOP_LEVEL_REMAINDER
          event_time_mapping:
            json_pointer: /timestamp
            format: RFC3339
          mappings:
            - target_field: message
              json_pointer: /message
"#;

#[test]
fn strict_manifest_loader_rejects_ambiguous_yaml_and_invalid_references() {
    let invalid_manifests = [
        (
            "duplicate key",
            BASE_MANIFEST.replacen("  name: logs", "  name: logs\n  name: duplicate", 1),
            CatalogErrorCode::ManifestInvalid,
        ),
        (
            "alias",
            BASE_MANIFEST
                .replacen("name: logs", "name: &source_name logs", 1)
                .replacen("display_name: Access logs", "display_name: *source_name", 1),
            CatalogErrorCode::ManifestInvalid,
        ),
        (
            "explicit tag",
            BASE_MANIFEST.replacen("name: logs", "name: !!str logs", 1),
            CatalogErrorCode::ManifestInvalid,
        ),
        (
            "merge key",
            BASE_MANIFEST.replacen("  name: logs", "  <<: {name: logs}\n  name: logs", 1),
            CatalogErrorCode::ManifestInvalid,
        ),
        (
            "non-string mapping key",
            BASE_MANIFEST.replacen("  name: logs", "  [name]: logs", 1),
            CatalogErrorCode::ManifestInvalid,
        ),
        (
            "unknown property",
            BASE_MANIFEST.replacen("  name: logs", "  name: logs\n  source_id: forbidden", 1),
            CatalogErrorCode::ManifestInvalid,
        ),
        (
            "multiple documents",
            format!("{BASE_MANIFEST}\n---\n{BASE_MANIFEST}"),
            CatalogErrorCode::ManifestInvalid,
        ),
        (
            "YAML 1.1 directive",
            format!("%YAML 1.1\n---\n{BASE_MANIFEST}"),
            CatalogErrorCode::ManifestInvalid,
        ),
        (
            "non-contiguous schema history",
            BASE_MANIFEST.replacen("- version: 1", "- version: 2", 1),
            CatalogErrorCode::ManifestInvalid,
        ),
        (
            "unresolved active schema",
            BASE_MANIFEST.replacen("active_schema_version: 1", "active_schema_version: 2", 1),
            CatalogErrorCode::ManifestInvalid,
        ),
        (
            "unresolved mapping target",
            BASE_MANIFEST.replacen("target_field: message", "target_field: absent", 1),
            CatalogErrorCode::ProfileTargetMismatch,
        ),
    ];

    for (case, manifest, expected_code) in invalid_manifests {
        let error = match CatalogManifest::decode(manifest.as_bytes()) {
            Ok(_) => panic!("{case} unexpectedly decoded"),
            Err(error) => error,
        };
        assert_eq!(error.code(), expected_code, "{case}: {error}");
        assert!(!error.path().as_str().is_empty(), "{case}: missing path");
    }
}

#[test]
fn reconciliation_is_canonical_idempotent_and_identity_preserving() {
    let manifest = CatalogManifest::decode(BASE_MANIFEST.as_bytes()).expect("manifest is valid");
    let mut identities = SequentialIdentities::new();

    let created = plan_catalog_application(&manifest, None, &mut identities)
        .expect("new catalog application is valid");
    assert_eq!(created.outcome(), CatalogApplicationOutcome::Created);
    assert_eq!(
        created.source_definition().disposition(),
        CatalogEntityDisposition::Create
    );
    assert_eq!(
        created.source_definition().declaration().as_str(),
        r#"{"name":"logs"}"#
    );

    let schema_one_definition = &created.schema_definitions()[0];
    let expected_schema_declaration = r#"{"fields":[{"description":"Rendered log message","logical_type":"utf8","name":"message","nullability":"NON_NULL","role":"DATA"}],"format_version":1,"version":1}"#;
    assert_eq!(
        schema_one_definition.declaration().as_str(),
        expected_schema_declaration
    );
    assert_digest(
        b"elucid:catalog:schema:v1\0",
        expected_schema_declaration.as_bytes(),
        schema_one_definition.declaration_digest().as_bytes(),
    );
    assert_digest(
        b"elucid:catalog:schema-materialized:v1\0",
        schema_one_definition.materialized_definition().as_bytes(),
        schema_one_definition.materialized_digest().as_bytes(),
    );

    let profile_definition = &created.ingest_profile_definitions()[0];
    let profile_materialized: serde_json::Value =
        serde_json::from_str(profile_definition.materialized_definition().as_str())
            .expect("materialized definition is JSON");
    let source = created.source();
    let schema_one = &source.schemas()[0];
    let message_id = schema_one.fields()[3].id();
    assert_eq!(
        profile_materialized["input_id"],
        source.inputs()[0].id().to_string()
    );
    assert_eq!(
        profile_materialized["target_schema_id"],
        schema_one.id().to_string()
    );
    assert_eq!(
        profile_materialized["event_time_mapping"]["json_pointer_tokens"][0],
        "timestamp"
    );
    assert_eq!(
        profile_materialized["mappings"][0]["target_field_id"],
        message_id.to_string()
    );
    assert_eq!(
        profile_materialized["mappings"][0]["json_pointer_tokens"][0],
        "message"
    );

    let allocations_after_creation = identities.allocations();
    let unchanged = plan_catalog_application(&manifest, Some(created.source()), &mut identities)
        .expect("reapplication is valid");
    assert_eq!(unchanged.outcome(), CatalogApplicationOutcome::Unchanged);
    assert_eq!(identities.allocations(), allocations_after_creation);
    assert_eq!(unchanged.source().id(), created.source().id());
    assert_eq!(
        unchanged.source().schemas()[0].id(),
        created.source().schemas()[0].id()
    );
    assert_eq!(
        unchanged.source().inputs()[0].id(),
        created.source().inputs()[0].id()
    );

    let extended_manifest =
        CatalogManifest::decode(EXTENDED_MANIFEST.as_bytes()).expect("manifest is valid");
    let extended = plan_catalog_application(
        &extended_manifest,
        Some(unchanged.source()),
        &mut identities,
    )
    .expect("compatible history extension is valid");
    assert_eq!(extended.outcome(), CatalogApplicationOutcome::Created);
    assert_eq!(
        extended.schema_definitions()[0].disposition(),
        CatalogEntityDisposition::Existing
    );
    assert_eq!(
        extended.schema_definitions()[1].disposition(),
        CatalogEntityDisposition::Create
    );
    assert_eq!(
        extended.source().schemas()[0].id(),
        created.source().schemas()[0].id()
    );
    assert_eq!(extended.source().schemas()[1].fields()[3].id(), message_id);
    assert_ne!(extended.source().schemas()[1].fields()[4].id(), message_id);
    assert_eq!(
        extended.source().inputs()[0].id(),
        created.source().inputs()[0].id()
    );

    let renamed_manifest = CatalogManifest::decode(
        EXTENDED_MANIFEST
            .replacen(
                "display_name: Access logs",
                "display_name: Security events",
                1,
            )
            .as_bytes(),
    )
    .expect("renamed manifest is valid");
    let allocations_before_update = identities.allocations();
    let updated =
        plan_catalog_application(&renamed_manifest, Some(extended.source()), &mut identities)
            .expect("mutable metadata update is valid");
    assert_eq!(updated.outcome(), CatalogApplicationOutcome::Updated);
    assert_eq!(updated.source().display_name(), "Security events");
    assert_eq!(identities.allocations(), allocations_before_update);
}

#[test]
fn reconciliation_rejects_history_rewrites_and_incompatible_activation() {
    let manifest = CatalogManifest::decode(BASE_MANIFEST.as_bytes()).expect("manifest is valid");
    let mut identities = SequentialIdentities::new();
    let created = plan_catalog_application(&manifest, None, &mut identities)
        .expect("new catalog application is valid");

    let rewritten_manifest = CatalogManifest::decode(
        BASE_MANIFEST
            .replacen("logical_type: utf8", "logical_type: int64", 1)
            .as_bytes(),
    )
    .expect("rewritten manifest is structurally valid");
    let conflict =
        plan_catalog_application(&rewritten_manifest, Some(created.source()), &mut identities)
            .expect_err("an immutable schema cannot be rewritten");
    assert_eq!(conflict.code(), CatalogErrorCode::DefinitionConflict);
    assert_eq!(conflict.path().as_str(), "source.schemas[0]");

    let required_extension = CatalogManifest::decode(
        EXTENDED_MANIFEST
            .replacen("nullability: NULLABLE", "nullability: NON_NULL", 1)
            .as_bytes(),
    )
    .expect("required extension is structurally valid");
    let incompatible =
        plan_catalog_application(&required_extension, Some(created.source()), &mut identities)
            .expect_err("historical rows cannot supply a new required field");
    assert_eq!(incompatible.code(), CatalogErrorCode::SchemaIncompatible);
    assert_eq!(incompatible.path().as_str(), "source.active_schema_version");

    let compatible = CatalogManifest::decode(EXTENDED_MANIFEST.as_bytes()).expect("manifest valid");
    let extended = plan_catalog_application(&compatible, Some(created.source()), &mut identities)
        .expect("compatible extension is valid");
    let diverged = plan_catalog_application(&manifest, Some(extended.source()), &mut identities)
        .expect_err("persisted history cannot be omitted");
    assert_eq!(diverged.code(), CatalogErrorCode::HistoryDiverged);
    assert_eq!(diverged.path().as_str(), "source.schemas");
}

fn assert_digest(domain: &[u8], document: &[u8], actual: &[u8; 32]) {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(document);
    assert_eq!(actual, hasher.finalize().as_bytes());
}

#[derive(Debug)]
struct SequentialIdentities {
    next: u64,
    allocations: usize,
}

impl SequentialIdentities {
    const fn new() -> Self {
        Self {
            next: 1,
            allocations: 0,
        }
    }

    const fn allocations(&self) -> usize {
        self.allocations
    }

    fn next_uuid(&mut self) -> Uuid {
        let value = self.next;
        self.next += 1;
        self.allocations += 1;
        Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 | u128::from(value))
    }
}

impl CatalogIdentityGenerator for SequentialIdentities {
    fn generate_source_id(&mut self) -> SourceId {
        SourceId::try_from(self.next_uuid()).expect("generated source identity is UUIDv7")
    }

    fn generate_schema_id(&mut self) -> SchemaId {
        SchemaId::try_from(self.next_uuid()).expect("generated schema identity is UUIDv7")
    }

    fn generate_field_id(&mut self) -> FieldId {
        FieldId::try_from(self.next_uuid()).expect("generated field identity is UUIDv7")
    }

    fn generate_input_id(&mut self) -> InputId {
        InputId::try_from(self.next_uuid()).expect("generated input identity is UUIDv7")
    }

    fn generate_ingest_profile_revision_id(&mut self) -> IngestProfileRevisionId {
        IngestProfileRevisionId::try_from(self.next_uuid())
            .expect("generated ingest profile revision identity is UUIDv7")
    }
}
