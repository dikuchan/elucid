# Elucid v0 Catalog Specification

- Status: `DRAFT`
- Depends on: [Elucid v0](README.md)

## 1. Terminology

| Term | Definition |
|---|---|
| Source | A queryable logical event collection with stable identity, immutable schema history, one active schema, and zero or more inputs. |
| Schema | An immutable ordered field definition identified by `schema_id` and a source-scoped positive `schema_version`. |
| Active schema | The schema referenced by a source for query field resolution and adaptation of stored segments. |
| Field | A resolved schema member with stable identity, name, logical type, nullability, role, and ordinal. |
| Input | A named HTTP ingestion endpoint configuration owned by one source, with immutable ingestion-profile history and one active revision. |
| Ingestion-profile revision | An immutable framing, mapping, conversion, and rejection contract that targets one schema. |
| Catalog manifest | The complete declarative catalog history and active pointers for exactly one source. |

## 2. Identity and names

Persistent identities MUST be UUIDv7 values except for the reserved system-field identities in Section 4. Generated identities MUST be stored as PostgreSQL `UUID` and rendered as lowercase hyphenated strings.

Source, input, and user-field names MUST match `[A-Za-z_][A-Za-z0-9_]*`. A source name MUST be globally unique. An input name MUST be unique within its source. A field name MUST be unique within its schema. Names MUST be compared by exact byte sequence after validation; case folding and locale-dependent normalization MUST NOT occur.

Catalog validation MUST NOT reject a valid name because it is a query-language keyword. A reserved name MUST remain addressable through the [quoted-identifier syntax](query-language.md#2-lexical-grammar).

Schema versions and ingestion-profile revisions MUST begin at `1`, increase by `1`, and remain immutable. Removing an existing version or revision from catalog history is invalid.

## 3. Logical types

Elucid v0 defines these logical types:

| Logical type | Arrow representation | Domain |
|---|---|---|
| `bool` | `Boolean` | Boolean |
| `int32` | `Int32` | Signed 32-bit integer |
| `int64` | `Int64` | Signed 64-bit integer |
| `uint32` | `UInt32` | Unsigned 32-bit integer |
| `uint64` | `UInt64` | Unsigned 64-bit integer |
| `float32` | `Float32` | Finite IEEE 754 binary32 |
| `float64` | `Float64` | Finite IEEE 754 binary64 |
| `utf8` | `Utf8` | UTF-8 string |
| `datetime` | `Timestamp(Millisecond, "UTC")` | UTC instant with millisecond precision |
| `eid` | `FixedSizeBinary(16)` with `elucid.logical_type=eid` | Opaque 128-bit event identity |
| `json` | `Utf8` with `elucid.logical_type=json` | Canonical JSON value |

User fields MAY use `bool`, numeric types, `utf8`, and `datetime`. The `eid` and `json` types are reserved for system fields in v0.

Nullability MUST be `NON_NULL` or `NULLABLE`. `NON_NULL` compiles to an Arrow field with `is_nullable = false`; `NULLABLE` compiles to `is_nullable = true`.

## 4. Schema

Every schema MUST contain the following system fields and MAY contain ordered user fields between `@event_id` and `@rest`:

| Ordinal | Field identity | Name | Logical type | Nullability | Role |
|---|---|---|---|---|---|
| `0` | `00000000-0000-7000-8000-000000000001` | `@event_time` | `datetime` | `NON_NULL` | `EVENT_TIME` |
| `1` | `00000000-0000-7000-8000-000000000002` | `@ingestion_time` | `datetime` | `NON_NULL` | `INGESTION_TIME` |
| `2` | `00000000-0000-7000-8000-000000000003` | `@event_id` | `eid` | `NON_NULL` | `EVENT_ID` |
| Final | `00000000-0000-7000-8000-000000000004` | `@rest` | `json` | `NULLABLE` | `REMAINDER` |

Every materialized field MUST contain `field_id`, `name`, `logical_type`, `nullability`, `role`, ordinal, and optional `description`. A user field's role MUST be `DATA`; a user-field manifest declaration MUST omit identity, role, and ordinal because catalog application derives them. Every compiled Arrow field MUST contain canonical `elucid.field_id` metadata. A schema definition MUST contain its ordered fields, canonical declaration, `declaration_digest`, materialized definition, `materialized_digest`, and exact Arrow schema descriptor.

Field names are query labels; `field_id` is identity across schema versions. Schema reconciliation MUST use `field_id`, never name or ordinal.

A later schema MUST resolve a user-field name that occurred historically to that name's most recent identity and then validate nullability and type compatibility. It MUST allocate a new identity only when the name has never occurred in the source:

```text
int32 -> int64
uint32 -> uint64
int32 -> float64
uint32 -> float64
float32 -> float64
```

Compatibility MUST be validated from every declared occurrence of a field to the candidate active occurrence. A nullability tightening, role change, rename, absent required active field, or type transition outside the relation MUST produce `CATALOG_SCHEMA_INCOMPATIBLE`. A rename creates a new field identity. Removing a field from a later schema preserves its historical identity.

Every source MUST reference one active schema owned by that source. Activating a schema MUST prove that every declared schema can be adapted to it according to the [Query Engine schema-adaptation contract](query-engine.md#3-schema-adaptation). Activation MUST be atomic and MUST NOT remove immutable history.

## 5. Inputs and ingestion profiles

Every input MUST contain `input_id`, `source_id`, name, input kind, immutable declaration and digest, one ordered ingestion-profile history, and one active ingestion-profile revision identity. Input kind MUST be `HTTP_NDJSON`.

Every ingestion-profile revision MUST contain `ingestion_profile_revision_id`, `input_id`, positive revision, target schema identity, parser kind `NDJSON`, encoding `UTF8`, line-boundary policy `LF_WITH_OPTIONAL_CR`, positive maximum record bytes, ordered mappings, event-time mapping, unknown-field policy, conversion policy, canonical declaration and digest, materialized definition and digest, and creation time.

Each promoted target field MUST have exactly one mapping from an RFC 6901 JSON Pointer to its target `field_id`. Two targets MAY read the same pointer. The separate event-time mapping MUST supply `@event_time`. `@ingestion_time`, `@event_id`, and `@rest` MUST be produced by ingestion and MUST NOT be mapped from input JSON.

The unknown-field policy MUST be `CAPTURE_TOP_LEVEL_REMAINDER`. The conversion policy MUST be `STRICT`. The event-time format MUST be `RFC3339` or `UNIX_MILLISECONDS`.

An input MUST have exactly one active ingestion-profile revision. The active revision MUST target any declared schema owned by the source that can be adapted to the source's active schema. Schema activation MUST NOT require a new profile revision when that adapter remains valid. Profile activation MUST change only the pointer and MUST leave every revision immutable. Ingestion-request claim MUST pin the active revision and its target schema; later schema or profile activation MUST NOT alter an existing ingestion request.

## 6. Manifest

A manifest MUST be one UTF-8 YAML 1.2 document, MUST declare exactly one source, and MUST use `format_version: 1`. Its structure MUST be:

```yaml
format_version: 1
source:
  name: example
  display_name: Example
  active_schema_version: 1
  schemas:
    - version: 1
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
          description: Event message
  inputs:
    - name: example_http
      kind: HTTP_NDJSON
      active_ingestion_profile_revision: 1
      ingestion_profile_revisions:
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
```

`description` is the only optional property shown. `schemas` and each `ingestion_profile_revisions` array MUST be non-empty; `fields`, `inputs`, and `mappings` MAY be empty. Schema field declarations describe only user fields because catalog application materializes the four system fields. Array order is significant where the owning contract defines an ordinal.

The loader MUST reject duplicate keys, aliases, merge keys, explicit tags, non-string mapping keys, unknown properties, invalid names, non-contiguous histories, unresolved pointers, and unresolved active values before mutation.

Generated identities, derived roles and ordinals, system-field declarations, materialized definitions, digests, and timestamps MUST be absent from the manifest. One source MUST have one current manifest file in a version-controlled catalog set. Runtime query and ingestion execution MUST use PostgreSQL state and MUST NOT read manifest files.

Canonical declarations and materialized definitions MUST use deterministic UTF-8 JSON with lexicographically ordered object keys, exact array order, no insignificant whitespace, and exact numeric tokens. A declaration digest MUST be the 32-byte BLAKE3 value over its declaration domain separator followed by canonical declaration bytes. Declaration domain separators MUST be `elucid:catalog:source:v1\0`, `elucid:catalog:schema:v1\0`, `elucid:catalog:input:v1\0`, and `elucid:catalog:ingestion-profile:v1\0`.

A materialized digest MUST be the 32-byte BLAKE3 value over its materialized domain separator followed by canonical materialized-definition bytes. Materialized domain separators MUST be `elucid:catalog:schema-materialized:v1\0`, `elucid:catalog:input-materialized:v1\0`, and `elucid:catalog:ingestion-profile-materialized:v1\0`. A schema materialization MUST cover resolved schema and field identities, derived roles and ordinals, logical metadata, and the Arrow schema descriptor. An input materialization MUST cover resolved source identity. An ingestion-profile materialization MUST cover resolved input, target-schema, and target-field identities plus parsed JSON Pointer tokens.

The source declaration digest MUST cover exact name. The schema declaration digest MUST cover format version, schema version, and ordered field names, logical types, nullability, roles, and descriptions. The input declaration digest MUST cover name and kind. The ingestion-profile declaration digest MUST cover revision, target schema version, framing, limits, conversion, remainder policy, event-time policy, and ordered mappings. Mutable display metadata, active pointers, generated identities, and timestamps MUST NOT participate in immutable declaration digests.

## 7. Catalog application

A catalog application MUST validate and canonicalize the complete manifest before opening its mutation transaction. The transaction MUST acquire a source-name advisory lock and perform these operations atomically:

1. Resolve or create the source.
2. Compare persisted and declared schema histories; reject an omitted persisted version as `CATALOG_HISTORY_DIVERGED`.
3. Reuse a persisted schema only when its declaration digest matches; otherwise return `CATALOG_DEFINITION_CONFLICT`.
4. Allocate missing schema and field identities, materialize their definitions, and persist their digests.
5. Compare persisted and declared input sets and profile histories with the same omission and digest rules.
6. Allocate missing input and profile identities and resolve schemas, target fields, and pointers into materialized definitions.
7. Validate every stored-to-active schema adapter and the adapter from each active profile's target schema to the active schema.
8. Update `display_name`, `active_schema_id`, and active ingestion-profile pointers.
9. Commit.

Failure MUST leave no partial history or pointer update. Reapplying a manifest whose complete declared state is already current MUST preserve every identity and return `UNCHANGED`. Repeating an application after an indeterminate transport result MUST NOT duplicate immutable history; it MAY return `CREATED`, `UPDATED`, `UNCHANGED`, or a catalog conflict according to the current durable state. Creating an immutable entity MUST return `CREATED`. Changing mutable display metadata or an active pointer MUST return `UPDATED`.

Successful JSON output MUST contain `outcome`, `source_id`, `active_schema_id`, `active_schema_version`, every declared schema identity and version, and every input identity with its active ingestion-profile revision identity and number. Outcome MUST be `CREATED`, `UPDATED`, or `UNCHANGED`.

## 8. Errors

The catalog MUST define stable errors `CATALOG_MANIFEST_INVALID`, `CATALOG_DEFINITION_CONFLICT`, `CATALOG_HISTORY_DIVERGED`, `CATALOG_PROFILE_TARGET_MISMATCH`, `CATALOG_SCHEMA_INCOMPATIBLE`, and `CATALOG_CORRUPTION`. A catalog error MUST identify the failed entity and declaration path without exposing credentials or generated SQL.
