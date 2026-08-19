# Elucid v0 Catalog Specification

This document owns sources, logical schemas, HTTP inputs, ingestion-profile revisions, schema adaptation declarations, and YAML catalog application.

## 1. Model

A source is a queryable logical event collection with a stable UUID, a unique name, immutable schema history, one active schema, and zero or more inputs.

An input is a named HTTP NDJSON admission point owned by one source. An ingestion-profile revision defines how one input maps JSON records into one stored schema. A source and input MAY change their active revision pointers, but every declared revision is immutable.

Runtime components load catalog state from PostgreSQL. They MUST NOT read YAML files while admitting, processing, or querying events.

Names MUST be valid unquoted query identifiers: an ASCII letter or underscore followed by ASCII letters, digits, or underscores. Names beginning with `@` are reserved for system fields. A field named after a query-language keyword is allowed only through the language's quoted-identifier syntax.

## 2. Logical types and fields

V0 user fields support `bool`, `int32`, `int64`, `uint32`, `uint64`, `float32`, `float64`, `utf8`, and `datetime`. Nullability is `NON_NULL` or `NULLABLE`.

Every schema contains these system fields around its ordered user fields:

| Position | Stable field identity | Name | Type | Nullability |
| --- | --- | --- | --- | --- |
| First | `00000000-0000-7000-8000-000000000001` | `@event_time` | `datetime` | `NON_NULL` |
| Second | `00000000-0000-7000-8000-000000000002` | `@ingestion_time` | `datetime` | `NON_NULL` |
| Third | `00000000-0000-7000-8000-000000000003` | `@event_id` | `eid` | `NON_NULL` |
| Last | `00000000-0000-7000-8000-000000000004` | `@rest` | `json` | `NULLABLE` |

Each user field has a stable UUID, name, logical type, nullability, ordinal, optional description, and optional `historical_remainder_pointer`. Field identity is reused by name across schema versions and is stored in Arrow field metadata as `elucid.field_id`.

## 3. Schema evolution

V0 schema evolution is deliberately additive. A later schema MUST preserve every existing user field's identity, name, type, nullability, historical remainder pointer, and relative order. It MAY append new `NULLABLE` fields. Rename, removal, type change, nullability tightening, and ordinal reuse are outside V0.

A new nullable field MAY declare an RFC 6901 `historical_remainder_pointer`. For a stored schema that predates the field, the query adapter reads that pointer from the stored row's `@rest` value and applies the field's ordinary JSON-to-logical conversion. Absence, JSON null, or conversion failure produces logical null. Conversion failures MUST increment a bounded metric and MUST NOT silently change another field.

This adapter is explicit catalog metadata, not general query-name fallback. An undeclared query identifier remains an error. A field without `historical_remainder_pointer` adapts to typed null in older stored schemas.

Catalog application MUST prove that every declared stored schema can adapt to the proposed active schema under these rules before changing the active pointer.

## 4. Inputs and ingestion profiles

Every input has one ordered history of ingestion-profile revisions and one active revision. A revision contains:

- target schema version;
- maximum record bytes;
- event-time JSON pointer and format `RFC3339` or `UNIX_MILLISECONDS`;
- one RFC 6901 JSON pointer for each promoted target field.

All V0 inputs use UTF-8 NDJSON, strict conversion, and top-level remainder capture. These are version semantics, not repeated per-profile switches.

Mappings distinguish an absent value from JSON null. Two target fields MAY read the same pointer. `@ingestion_time`, `@event_id`, and `@rest` are produced by Elucid and cannot be mapped from input JSON.

Remainder capture removes a top-level property only when a promoted-field or event-time pointer addresses that property exactly. A pointer below a top-level property leaves the complete top-level value in `@rest`, including the mapped descendant, so sibling data is preserved.

`@rest` is null when no properties remain and otherwise contains one JSON object. Its physical JSON serialization is not part of the query contract.

An active profile MAY target an older declared schema while a newer schema is active for queries. Admission pins the active profile revision and its target stored schema in the durable local spool; later catalog changes MUST NOT reinterpret already acknowledged bytes.

## 5. YAML manifest

One YAML document declares one source and its complete known history. A minimal example is:

```yaml
format_version: 1
source:
  name: demo_logs
  display_name: Demo logs
  active_schema_version: 2
  schemas:
    - version: 1
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
    - version: 2
      fields:
        - name: message
          logical_type: utf8
          nullability: NON_NULL
        - name: region
          logical_type: utf8
          nullability: NULLABLE
          historical_remainder_pointer: /region
  inputs:
    - name: vector
      active_ingestion_profile_revision: 2
      ingestion_profile_revisions:
        - revision: 1
          target_schema_version: 1
          maximum_record_bytes: 1048576
          event_time: { json_pointer: /timestamp, format: RFC3339 }
          mappings:
            - { target_field: message, json_pointer: /message }
        - revision: 2
          target_schema_version: 2
          maximum_record_bytes: 1048576
          event_time: { json_pointer: /timestamp, format: RFC3339 }
          mappings:
            - { target_field: message, json_pointer: /message }
            - { target_field: region, json_pointer: /region }
```

The loader MUST reject duplicate YAML keys, unknown properties, duplicate versions or names, unresolved active pointers, invalid JSON pointers, missing mappings for non-null target fields, incompatible schema history, and manifest, history, field, input, profile, or mapping counts above the reported implementation limits before opening a transaction.

Generated UUIDs, timestamps, and materialized field identities are PostgreSQL state and MUST NOT appear in the YAML manifest. The Arrow schema is derived from the validated materialized field list.

## 6. Catalog application

Catalog application validates the complete document before mutation, then performs one PostgreSQL transaction:

1. Lock the source row by name or create it.
2. Compare every already known immutable schema and profile revision with the supplied definition; reject conflicting reuse of a version number.
3. Insert new schema and profile revisions with generated stable identities.
4. Validate profile ownership, target schemas, mappings, and every stored-to-active adapter.
5. Replace the source and input active pointers.
6. Commit.

Reapplying the same manifest is a no-op. A failure leaves catalog state unchanged. Catalog application serializes only changes to the same source and performs no object-store or local-filesystem I/O inside the transaction. After commit, the process atomically replaces its in-memory catalog snapshot; a concurrent admission observes either the complete old revision set or the complete new one.

## 7. Errors

The public catalog errors are `CATALOG_MANIFEST_INVALID`, `CATALOG_DEFINITION_CONFLICT`, `CATALOG_SCHEMA_INCOMPATIBLE`, `CATALOG_PROFILE_INVALID`, and `CATALOG_CORRUPT`.
