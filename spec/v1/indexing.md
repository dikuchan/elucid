# Selective Tantivy indexing over Parquet

## 1. Status and decision

This document is a deferred design hypothesis, not an implementation contract. It records enough of the intended direction to avoid accidentally hard-coding Tantivy out of the storage and query models while keeping it out of the v0 prototype's two-week delivery scope.

The working decision is:

- Parquet remains the authoritative and complete event representation.
- Tantivy is a rebuildable selective secondary index.
- A user explicitly configures which logical fields are indexed and how.
- Tantivy does not store the complete event, `@rest`, or copies of unindexed user fields.
- Query execution uses Tantivy to find candidate rows and reads projected values from Parquet. DataFusion evaluates the remaining relational plan.
- v0 remains Parquet-only and must not depend on Tantivy artifacts, index coverage, or index services.

This design does not yet select concrete crate versions, artifact packaging, cache implementation, cost thresholds, or a distributed indexing topology.

## 2. Non-goals

The first indexing design does not attempt to:

- replace Parquet with the Tantivy document store;
- index every field or arbitrary contents of `@rest` automatically;
- make an index result a complete event result;
- expose Tantivy document identities or Parquet row ordinals through the public API;
- promise index-only execution for general queries;
- distribute one segment build across nodes;
- make partial index coverage look like a complete query result.

## 3. Catalog model

Index configuration belongs in the version-controlled YAML catalog, but it has a different lifecycle from the logical schema and ingestion profile:

- a schema revision defines stable field identities, names, logical types, and nullability;
- an ingestion-profile revision defines how input JSON produces logical fields;
- an index revision defines which logical fields are indexed and the indexing semantics used for them.

Changing a tokenizer or enabling an index does not change a field's logical type. It should therefore create a new index revision rather than a new logical schema revision.

A boolean `indexed: true` is insufficient because it hides incompatible search semantics. A possible non-final declaration is:

```yaml
index_revisions:
  - revision: 1
    fields:
      - field: message
        mode: FULL_TEXT
        tokenizer: default
        record: POSITIONS
      - field: hostname
        mode: EXACT
      - field: status
        mode: NUMERIC_RANGE
active_index_revision: 1
```

Catalog application resolves field names to stable `field_id` values. The materialized index revision contains field identities rather than relying on names or ordinals. Supported modes, tokenizer configuration, array behavior, null behavior, and maximum indexed value sizes require an explicit future contract.

User fields are indexed but not marked as stored in Tantivy. The index contains only the structures required by their declared modes plus internal row-location metadata.

## 4. Segment artifacts and row identity

One logical segment owns one authoritative Parquet object and may own one active Tantivy index generation:

```text
logical segment
  |-- immutable Parquet object
  `-- immutable Tantivy index generation
```

"Generation" distinguishes rebuilding an index for the same immutable Parquet object from producing a new logical segment through ingestion or compaction.

Both artifacts describe exactly the same ordered rows. While building a segment, the producer assigns each Parquet row an absolute, zero-based `row_ordinal`. Each Tantivy document contains that ordinal in a private `u64` fast field. It is physical metadata scoped to one segment generation, not an Elucid event field.

The implementation must not assume that a Tantivy `DocId` equals a Parquet row ordinal. Tantivy document addresses are internal and may change after index merge or rebuild. `@event_id` also does not replace the row locator: it is a logical event identity, while locating arbitrary event IDs in Parquet would require another scan or index.

An index manifest needs to bind at least:

- logical segment identity;
- Parquet object identity and byte digest;
- exact Parquet row count;
- stored schema identity;
- index revision and generation identities;
- Tantivy format and compatibility version;
- index artifact byte counts and digests.

A query snapshot must pin exact compatible Parquet and index artifacts. A row ordinal obtained from one generation must never be applied to another Parquet object.

The logical bundle is a correctness invariant, not a requirement to introduce a generic artifact framework. The durable representation may use explicit Parquet and index references if that is the smallest adequate model.

## 5. Build and publication

For a newly produced indexed segment, Parquet and Tantivy are built from the same bounded Arrow stream and final row ordering. The producer:

1. resolves and pins the stored schema and active index revision;
2. normalizes and orders the bounded segment rows;
3. writes the complete rows to Parquet;
4. writes only declared indexed values and the private row ordinal to Tantivy;
5. finalizes and verifies both artifacts and their manifest;
6. uploads the immutable artifacts;
7. publishes a compatible artifact set through PostgreSQL.

The future contract must decide whether an indexed segment may become visible before its required index is available. Returning partial results is never an option. Plausible policies are:

- require complete index coverage before activating an index revision; or
- permit Parquet-only visibility and execute a semantically equivalent full scan for segments without a compatible index.

The second policy is available only for predicates whose semantics can be implemented identically without Tantivy. In particular, a tokenized full-text operator cannot silently disappear or use different tokenization during a fallback scan.

## 6. Query planning and execution

The planner divides a filter into:

- an indexable candidate-producing expression;
- a residual expression evaluated over values read from Parquet.

For each selected logical segment, an index-assisted scan is expected to:

1. execute the compatible Tantivy expression;
2. collect segment-scoped Parquet row ordinals;
3. group and sort them into row groups and contiguous row ranges;
4. construct a Parquet row-selection/access plan;
5. read only required Parquet columns for the selected ranges;
6. evaluate the residual filter and the remaining DataFusion plan.

Predicate extraction must preserve the complete query semantics. For example:

```text
full_text(message, "failed password") and status >= 500
```

may use the text index to produce candidates and evaluate `status` from Parquet. In contrast:

```text
full_text(message, "failed password") or status >= 500
```

cannot restrict the scan to text matches unless the other branch is also evaluated and the result sets are combined correctly. `not`, nulls, arrays, casts, schema adapters, and mixed index revisions require similarly explicit rules.

Index use is a physical optimization except for operators, such as tokenized full-text search, whose semantics are defined by an index revision. Query results must remain complete when some selected segments do not have a usable index: the engine either uses a correct fallback for those segments or rejects the query with an index-coverage error.

## 7. Cost and resource model

An index hit set is not automatically cheaper than a Parquet scan. The physical planner must be able to choose among:

- skipping an entire Parquet object when the index has no matches;
- reading selected row groups;
- reading sparse row ranges within row groups;
- scanning a complete row group or object when candidates are dense or highly fragmented.

The choice depends on candidate density, number of disjoint ranges, projected columns, Parquet page and row-group layout, object-store range-request cost, local cache state, and result limits. Initial thresholds should be measured, not specified from intuition.

Every build and query path needs explicit bounds for:

- indexed fields per revision;
- indexed bytes, tokens, and terms per event;
- token and term length;
- candidate rows and candidate bitmap memory;
- disjoint Parquet ranges;
- concurrently opened index generations;
- local index-cache bytes;
- object-store requests and fetched bytes;
- indexing memory, local working disk, and build time;
- concurrent reindex and compaction work.

If a candidate set exceeds its bound, the engine may choose a bounded full scan when semantically valid. It must not truncate candidates.

## 8. Compaction, reindexing, and retention

Compaction produces a new logical segment, new Parquet row ordinals, and a new Tantivy generation. Parquet and index inputs are retired according to the same snapshot and grace-period rules as the logical input segments.

Reindexing is different from compaction. It reads an immutable Parquet object, builds a new Tantivy generation for the same logical segment, verifies its binding manifest, and atomically switches the active index reference. It does not rewrite Parquet or change event identities.

Schema adaptation applies before reindexing. If a declared logical field was historically stored inside `@rest`, an explicit stored-to-logical adapter may extract and convert that value while building an index for an old segment. An undeclared remainder key is not implicitly promoted into an index.

Old index generations remain readable for snapshots that pinned them and become reclaimable only after the maximum snapshot lifetime. Expiration of the authoritative Parquet segment makes all of its index generations reclaimable under the corresponding retention grace period.

## 9. Failure and consistency rules

The future contract must preserve these invariants:

- Parquet remains sufficient to reconstruct every index generation.
- An index is never applied to a different Parquet object or row order.
- Missing, stale, corrupt, or incompatible indexes never cause partial query results.
- Failed index builds remain invisible and reclaimable.
- An ambiguous PostgreSQL publication is resolved by persisted generation identity rather than by object-store listing.
- Query planning uses exact metastore references and never discovers indexes by listing an object-store prefix.
- Reindex and compaction ownership are fenced and bounded, but their detailed state machines should be introduced only when an implementation requires them.

## 10. Questions to validate before a v1 contract

The following questions require experiments or explicit product decisions:

1. Should a Tantivy generation be stored as a directory, an archive, or a Quickwit-like packaged split with a hot cache?
2. What local cache is required to avoid reopening or downloading remote index data for every query?
3. Is absolute Parquet row ordinal the best locator after measuring sparse and dense reads over S3?
4. At what candidate density and fragmentation does a full row-group or file scan become cheaper?
5. Which logical types and operators are supported by the first index revision?
6. How are BM25 scores represented and combined with DataFusion filtering, sorting, and limits?
7. Must an index revision have complete retained-data coverage before activation, or which operators have an exact scan fallback?
8. Can existing DataFusion Parquet access-plan hooks carry the candidate row selections, or is a custom table provider or execution node required?
9. How are tokenizer and Tantivy format upgrades rolled out while old query snapshots remain executable?
10. How much ingestion throughput, query latency, object-store traffic, local disk, and memory does indexing add on representative SIEM data?

The first useful spike should build one immutable Parquet/Tantivy pair, query a selective text field, translate candidates to Parquet row selections, and measure the crossover against a direct Parquet scan. It should not start with a distributed index service or a general indexing framework.
