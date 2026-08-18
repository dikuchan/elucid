# Elucid v0

- Status: `DRAFT`
- Last updated: 2026-08-18

Elucid v0 is the product contract formed by [Catalog](catalog.md), [Query Language](query-language.md), [Query Engine](query-engine.md), [Storage](storage.md), [Metastore](metastore.md), [Ingestion](ingestion.md), [Compaction](compaction.md), [Retention](retention.md), and [Service](service.md). [Showcase](showcase.md) is its executable delivery profile.

Each shared concept has one owning document. Another document MAY add constraints at its own boundary, MUST reference the owning contract for shared semantics, and MUST NOT redefine or weaken that contract. If two requirements conflict, the requirement in the owning document prevails.

The uppercase key words `MUST`, `MUST NOT`, `SHOULD`, `SHOULD NOT`, and `MAY` are interpreted according to BCP 14, RFC 2119, and RFC 8174.
