use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use elucid_catalog::{
    CatalogApplicationOutcome, CatalogEntityDisposition, CatalogIdentityGenerator, CatalogManifest,
    IngestionProfileRevisionId, InputId, InputName, ProfileRevision, SchemaId, SchemaVersion,
    Source, SourceId, SourceName, assemble_stored_input, assemble_stored_source,
    decode_stored_profile_definition, decode_stored_schema_definition, plan_catalog_application,
};
use serde_json::Value;
use sqlx::FromRow;
use sqlx::postgres::{PgConnection, PgPool};
use sqlx::types::Json;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::CatalogPersistenceError;

const LOAD_SOURCES: &str =
    "SELECT source_id, name, display_name, active_schema_id FROM sources ORDER BY name";
const LOAD_SCHEMAS: &str = "SELECT schema_id, source_id, version, definition FROM schema_versions ORDER BY source_id, version";
const LOAD_INPUTS: &str = "SELECT input_id, source_id, name, active_profile_revision_id FROM inputs ORDER BY source_id, name";
const LOAD_PROFILES: &str = "SELECT profile_revision_id, input_id, source_id, revision, target_schema_id, definition FROM ingestion_profile_revisions ORDER BY source_id, input_id, revision";

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct CatalogSnapshot {
    sources: Vec<Arc<Source>>,
}

impl CatalogSnapshot {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    pub fn sources(&self) -> impl ExactSizeIterator<Item = &Source> {
        self.sources.iter().map(AsRef::as_ref)
    }

    #[must_use]
    pub fn source_by_name(&self, name: &SourceName) -> Option<&Source> {
        self.sources
            .iter()
            .find(|source| source.name() == name)
            .map(AsRef::as_ref)
    }

    #[must_use]
    pub fn source_by_id(&self, source_id: SourceId) -> Option<&Source> {
        self.sources
            .iter()
            .find(|source| source.id() == source_id)
            .map(AsRef::as_ref)
    }

    fn new(mut sources: Vec<Arc<Source>>) -> Result<Self, CatalogPersistenceError> {
        sources.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        let mut identities = HashSet::with_capacity(sources.len());
        let mut names = HashSet::with_capacity(sources.len());
        for source in &sources {
            if !identities.insert(source.id()) {
                return Err(CatalogPersistenceError::corrupt(
                    "stored catalog contains a duplicate source identity",
                ));
            }
            if !names.insert(source.name().clone()) {
                return Err(CatalogPersistenceError::corrupt(
                    "stored catalog contains a duplicate source name",
                ));
            }
        }
        Ok(Self { sources })
    }

    fn replacing(&self, source: Arc<Source>) -> Result<Self, CatalogPersistenceError> {
        if self
            .sources
            .iter()
            .any(|current| current.id() == source.id() && current.name() != source.name())
        {
            return Err(CatalogPersistenceError::corrupt(
                "source identity changed its immutable name",
            ));
        }
        let mut sources = self.sources.clone();
        if let Some(index) = sources
            .iter()
            .position(|current| current.name() == source.name())
        {
            if sources[index].id() != source.id() {
                return Err(CatalogPersistenceError::corrupt(
                    "source name changed its immutable identity",
                ));
            }
            sources[index] = source;
        } else {
            sources.push(source);
        }
        Self::new(sources)
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum CatalogApplyOutcome {
    Applied { source: Arc<Source> },
    Unchanged { source: Arc<Source> },
}

impl CatalogApplyOutcome {
    #[must_use]
    pub const fn source(&self) -> &Arc<Source> {
        match self {
            Self::Applied { source } | Self::Unchanged { source } => source,
        }
    }
}

#[derive(Debug)]
pub struct CatalogStore {
    pool: PgPool,
    snapshot: RwLock<Arc<CatalogSnapshot>>,
    mutation_gate: Mutex<()>,
}

impl CatalogStore {
    /// Loads one consistent catalog snapshot from an already migrated PostgreSQL pool.
    ///
    /// # Errors
    ///
    /// Returns an unavailable error when PostgreSQL cannot serve the snapshot, or a corrupt error
    /// when stored rows cannot be converted into the catalog domain model.
    pub async fn load(pool: PgPool) -> Result<Self, CatalogPersistenceError> {
        let snapshot = load_consistent_snapshot(&pool).await?;
        Ok(Self {
            pool,
            snapshot: RwLock::new(Arc::new(snapshot)),
            mutation_gate: Mutex::new(()),
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<CatalogSnapshot> {
        match self.snapshot.read() {
            Ok(snapshot) => Arc::clone(&snapshot),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        }
    }

    /// Reloads the complete catalog under one repeatable-read database snapshot.
    ///
    /// # Errors
    ///
    /// Returns an unavailable or corrupt persistence error. The previous in-memory snapshot stays
    /// installed when loading or validation fails.
    pub async fn refresh(&self) -> Result<(), CatalogPersistenceError> {
        let _mutation = self.mutation_gate.lock().await;
        let snapshot = load_consistent_snapshot(&self.pool).await?;
        self.replace_snapshot(snapshot);
        Ok(())
    }

    /// Applies one validated complete source manifest in a single PostgreSQL transaction.
    ///
    /// # Errors
    ///
    /// Returns a named conflict, unavailable, or corrupt persistence error. Durable state and the
    /// in-memory snapshot remain unchanged when the transaction does not commit.
    pub async fn apply(
        &self,
        manifest: &CatalogManifest,
    ) -> Result<CatalogApplyOutcome, CatalogPersistenceError> {
        let _mutation = self.mutation_gate.lock().await;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(CatalogPersistenceError::unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(manifest.source_name().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(CatalogPersistenceError::unavailable)?;
        let current = load_source_for_update(&mut transaction, manifest.source_name()).await?;
        let mut identities = UuidV7CatalogIdentityGenerator;
        let plan = plan_catalog_application(manifest, current.as_ref(), &mut identities)
            .map_err(CatalogPersistenceError::from_catalog)?;
        let change = classify_application_outcome(plan.outcome())?;
        persist_plan(&mut transaction, &plan, current.as_ref()).await?;
        transaction
            .commit()
            .await
            .map_err(CatalogPersistenceError::write)?;

        let source = Arc::new(plan.into_source());
        self.merge_source(Arc::clone(&source))?;
        Ok(match change {
            DurableCatalogChange::Applied => CatalogApplyOutcome::Applied { source },
            DurableCatalogChange::Unchanged => CatalogApplyOutcome::Unchanged { source },
        })
    }

    fn replace_snapshot(&self, snapshot: CatalogSnapshot) {
        let mut current = match self.snapshot.write() {
            Ok(current) => current,
            Err(poisoned) => poisoned.into_inner(),
        };
        *current = Arc::new(snapshot);
    }

    fn merge_source(&self, source: Arc<Source>) -> Result<(), CatalogPersistenceError> {
        let mut current = match self.snapshot.write() {
            Ok(current) => current,
            Err(poisoned) => poisoned.into_inner(),
        };
        let next = current.replacing(source)?;
        *current = Arc::new(next);
        Ok(())
    }
}

async fn load_consistent_snapshot(
    pool: &PgPool,
) -> Result<CatalogSnapshot, CatalogPersistenceError> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(CatalogPersistenceError::unavailable)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(CatalogPersistenceError::unavailable)?;
    let rows = load_all_rows(&mut transaction).await?;
    transaction
        .commit()
        .await
        .map_err(CatalogPersistenceError::unavailable)?;
    assemble_snapshot(rows)
}

async fn load_all_rows(
    connection: &mut PgConnection,
) -> Result<CatalogRows, CatalogPersistenceError> {
    let sources = sqlx::query_as::<_, SourceRow>(LOAD_SOURCES)
        .fetch_all(&mut *connection)
        .await
        .map_err(CatalogPersistenceError::read)?;
    let schemas = sqlx::query_as::<_, SchemaRow>(LOAD_SCHEMAS)
        .fetch_all(&mut *connection)
        .await
        .map_err(CatalogPersistenceError::read)?;
    let inputs = sqlx::query_as::<_, InputRow>(LOAD_INPUTS)
        .fetch_all(&mut *connection)
        .await
        .map_err(CatalogPersistenceError::read)?;
    let profiles = sqlx::query_as::<_, ProfileRow>(LOAD_PROFILES)
        .fetch_all(&mut *connection)
        .await
        .map_err(CatalogPersistenceError::read)?;
    Ok(CatalogRows {
        sources,
        schemas,
        inputs,
        profiles,
    })
}

async fn load_source_for_update(
    connection: &mut PgConnection,
    source_name: &SourceName,
) -> Result<Option<Source>, CatalogPersistenceError> {
    let source = sqlx::query_as::<_, SourceRow>(
        "SELECT source_id, name, display_name, active_schema_id FROM sources WHERE name = $1 FOR UPDATE",
    )
    .bind(source_name.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(CatalogPersistenceError::read)?;
    let Some(source) = source else {
        return Ok(None);
    };
    let source_id = source.source_id;
    let schemas = sqlx::query_as::<_, SchemaRow>(
        "SELECT schema_id, source_id, version, definition FROM schema_versions WHERE source_id = $1 ORDER BY version",
    )
    .bind(source_id)
    .fetch_all(&mut *connection)
    .await
    .map_err(CatalogPersistenceError::read)?;
    let inputs = sqlx::query_as::<_, InputRow>(
        "SELECT input_id, source_id, name, active_profile_revision_id FROM inputs WHERE source_id = $1 ORDER BY name",
    )
    .bind(source_id)
    .fetch_all(&mut *connection)
    .await
    .map_err(CatalogPersistenceError::read)?;
    let profiles = sqlx::query_as::<_, ProfileRow>(
        "SELECT profile_revision_id, input_id, source_id, revision, target_schema_id, definition FROM ingestion_profile_revisions WHERE source_id = $1 ORDER BY input_id, revision",
    )
    .bind(source_id)
    .fetch_all(&mut *connection)
    .await
    .map_err(CatalogPersistenceError::read)?;
    let snapshot = assemble_snapshot(CatalogRows {
        sources: vec![source],
        schemas,
        inputs,
        profiles,
    })?;
    match snapshot.sources.into_iter().next() {
        Some(source) => Arc::try_unwrap(source).map(Some).map_err(|_| {
            CatalogPersistenceError::corrupt("loaded source has an unexpected shared owner")
        }),
        None => Ok(None),
    }
}

fn assemble_snapshot(rows: CatalogRows) -> Result<CatalogSnapshot, CatalogPersistenceError> {
    let mut schema_rows = group_by(rows.schemas, |row| row.source_id);
    let mut input_rows = group_by(rows.inputs, |row| row.source_id);
    let mut profile_rows = group_by(rows.profiles, |row| row.input_id);
    let mut sources = Vec::with_capacity(rows.sources.len());

    for row in rows.sources {
        let source_id =
            SourceId::try_from(row.source_id).map_err(CatalogPersistenceError::corrupt_model)?;
        let source_name =
            SourceName::try_from(row.name).map_err(CatalogPersistenceError::corrupt_model)?;
        let active_schema_id = SchemaId::try_from(row.active_schema_id)
            .map_err(CatalogPersistenceError::corrupt_model)?;
        let schemas = schema_rows
            .remove(&row.source_id)
            .unwrap_or_default()
            .into_iter()
            .map(decode_schema_row)
            .collect::<Result<Vec<_>, _>>()?;
        let schemas_by_id = schemas
            .iter()
            .map(|schema| (schema.id(), schema))
            .collect::<HashMap<_, _>>();

        let inputs = input_rows
            .remove(&row.source_id)
            .unwrap_or_default()
            .into_iter()
            .map(|input_row| {
                decode_input_row(input_row, source_id, &schemas_by_id, &mut profile_rows)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let source = assemble_stored_source(
            source_id,
            source_name,
            row.display_name,
            active_schema_id,
            schemas,
            inputs,
        )
        .map_err(CatalogPersistenceError::from_catalog)?;
        sources.push(Arc::new(source));
    }

    if !schema_rows.is_empty() || !input_rows.is_empty() || !profile_rows.is_empty() {
        return Err(CatalogPersistenceError::corrupt(
            "stored catalog contains orphaned child rows",
        ));
    }
    CatalogSnapshot::new(sources)
}

fn decode_schema_row(row: SchemaRow) -> Result<elucid_catalog::Schema, CatalogPersistenceError> {
    let schema_id =
        SchemaId::try_from(row.schema_id).map_err(CatalogPersistenceError::corrupt_model)?;
    let source_id =
        SourceId::try_from(row.source_id).map_err(CatalogPersistenceError::corrupt_model)?;
    let version = stored_schema_version(row.version)?;
    decode_stored_schema_definition(schema_id, source_id, version, row.definition.0)
        .map_err(CatalogPersistenceError::from_catalog)
}

fn decode_input_row(
    row: InputRow,
    expected_source_id: SourceId,
    schemas: &HashMap<SchemaId, &elucid_catalog::Schema>,
    profile_rows: &mut HashMap<Uuid, Vec<ProfileRow>>,
) -> Result<elucid_catalog::Input, CatalogPersistenceError> {
    let input_id =
        InputId::try_from(row.input_id).map_err(CatalogPersistenceError::corrupt_model)?;
    let source_id =
        SourceId::try_from(row.source_id).map_err(CatalogPersistenceError::corrupt_model)?;
    if source_id != expected_source_id {
        return Err(CatalogPersistenceError::corrupt(
            "stored input belongs to the wrong source",
        ));
    }
    let name = InputName::try_from(row.name).map_err(CatalogPersistenceError::corrupt_model)?;
    let active_profile_revision_id =
        IngestionProfileRevisionId::try_from(row.active_profile_revision_id)
            .map_err(CatalogPersistenceError::corrupt_model)?;
    let revisions = profile_rows
        .remove(&row.input_id)
        .unwrap_or_default()
        .into_iter()
        .map(|profile| decode_profile_row(profile, input_id, source_id, schemas))
        .collect::<Result<Vec<_>, _>>()?;
    assemble_stored_input(
        input_id,
        source_id,
        name,
        active_profile_revision_id,
        revisions,
    )
    .map_err(CatalogPersistenceError::from_catalog)
}

fn decode_profile_row(
    row: ProfileRow,
    expected_input_id: InputId,
    expected_source_id: SourceId,
    schemas: &HashMap<SchemaId, &elucid_catalog::Schema>,
) -> Result<elucid_catalog::IngestionProfileRevision, CatalogPersistenceError> {
    let revision_id = IngestionProfileRevisionId::try_from(row.profile_revision_id)
        .map_err(CatalogPersistenceError::corrupt_model)?;
    let input_id =
        InputId::try_from(row.input_id).map_err(CatalogPersistenceError::corrupt_model)?;
    let source_id =
        SourceId::try_from(row.source_id).map_err(CatalogPersistenceError::corrupt_model)?;
    if input_id != expected_input_id || source_id != expected_source_id {
        return Err(CatalogPersistenceError::corrupt(
            "stored ingestion profile belongs to the wrong input or source",
        ));
    }
    let revision = stored_profile_revision(row.revision)?;
    let target_schema_id =
        SchemaId::try_from(row.target_schema_id).map_err(CatalogPersistenceError::corrupt_model)?;
    let target_schema = schemas.get(&target_schema_id).ok_or_else(|| {
        CatalogPersistenceError::corrupt("stored ingestion profile targets a missing schema")
    })?;
    decode_stored_profile_definition(
        revision_id,
        input_id,
        revision,
        target_schema,
        row.definition.0,
    )
    .map_err(CatalogPersistenceError::from_catalog)
}

async fn persist_plan(
    connection: &mut PgConnection,
    plan: &elucid_catalog::CatalogApplicationPlan,
    current: Option<&Source>,
) -> Result<(), CatalogPersistenceError> {
    let source = plan.source();
    match plan.source_definition().disposition() {
        CatalogEntityDisposition::Create => {
            require_one_row(
                sqlx::query(
                    "INSERT INTO sources (source_id, name, display_name, active_schema_id) VALUES ($1, $2, $3, $4)",
                )
                .bind(source.id().as_uuid())
                .bind(source.name().as_str())
                .bind(source.display_name())
                .bind(source.active_schema().id().as_uuid())
                .execute(&mut *connection)
                .await
                .map_err(CatalogPersistenceError::write)?,
                "source insert affected an unexpected number of rows",
            )?;
        }
        CatalogEntityDisposition::Existing
            if plan.outcome() != CatalogApplicationOutcome::Unchanged =>
        {
            require_one_row(
                sqlx::query(
                    "UPDATE sources SET display_name = $2, active_schema_id = $3, updated_at = CURRENT_TIMESTAMP WHERE source_id = $1",
                )
                .bind(source.id().as_uuid())
                .bind(source.display_name())
                .bind(source.active_schema().id().as_uuid())
                .execute(&mut *connection)
                .await
                .map_err(CatalogPersistenceError::write)?,
                "source update lost its locked row",
            )?;
        }
        CatalogEntityDisposition::Existing => {}
        _ => {
            return Err(CatalogPersistenceError::corrupt(
                "catalog plan contains an unknown source disposition",
            ));
        }
    }

    for definition in plan.schema_definitions() {
        match definition.disposition() {
            CatalogEntityDisposition::Create => {}
            CatalogEntityDisposition::Existing => continue,
            _ => {
                return Err(CatalogPersistenceError::corrupt(
                    "catalog plan contains an unknown schema disposition",
                ));
            }
        }
        let schema = source
            .schema(definition.schema_id())
            .ok_or_else(|| CatalogPersistenceError::corrupt("planned schema is absent"))?;
        let document = canonical_json_value(definition.materialized_definition().as_str())?;
        require_one_row(
            sqlx::query(
                "INSERT INTO schema_versions (schema_id, source_id, version, definition) VALUES ($1, $2, $3, $4)",
            )
            .bind(schema.id().as_uuid())
            .bind(source.id().as_uuid())
            .bind(database_version(schema.version().get())?)
            .bind(Json(document))
            .execute(&mut *connection)
            .await
            .map_err(CatalogPersistenceError::write)?,
            "schema insert affected an unexpected number of rows",
        )?;
    }

    for definition in plan.input_definitions() {
        let input = source
            .input(definition.input_id())
            .ok_or_else(|| CatalogPersistenceError::corrupt("planned input is absent"))?;
        match definition.disposition() {
            CatalogEntityDisposition::Create => {
                require_one_row(
                    sqlx::query(
                        "INSERT INTO inputs (input_id, source_id, name, active_profile_revision_id) VALUES ($1, $2, $3, $4)",
                    )
                    .bind(input.id().as_uuid())
                    .bind(source.id().as_uuid())
                    .bind(input.name().as_str())
                    .bind(input.active_profile_revision().id().as_uuid())
                    .execute(&mut *connection)
                    .await
                    .map_err(CatalogPersistenceError::write)?,
                    "input insert affected an unexpected number of rows",
                )?;
            }
            CatalogEntityDisposition::Existing if input_active_pointer_changed(current, input) => {
                require_one_row(
                    sqlx::query(
                        "UPDATE inputs SET active_profile_revision_id = $2, updated_at = CURRENT_TIMESTAMP WHERE input_id = $1",
                    )
                    .bind(input.id().as_uuid())
                    .bind(input.active_profile_revision().id().as_uuid())
                    .execute(&mut *connection)
                    .await
                    .map_err(CatalogPersistenceError::write)?,
                    "input update lost its row",
                )?;
            }
            CatalogEntityDisposition::Existing => {}
            _ => {
                return Err(CatalogPersistenceError::corrupt(
                    "catalog plan contains an unknown input disposition",
                ));
            }
        }
    }

    for definition in plan.ingestion_profile_definitions() {
        match definition.disposition() {
            CatalogEntityDisposition::Create => {}
            CatalogEntityDisposition::Existing => continue,
            _ => {
                return Err(CatalogPersistenceError::corrupt(
                    "catalog plan contains an unknown ingestion profile disposition",
                ));
            }
        }
        let revision = source
            .inputs()
            .iter()
            .flat_map(elucid_catalog::Input::profile_revisions)
            .find(|revision| revision.id() == definition.ingestion_profile_revision_id())
            .ok_or_else(|| CatalogPersistenceError::corrupt("planned profile is absent"))?;
        let document = canonical_json_value(definition.materialized_definition().as_str())?;
        require_one_row(
            sqlx::query(
                "INSERT INTO ingestion_profile_revisions (profile_revision_id, input_id, source_id, revision, target_schema_id, definition) VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(revision.id().as_uuid())
            .bind(revision.input_id().as_uuid())
            .bind(source.id().as_uuid())
            .bind(database_version(revision.revision().get())?)
            .bind(revision.target_schema_id().as_uuid())
            .bind(Json(document))
            .execute(&mut *connection)
            .await
            .map_err(CatalogPersistenceError::write)?,
            "profile insert affected an unexpected number of rows",
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DurableCatalogChange {
    Applied,
    Unchanged,
}

fn classify_application_outcome(
    outcome: CatalogApplicationOutcome,
) -> Result<DurableCatalogChange, CatalogPersistenceError> {
    match outcome {
        CatalogApplicationOutcome::Created | CatalogApplicationOutcome::Updated => {
            Ok(DurableCatalogChange::Applied)
        }
        CatalogApplicationOutcome::Unchanged => Ok(DurableCatalogChange::Unchanged),
        _ => Err(CatalogPersistenceError::corrupt(
            "catalog plan contains an unknown application outcome",
        )),
    }
}

fn input_active_pointer_changed(current: Option<&Source>, desired: &elucid_catalog::Input) -> bool {
    current
        .and_then(|source| source.input(desired.id()))
        .is_none_or(|stored| {
            stored.active_profile_revision().id() != desired.active_profile_revision().id()
        })
}

fn canonical_json_value(document: &str) -> Result<Value, CatalogPersistenceError> {
    serde_json::from_str(document)
        .map_err(|_| CatalogPersistenceError::corrupt("generated catalog JSON is invalid"))
}

fn database_version(version: u64) -> Result<i64, CatalogPersistenceError> {
    i64::try_from(version)
        .map_err(|_| CatalogPersistenceError::conflict("catalog version exceeds BIGINT"))
}

fn stored_schema_version(version: i64) -> Result<SchemaVersion, CatalogPersistenceError> {
    let version = u64::try_from(version)
        .map_err(|_| CatalogPersistenceError::corrupt("stored schema version is negative"))?;
    SchemaVersion::new(version).map_err(CatalogPersistenceError::corrupt_model)
}

fn stored_profile_revision(version: i64) -> Result<ProfileRevision, CatalogPersistenceError> {
    let version = u64::try_from(version).map_err(|_| {
        CatalogPersistenceError::corrupt("stored ingestion profile revision is negative")
    })?;
    ProfileRevision::new(version).map_err(CatalogPersistenceError::corrupt_model)
}

fn require_one_row(
    result: sqlx::postgres::PgQueryResult,
    message: &'static str,
) -> Result<(), CatalogPersistenceError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(CatalogPersistenceError::conflict(message))
    }
}

fn group_by<T, K>(values: Vec<T>, key: impl Fn(&T) -> K) -> HashMap<K, Vec<T>>
where
    K: Eq + std::hash::Hash,
{
    let mut grouped = HashMap::new();
    for value in values {
        grouped
            .entry(key(&value))
            .or_insert_with(Vec::new)
            .push(value);
    }
    grouped
}

struct UuidV7CatalogIdentityGenerator;

impl CatalogIdentityGenerator for UuidV7CatalogIdentityGenerator {
    fn generate_source_id(&mut self) -> SourceId {
        let uuid = Uuid::now_v7();
        SourceId::try_from(uuid).unwrap_or_else(|_| {
            // `Uuid::now_v7` always produces an RFC 9562 UUIDv7.
            unreachable!("UUIDv7 generator returned an invalid source identity")
        })
    }

    fn generate_schema_id(&mut self) -> SchemaId {
        let uuid = Uuid::now_v7();
        SchemaId::try_from(uuid).unwrap_or_else(|_| {
            // `Uuid::now_v7` always produces an RFC 9562 UUIDv7.
            unreachable!("UUIDv7 generator returned an invalid schema identity")
        })
    }

    fn generate_field_id(&mut self) -> elucid_catalog::FieldId {
        let uuid = Uuid::now_v7();
        elucid_catalog::FieldId::try_from(uuid).unwrap_or_else(|_| {
            // `Uuid::now_v7` always produces an RFC 9562 UUIDv7.
            unreachable!("UUIDv7 generator returned an invalid field identity")
        })
    }

    fn generate_input_id(&mut self) -> InputId {
        let uuid = Uuid::now_v7();
        InputId::try_from(uuid).unwrap_or_else(|_| {
            // `Uuid::now_v7` always produces an RFC 9562 UUIDv7.
            unreachable!("UUIDv7 generator returned an invalid input identity")
        })
    }

    fn generate_ingestion_profile_revision_id(&mut self) -> IngestionProfileRevisionId {
        let uuid = Uuid::now_v7();
        IngestionProfileRevisionId::try_from(uuid).unwrap_or_else(|_| {
            // `Uuid::now_v7` always produces an RFC 9562 UUIDv7.
            unreachable!("UUIDv7 generator returned an invalid profile identity")
        })
    }
}

#[derive(Debug)]
struct CatalogRows {
    sources: Vec<SourceRow>,
    schemas: Vec<SchemaRow>,
    inputs: Vec<InputRow>,
    profiles: Vec<ProfileRow>,
}

#[derive(Debug, FromRow)]
struct SourceRow {
    source_id: Uuid,
    name: String,
    display_name: String,
    active_schema_id: Uuid,
}

#[derive(Debug, FromRow)]
struct SchemaRow {
    schema_id: Uuid,
    source_id: Uuid,
    version: i64,
    definition: Json<Value>,
}

#[derive(Debug, FromRow)]
struct InputRow {
    input_id: Uuid,
    source_id: Uuid,
    name: String,
    active_profile_revision_id: Uuid,
}

#[derive(Debug, FromRow)]
struct ProfileRow {
    profile_revision_id: Uuid,
    input_id: Uuid,
    source_id: Uuid,
    revision: i64,
    target_schema_id: Uuid,
    definition: Json<Value>,
}
