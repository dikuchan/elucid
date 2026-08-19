use std::collections::{HashMap, HashSet};

use crate::canonical::{
    DeclarationDocument, input_declaration, input_materialized, profile_materialized,
    profile_materialized_parts, schema_materialized, schema_materialized_parts,
    stored_input_materialized, stored_profile_declaration, stored_schema_declaration,
};
use crate::manifest::{ManifestIngestionProfileRevision, ManifestInput};
use crate::{
    CatalogApplicationError, CatalogManifest, CatalogModelError, CatalogPath, DeclarationDigest,
    DefinitionDigests, EventTimeMapping, FieldId, FieldMapping, FieldRole, IngestionProfile,
    IngestionProfileRevision, IngestionProfileRevisionId, Input, InputId, MaterializedDigest,
    ProfileRevision, Schema, SchemaId, SchemaVersion, Source, SourceId, UserField,
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CanonicalJson(String);

impl CanonicalJson {
    #[must_use]
    pub(crate) const fn new(value: String) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CatalogApplicationOutcome {
    Created,
    Updated,
    Unchanged,
}

impl CatalogApplicationOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "CREATED",
            Self::Updated => "UPDATED",
            Self::Unchanged => "UNCHANGED",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CatalogEntityDisposition {
    Existing,
    Create,
}

pub trait CatalogIdentityGenerator {
    fn generate_source_id(&mut self) -> SourceId;
    fn generate_schema_id(&mut self) -> SchemaId;
    fn generate_field_id(&mut self) -> FieldId;
    fn generate_input_id(&mut self) -> InputId;
    fn generate_ingestion_profile_revision_id(&mut self) -> IngestionProfileRevisionId;
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PlannedSourceDefinition {
    source_id: SourceId,
    disposition: CatalogEntityDisposition,
    declaration: CanonicalJson,
    declaration_digest: DeclarationDigest,
}

impl PlannedSourceDefinition {
    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn disposition(&self) -> CatalogEntityDisposition {
        self.disposition
    }

    #[must_use]
    pub const fn declaration(&self) -> &CanonicalJson {
        &self.declaration
    }

    #[must_use]
    pub const fn declaration_digest(&self) -> DeclarationDigest {
        self.declaration_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PlannedSchemaDefinition {
    schema_id: SchemaId,
    schema_version: SchemaVersion,
    disposition: CatalogEntityDisposition,
    declaration: CanonicalJson,
    declaration_digest: DeclarationDigest,
    materialized_definition: CanonicalJson,
    materialized_digest: MaterializedDigest,
}

impl PlannedSchemaDefinition {
    #[must_use]
    pub const fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    #[must_use]
    pub const fn disposition(&self) -> CatalogEntityDisposition {
        self.disposition
    }

    #[must_use]
    pub const fn declaration(&self) -> &CanonicalJson {
        &self.declaration
    }

    #[must_use]
    pub const fn declaration_digest(&self) -> DeclarationDigest {
        self.declaration_digest
    }

    #[must_use]
    pub const fn materialized_definition(&self) -> &CanonicalJson {
        &self.materialized_definition
    }

    #[must_use]
    pub const fn materialized_digest(&self) -> MaterializedDigest {
        self.materialized_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PlannedIngestionProfileDefinition {
    ingestion_profile_revision_id: IngestionProfileRevisionId,
    revision: ProfileRevision,
    disposition: CatalogEntityDisposition,
    declaration: CanonicalJson,
    declaration_digest: DeclarationDigest,
    materialized_definition: CanonicalJson,
    materialized_digest: MaterializedDigest,
}

impl PlannedIngestionProfileDefinition {
    #[must_use]
    pub const fn ingestion_profile_revision_id(&self) -> IngestionProfileRevisionId {
        self.ingestion_profile_revision_id
    }

    #[must_use]
    pub const fn revision(&self) -> ProfileRevision {
        self.revision
    }

    #[must_use]
    pub const fn disposition(&self) -> CatalogEntityDisposition {
        self.disposition
    }

    #[must_use]
    pub const fn declaration(&self) -> &CanonicalJson {
        &self.declaration
    }

    #[must_use]
    pub const fn declaration_digest(&self) -> DeclarationDigest {
        self.declaration_digest
    }

    #[must_use]
    pub const fn materialized_definition(&self) -> &CanonicalJson {
        &self.materialized_definition
    }

    #[must_use]
    pub const fn materialized_digest(&self) -> MaterializedDigest {
        self.materialized_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PlannedInputDefinition {
    input_id: InputId,
    disposition: CatalogEntityDisposition,
    declaration: CanonicalJson,
    declaration_digest: DeclarationDigest,
    materialized_definition: CanonicalJson,
    materialized_digest: MaterializedDigest,
}

impl PlannedInputDefinition {
    #[must_use]
    pub const fn input_id(&self) -> InputId {
        self.input_id
    }

    #[must_use]
    pub const fn disposition(&self) -> CatalogEntityDisposition {
        self.disposition
    }

    #[must_use]
    pub const fn declaration(&self) -> &CanonicalJson {
        &self.declaration
    }

    #[must_use]
    pub const fn declaration_digest(&self) -> DeclarationDigest {
        self.declaration_digest
    }

    #[must_use]
    pub const fn materialized_definition(&self) -> &CanonicalJson {
        &self.materialized_definition
    }

    #[must_use]
    pub const fn materialized_digest(&self) -> MaterializedDigest {
        self.materialized_digest
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CatalogApplicationPlan {
    outcome: CatalogApplicationOutcome,
    source: Source,
    source_definition: PlannedSourceDefinition,
    schema_definitions: Vec<PlannedSchemaDefinition>,
    input_definitions: Vec<PlannedInputDefinition>,
    ingestion_profile_definitions: Vec<PlannedIngestionProfileDefinition>,
}

impl CatalogApplicationPlan {
    #[must_use]
    pub const fn outcome(&self) -> CatalogApplicationOutcome {
        self.outcome
    }

    #[must_use]
    pub const fn source(&self) -> &Source {
        &self.source
    }

    #[must_use]
    pub const fn source_definition(&self) -> &PlannedSourceDefinition {
        &self.source_definition
    }

    #[must_use]
    pub fn schema_definitions(&self) -> &[PlannedSchemaDefinition] {
        &self.schema_definitions
    }

    #[must_use]
    pub fn input_definitions(&self) -> &[PlannedInputDefinition] {
        &self.input_definitions
    }

    #[must_use]
    pub fn ingestion_profile_definitions(&self) -> &[PlannedIngestionProfileDefinition] {
        &self.ingestion_profile_definitions
    }
}

pub fn plan_catalog_application(
    manifest: &CatalogManifest,
    current: Option<&Source>,
    identities: &mut impl CatalogIdentityGenerator,
) -> Result<CatalogApplicationPlan, CatalogApplicationError> {
    validate_current_source_name(manifest, current)?;
    let source_declaration = manifest.declarations.source.clone();
    let (source_id, source_disposition) = match current {
        Some(source) => {
            verify_declaration_digest(
                "source",
                source.declaration_digest(),
                source_declaration.digest,
            )?;
            (source.id(), CatalogEntityDisposition::Existing)
        }
        None => (
            identities.generate_source_id(),
            CatalogEntityDisposition::Create,
        ),
    };

    let PlannedSchemas {
        schemas,
        definitions: schema_definitions,
    } = plan_schemas(manifest, current, source_id, identities)?;
    let active_schema_id = schemas
        .iter()
        .find(|schema| schema.version() == manifest.source.active_schema_version)
        .map(Schema::id)
        .ok_or_else(|| {
            CatalogApplicationError::corruption(
                "source.active_schema_version",
                "validated active schema is absent from materialized history",
            )
        })?;
    let PlannedInputs {
        inputs,
        input_definitions,
        ingestion_profile_definitions,
    } = plan_inputs(manifest, current, source_id, &schemas, identities)?;

    let mutable_change = current.is_some_and(|source| {
        source.display_name() != manifest.source.display_name.as_str()
            || source.active_schema().id() != active_schema_id
            || active_profile_pointer_changed(source, &inputs)
    });
    let source = Source::new(
        source_id,
        manifest.source.name.clone(),
        manifest.source.display_name.clone(),
        source_declaration.digest,
        active_schema_id,
        schemas,
        inputs,
    )
    .map_err(catalog_source_error)?;

    let created_immutable = source_disposition == CatalogEntityDisposition::Create
        || schema_definitions
            .iter()
            .any(|definition| definition.disposition == CatalogEntityDisposition::Create)
        || input_definitions
            .iter()
            .any(|definition| definition.disposition == CatalogEntityDisposition::Create)
        || ingestion_profile_definitions
            .iter()
            .any(|definition| definition.disposition == CatalogEntityDisposition::Create);
    let outcome = if created_immutable {
        CatalogApplicationOutcome::Created
    } else if mutable_change {
        CatalogApplicationOutcome::Updated
    } else {
        CatalogApplicationOutcome::Unchanged
    };

    Ok(CatalogApplicationPlan {
        outcome,
        source,
        source_definition: PlannedSourceDefinition {
            source_id,
            disposition: source_disposition,
            declaration: source_declaration.json,
            declaration_digest: source_declaration.digest,
        },
        schema_definitions,
        input_definitions,
        ingestion_profile_definitions,
    })
}

fn plan_schemas(
    manifest: &CatalogManifest,
    current: Option<&Source>,
    source_id: SourceId,
    identities: &mut impl CatalogIdentityGenerator,
) -> Result<PlannedSchemas, CatalogApplicationError> {
    let current_schemas = current.map_or(&[][..], Source::schemas);
    if current_schemas.len() > manifest.source.schemas.len() {
        return Err(CatalogApplicationError::HistoryDiverged {
            path: CatalogPath::new("source.schemas"),
            message: format!(
                "manifest declares {} schema versions but persisted history contains {}",
                manifest.source.schemas.len(),
                current_schemas.len()
            ),
        });
    }

    let mut field_id_by_name = HashMap::new();
    let mut schemas = Vec::with_capacity(manifest.source.schemas.len());
    let mut definitions = Vec::with_capacity(manifest.source.schemas.len());
    for (index, declared_schema) in manifest.source.schemas.iter().enumerate() {
        let path = format!("source.schemas[{index}]");
        let declaration = manifest.declarations.schemas[index].clone();
        let (schema, materialized, disposition) = match current_schemas.get(index) {
            Some(stored) => {
                verify_stored_schema(stored, &path)?;
                if stored.declaration_digest() != declaration.digest {
                    return Err(CatalogApplicationError::DefinitionConflict {
                        path: CatalogPath::new(path),
                    });
                }
                let materialized = schema_materialized(stored, &path)?;
                (
                    stored.clone(),
                    materialized,
                    CatalogEntityDisposition::Existing,
                )
            }
            None => {
                let schema_id = identities.generate_schema_id();
                let user_fields = declared_schema
                    .fields
                    .iter()
                    .map(|field| {
                        let field_id = field_id_by_name
                            .get(field.name.as_str())
                            .copied()
                            .unwrap_or_else(|| identities.generate_field_id());
                        let user_field = UserField::new(
                            field_id,
                            field.name.clone(),
                            field.logical_type,
                            field.nullability,
                        )
                        .map_err(|error| {
                            CatalogApplicationError::manifest_model(format!("{path}.fields"), error)
                        })?;
                        let user_field = match &field.description {
                            Some(description) => user_field.with_description(description.clone()),
                            None => user_field,
                        };
                        match &field.historical_remainder_pointer {
                            Some(pointer) => user_field
                                .with_historical_remainder_pointer(pointer.clone())
                                .map_err(|error| {
                                    CatalogApplicationError::manifest_model(
                                        format!("{path}.fields"),
                                        error,
                                    )
                                }),
                            None => Ok(user_field),
                        }
                    })
                    .collect::<Result<Vec<_>, CatalogApplicationError>>()?;
                let schema_materialization = Schema::materialize_user_fields(user_fields)
                    .map_err(|error| CatalogApplicationError::manifest_model(&path, error))?;
                let materialized = schema_materialized_parts(
                    schema_id,
                    source_id,
                    declared_schema.version,
                    schema_materialization.fields(),
                    &path,
                )?;
                let schema = Schema::from_materialization(
                    schema_id,
                    source_id,
                    declared_schema.version,
                    DefinitionDigests::new(declaration.digest, materialized.digest),
                    schema_materialization,
                );
                (schema, materialized, CatalogEntityDisposition::Create)
            }
        };

        for field in schema
            .fields()
            .iter()
            .filter(|field| field.role() == FieldRole::Data)
        {
            field_id_by_name.insert(field.name().to_owned(), field.id());
        }
        definitions.push(PlannedSchemaDefinition {
            schema_id: schema.id(),
            schema_version: schema.version(),
            disposition,
            declaration: declaration.json,
            declaration_digest: declaration.digest,
            materialized_definition: materialized.json,
            materialized_digest: materialized.digest,
        });
        schemas.push(schema);
    }
    Ok(PlannedSchemas {
        schemas,
        definitions,
    })
}

struct PlannedSchemas {
    schemas: Vec<Schema>,
    definitions: Vec<PlannedSchemaDefinition>,
}

fn plan_inputs(
    manifest: &CatalogManifest,
    current: Option<&Source>,
    source_id: SourceId,
    schemas: &[Schema],
    identities: &mut impl CatalogIdentityGenerator,
) -> Result<PlannedInputs, CatalogApplicationError> {
    let current_inputs = current.map_or(&[][..], Source::inputs);
    let declared_names = manifest
        .source
        .inputs
        .iter()
        .map(|input| input.name.as_str())
        .collect::<HashSet<_>>();
    if let Some(omitted) = current_inputs
        .iter()
        .find(|input| !declared_names.contains(input.name().as_str()))
    {
        return Err(CatalogApplicationError::HistoryDiverged {
            path: CatalogPath::new("source.inputs"),
            message: format!("persisted input {:?} is omitted", omitted.name().as_str()),
        });
    }
    let current_by_name = current_inputs
        .iter()
        .map(|input| (input.name().as_str(), input))
        .collect::<HashMap<_, _>>();
    let mut declared_inputs = manifest
        .source
        .inputs
        .iter()
        .enumerate()
        .collect::<Vec<_>>();
    declared_inputs.sort_unstable_by(|(_, left), (_, right)| left.name.cmp(&right.name));

    let mut inputs = Vec::with_capacity(declared_inputs.len());
    let mut input_definitions = Vec::with_capacity(declared_inputs.len());
    let mut profile_definitions = Vec::new();
    for (input_index, declared_input) in declared_inputs {
        let path = format!("source.inputs[{:?}]", declared_input.name.as_str());
        let declarations = &manifest.declarations.inputs[input_index];
        let declaration = declarations.input.clone();
        let stored = current_by_name.get(declared_input.name.as_str()).copied();
        let (input_id, input_disposition) = match stored {
            Some(stored) => {
                verify_stored_input(stored, &path)?;
                if stored.declaration_digest() != declaration.digest {
                    return Err(CatalogApplicationError::DefinitionConflict {
                        path: CatalogPath::new(path),
                    });
                }
                (stored.id(), CatalogEntityDisposition::Existing)
            }
            None => (
                identities.generate_input_id(),
                CatalogEntityDisposition::Create,
            ),
        };
        let materialized = input_materialized(input_id, source_id, &declared_input.name, &path)?;
        if let Some(stored) = stored {
            verify_materialized_digest(&path, stored.materialized_digest(), materialized.digest)?;
        }

        let PlannedProfiles {
            revisions,
            mut definitions,
        } = plan_profiles(
            declared_input,
            &declarations.ingestion_profiles,
            stored,
            input_id,
            schemas,
            identities,
            &path,
        )?;
        let active_profile_revision_id = revisions
            .iter()
            .find(|revision| {
                revision.revision() == declared_input.active_ingestion_profile_revision
            })
            .map(IngestionProfileRevision::id)
            .ok_or_else(|| {
                CatalogApplicationError::corruption(
                    &path,
                    "validated active ingestion profile is absent from materialized history",
                )
            })?;
        let input = Input::new(
            input_id,
            source_id,
            declared_input.name.clone(),
            DefinitionDigests::new(declaration.digest, materialized.digest),
            active_profile_revision_id,
            revisions,
        )
        .map_err(|error| CatalogApplicationError::manifest_model(&path, error))?;
        input_definitions.push(PlannedInputDefinition {
            input_id,
            disposition: input_disposition,
            declaration: declaration.json,
            declaration_digest: declaration.digest,
            materialized_definition: materialized.json,
            materialized_digest: materialized.digest,
        });
        profile_definitions.append(&mut definitions);
        inputs.push(input);
    }
    Ok(PlannedInputs {
        inputs,
        input_definitions,
        ingestion_profile_definitions: profile_definitions,
    })
}

struct PlannedInputs {
    inputs: Vec<Input>,
    input_definitions: Vec<PlannedInputDefinition>,
    ingestion_profile_definitions: Vec<PlannedIngestionProfileDefinition>,
}

fn plan_profiles(
    declared_input: &ManifestInput,
    declarations: &[DeclarationDocument],
    stored_input: Option<&Input>,
    input_id: InputId,
    schemas: &[Schema],
    identities: &mut impl CatalogIdentityGenerator,
    input_path: &str,
) -> Result<PlannedProfiles, CatalogApplicationError> {
    let stored_revisions = stored_input.map_or(&[][..], Input::profile_revisions);
    if stored_revisions.len() > declared_input.ingestion_profile_revisions.len() {
        return Err(CatalogApplicationError::HistoryDiverged {
            path: CatalogPath::new(format!("{input_path}.ingestion_profile_revisions")),
            message: format!(
                "manifest declares {} revisions but persisted history contains {}",
                declared_input.ingestion_profile_revisions.len(),
                stored_revisions.len()
            ),
        });
    }

    let mut revisions = Vec::with_capacity(declared_input.ingestion_profile_revisions.len());
    let mut definitions = Vec::with_capacity(declared_input.ingestion_profile_revisions.len());
    for (index, declared_revision) in declared_input
        .ingestion_profile_revisions
        .iter()
        .enumerate()
    {
        let path = format!("{input_path}.ingestion_profile_revisions[{index}]");
        let declaration = declarations[index].clone();
        let target_schema = schema_by_version(schemas, declared_revision.target_schema_version)
            .ok_or_else(|| {
                CatalogApplicationError::corruption(
                    &path,
                    "validated target schema is absent from materialized history",
                )
            })?;
        let (revision, materialized, disposition) = match stored_revisions.get(index) {
            Some(stored) => {
                let stored_target = schemas
                    .iter()
                    .find(|schema| schema.id() == stored.target_schema_id())
                    .ok_or_else(|| {
                        CatalogApplicationError::corruption(
                            &path,
                            format!(
                                "persisted target schema {} is absent",
                                stored.target_schema_id()
                            ),
                        )
                    })?;
                verify_stored_profile(stored, stored_target, &path)?;
                if stored.declaration_digest() != declaration.digest {
                    return Err(CatalogApplicationError::DefinitionConflict {
                        path: CatalogPath::new(path),
                    });
                }
                let materialized = profile_materialized(stored, &path)?;
                (
                    stored.clone(),
                    materialized,
                    CatalogEntityDisposition::Existing,
                )
            }
            None => {
                let revision_id = identities.generate_ingestion_profile_revision_id();
                let profile = materialize_profile(declared_revision, target_schema, &path)?;
                let materialized = profile_materialized_parts(
                    revision_id,
                    input_id,
                    declared_revision.revision,
                    target_schema.id(),
                    &profile,
                    &path,
                )?;
                let revision = IngestionProfileRevision::new(
                    revision_id,
                    input_id,
                    declared_revision.revision,
                    target_schema.id(),
                    DefinitionDigests::new(declaration.digest, materialized.digest),
                    profile,
                );
                (revision, materialized, CatalogEntityDisposition::Create)
            }
        };
        definitions.push(PlannedIngestionProfileDefinition {
            ingestion_profile_revision_id: revision.id(),
            revision: revision.revision(),
            disposition,
            declaration: declaration.json,
            declaration_digest: declaration.digest,
            materialized_definition: materialized.json,
            materialized_digest: materialized.digest,
        });
        revisions.push(revision);
    }
    Ok(PlannedProfiles {
        revisions,
        definitions,
    })
}

struct PlannedProfiles {
    revisions: Vec<IngestionProfileRevision>,
    definitions: Vec<PlannedIngestionProfileDefinition>,
}

fn materialize_profile(
    revision: &ManifestIngestionProfileRevision,
    target_schema: &Schema,
    path: &str,
) -> Result<IngestionProfile, CatalogApplicationError> {
    let mappings = revision
        .mappings
        .iter()
        .map(|mapping| {
            let field = target_schema
                .fields()
                .iter()
                .find(|field| {
                    field.role() == FieldRole::Data && field.name() == mapping.target_field.as_str()
                })
                .ok_or_else(|| {
                    CatalogApplicationError::profile_invalid(
                        path,
                        format!(
                            "field {:?} is absent from target schema {}",
                            mapping.target_field.as_str(),
                            target_schema.id()
                        ),
                    )
                })?;
            FieldMapping::new(field.id(), mapping.json_pointer.clone())
                .map_err(|error| CatalogApplicationError::manifest_model(path, error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    IngestionProfile::new(
        revision.maximum_record_bytes,
        EventTimeMapping::new(
            revision.event_time.json_pointer.clone(),
            revision.event_time.format,
        ),
        mappings,
    )
    .map_err(|error| CatalogApplicationError::manifest_model(path, error))
}

fn verify_stored_schema(schema: &Schema, path: &str) -> Result<(), CatalogApplicationError> {
    let declaration = stored_schema_declaration(schema, path)?;
    verify_declaration_digest(path, schema.declaration_digest(), declaration.digest)?;
    let materialized = schema_materialized(schema, path)?;
    verify_materialized_digest(path, schema.materialized_digest(), materialized.digest)
}

fn verify_stored_input(input: &Input, path: &str) -> Result<(), CatalogApplicationError> {
    let declaration = input_declaration(input.name(), path)?;
    verify_declaration_digest(path, input.declaration_digest(), declaration.digest)?;
    let materialized = stored_input_materialized(input, path)?;
    verify_materialized_digest(path, input.materialized_digest(), materialized.digest)
}

fn verify_stored_profile(
    revision: &IngestionProfileRevision,
    target_schema: &Schema,
    path: &str,
) -> Result<(), CatalogApplicationError> {
    let declaration = stored_profile_declaration(revision, target_schema, path)?;
    verify_declaration_digest(path, revision.declaration_digest(), declaration.digest)?;
    let materialized = profile_materialized(revision, path)?;
    verify_materialized_digest(path, revision.materialized_digest(), materialized.digest)
}

fn verify_declaration_digest(
    path: &str,
    stored: DeclarationDigest,
    computed: DeclarationDigest,
) -> Result<(), CatalogApplicationError> {
    if stored == computed {
        Ok(())
    } else {
        Err(CatalogApplicationError::corruption(
            path,
            "persisted declaration digest does not match its definition",
        ))
    }
}

fn verify_materialized_digest(
    path: &str,
    stored: MaterializedDigest,
    computed: MaterializedDigest,
) -> Result<(), CatalogApplicationError> {
    if stored == computed {
        Ok(())
    } else {
        Err(CatalogApplicationError::corruption(
            path,
            "persisted materialized digest does not match its definition",
        ))
    }
}

fn validate_current_source_name(
    manifest: &CatalogManifest,
    current: Option<&Source>,
) -> Result<(), CatalogApplicationError> {
    if let Some(source) = current
        && source.name() != &manifest.source.name
    {
        return Err(CatalogApplicationError::corruption(
            "source.name",
            format!(
                "resolved source is named {:?}, manifest declares {:?}",
                source.name().as_str(),
                manifest.source.name.as_str()
            ),
        ));
    }
    Ok(())
}

fn active_profile_pointer_changed(current: &Source, desired_inputs: &[Input]) -> bool {
    desired_inputs.iter().any(|desired| {
        current
            .inputs()
            .iter()
            .find(|stored| stored.name() == desired.name())
            .is_some_and(|stored| {
                stored.active_profile_revision().id() != desired.active_profile_revision().id()
            })
    })
}

fn catalog_source_error(error: CatalogModelError) -> CatalogApplicationError {
    match error {
        CatalogModelError::SchemaHistoryIncompatible {
            earlier_schema_version,
            later_schema_version,
            reason,
        } => CatalogApplicationError::SchemaIncompatible {
            path: CatalogPath::new("source.schemas"),
            earlier_schema_version,
            later_schema_version,
            reason,
        },
        error => CatalogApplicationError::corruption("source", error.to_string()),
    }
}

fn schema_by_version(schemas: &[Schema], version: SchemaVersion) -> Option<&Schema> {
    schemas.iter().find(|schema| schema.version() == version)
}
