use std::collections::{HashMap, HashSet};

use crate::{
    CatalogModelError, DeclarationDigest, FieldId, FieldRole, Input, InputId, Schema, SchemaId,
    SourceId, SourceName, VersionKind,
};

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Source {
    id: SourceId,
    name: SourceName,
    display_name: Box<str>,
    declaration_digest: DeclarationDigest,
    schemas: Box<[Schema]>,
    active_schema_index: usize,
    inputs: Box<[Input]>,
}

impl Source {
    pub fn new(
        id: SourceId,
        name: SourceName,
        display_name: impl Into<Box<str>>,
        declaration_digest: DeclarationDigest,
        active_schema_id: SchemaId,
        schemas: Vec<Schema>,
        inputs: Vec<Input>,
    ) -> Result<Self, CatalogModelError> {
        if schemas.is_empty() {
            return Err(CatalogModelError::EmptySchemaHistory);
        }

        let (schema_indexes, active_schema_index) =
            validate_schema_history(id, active_schema_id, &schemas)?;
        validate_inputs(id, &schema_indexes, &schemas, &inputs)?;

        Ok(Self {
            id,
            name,
            display_name: display_name.into(),
            declaration_digest,
            schemas: schemas.into_boxed_slice(),
            active_schema_index,
            inputs: inputs.into_boxed_slice(),
        })
    }

    #[must_use]
    pub const fn id(&self) -> SourceId {
        self.id
    }

    #[must_use]
    pub const fn name(&self) -> &SourceName {
        &self.name
    }

    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    #[must_use]
    pub const fn declaration_digest(&self) -> DeclarationDigest {
        self.declaration_digest
    }

    #[must_use]
    pub fn schemas(&self) -> &[Schema] {
        &self.schemas
    }

    #[must_use]
    pub fn active_schema(&self) -> &Schema {
        &self.schemas[self.active_schema_index]
    }

    #[must_use]
    pub fn inputs(&self) -> &[Input] {
        &self.inputs
    }

    #[must_use]
    pub fn schema(&self, schema_id: SchemaId) -> Option<&Schema> {
        self.schemas.iter().find(|schema| schema.id() == schema_id)
    }

    #[must_use]
    pub fn input(&self, input_id: InputId) -> Option<&Input> {
        self.inputs.iter().find(|input| input.id() == input_id)
    }
}

fn validate_schema_history(
    source_id: SourceId,
    active_schema_id: SchemaId,
    schemas: &[Schema],
) -> Result<(HashMap<SchemaId, usize>, usize), CatalogModelError> {
    let mut schema_indexes = HashMap::with_capacity(schemas.len());
    let mut active_schema_index = None;
    let mut names_by_id: HashMap<FieldId, String> = HashMap::new();
    let mut ids_by_name: HashMap<String, FieldId> = HashMap::new();

    for (index, schema) in schemas.iter().enumerate() {
        if schema.source_id() != source_id {
            return Err(CatalogModelError::SchemaSourceMismatch {
                schema_id: schema.id(),
                expected_source_id: source_id,
                actual_source_id: schema.source_id(),
            });
        }
        let expected = expected_sequence_value(index)?;
        let actual = schema.version().get();
        if actual != expected {
            return Err(CatalogModelError::SchemaVersionsMustBeContiguous { expected, actual });
        }
        if schema_indexes.insert(schema.id(), index).is_some() {
            return Err(CatalogModelError::DuplicateSchemaIdentity {
                schema_id: schema.id(),
            });
        }
        if schema.id() == active_schema_id {
            active_schema_index = Some(index);
        }
        validate_historical_field_identity(schema, &mut names_by_id, &mut ids_by_name)?;
    }

    let active_schema_index =
        active_schema_index.ok_or(CatalogModelError::ActiveSchemaNotFound {
            source_id,
            schema_id: active_schema_id,
        })?;
    Ok((schema_indexes, active_schema_index))
}

fn validate_historical_field_identity(
    schema: &Schema,
    names_by_id: &mut HashMap<FieldId, String>,
    ids_by_name: &mut HashMap<String, FieldId>,
) -> Result<(), CatalogModelError> {
    for field in schema
        .fields()
        .iter()
        .filter(|field| field.role() == FieldRole::Data)
    {
        if let Some(previous_name) = names_by_id.get(&field.id()) {
            if previous_name != field.name() {
                return Err(CatalogModelError::FieldIdentityReusedWithDifferentName {
                    field_id: field.id(),
                    previous_name: previous_name.clone(),
                    current_name: field.name().to_owned(),
                });
            }
        } else {
            names_by_id.insert(field.id(), field.name().to_owned());
        }

        if let Some(previous_field_id) = ids_by_name.get(field.name()) {
            if *previous_field_id != field.id() {
                return Err(CatalogModelError::FieldNameReusedWithDifferentIdentity {
                    name: field.name().to_owned(),
                    previous_field_id: *previous_field_id,
                    current_field_id: field.id(),
                });
            }
        } else {
            ids_by_name.insert(field.name().to_owned(), field.id());
        }
    }
    Ok(())
}

fn validate_inputs(
    source_id: SourceId,
    schema_indexes: &HashMap<SchemaId, usize>,
    schemas: &[Schema],
    inputs: &[Input],
) -> Result<(), CatalogModelError> {
    let mut input_ids = HashSet::with_capacity(inputs.len());
    let mut input_names = HashSet::with_capacity(inputs.len());
    for input in inputs {
        if input.source_id() != source_id {
            return Err(CatalogModelError::InputSourceMismatch {
                input_id: input.id(),
                expected_source_id: source_id,
                actual_source_id: input.source_id(),
            });
        }
        if !input_ids.insert(input.id()) {
            return Err(CatalogModelError::DuplicateInputIdentity {
                input_id: input.id(),
            });
        }
        if !input_names.insert(input.name().as_str()) {
            return Err(CatalogModelError::DuplicateInputName {
                name: input.name().as_str().to_owned(),
            });
        }
        for revision in input.profile_revisions() {
            let Some(schema_index) = schema_indexes.get(&revision.target_schema_id()) else {
                return Err(CatalogModelError::ProfileTargetSchemaNotFound {
                    profile_revision_id: revision.id(),
                    schema_id: revision.target_schema_id(),
                });
            };
            validate_profile_mappings(revision, &schemas[*schema_index])?;
        }
    }
    Ok(())
}

fn validate_profile_mappings(
    revision: &crate::IngestProfileRevision,
    schema: &Schema,
) -> Result<(), CatalogModelError> {
    for mapping in revision.profile().mappings() {
        let target = schema.field(mapping.target_field_id());
        if !matches!(target, Some(field) if field.role() == FieldRole::Data) {
            return Err(CatalogModelError::ProfileMappingTargetNotFound {
                profile_revision_id: revision.id(),
                schema_id: schema.id(),
                field_id: mapping.target_field_id(),
            });
        }
    }
    for field in schema
        .fields()
        .iter()
        .filter(|field| field.role() == FieldRole::Data)
    {
        if !revision
            .profile()
            .mappings()
            .iter()
            .any(|mapping| mapping.target_field_id() == field.id())
        {
            return Err(CatalogModelError::ProfileMappingMissing {
                profile_revision_id: revision.id(),
                schema_id: schema.id(),
                field_id: field.id(),
            });
        }
    }
    Ok(())
}

fn expected_sequence_value(index: usize) -> Result<u64, CatalogModelError> {
    u64::try_from(index)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or(CatalogModelError::HistoryLengthExceedsVersionRange {
            kind: VersionKind::Schema,
        })
}
