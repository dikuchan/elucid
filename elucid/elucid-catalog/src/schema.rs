use std::collections::{HashMap, HashSet};

use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};

use crate::{
    CatalogModelError, DeclarationDigest, DefinitionDigests, FieldId, FieldOrdinal, FieldRole,
    LogicalType, MaterializedDigest, Nullability, SchemaId, SchemaVersion, SourceId, UserFieldName,
    UserLogicalType,
};

const FIELD_ID_METADATA_KEY: &str = "elucid.field_id";
const LOGICAL_TYPE_METADATA_KEY: &str = "elucid.logical_type";

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct UserField {
    id: FieldId,
    name: UserFieldName,
    logical_type: UserLogicalType,
    nullability: Nullability,
    description: Option<Box<str>>,
}

impl UserField {
    pub fn new(
        id: FieldId,
        name: UserFieldName,
        logical_type: UserLogicalType,
        nullability: Nullability,
    ) -> Result<Self, CatalogModelError> {
        if id.is_system() {
            return Err(CatalogModelError::SystemFieldIdentityIsReserved { field_id: id });
        }
        Ok(Self {
            id,
            name,
            logical_type,
            nullability,
            description: None,
        })
    }

    #[must_use]
    pub fn with_description(mut self, description: impl Into<Box<str>>) -> Self {
        self.description = Some(description.into());
        self
    }

    #[must_use]
    pub const fn id(&self) -> FieldId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &UserFieldName {
        &self.name
    }

    #[must_use]
    pub const fn logical_type(&self) -> UserLogicalType {
        self.logical_type
    }

    #[must_use]
    pub const fn nullability(&self) -> Nullability {
        self.nullability
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Field {
    id: FieldId,
    name: Box<str>,
    logical_type: LogicalType,
    nullability: Nullability,
    role: FieldRole,
    ordinal: FieldOrdinal,
    description: Option<Box<str>>,
}

impl Field {
    #[must_use]
    pub const fn id(&self) -> FieldId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn logical_type(&self) -> LogicalType {
        self.logical_type
    }

    #[must_use]
    pub const fn nullability(&self) -> Nullability {
        self.nullability
    }

    #[must_use]
    pub const fn role(&self) -> FieldRole {
        self.role
    }

    #[must_use]
    pub const fn ordinal(&self) -> FieldOrdinal {
        self.ordinal
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn system(
        id: FieldId,
        name: &'static str,
        logical_type: LogicalType,
        nullability: Nullability,
        role: FieldRole,
        ordinal: usize,
    ) -> Result<Self, CatalogModelError> {
        Ok(Self {
            id,
            name: name.into(),
            logical_type,
            nullability,
            role,
            ordinal: FieldOrdinal::from_index(ordinal)?,
            description: None,
        })
    }

    fn user(field: UserField, ordinal: usize) -> Result<Self, CatalogModelError> {
        Ok(Self {
            id: field.id,
            name: field.name.as_str().into(),
            logical_type: field.logical_type.into(),
            nullability: field.nullability,
            role: FieldRole::Data,
            ordinal: FieldOrdinal::from_index(ordinal)?,
            description: field.description,
        })
    }

    fn to_arrow(&self) -> ArrowField {
        let mut metadata = HashMap::with_capacity(2);
        metadata.insert(FIELD_ID_METADATA_KEY.to_owned(), self.id.to_string());
        if let Some(value) = self.logical_type.metadata_value() {
            metadata.insert(LOGICAL_TYPE_METADATA_KEY.to_owned(), value.to_owned());
        }
        ArrowField::new(
            self.name(),
            self.logical_type.arrow_data_type(),
            self.nullability.is_nullable(),
        )
        .with_metadata(metadata)
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Schema {
    id: SchemaId,
    source_id: SourceId,
    version: SchemaVersion,
    digests: DefinitionDigests,
    fields: Box<[Field]>,
    arrow_schema: ArrowSchema,
}

impl Schema {
    pub fn new(
        id: SchemaId,
        source_id: SourceId,
        version: SchemaVersion,
        digests: DefinitionDigests,
        user_fields: Vec<UserField>,
    ) -> Result<Self, CatalogModelError> {
        validate_unique_user_fields(&user_fields)?;

        let field_capacity = user_fields
            .len()
            .checked_add(4)
            .ok_or(CatalogModelError::FieldOrdinalOverflow)?;
        FieldOrdinal::from_index(field_capacity - 1)?;
        let mut fields = Vec::with_capacity(field_capacity);
        fields.push(Field::system(
            FieldId::EVENT_TIME,
            "@event_time",
            LogicalType::Datetime,
            Nullability::NonNull,
            FieldRole::EventTime,
            0,
        )?);
        fields.push(Field::system(
            FieldId::INGEST_TIME,
            "@ingest_time",
            LogicalType::Datetime,
            Nullability::NonNull,
            FieldRole::IngestTime,
            1,
        )?);
        fields.push(Field::system(
            FieldId::EVENT_ID,
            "@event_id",
            LogicalType::Eid,
            Nullability::NonNull,
            FieldRole::EventId,
            2,
        )?);
        for (index, user_field) in user_fields.into_iter().enumerate() {
            let ordinal = index
                .checked_add(3)
                .ok_or(CatalogModelError::FieldOrdinalOverflow)?;
            fields.push(Field::user(user_field, ordinal)?);
        }
        let remainder_ordinal = fields.len();
        fields.push(Field::system(
            FieldId::REMAINDER,
            "@rest",
            LogicalType::Json,
            Nullability::Nullable,
            FieldRole::Remainder,
            remainder_ordinal,
        )?);

        let arrow_fields = fields.iter().map(Field::to_arrow).collect::<Vec<_>>();
        let arrow_schema = ArrowSchema::new(arrow_fields);
        Ok(Self {
            id,
            source_id,
            version,
            digests,
            fields: fields.into_boxed_slice(),
            arrow_schema,
        })
    }

    #[must_use]
    pub const fn id(&self) -> SchemaId {
        self.id
    }

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn version(&self) -> SchemaVersion {
        self.version
    }

    #[must_use]
    pub const fn declaration_digest(&self) -> DeclarationDigest {
        self.digests.declaration()
    }

    #[must_use]
    pub const fn materialized_digest(&self) -> MaterializedDigest {
        self.digests.materialized()
    }

    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    #[must_use]
    pub fn arrow_schema(&self) -> &ArrowSchema {
        &self.arrow_schema
    }

    #[must_use]
    pub fn field(&self, field_id: FieldId) -> Option<&Field> {
        self.fields.iter().find(|field| field.id == field_id)
    }
}

fn validate_unique_user_fields(user_fields: &[UserField]) -> Result<(), CatalogModelError> {
    let mut identities = HashSet::with_capacity(user_fields.len());
    let mut names = HashSet::with_capacity(user_fields.len());
    for field in user_fields {
        if !identities.insert(field.id) {
            return Err(CatalogModelError::DuplicateFieldIdentity { field_id: field.id });
        }
        if !names.insert(field.name.as_str()) {
            return Err(CatalogModelError::DuplicateFieldName {
                name: field.name.as_str().to_owned(),
            });
        }
    }
    Ok(())
}
