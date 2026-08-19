use std::collections::HashSet;

use crate::{
    CatalogModelError, DeclarationDigest, DefinitionDigests, FieldId, IngestionProfileRevisionId,
    InputId, InputName, JsonPointer, MaterializedDigest, MaximumRecordBytes, ProfileRevision,
    SchemaId, SourceId,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum EventTimeFormat {
    Rfc3339,
    UnixMilliseconds,
}

impl EventTimeFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rfc3339 => "RFC3339",
            Self::UnixMilliseconds => "UNIX_MILLISECONDS",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct FieldMapping {
    target_field_id: FieldId,
    json_pointer: JsonPointer,
}

impl FieldMapping {
    pub fn new(
        target_field_id: FieldId,
        json_pointer: JsonPointer,
    ) -> Result<Self, CatalogModelError> {
        if target_field_id.is_system() {
            return Err(CatalogModelError::SystemFieldCannotBeMapped {
                field_id: target_field_id,
            });
        }
        Ok(Self {
            target_field_id,
            json_pointer,
        })
    }

    #[must_use]
    pub const fn target_field_id(&self) -> FieldId {
        self.target_field_id
    }

    #[must_use]
    pub const fn json_pointer(&self) -> &JsonPointer {
        &self.json_pointer
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct EventTimeMapping {
    json_pointer: JsonPointer,
    format: EventTimeFormat,
}

impl EventTimeMapping {
    #[must_use]
    pub const fn new(json_pointer: JsonPointer, format: EventTimeFormat) -> Self {
        Self {
            json_pointer,
            format,
        }
    }

    #[must_use]
    pub const fn json_pointer(&self) -> &JsonPointer {
        &self.json_pointer
    }

    #[must_use]
    pub const fn format(&self) -> EventTimeFormat {
        self.format
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct IngestionProfile {
    maximum_record_bytes: MaximumRecordBytes,
    event_time: EventTimeMapping,
    mappings: Vec<FieldMapping>,
}

impl IngestionProfile {
    pub fn new(
        maximum_record_bytes: MaximumRecordBytes,
        event_time: EventTimeMapping,
        mappings: Vec<FieldMapping>,
    ) -> Result<Self, CatalogModelError> {
        let mut targets = HashSet::with_capacity(mappings.len());
        for mapping in &mappings {
            if !targets.insert(mapping.target_field_id) {
                return Err(CatalogModelError::DuplicateProfileMappingTarget {
                    field_id: mapping.target_field_id,
                });
            }
        }
        Ok(Self {
            maximum_record_bytes,
            event_time,
            mappings,
        })
    }

    #[must_use]
    pub const fn maximum_record_bytes(&self) -> MaximumRecordBytes {
        self.maximum_record_bytes
    }

    #[must_use]
    pub const fn event_time(&self) -> &EventTimeMapping {
        &self.event_time
    }

    #[must_use]
    pub fn mappings(&self) -> &[FieldMapping] {
        &self.mappings
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct IngestionProfileRevision {
    id: IngestionProfileRevisionId,
    input_id: InputId,
    revision: ProfileRevision,
    target_schema_id: SchemaId,
    digests: DefinitionDigests,
    profile: IngestionProfile,
}

impl IngestionProfileRevision {
    #[must_use]
    pub const fn new(
        id: IngestionProfileRevisionId,
        input_id: InputId,
        revision: ProfileRevision,
        target_schema_id: SchemaId,
        digests: DefinitionDigests,
        profile: IngestionProfile,
    ) -> Self {
        Self {
            id,
            input_id,
            revision,
            target_schema_id,
            digests,
            profile,
        }
    }

    #[must_use]
    pub const fn id(&self) -> IngestionProfileRevisionId {
        self.id
    }

    #[must_use]
    pub const fn input_id(&self) -> InputId {
        self.input_id
    }

    #[must_use]
    pub const fn revision(&self) -> ProfileRevision {
        self.revision
    }

    #[must_use]
    pub const fn target_schema_id(&self) -> SchemaId {
        self.target_schema_id
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
    pub const fn profile(&self) -> &IngestionProfile {
        &self.profile
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Input {
    id: InputId,
    source_id: SourceId,
    name: InputName,
    digests: DefinitionDigests,
    profile_revisions: Vec<IngestionProfileRevision>,
    active_profile_revision_index: usize,
}

impl Input {
    pub fn new(
        id: InputId,
        source_id: SourceId,
        name: InputName,
        digests: DefinitionDigests,
        active_profile_revision_id: IngestionProfileRevisionId,
        profile_revisions: Vec<IngestionProfileRevision>,
    ) -> Result<Self, CatalogModelError> {
        if profile_revisions.is_empty() {
            return Err(CatalogModelError::EmptyProfileRevisionHistory { input_id: id });
        }

        let mut revision_ids = HashSet::with_capacity(profile_revisions.len());
        let mut active_profile_revision_index = None;
        let mut previous_revision = None;
        for (index, revision) in profile_revisions.iter().enumerate() {
            if revision.input_id != id {
                return Err(CatalogModelError::ProfileRevisionInputMismatch {
                    profile_revision_id: revision.id,
                    expected_input_id: id,
                    actual_input_id: revision.input_id,
                });
            }
            let actual = revision.revision.get();
            if let Some(previous) = previous_revision
                && actual <= previous
            {
                return Err(CatalogModelError::ProfileRevisionsMustIncrease { previous, actual });
            }
            previous_revision = Some(actual);
            if !revision_ids.insert(revision.id) {
                return Err(CatalogModelError::DuplicateProfileRevisionIdentity {
                    profile_revision_id: revision.id,
                });
            }
            if revision.id == active_profile_revision_id {
                active_profile_revision_index = Some(index);
            }
        }
        let active_profile_revision_index = active_profile_revision_index.ok_or(
            CatalogModelError::ActiveProfileRevisionNotFound {
                input_id: id,
                profile_revision_id: active_profile_revision_id,
            },
        )?;

        Ok(Self {
            id,
            source_id,
            name,
            digests,
            profile_revisions,
            active_profile_revision_index,
        })
    }

    #[must_use]
    pub const fn id(&self) -> InputId {
        self.id
    }

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    #[must_use]
    pub const fn name(&self) -> &InputName {
        &self.name
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
    pub fn profile_revisions(&self) -> &[IngestionProfileRevision] {
        &self.profile_revisions
    }

    #[must_use]
    pub fn active_profile_revision(&self) -> &IngestionProfileRevision {
        &self.profile_revisions[self.active_profile_revision_index]
    }
}
