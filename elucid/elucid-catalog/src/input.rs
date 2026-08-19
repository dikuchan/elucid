use std::collections::HashSet;

use crate::{
    CatalogModelError, DeclarationDigest, DefinitionDigests, FieldId, IngestionProfileRevisionId,
    InputId, InputName, JsonPointer, MaterializedDigest, MaximumRecordBytes, ProfileRevision,
    SchemaId, SourceId, VersionKind,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum InputKind {
    HttpNdjson,
}

impl InputKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HttpNdjson => "HTTP_NDJSON",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ParserKind {
    Ndjson,
}

impl ParserKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ndjson => "NDJSON",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum InputEncoding {
    Utf8,
}

impl InputEncoding {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Utf8 => "UTF8",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum LineBoundaryPolicy {
    LfWithOptionalCr,
}

impl LineBoundaryPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LfWithOptionalCr => "LF_WITH_OPTIONAL_CR",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum UnknownFieldPolicy {
    CaptureTopLevelRemainder,
}

impl UnknownFieldPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CaptureTopLevelRemainder => "CAPTURE_TOP_LEVEL_REMAINDER",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ConversionPolicy {
    Strict,
}

impl ConversionPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "STRICT",
        }
    }
}

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
    event_time_mapping: EventTimeMapping,
    mappings: Box<[FieldMapping]>,
}

impl IngestionProfile {
    pub fn new(
        maximum_record_bytes: MaximumRecordBytes,
        event_time_mapping: EventTimeMapping,
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
            event_time_mapping,
            mappings: mappings.into_boxed_slice(),
        })
    }

    #[must_use]
    pub const fn parser_kind(&self) -> ParserKind {
        ParserKind::Ndjson
    }

    #[must_use]
    pub const fn encoding(&self) -> InputEncoding {
        InputEncoding::Utf8
    }

    #[must_use]
    pub const fn line_boundary_policy(&self) -> LineBoundaryPolicy {
        LineBoundaryPolicy::LfWithOptionalCr
    }

    #[must_use]
    pub const fn maximum_record_bytes(&self) -> MaximumRecordBytes {
        self.maximum_record_bytes
    }

    #[must_use]
    pub const fn event_time_mapping(&self) -> &EventTimeMapping {
        &self.event_time_mapping
    }

    #[must_use]
    pub fn mappings(&self) -> &[FieldMapping] {
        &self.mappings
    }

    #[must_use]
    pub const fn unknown_field_policy(&self) -> UnknownFieldPolicy {
        UnknownFieldPolicy::CaptureTopLevelRemainder
    }

    #[must_use]
    pub const fn conversion_policy(&self) -> ConversionPolicy {
        ConversionPolicy::Strict
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
    kind: InputKind,
    digests: DefinitionDigests,
    profile_revisions: Box<[IngestionProfileRevision]>,
    active_profile_revision_index: usize,
}

impl Input {
    pub fn new(
        id: InputId,
        source_id: SourceId,
        name: InputName,
        kind: InputKind,
        digests: DefinitionDigests,
        active_profile_revision_id: IngestionProfileRevisionId,
        profile_revisions: Vec<IngestionProfileRevision>,
    ) -> Result<Self, CatalogModelError> {
        if profile_revisions.is_empty() {
            return Err(CatalogModelError::EmptyProfileRevisionHistory { input_id: id });
        }

        let mut revision_ids = HashSet::with_capacity(profile_revisions.len());
        let mut active_profile_revision_index = None;
        for (index, revision) in profile_revisions.iter().enumerate() {
            if revision.input_id != id {
                return Err(CatalogModelError::ProfileRevisionInputMismatch {
                    profile_revision_id: revision.id,
                    expected_input_id: id,
                    actual_input_id: revision.input_id,
                });
            }
            let expected = expected_sequence_value(index)?;
            let actual = revision.revision.get();
            if actual != expected {
                return Err(CatalogModelError::ProfileRevisionsMustBeContiguous {
                    expected,
                    actual,
                });
            }
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
            kind,
            digests,
            profile_revisions: profile_revisions.into_boxed_slice(),
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
    pub const fn kind(&self) -> InputKind {
        self.kind
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

fn expected_sequence_value(index: usize) -> Result<u64, CatalogModelError> {
    u64::try_from(index)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or(CatalogModelError::HistoryLengthExceedsVersionRange {
            kind: VersionKind::IngestionProfileRevision,
        })
}
