use std::borrow::Cow;
use std::sync::Arc;

use bytes::Bytes;
use object_store::{
    Attribute, Attributes, GetOptions, GetResult, ObjectMeta, ObjectStore, PutMode, PutOptions,
};

use crate::value::byte_size;
use crate::{ObjectDescriptor, ObjectDigest, ObjectReadRange, StorageError, TransferLimit};

const DIGEST_METADATA_KEY: &str = "elucid-blake3";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectUploadOutcome {
    Uploaded,
    AlreadyPresent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectVerificationOutcome {
    Verified,
    Absent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectDeleteOutcome {
    Deleted,
    AlreadyAbsent,
}

#[derive(Debug)]
pub struct ImmutableObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl ImmutableObjectStore {
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    /// Creates an object exactly once or accepts an already matching retry.
    ///
    /// # Errors
    ///
    /// Returns a typed capacity, integrity, upload, or verification failure.
    pub async fn upload(
        &self,
        descriptor: &ObjectDescriptor,
        bytes: Bytes,
        limit: TransferLimit,
    ) -> Result<ObjectUploadOutcome, StorageError> {
        enforce_limit(descriptor.expected_byte_size().get(), limit)?;
        verify_payload(descriptor, &bytes)?;
        if self.probe(descriptor, StorageError::verification).await?
            == ObjectVerificationOutcome::Verified
        {
            return Ok(ObjectUploadOutcome::AlreadyPresent);
        }

        let options = PutOptions {
            mode: PutMode::Create,
            attributes: expected_attributes(descriptor),
            ..PutOptions::default()
        };
        match self
            .inner
            .put_opts(descriptor.key().as_object_path(), bytes.into(), options)
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                return match self.probe(descriptor, StorageError::verification).await? {
                    ObjectVerificationOutcome::Verified => Ok(ObjectUploadOutcome::AlreadyPresent),
                    ObjectVerificationOutcome::Absent => Err(StorageError::verification_invariant(
                        "create-only upload reported a collision but the key is absent",
                    )),
                };
            }
            Err(source) => return Err(StorageError::upload(source)),
        }

        match self.probe(descriptor, StorageError::verification).await? {
            ObjectVerificationOutcome::Verified => Ok(ObjectUploadOutcome::Uploaded),
            ObjectVerificationOutcome::Absent => Err(StorageError::verification_invariant(
                "completed upload is absent during fresh verification",
            )),
        }
    }

    /// Verifies exact-key length, media type, and Elucid digest metadata.
    ///
    /// # Errors
    ///
    /// Returns a verification failure when metadata cannot be fetched and an integrity failure
    /// when the stored metadata contradicts the descriptor.
    pub async fn verify(
        &self,
        descriptor: &ObjectDescriptor,
    ) -> Result<ObjectVerificationOutcome, StorageError> {
        self.probe(descriptor, StorageError::verification).await
    }

    /// Reads and hashes one complete object within the caller's byte limit.
    ///
    /// # Errors
    ///
    /// Returns a typed capacity, availability, or integrity failure.
    pub async fn read_exact(
        &self,
        descriptor: &ObjectDescriptor,
        limit: TransferLimit,
    ) -> Result<Bytes, StorageError> {
        enforce_limit(descriptor.expected_byte_size().get(), limit)?;
        let result = self.get(descriptor, GetOptions::default()).await?;
        verify_result_metadata(descriptor, &result)?;
        let bytes = result.bytes().await.map_err(StorageError::unavailable)?;
        verify_payload(descriptor, &bytes)?;
        Ok(bytes)
    }

    /// Reads one validated non-empty byte range within the caller's byte limit.
    ///
    /// # Errors
    ///
    /// Returns a typed capacity, availability, or integrity failure.
    pub async fn read_range(
        &self,
        descriptor: &ObjectDescriptor,
        range: ObjectReadRange,
        limit: TransferLimit,
    ) -> Result<Bytes, StorageError> {
        if range.object_size() != descriptor.expected_byte_size() {
            return Err(StorageError::integrity(
                "object range was validated against a different object size",
            ));
        }
        enforce_limit(range.byte_length(), limit)?;
        let expected_range = range.start()..range.end();
        let result = self
            .get(
                descriptor,
                GetOptions {
                    range: Some(expected_range.clone().into()),
                    ..GetOptions::default()
                },
            )
            .await?;
        verify_result_metadata(descriptor, &result)?;
        if result.range != expected_range {
            return Err(StorageError::integrity(
                "object store returned a different byte range",
            ));
        }
        let bytes = result.bytes().await.map_err(StorageError::unavailable)?;
        if byte_size(&bytes)
            .map_err(|_| StorageError::integrity("range byte size overflow"))?
            .get()
            != range.byte_length()
        {
            return Err(StorageError::integrity(
                "object store returned an incomplete byte range",
            ));
        }
        Ok(bytes)
    }

    /// Deletes only the descriptor's exact key and confirms its absence.
    ///
    /// # Errors
    ///
    /// Returns an integrity failure before deletion when existing metadata does not match, or a
    /// typed delete failure when the exact key cannot be removed and confirmed absent.
    pub async fn delete_exact(
        &self,
        descriptor: &ObjectDescriptor,
    ) -> Result<ObjectDeleteOutcome, StorageError> {
        if self.probe(descriptor, StorageError::delete).await? == ObjectVerificationOutcome::Absent
        {
            return Ok(ObjectDeleteOutcome::AlreadyAbsent);
        }
        match self.inner.delete(descriptor.key().as_object_path()).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(source) => return Err(StorageError::delete(source)),
        }
        match self.probe(descriptor, StorageError::delete).await? {
            ObjectVerificationOutcome::Absent => Ok(ObjectDeleteOutcome::Deleted),
            ObjectVerificationOutcome::Verified => Err(StorageError::delete_invariant(
                "deleted object remains available at its exact key",
            )),
        }
    }

    async fn probe(
        &self,
        descriptor: &ObjectDescriptor,
        map_error: fn(object_store::Error) -> StorageError,
    ) -> Result<ObjectVerificationOutcome, StorageError> {
        let result = self
            .inner
            .get_opts(
                descriptor.key().as_object_path(),
                GetOptions {
                    head: true,
                    ..GetOptions::default()
                },
            )
            .await;
        match result {
            Ok(result) => {
                verify_result_metadata(descriptor, &result)?;
                Ok(ObjectVerificationOutcome::Verified)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(ObjectVerificationOutcome::Absent),
            Err(source) => Err(map_error(source)),
        }
    }

    async fn get(
        &self,
        descriptor: &ObjectDescriptor,
        options: GetOptions,
    ) -> Result<GetResult, StorageError> {
        match self
            .inner
            .get_opts(descriptor.key().as_object_path(), options)
            .await
        {
            Ok(result) => Ok(result),
            Err(object_store::Error::NotFound { .. }) => Err(StorageError::integrity(
                "registered object is absent at its exact key",
            )),
            Err(source) => Err(StorageError::unavailable(source)),
        }
    }
}

fn enforce_limit(bytes: u64, limit: TransferLimit) -> Result<(), StorageError> {
    if bytes <= limit.get() {
        Ok(())
    } else {
        Err(StorageError::capacity(
            "object transfer exceeds the caller's byte limit",
        ))
    }
}

fn verify_payload(descriptor: &ObjectDescriptor, bytes: &Bytes) -> Result<(), StorageError> {
    let actual_size = byte_size(bytes)
        .map_err(|_| StorageError::integrity("object byte size cannot be represented"))?;
    if actual_size != descriptor.expected_byte_size() {
        return Err(StorageError::integrity(
            "object bytes do not match the registered length",
        ));
    }
    if ObjectDigest::calculate(bytes) != descriptor.digest() {
        return Err(StorageError::integrity(
            "object bytes do not match the registered digest",
        ));
    }
    Ok(())
}

fn expected_attributes(descriptor: &ObjectDescriptor) -> Attributes {
    let mut attributes = Attributes::with_capacity(2);
    attributes.insert(
        Attribute::ContentType,
        descriptor.media_type().as_str().into(),
    );
    attributes.insert(
        digest_attribute(),
        descriptor.digest().metadata_value().into(),
    );
    attributes
}

fn verify_result_metadata(
    descriptor: &ObjectDescriptor,
    result: &GetResult,
) -> Result<(), StorageError> {
    verify_metadata(descriptor, &result.meta, &result.attributes)
}

fn verify_metadata(
    descriptor: &ObjectDescriptor,
    metadata: &ObjectMeta,
    attributes: &Attributes,
) -> Result<(), StorageError> {
    if &metadata.location != descriptor.key().as_object_path() {
        return Err(StorageError::integrity(
            "object store returned metadata for a different key",
        ));
    }
    if metadata.size != descriptor.expected_byte_size().get() {
        return Err(StorageError::integrity(
            "object metadata does not match the registered length",
        ));
    }
    let content_type = attributes.get(&Attribute::ContentType).ok_or_else(|| {
        StorageError::integrity("object metadata is missing the registered media type")
    })?;
    if content_type.as_ref() != descriptor.media_type().as_str() {
        return Err(StorageError::integrity(
            "object metadata does not match the registered media type",
        ));
    }
    let digest = attributes
        .get(&digest_attribute())
        .ok_or_else(|| StorageError::integrity("object metadata is missing the Elucid digest"))?;
    if digest.as_ref() != descriptor.digest().metadata_value() {
        return Err(StorageError::integrity(
            "object metadata does not match the registered digest",
        ));
    }
    Ok(())
}

fn digest_attribute() -> Attribute {
    Attribute::Metadata(Cow::Borrowed(DIGEST_METADATA_KEY))
}
