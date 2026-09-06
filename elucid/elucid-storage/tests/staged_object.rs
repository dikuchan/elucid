use bytes::Bytes;
use elucid_storage::{
    ManagedObjectKey, ManagedRoot, ObjectDescriptor, ObjectFormatVersion, ObjectMediaType,
    SegmentId, StagedObjectReadError, StoredObjectId, TransferLimit, read_staged_object,
};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn staged_reads_enforce_the_limit_exact_size_and_digest() {
    let directory = TempDir::new().expect("staging directory");
    let path = directory.path().join("segment.parquet");
    let bytes = Bytes::from_static(b"immutable staged bytes");
    let descriptor = ObjectDescriptor::for_bytes(
        ManagedObjectKey::parquet(
            &ManagedRoot::parse("staged").expect("managed root"),
            SegmentId::from(Uuid::from_u128(1)),
            StoredObjectId::from(Uuid::from_u128(2)),
        ),
        &bytes,
        ObjectMediaType::ParquetData,
        ObjectFormatVersion::new(1).expect("format version"),
    )
    .expect("descriptor");
    let limit = TransferLimit::new(bytes.len() as u64).expect("transfer limit");
    tokio::fs::write(&path, &bytes).await.expect("stage bytes");
    assert_eq!(
        read_staged_object(&path, &descriptor, limit)
            .await
            .expect("read exact bytes"),
        bytes
    );
    assert!(matches!(
        read_staged_object(
            &path,
            &descriptor,
            TransferLimit::new(limit.get() - 1).expect("smaller limit"),
        )
        .await,
        Err(StagedObjectReadError::CapacityExceeded)
    ));

    tokio::fs::write(&path, &bytes[..bytes.len() - 1])
        .await
        .expect("truncate staged bytes");
    assert!(matches!(
        read_staged_object(&path, &descriptor, limit).await,
        Err(StagedObjectReadError::SizeMismatch)
    ));

    let mut changed = bytes.to_vec();
    changed[0] ^= 1;
    tokio::fs::write(&path, &changed)
        .await
        .expect("corrupt bytes");
    assert!(matches!(
        read_staged_object(&path, &descriptor, limit).await,
        Err(StagedObjectReadError::DigestMismatch)
    ));

    changed.push(0);
    tokio::fs::write(&path, &changed)
        .await
        .expect("grow staged file");
    assert!(matches!(
        read_staged_object(&path, &descriptor, limit).await,
        Err(StagedObjectReadError::SizeMismatch)
    ));

    tokio::fs::remove_file(&path)
        .await
        .expect("remove staged file");
    let error = read_staged_object(&path, &descriptor, limit)
        .await
        .expect_err("missing file");
    assert!(matches!(
        error,
        StagedObjectReadError::Io(ref source) if source.kind() == std::io::ErrorKind::NotFound
    ));
}
