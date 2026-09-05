use std::sync::Arc;
use std::time::Duration;

use elucid_core::{CodedError, ErrorCode};

use bytes::Bytes;
use elucid_storage::{
    ImmutableObjectStore, ManagedObjectKey, ManagedRoot, ObjectDeleteOutcome, ObjectDescriptor,
    ObjectFormatVersion, ObjectMediaType, ObjectReadRange, ObjectUploadOutcome,
    ObjectVerificationOutcome, SegmentId, StoredObjectId, TransferLimit,
};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions};
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::testcontainers::GenericImage;
use testcontainers_modules::testcontainers::core::{ImageExt as _, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner as _;
use uuid::Uuid;

const BUCKET: &str = "elucid-storage-test";
const MINIO_CLIENT_TAG: &str = "RELEASE.2025-02-21T16-00-46Z";

#[tokio::test]
#[ignore = "requires Docker"]
async fn exact_minio_operations_preserve_immutable_object_identity() {
    let test_identity = Uuid::now_v7().simple().to_string();
    let network = format!("elucid-storage-{test_identity}");
    let server_name = format!("elucid-storage-minio-{test_identity}");
    let minio = MinIO::default()
        .with_network(network.clone())
        .with_container_name(server_name.clone())
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("start MinIO");
    let minio_alias = format!("http://minioadmin:minioadmin@{server_name}:9000");
    let bucket_path = format!("local/{BUCKET}");
    let _bucket = GenericImage::new("minio/mc", MINIO_CLIENT_TAG)
        .with_wait_for(WaitFor::message_on_stdout("Bucket created successfully"))
        .with_network(network)
        .with_env_var("MC_HOST_local", minio_alias)
        .with_cmd(["mb", bucket_path.as_str()])
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .expect("create MinIO bucket");

    let host = minio.get_host().await.expect("MinIO host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("MinIO API port");
    let backend = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://{host}:{port}"))
            .with_bucket_name(BUCKET)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .expect("build S3 client"),
    );
    let objects = ImmutableObjectStore::new(backend.clone());
    let root = ManagedRoot::parse("showcase").expect("managed root");
    let key = ManagedObjectKey::parquet(
        &root,
        SegmentId::from(uuid(0x019d_0000_0000_7000_8000_0000_0000_0001)),
        StoredObjectId::from(uuid(0x019d_0000_0000_7000_8000_0000_0000_0002)),
    );
    assert_eq!(
        key.as_str(),
        "showcase/segments/019d0000-0000-7000-8000-000000000001/019d0000-0000-7000-8000-000000000002.parquet"
    );

    let bytes = Bytes::from_static(b"immutable parquet bytes");
    let descriptor = ObjectDescriptor::for_bytes(
        key,
        &bytes,
        ObjectMediaType::ParquetData,
        ObjectFormatVersion::new(1).expect("format version"),
    )
    .expect("object descriptor");
    let limit = TransferLimit::new(1_024).expect("transfer limit");
    let insufficient_limit = TransferLimit::new(descriptor.expected_byte_size().get() - 1)
        .expect("insufficient transfer limit");

    let error = objects
        .upload(&descriptor, bytes.clone(), insufficient_limit)
        .await
        .expect_err("upload must be rejected before exceeding its byte limit");
    assert_eq!(error.error_code(), ErrorCode::LocalCapacityExhausted);
    assert_eq!(
        objects
            .verify(&descriptor)
            .await
            .expect("verify rejected upload"),
        ObjectVerificationOutcome::Absent
    );

    assert_eq!(
        objects
            .upload(&descriptor, bytes.clone(), limit)
            .await
            .expect("upload object"),
        ObjectUploadOutcome::Uploaded
    );
    assert_eq!(
        objects
            .upload(&descriptor, bytes.clone(), limit)
            .await
            .expect("retry matching object"),
        ObjectUploadOutcome::AlreadyPresent
    );
    assert_eq!(
        objects.verify(&descriptor).await.expect("verify object"),
        ObjectVerificationOutcome::Verified
    );
    assert_eq!(
        objects
            .read_exact(&descriptor, limit)
            .await
            .expect("read exact object"),
        bytes
    );
    assert_eq!(
        objects
            .read_range(
                &descriptor,
                ObjectReadRange::new(10, 17, descriptor.expected_byte_size()).expect("read range"),
                limit,
            )
            .await
            .expect("read exact range"),
        Bytes::from_static(b"parquet")
    );

    let error = objects
        .upload(
            &descriptor,
            Bytes::from_static(b"different parquet bytes"),
            limit,
        )
        .await
        .expect_err("different bytes must not replace the object");
    assert_eq!(error.error_code(), ErrorCode::ObjectIntegrityError);
    assert_eq!(
        objects
            .read_exact(&descriptor, limit)
            .await
            .expect("read object after rejected overwrite"),
        bytes
    );

    let raw_path = ObjectPath::parse(descriptor.key().as_str()).expect("raw object path");
    backend
        .put_opts(
            &raw_path,
            Bytes::from_static(b"corrupt bytes").into(),
            PutOptions {
                mode: PutMode::Overwrite,
                ..PutOptions::default()
            },
        )
        .await
        .expect("corrupt object outside the boundary");
    let error = objects
        .verify(&descriptor)
        .await
        .expect_err("metadata mismatch must be an integrity error");
    assert_eq!(error.error_code(), ErrorCode::ObjectIntegrityError);

    backend
        .delete(&raw_path)
        .await
        .expect("remove corrupt object");
    assert_eq!(
        objects
            .upload(&descriptor, bytes, limit)
            .await
            .expect("restore object"),
        ObjectUploadOutcome::Uploaded
    );
    assert_eq!(
        objects
            .delete_exact(&descriptor)
            .await
            .expect("delete exact object"),
        ObjectDeleteOutcome::Deleted
    );
    assert_eq!(
        objects
            .delete_exact(&descriptor)
            .await
            .expect("confirm absent object"),
        ObjectDeleteOutcome::AlreadyAbsent
    );
    assert_eq!(
        objects.verify(&descriptor).await.expect("confirm absence"),
        ObjectVerificationOutcome::Absent
    );

    let error = objects
        .read_exact(&descriptor, limit)
        .await
        .expect_err("registered object is missing");
    assert_eq!(error.error_code(), ErrorCode::ObjectIntegrityError);
}

const fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
