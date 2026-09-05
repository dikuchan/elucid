use elucid_core::{UuidV7, UuidV7Error};
use uuid::Uuid;

#[test]
fn uuid_v7_preserves_valid_identities_and_rejects_other_versions_and_variants() {
    for canonical in [
        "00000000-0000-7000-8000-000000000001",
        "0198ae1d-2910-7abc-bdef-0123456789ab",
        "ffffffff-ffff-7fff-bfff-ffffffffffff",
    ] {
        let uuid = canonical.parse::<Uuid>().expect("valid UUID fixture");
        let identity = UuidV7::try_from(uuid).expect("valid UUIDv7");
        assert_eq!(identity.to_string(), canonical);
        assert_eq!(canonical.parse::<UuidV7>(), Ok(identity));
    }

    for value in [
        Uuid::nil(),
        Uuid::max(),
        Uuid::from_u128(0x0198_ae1d_2910_4abc_bdef_0123_4567_89ab),
        Uuid::from_u128(0x0198_ae1d_2910_8abc_bdef_0123_4567_89ab),
        Uuid::from_u128(0x0198_ae1d_2910_7abc_0def_0123_4567_89ab),
        Uuid::from_u128(0x0198_ae1d_2910_7abc_cdef_0123_4567_89ab),
        Uuid::from_u128(0x0198_ae1d_2910_7abc_edef_0123_4567_89ab),
    ] {
        assert!(matches!(
            UuidV7::try_from(value),
            Err(UuidV7Error::InvalidVersionOrVariant { value: rejected }) if rejected == value
        ));
        assert!(matches!(
            value.to_string().parse::<UuidV7>(),
            Err(UuidV7Error::InvalidVersionOrVariant { value: rejected }) if rejected == value
        ));
    }

    for malformed in ["", "0198ae1d-2910-7abc-bdef-0123456789ag"] {
        assert!(matches!(
            malformed.parse::<UuidV7>(),
            Err(UuidV7Error::Malformed { .. })
        ));
    }
}
