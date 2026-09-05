use elucid_core::EventId;

#[test]
fn event_ids_preserve_exact_bytes_and_require_canonical_hex() {
    for (bytes, encoded) in [
        ([0_u8; 16], "00000000000000000000000000000000"),
        ([0xff; 16], "ffffffffffffffffffffffffffffffff"),
        (
            [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba,
                0xdc, 0xfe,
            ],
            "0123456789abcdef1032547698badcfe",
        ),
    ] {
        let event_id = encoded.parse::<EventId>().expect("canonical event ID");
        assert_eq!(event_id.as_bytes(), &bytes);
        assert_eq!(EventId::from(bytes).to_string(), encoded);
    }

    for invalid in [
        "",
        "0123456789abcdef1032547698badcf",
        "0123456789abcdef1032547698badcfe0",
        "0123456789ABCDEF1032547698BADCFE",
        "0123456789abcdef1032547698badcfg",
        "01234567-89ab-cdef-1032-547698badcfe",
        " 123456789abcdef1032547698badcfe",
        "0123456789abcdef1032547698badcf ",
        "éééééééééééééééé",
    ] {
        assert!(invalid.parse::<EventId>().is_err(), "{invalid:?}");
    }
}
