import pytest

from taskiq.refs import (
    BLOB_MARKER_KEY,
    BLOB_PROTOCOL_VERSION,
    Blob,
    BlobRefError,
    StoredBlob,
    digest_blob,
    is_wire_ref,
    parse_wire_ref,
)


def test_blob_digest_and_length() -> None:
    blob = Blob(b"large payload")

    assert blob.length == len(b"large payload")
    assert blob.digest == digest_blob(b"large payload")


def test_stored_blob_roundtrip() -> None:
    handle = StoredBlob(ref="file:abc", length=3, digest="a" * 64)
    wire = handle.to_wire()

    assert wire == {
        BLOB_MARKER_KEY: BLOB_PROTOCOL_VERSION,
        "ref": "file:abc",
        "length": 3,
        "digest": "a" * 64,
    }
    assert is_wire_ref(wire)
    assert parse_wire_ref(wire) == handle


@pytest.mark.parametrize(
    "wire",
    [
        "not a dict",
        {"ref": "file:a", "length": 1, "digest": "a" * 64},
        {
            BLOB_MARKER_KEY: BLOB_PROTOCOL_VERSION + 1,
            "ref": "x",
            "length": 1,
            "digest": "a" * 64,
        },
        {
            BLOB_MARKER_KEY: BLOB_PROTOCOL_VERSION,
            "ref": "",
            "length": 1,
            "digest": "a" * 64,
        },
        {
            BLOB_MARKER_KEY: BLOB_PROTOCOL_VERSION,
            "ref": "x",
            "length": -1,
            "digest": "a" * 64,
        },
        {
            BLOB_MARKER_KEY: BLOB_PROTOCOL_VERSION,
            "ref": "x",
            "length": 1,
            "digest": "z" * 64,
        },
        {
            BLOB_MARKER_KEY: BLOB_PROTOCOL_VERSION,
            "ref": "x",
            "length": "1",
            "digest": "a" * 64,
        },
    ],
)
def test_parse_wire_ref_rejects_malformed(wire: object) -> None:
    with pytest.raises(BlobRefError):
        parse_wire_ref(wire)


def test_plain_user_dict_is_not_a_reference() -> None:
    assert not is_wire_ref({"ref": "file:a", "length": 1, "digest": "a" * 64})


def test_bool_is_not_a_valid_length() -> None:
    wire = {
        BLOB_MARKER_KEY: BLOB_PROTOCOL_VERSION,
        "ref": "x",
        "length": True,
        "digest": "a" * 64,
    }
    with pytest.raises(BlobRefError):
        parse_wire_ref(wire)
