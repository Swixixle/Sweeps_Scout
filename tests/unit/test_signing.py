from __future__ import annotations

import base64
import json
import os
import tempfile
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from sweep_scout._signing import (
    CanonicalizationError,
    SigningKeyError,
    canonical_payload_bytes,
    compute_payload_hash_hex,
    generate_keypair,
    load_private_key_pem,
    sign_envelope,
)


def test_canonical_payload_bytes_stable_key_order() -> None:
    a = {"z": 1, "a": {"nested": True}, "m": [3, 2, 1]}
    b = {"m": [3, 2, 1], "z": 1, "a": {"nested": True}}
    assert canonical_payload_bytes(a) == canonical_payload_bytes(b)


def test_compute_payload_hash_hex_known_vector() -> None:
    # echo -n 'hello' | shasum -a 256
    b = b"hello"
    expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    assert compute_payload_hash_hex(b) == expected


def test_generate_keypair_pem_0600_posix() -> None:
    if os.name == "nt":
        pytest.skip("POSIX permissions only")
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp) / "kp"
        priv, pub = generate_keypair(d)
        assert priv.read_bytes().startswith(b"-----BEGIN PRIVATE KEY-----")
        assert pub.read_bytes().startswith(b"-----BEGIN PUBLIC KEY-----")
        mode = priv.stat().st_mode & 0o777
        assert mode == 0o600


def test_sign_envelope_shape_and_hash_and_length() -> None:
    priv = Ed25519PrivateKey.generate()
    payload = {"artifact_type": "domain_fingerprints", "fingerprints": [], "generated_at": "2026-01-01T00:00:00Z"}
    key_id = "scout-fingerprint-key-v1"
    env = sign_envelope(payload, priv, key_id)
    raw = json.dumps(env)
    parsed = json.loads(raw)
    assert "payload" in parsed and "signature" in parsed
    sig = parsed["signature"]
    assert sig["algorithm"] == "ed25519"
    assert sig["key_id"] == key_id
    assert sig["signed_at"]
    assert len(sig["payload_hash_sha256"]) == 64
    canon = canonical_payload_bytes(payload)
    assert sig["payload_hash_sha256"] == compute_payload_hash_hex(canon)
    # base64url, no padding
    sb64 = sig["signature_b64"]
    assert "=" not in sb64
    assert "+" not in sb64 and "/" not in sb64
    pad = "=" * ((4 - len(sb64) % 4) % 4)
    sig_raw = base64.urlsafe_b64decode((sb64 + pad).encode("ascii"))
    assert len(sig_raw) == 64


def test_sign_envelope_verify_raw_ed25519() -> None:
    priv = Ed25519PrivateKey.generate()
    pub = priv.public_key()
    payload = {"x": 1, "y": "z"}
    env = sign_envelope(payload, priv, "kid-1")
    canon = canonical_payload_bytes(env["payload"])
    sb64 = env["signature"]["signature_b64"]
    pad = "=" * ((4 - len(sb64) % 4) % 4)
    sig_raw = base64.urlsafe_b64decode((sb64 + pad).encode("ascii"))
    pub.verify(sig_raw, canon)


def test_key_id_preserved() -> None:
    priv = Ed25519PrivateKey.generate()
    env = sign_envelope({}, priv, "my-custom-key-id")
    assert env["signature"]["key_id"] == "my-custom-key-id"


def test_canonical_numeric_key_order_bytes_identical() -> None:
    """Dict key insertion order for string keys should not change canonical bytes."""
    p1 = {"a": 1, "b": 2}
    p2 = {"b": 2, "a": 1}
    assert canonical_payload_bytes(p1) == canonical_payload_bytes(p2)


def test_load_private_key_missing() -> None:
    with pytest.raises(SigningKeyError):
        load_private_key_pem(Path("/nonexistent/nope.pem"))


def test_sign_envelope_non_serializable_raises() -> None:
    class Bad:
        pass

    priv = Ed25519PrivateKey.generate()
    with pytest.raises(CanonicalizationError):
        sign_envelope({"bad": Bad()}, priv, "k")  # type: ignore[arg-type]
