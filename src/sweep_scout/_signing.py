"""Ed25519 signing for signed JSON artifacts (envelope per docs/SIGNING.md in Sweeps_Intel)."""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key

logger = logging.getLogger(__name__)


class SigningError(Exception):
    """Base class for signing failures."""


class SigningKeyError(SigningError):
    """Key load or key generation failed."""


class CanonicalizationError(SigningError):
    """Payload cannot be serialized to canonical JSON."""


def canonical_payload_bytes(payload: dict[str, Any]) -> bytes:
    try:
        s = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    except (TypeError, ValueError) as e:
        logger.warning("canonical JSON failed: %s", e)
        raise CanonicalizationError(str(e)) from e
    return s.encode("utf-8")


def compute_payload_hash_hex(canonical_bytes: bytes) -> str:
    return hashlib.sha256(canonical_bytes).hexdigest()


def load_private_key_pem(path: Path) -> Ed25519PrivateKey:
    try:
        raw = path.read_bytes()
    except FileNotFoundError as e:
        logger.warning("private key file not found: %s", path)
        raise SigningKeyError(f"private key not found: {path}") from e
    try:
        key = load_pem_private_key(raw, password=None)
    except ValueError as e:
        logger.warning("failed to load PEM private key: %s", e)
        raise SigningKeyError(f"invalid PEM private key: {e}") from e
    except UnsupportedAlgorithm as e:
        logger.warning("unsupported key algorithm: %s", e)
        raise SigningKeyError(str(e)) from e
    if not isinstance(key, Ed25519PrivateKey):
        logger.warning("private key is not Ed25519")
        raise SigningKeyError("private key must be Ed25519")
    return key


def _signature_b64url_no_padding(sig: bytes) -> str:
    return base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")


def sign_envelope(payload: dict[str, Any], private_key: Ed25519PrivateKey, key_id: str) -> dict[str, Any]:
    canonical = canonical_payload_bytes(payload)
    payload_hash_hex = compute_payload_hash_hex(canonical)
    sig_bytes = private_key.sign(canonical)
    signed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "payload": payload,
        "signature": {
            "algorithm": "ed25519",
            "key_id": key_id,
            "signed_at": signed_at,
            "payload_hash_sha256": payload_hash_hex,
            "signature_b64": _signature_b64url_no_padding(sig_bytes),
        },
    }


def generate_keypair(out_dir: Path) -> tuple[Path, Path]:
    """Generate Ed25519 keypair; write private.pem and public.pem to out_dir. key_id is assigned at sign time, not at key generation."""
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    priv = Ed25519PrivateKey.generate()
    pub = priv.public_key()
    priv_pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_pem = pub.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    priv_path = out_dir / "private.pem"
    pub_path = out_dir / "public.pem"
    try:
        priv_path.write_bytes(priv_pem)
        pub_path.write_bytes(pub_pem)
    except OSError as e:
        logger.warning("failed writing key files: %s", e)
        raise SigningKeyError(f"failed to write keypair: {e}") from e
    if os.name != "nt":
        try:
            os.chmod(priv_path, 0o600)
        except OSError as e:
            logger.warning("chmod private key: %s", e)
            raise SigningKeyError(f"failed to set private key mode: {e}") from e
    return priv_path, pub_path
