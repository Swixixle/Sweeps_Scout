from __future__ import annotations

import json
import socket
from pathlib import Path

import pytest

from sweep_scout.fingerprint import fingerprint_domain, run_fingerprint


def _dns_reachable() -> bool:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2.0)
        sock.sendto(
            b"\xab\xcd\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x07example\x03com\x00\x00\x01\x00\x01",
            ("8.8.8.8", 53),
        )
        sock.recvfrom(512)
        sock.close()
        return True
    except OSError:
        return False


requires_net = pytest.mark.skipif(not _dns_reachable(), reason="8.8.8.8:53 unreachable (offline sandbox?)")


def test_fingerprint_empty_domain_schema():
    fp = fingerprint_domain(" \t ", dns_timeout=1.0, tls_timeout=1.0)
    assert fp["domain"] == ""
    assert fp["partial"] is True
    assert set(fp["errors"]) == {"dns_a", "dns_aaaa", "dns_ns", "dns_mx", "tls"}
    assert all(fp["errors"][k] for k in fp["errors"])


@requires_net
def test_fingerprint_graceful_degradation_bogus():
    fp = fingerprint_domain("this-does-not-exist.invalid", dns_timeout=5.0, tls_timeout=5.0)
    assert fp["partial"] is True
    assert any(v is not None for v in fp["errors"].values())
    assert fp["domain"] == "this-does-not-exist.invalid"


@requires_net
def test_fingerprint_example_com():
    fp = fingerprint_domain("example.com", dns_timeout=8.0, tls_timeout=8.0)
    assert fp["domain"] == "example.com"
    assert isinstance(fp["dns"]["a"], list)
    assert len(fp["dns"]["a"]) >= 1
    assert fp["errors"]["dns_a"] is None
    assert fp["dns"]["ns"]
    assert fp["errors"]["dns_ns"] is None
    assert fp["dns"]["mx"]
    assert fp["errors"]["dns_mx"] is None
    san = fp["tls"].get("san") or []
    assert "example.com" in san or "example.edu" in san or fp["tls"].get("subject_cn")
    assert fp["errors"]["tls"] is None
    assert fp["partial"] is False


@requires_net
def test_fingerprint_www_iana_org():
    fp = fingerprint_domain("www.iana.org", dns_timeout=8.0, tls_timeout=8.0)
    assert fp["domain"] == "iana.org"
    assert fp["errors"]["tls"] is None
    assert fp["partial"] is False


@requires_net
def test_run_fingerprint_writes_artifacts(tmp_path: Path):
    (tmp_path / "data" / "candidates").mkdir(parents=True)
    (tmp_path / "data" / "candidates" / "discovered_domains.json").write_text(
        json.dumps(
            [
                {
                    "domain": "example.com",
                    "source_urls": ["https://example.com/"],
                    "discovery_type": "outbound_link",
                }
            ]
        ),
        encoding="utf-8",
    )
    rep = run_fingerprint(tmp_path, max_domains=5)
    out = tmp_path / "data" / "candidates" / "domain_fingerprints.json"
    assert out.is_file()
    rows = json.loads(out.read_text(encoding="utf-8"))
    assert len(rows) == 1
    assert rows[0]["domain"] == "example.com"
    assert rep["domains_processed"] == 1
    reps = list((tmp_path / "reports" / "fingerprinting").glob("fingerprint-*.json"))
    assert len(reps) == 1
