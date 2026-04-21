"""Tests for sweep_scout.monitor signing wiring."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from sweep_scout._signing import generate_keypair
from sweep_scout.config import ensure_dirs, paths_for_repo
from sweep_scout.monitor import run_monitor

import sweep_scout.monitor as monitor_mod


def _stub_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid network-heavy pipeline stages; exercise fingerprint wiring only."""
    monkeypatch.setattr(monitor_mod, "run_discover", lambda *a, **k: {"stub": True})
    monkeypatch.setattr(monitor_mod, "run_extract", lambda *a, **k: {"stub": True})
    monkeypatch.setattr(monitor_mod, "normalize_redirect_records", lambda *a, **k: {"stub": True})
    monkeypatch.setattr(monitor_mod, "run_classifier", lambda *a, **k: {"stub": True})
    monkeypatch.setattr(monitor_mod, "run_emit", lambda *a, **k: {"stub": True})
    monkeypatch.setattr(monitor_mod, "run_intel_bridge", lambda *a, **k: {"stub": True})


def _fake_fingerprint_domain(domain: str, **_kwargs: object) -> dict:
    return {
        "domain": domain,
        "fingerprinted_at": "2026-01-01T00:00:00Z",
        "dns": {},
        "tls": {},
        "errors": {},
        "partial": False,
    }


def _write_discovered(tmp_path: Path) -> None:
    ensure_dirs(tmp_path)
    p = paths_for_repo(tmp_path)
    p["discovered_domains"].parent.mkdir(parents=True, exist_ok=True)
    p["discovered_domains"].write_text(
        json.dumps([{"domain": "example.com"}]),
        encoding="utf-8",
    )


def test_run_monitor_default_unsigned_fingerprint_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pipeline(monkeypatch)
    monkeypatch.setattr("sweep_scout.fingerprint.fingerprint_domain", _fake_fingerprint_domain)
    _write_discovered(tmp_path)

    report = run_monitor(tmp_path, max_fingerprint_domains=2)

    assert report["fingerprint_signed"] is False
    assert report["fingerprint_key_id"] is None

    raw = json.loads(paths_for_repo(tmp_path)["domain_fingerprints"].read_text(encoding="utf-8"))
    assert isinstance(raw, list)


def test_run_monitor_signed_fingerprint_envelope(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pipeline(monkeypatch)
    monkeypatch.setattr("sweep_scout.fingerprint.fingerprint_domain", _fake_fingerprint_domain)
    _write_discovered(tmp_path)
    keys_dir = tmp_path / "keys"
    keys_dir.mkdir(parents=True)
    priv, _pub = generate_keypair(keys_dir)

    report = run_monitor(
        tmp_path,
        max_fingerprint_domains=2,
        sign=True,
        private_key_path=priv,
        key_id="scout-fingerprint-key-v1",
    )

    assert report["fingerprint_signed"] is True
    assert report["fingerprint_key_id"] == "scout-fingerprint-key-v1"

    raw = json.loads(paths_for_repo(tmp_path)["domain_fingerprints"].read_text(encoding="utf-8"))
    assert "payload" in raw and "signature" in raw
    assert raw["signature"]["key_id"] == "scout-fingerprint-key-v1"


def test_monitor_cli_sign_without_private_key_errors() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "sweep_scout.monitor", "--sign"],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    combined = (result.stderr or "") + (result.stdout or "")
    assert "private-key" in combined.lower()


def test_report_includes_fingerprint_signed_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pipeline(monkeypatch)
    monkeypatch.setattr("sweep_scout.fingerprint.fingerprint_domain", _fake_fingerprint_domain)
    _write_discovered(tmp_path)

    r = run_monitor(tmp_path, max_fingerprint_domains=1)
    assert "fingerprint_signed" in r
    assert r["fingerprint_signed"] is False
    assert r.get("fingerprint_key_id") is None
