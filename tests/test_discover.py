from __future__ import annotations

import json
from pathlib import Path

from sweep_scout.discover import run_discover
from sweep_scout.utils import normalize_host, normalize_url


def test_normalize_url_and_domain():
    assert normalize_url("HTTPS://Example.COM/path?b=2&a=1", None) == "https://example.com/path?a=1&b=2"
    assert normalize_host("WWW.EXAMPLE.COM") == "example.com"


def test_domain_dedupe_in_discover(tmp_path: Path, monkeypatch):
    seeds = tmp_path / "data" / "seeds"
    seeds.mkdir(parents=True)
    (seeds / "seed_urls.txt").write_text(
        "https://example.com/\n", encoding="utf-8"
    )
    (seeds / "allow_domains.txt").write_text("", encoding="utf-8")
    (seeds / "deny_domains.txt").write_text("", encoding="utf-8")
    (seeds / "bootstrap_domains.txt").write_text("", encoding="utf-8")

    import sweep_scout.discover as d

    def fake_fetch(url, **kwargs):
        class R:
            final_url = url
            status = 200
            content_type = "text/html"
            body = b"<html><body></body></html>"
            error = None
            fetched_at = "t"
            headers = {}

        return R()

    monkeypatch.setattr(d, "fetch_url", fake_fetch)

    run_discover(tmp_path, max_depth=0, max_pages=5)

    dom_path = tmp_path / "data" / "candidates" / "discovered_domains.json"
    assert dom_path.is_file()
    rows = json.loads(dom_path.read_text(encoding="utf-8"))
    domains = {r["domain"] for r in rows}
    assert len(domains) == len(rows)


def test_redirect_capture_recorded(tmp_path: Path, monkeypatch):
    seeds = tmp_path / "data" / "seeds"
    seeds.mkdir(parents=True)
    (seeds / "seed_urls.txt").write_text("https://httpbin.org/redirect-to?url=https://example.com/", encoding="utf-8")
    (seeds / "allow_domains.txt").write_text("", encoding="utf-8")
    (seeds / "deny_domains.txt").write_text("", encoding="utf-8")

    import sweep_scout.discover as d

    def fake_fetch(url, **kwargs):
        class R:
            final_url = "https://example.com/landing"
            status = 200
            content_type = "text/html"
            body = b"<html><body><a href='https://other.test/'>x</a></body></html>"
            error = None
            fetched_at = "t"
            headers = {}

        if "httpbin" in url:
            R.final_url = "https://example.com/landing"
            R.body = b""
            return R()
        return R()

    monkeypatch.setattr(d, "fetch_url", fake_fetch)

    run_discover(tmp_path, max_depth=0, max_pages=3)
    redir_path = tmp_path / "data" / "candidates" / "discovered_redirects.json"
    data = json.loads(redir_path.read_text(encoding="utf-8"))
    assert isinstance(data, list)
