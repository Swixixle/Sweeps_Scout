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


def _discover_fixtures(tmp_path: Path, seed_text: str) -> None:
    seeds = tmp_path / "data" / "seeds"
    seeds.mkdir(parents=True)
    (seeds / "seed_urls.txt").write_text(seed_text, encoding="utf-8")
    (seeds / "allow_domains.txt").write_text("", encoding="utf-8")
    (seeds / "deny_domains.txt").write_text("", encoding="utf-8")
    (seeds / "bootstrap_domains.txt").write_text("", encoding="utf-8")


def test_max_pages_does_not_cap_seed_fetches(tmp_path: Path, monkeypatch) -> None:
    """With max_pages=2, all three seeds are still fetched; budget applies to crawls only."""
    _discover_fixtures(
        tmp_path,
        "https://seed-a.example/\nhttps://seed-b.example/\nhttps://seed-c.example/\n",
    )
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

        r = R()
        if "seed-a" in url:
            r.body = b'<html><body><a href="https://crawl.example/a-only">x</a></body></html>'
        elif "seed-b" in url:
            r.body = b'<html><body><a href="https://crawl.example/b-only">x</a></body></html>'
        elif "seed-c" in url:
            r.body = b'<html><body><a href="https://crawl.example/c-only">x</a></body></html>'
        return r

    monkeypatch.setattr(d, "fetch_url", fake_fetch)
    run_discover(tmp_path, max_depth=1, max_pages=2)
    pages_path = tmp_path / "data" / "candidates" / "discovered_pages.json"
    pages = json.loads(pages_path.read_text(encoding="utf-8"))
    depth0 = [p for p in pages if p["depth"] == 0]
    depth1 = [p for p in pages if p["depth"] == 1]
    assert len(depth0) == 3, "all seeds must be fetched"
    assert len(depth1) == 2, "max_pages=2 caps discovered (crawl) fetches only"
    assert len(pages) == 5


def test_max_pages_budget_for_discovered_only(tmp_path: Path, monkeypatch) -> None:
    """Three seeds plus up to max_pages crawl fetches (unique crawl URLs)."""
    _discover_fixtures(
        tmp_path,
        "https://s1.example/\nhttps://s2.example/\nhttps://s3.example/\n",
    )
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

        r = R()
        # Each seed emits four unique crawl targets (12 total) so the crawl budget can reach 10.
        if "s1.example" in url:
            r.body = (
                b"<html><body>"
                + b"".join(
                    f'<a href="https://crawl.example/s1-{i}">x</a>'.encode()
                    for i in range(4)
                )
                + b"</body></html>"
            )
        elif "s2.example" in url:
            r.body = (
                b"<html><body>"
                + b"".join(
                    f'<a href="https://crawl.example/s2-{i}">x</a>'.encode()
                    for i in range(4)
                )
                + b"</body></html>"
            )
        elif "s3.example" in url:
            r.body = (
                b"<html><body>"
                + b"".join(
                    f'<a href="https://crawl.example/s3-{i}">x</a>'.encode()
                    for i in range(4)
                )
                + b"</body></html>"
            )
        return r

    monkeypatch.setattr(d, "fetch_url", fake_fetch)
    run_discover(tmp_path, max_depth=1, max_pages=10)
    pages = json.loads(
        (tmp_path / "data" / "candidates" / "discovered_pages.json").read_text(
            encoding="utf-8"
        )
    )
    depth0 = [p for p in pages if p["depth"] == 0]
    depth1 = [p for p in pages if p["depth"] == 1]
    assert len(depth0) == 3
    assert len(depth1) == 10, "at most 10 crawl fetches when max_pages=10"


def test_seed_404_does_not_abort_other_seeds(tmp_path: Path, monkeypatch) -> None:
    _discover_fixtures(
        tmp_path,
        "https://bad.example/\nhttps://good.example/\n",
    )
    import sweep_scout.discover as d

    def fake_fetch(url, **kwargs):
        class R:
            def __init__(self) -> None:
                self.final_url = url
                self.status = 200
                self.content_type = "text/html"
                self.body = b"<html><body></body></html>"
                self.error = None
                self.fetched_at = "t"
                self.headers = {}

        r = R()
        if "bad.example" in url:
            r.status = 404
            r.body = b""
        return r

    monkeypatch.setattr(d, "fetch_url", fake_fetch)
    run_discover(tmp_path, max_depth=0, max_pages=5)
    pages = json.loads(
        (tmp_path / "data" / "candidates" / "discovered_pages.json").read_text(
            encoding="utf-8"
        )
    )
    assert any(p["status"] == 404 for p in pages)
    good_pages = [p for p in pages if p["requested_url"].startswith("https://good.example")]
    assert len(good_pages) == 1 and good_pages[0]["status"] == 200
    assert len(pages) == 2


def test_duplicate_seed_urls_fetched_once(tmp_path: Path, monkeypatch) -> None:
    _discover_fixtures(
        tmp_path,
        "https://same.example/\nhttps://same.example/\n",
    )
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
    pages = json.loads(
        (tmp_path / "data" / "candidates" / "discovered_pages.json").read_text(
            encoding="utf-8"
        )
    )
    assert len(pages) == 1
