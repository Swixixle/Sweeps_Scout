from __future__ import annotations

from pathlib import Path

from sweep_scout.extract import extract_record_for_url
from sweep_scout.html_sniff import extract_links_from_html, parse_signals


SAMPLE_HTML = """<!doctype html>
<html><head>
<title>Top Sweeps Reviews — Compare sweepstakes sites</title>
<meta name="description" content="Compare social casino offers and sweeps coins.">
</head>
<body>
<footer>
Terms: amusement only. Contact support@example.com
</footer>
<script src="https://cdn.example.net/app.js"></script>
<iframe src="https://embed.vendor.io/widget"></iframe>
<a href="/terms-of-service">Terms</a>
<a href="/privacy-policy">Privacy</a>
<a href="mailto:help@example.com">Help</a>
<a href="https://operator-one.test/play">Play</a>
</body></html>"""


def test_parse_signals_and_links():
    sig = parse_signals(SAMPLE_HTML, "https://reviews.example.com/page")
    assert "sweeps" in sig["meta_description"].lower() or "sweeps" in sig["title"].lower()
    assert any("cdn.example.net" in s for s in sig["script_src"])
    assert any("vendor.io" in s for s in sig["iframe_src"])
    links = extract_links_from_html(SAMPLE_HTML, "https://reviews.example.com/page")
    assert any("terms" in x.lower() for x in links)


def test_extract_record_uses_cache(tmp_path: Path, monkeypatch):
    cache = tmp_path / "cache"

    def fake_fetch(url, **kwargs):
        class R:
            final_url = url
            status = 200
            content_type = "text/html; charset=utf-8"
            body = SAMPLE_HTML.encode("utf-8")
            error = None
            fetched_at = "now"
            headers = {}

        return R()

    monkeypatch.setattr("sweep_scout.extract.fetch_url", fake_fetch)
    rec = extract_record_for_url("https://reviews.example.com/", cache_dir=cache)
    assert rec["title"]
    assert rec["policy_links"]
    assert rec["contact_emails"]
