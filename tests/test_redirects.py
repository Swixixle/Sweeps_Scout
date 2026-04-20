from __future__ import annotations

import json
from pathlib import Path

from sweep_scout.redirects import normalize_redirect_records


def test_redirects_merges_phrase_hits(tmp_path: Path):
    cand = tmp_path / "data" / "candidates"
    cand.mkdir(parents=True)
    (tmp_path / "reports" / "discovery").mkdir(parents=True)

    (cand / "discovered_redirects.json").write_text(
        json.dumps(
            [
                {
                    "from_url": "https://a.test/x",
                    "to_url": "https://b.test/y",
                    "domain_from": "a.test",
                    "domain_to": "b.test",
                    "discovery_type": "http_redirect",
                    "source_url": "https://seed/x",
                    "first_seen": "t",
                }
            ]
        ),
        encoding="utf-8",
    )
    (cand / "extracted_signals.json").write_text(
        json.dumps(
            [
                {
                    "url": "https://c.test/",
                    "final_url": "https://c.test/",
                    "domain": "c.test",
                    "rebrand_phrase_hits": ["formerly"],
                }
            ]
        ),
        encoding="utf-8",
    )

    normalize_redirect_records(tmp_path)
    merged = json.loads((cand / "discovered_redirects.json").read_text(encoding="utf-8"))
    kinds = {m.get("discovery_type") for m in merged}
    assert "http_redirect" in kinds
    assert "page_phrase_rebrand" in kinds
