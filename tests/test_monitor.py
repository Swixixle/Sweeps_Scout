from __future__ import annotations

import json
from pathlib import Path

from sweep_scout.intel_bridge import compare_domains, run_intel_bridge


def test_intel_bridge_mock_snapshot(tmp_path: Path):
    snap = {"domains": ["known.example"]}
    rows = compare_domains(
        [
            {"domain": "known.example", "source_urls": ["https://s/"], "discovery_type": "outbound_link"},
            {"domain": "new.example", "source_urls": ["https://s/"], "discovery_type": "outbound_link"},
        ],
        snap,
    )
    by = {r["domain"]: r["bridge_status"] for r in rows}
    assert by["known.example"] == "already_known"
    assert by["new.example"] == "net_new"

    (tmp_path / "data" / "candidates").mkdir(parents=True)
    (tmp_path / "data" / "candidates" / "discovered_domains.json").write_text(
        json.dumps(
            [
                {
                    "domain": "a.com",
                    "source_urls": ["https://seed/"],
                    "discovery_type": "outbound_link",
                }
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "data" / "intel_snapshot.json").write_text(
        json.dumps({"domains": ["a.com"]}), encoding="utf-8"
    )
    summary = run_intel_bridge(tmp_path)
    assert summary["snapshot_loaded"] is True
