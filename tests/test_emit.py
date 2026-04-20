from __future__ import annotations

import json
from pathlib import Path

from sweep_scout.classifier import classify_signal
from sweep_scout.emit import run_emit


def test_classifier_operator_vs_promoter():
    op = classify_signal(
        {
            "title": "Social casino — gold coins & sweeps",
            "meta_description": "Play slots",
            "text_hits": ["gold coins", "sweepstakes"],
            "policy_links": ["https://x.com/terms"],
            "support_links": [],
            "provider_mentions": [],
            "url": "https://x.com/",
            "domain": "x.com",
        }
    )
    assert op["label"] in ("likely_operator", "likely_promoter", "likely_payment_path", "unknown")

    prom = classify_signal(
        {
            "title": "Best sweeps — compare top sweepstakes sites",
            "meta_description": "Reviews",
            "text_hits": [],
            "policy_links": [],
            "support_links": ["https://x.com/a"] * 10,
            "provider_mentions": [],
            "url": "https://y.com/",
            "domain": "y.com",
        }
    )
    assert prom["label"] in ("likely_promoter", "unknown", "likely_operator")


def test_emit_preserves_reasoning(tmp_path: Path):
    cand = tmp_path / "data" / "candidates"
    cand.mkdir(parents=True)
    (tmp_path / "reports" / "extraction").mkdir(parents=True)
    (tmp_path / "reports" / "discovery").mkdir(parents=True)

    (cand / "discovered_domains.json").write_text(
        json.dumps(
            [
                {
                    "domain": "z.com",
                    "source_urls": ["https://seed.example/a"],
                    "discovered_url": "https://z.com/",
                    "final_url": "https://z.com/",
                    "first_seen": "t",
                    "discovery_type": "outbound_link",
                }
            ]
        ),
        encoding="utf-8",
    )
    (cand / "extracted_signals.json").write_text(
        json.dumps(
            [
                {
                    "url": "https://z.com/",
                    "final_url": "https://z.com/",
                    "domain": "z.com",
                    "title": "Social casino",
                    "meta_description": "sweeps coins",
                    "text_hits": ["gold coins"],
                    "policy_links": ["https://z.com/terms"],
                    "support_links": [],
                    "provider_mentions": ["stripe"],
                    "rebrand_phrase_hits": [],
                    "notes": [],
                }
            ]
        ),
        encoding="utf-8",
    )

    from sweep_scout.classifier import run_classifier

    run_classifier(tmp_path)
    run_emit(tmp_path)

    ents = json.loads((cand / "proposed_entities.json").read_text(encoding="utf-8"))
    assert ents[0]["reasoning"]
    assert ents[0]["candidate_id"].startswith("candidate_")
