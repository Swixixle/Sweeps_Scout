from __future__ import annotations

import json
from pathlib import Path

from sweep_scout.intake_bulk_text import (
    normalize_bulk_raw_row,
    parse_bulk_text,
    parse_bulk_line,
    run_bulk_dedupe_merge,
    run_bulk_normalize,
)
from sweep_scout.normalize_candidates import normalize_domain
from sweep_scout.verification_queue import run_build_verification_queue


def test_parse_bulk_extracts_brand_domain_pairs():
    text = """
Crown Coins\thttps://crowncoins.com
NoLimitCoins\tnolimitcoins.com
No Limit Coins\tnolimitcoins.com
FunzCity\tfunzcity.com
Funzcity\tfunzcity.com
garbage no domain here
||| also skip
"""
    rows, _stats = parse_bulk_text(text, "test_dump")
    brands_domains = {(r["brand"], normalize_domain(r["primary_domain"])) for r in rows}
    assert ("Crown Coins", "crowncoins.com") in brands_domains
    assert len([r for r in rows if r["brand"].startswith("No") and "limit" in r["brand"].lower()]) == 2
    assert len([r for r in rows if "funz" in r["brand"].lower()]) == 2


def test_parse_optional_metadata_not_trusted():
    line = "X https://x.com rating: 4.5 bonus: 200% launch: 2020 games: 99"
    r = parse_bulk_line(line, 0, "f")
    assert r
    assert r.get("optional_rating") or r.get("optional_bonus")


def test_normalize_no_limit_coins_same_domain_dedupe(monkeypatch, tmp_path: Path):
    root = tmp_path
    raw = [
        {"brand": "NoLimitCoins", "primary_domain": "nolimitcoins.com", "raw_line_index": 0, "source_file_id": "t", "source_fragment": "x", "notes": "", "category": ""},
        {"brand": "No Limit Coins", "primary_domain": "nolimitcoins.com", "raw_line_index": 1, "source_file_id": "t", "source_fragment": "y", "notes": "", "category": ""},
    ]
    (root / "data/candidates").mkdir(parents=True)
    import sweep_scout.intake_bulk_text as ib

    monkeypatch.setattr(ib, "repo_root", lambda: root)
    (root / "data/candidates/bulk_raw_rows.json").write_text(
        __import__("json").dumps(raw),
        encoding="utf-8",
    )
    run_bulk_normalize(
        raw_path=root / "data/candidates/bulk_raw_rows.json",
        out_path=root / "data/candidates/bulk_normalized_rows.json",
    )
    (root / "data/candidates/normalized_candidate_rows.json").write_text("[]", encoding="utf-8")
    run_bulk_dedupe_merge(
        bulk_normalized_path=root / "data/candidates/bulk_normalized_rows.json",
        markdown_normalized_path=root / "data/candidates/normalized_candidate_rows.json",
        out_path=root / "data/candidates/bulk_deduped_rows.json",
    )
    ded = json.loads((root / "data/candidates/bulk_deduped_rows.json").read_text())
    assert len(ded) == 2
    assert sum(1 for r in ded if r.get("duplicate_of") is None) == 1


def test_verification_queue_csv(monkeypatch, tmp_path: Path):
    ded = [
        {
            "candidate_id": "cand_a",
            "brand": "Zed",
            "normalized_primary_domain": "zed.com",
            "duplicate_of": None,
            "duplicate_group_id": "dg_x",
            "merge_notes": "",
            "confidence": 0.35,
            "alias_candidates": [],
            "notes": "sweeps casino",
            "category": "",
            "bulk_metadata": {"source_file_id": "bulk1", "domain_repeat_count": 2},
            "intake_channel": "bulk",
        }
    ]
    (tmp_path / "data/candidates").mkdir(parents=True)
    (tmp_path / "data/candidates/bulk_deduped_rows.json").write_text(json.dumps(ded), encoding="utf-8")
    monkeypatch.setattr("sweep_scout.verification_queue.repo_root", lambda: tmp_path)
    monkeypatch.setattr("sweep_scout.verification_queue.classify_canonical", lambda r: ("operators", "operator"))
    rows = run_build_verification_queue(
        deduped_path=tmp_path / "data/candidates/bulk_deduped_rows.json",
        out_csv=tmp_path / "data/candidates/verification_queue.csv",
    )
    assert len(rows) == 1
    assert float(rows[0]["priority_score"]) > 0
    assert rows[0]["primary_domain"] == "zed.com"


def test_malformed_lines_do_not_crash():
    lines = ["", "# c", "|||", "no domain at all", "x" * 500]
    for i, line in enumerate(lines):
        parse_bulk_line(line, i, "t")


def test_parse_stats_skipped_lines():
    text = "good\tcrowncoins.com\n\nnot a line with domain at all\n"
    rows, stats = parse_bulk_text(text, "t")
    assert len(rows) == 1
    assert stats["parsed_rows"] == 1
    assert stats["skipped_lines_no_extractable_domain"] >= 1


def test_verification_record_shape(monkeypatch):
    from sweep_scout import verify_candidates as vc

    monkeypatch.setattr(
        vc,
        "verify_domain",
        lambda dom, **kw: {
            "primary_domain": dom,
            "verification_status": "reachable",
            "verification_score": 0.5,
            "verification_notes": "test",
            "dns_ok": True,
            "http_status": 200,
            "final_url": f"https://{dom}/",
            "keyword_hits": ["sweeps"],
            "policy_links_found": [],
        },
    )
    import tempfile
    from pathlib import Path

    d = Path(tempfile.mkdtemp())
    q = d / "q.csv"
    q.write_text(
        "brand,primary_domain,entity_type_hint,priority_score,why_flagged,source_file,duplicate_group_id,needs_manual_verification\n"
        "X,example.com,operator,10.0,why,f,dg,no\n",
        encoding="utf-8",
    )
    out = d / "v.json"
    vc.run_verify_from_queue(queue_csv=q, out_json=out, limit=5, reviewed_queue_csv=d / "qv.csv")
    data = json.loads(out.read_text())
    assert len(data) == 1
    assert "verification_status" in data[0]
    assert (d / "qv.csv").is_file()
