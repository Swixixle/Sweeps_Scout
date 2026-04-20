from __future__ import annotations

import json
from pathlib import Path

from sweep_scout.bucket_candidates import CSV_FIELDS, classify_canonical, run_bucket
from sweep_scout.dedupe_candidates import brand_fold, run_dedupe
from sweep_scout.intake_tables import parse_raw_source_file, run_intake
from sweep_scout.normalize_candidates import normalize_domain, run_normalize


SET1_MD = """| brand | primary_domain | other_domains | category | notes | source_url |
| --- | --- | --- | --- | --- | --- |
| Chanced | https://chanced.com/x | www.chanced.com | Social | A | https://a |
| NoLimitCoins | nolimitcoins.com | | Sweeps | B | https://b |
| FunzCity | FunzCity.com | | Sweeps | C | https://c |
"""

SET2_MD = """| brand | primary_domain | other_domains | category | notes | source_url |
| --- | --- | --- | --- | --- | --- |
| Chanced | chanced.com | | Social | dup | https://a2 |
| No Limit Coins | https://www.nolimitcoins.com/path | | Sweeps | B2 | https://b2 |
| Funzcity | funzcity.com | | Sweeps | C2 | https://c2 |
| VGW | vgw.co | | Corporate | parent | https://p |
| Social sweepstakes | sweepstakes.com | | Aggregator | generic | https://g |
"""


def test_parse_markdown_tables(tmp_path: Path):
    p1 = tmp_path / "sweeps_lists_set1.md"
    p1.write_text(SET1_MD, encoding="utf-8")
    rows = parse_raw_source_file(p1)
    assert len(rows) == 3
    assert rows[0]["brand"] == "Chanced"
    assert rows[0]["source_set"] == "set1"
    assert rows[0]["intake_row_index"] == 0


def test_normalize_domains():
    assert normalize_domain("HTTPS://WWW.Chanced.COM/path?q=1") == "chanced.com"
    assert normalize_domain("nolimitcoins.com") == "nolimitcoins.com"


def test_dedupe_chanced_nolimit_funz(tmp_path: Path, monkeypatch):
    root = tmp_path
    raw_dir = root / "data" / "raw_sources"
    cand_dir = root / "data" / "candidates"
    raw_dir.mkdir(parents=True)
    (raw_dir / "sweeps_lists_set1.md").write_text(SET1_MD, encoding="utf-8")
    (raw_dir / "sweeps_lists_set2.md").write_text(SET2_MD, encoding="utf-8")

    monkeypatch.setattr("sweep_scout.intake_tables.repo_root", lambda: root)
    monkeypatch.setattr("sweep_scout.normalize_candidates.repo_root", lambda: root)
    monkeypatch.setattr("sweep_scout.dedupe_candidates.repo_root", lambda: root)

    run_intake(raw_dir=raw_dir, out_path=cand_dir / "raw_intake_rows.json")
    run_normalize(intake_path=cand_dir / "raw_intake_rows.json", out_path=cand_dir / "normalized_candidate_rows.json")
    deduped = run_dedupe(
        normalized_path=cand_dir / "normalized_candidate_rows.json",
        out_path=cand_dir / "deduped_candidates.json",
    )

    chanced = [r for r in deduped if r.get("brand") == "Chanced"]
    assert len(chanced) == 2
    canon_chanced = [r for r in chanced if r.get("duplicate_of") is None]
    assert len(canon_chanced) == 1
    dup_chanced = [r for r in chanced if r.get("duplicate_of")]
    assert len(dup_chanced) == 1
    assert dup_chanced[0]["duplicate_of"] == canon_chanced[0]["candidate_id"]

    nlc = [r for r in deduped if brand_fold(str(r.get("brand", ""))) == "nolimitcoins"]
    assert len(nlc) == 2
    assert sum(1 for r in nlc if r.get("duplicate_of") is None) == 1

    fz = [r for r in deduped if brand_fold(str(r.get("brand", ""))) == "funzcity"]
    assert len(fz) == 2
    assert sum(1 for r in fz if r.get("duplicate_of") is None) == 1


def test_bucket_vgw_corporate():
    row = {
        "brand": "VGW",
        "normalized_primary_domain": "vgw.co",
        "category": "Corporate",
        "notes": "parent",
        "alias_candidates": [],
        "merge_notes": "",
        "duplicate_of": None,
    }
    b, hint = classify_canonical(row)
    assert b == "corporate_entities"
    assert hint == "corporate"


def test_bucket_sweepstakes_promoter_or_ambiguous():
    row = {
        "brand": "Social sweepstakes",
        "normalized_primary_domain": "sweepstakes.com",
        "category": "Aggregator",
        "notes": "generic listing",
        "alias_candidates": [],
        "merge_notes": "",
        "duplicate_of": None,
    }
    b, hint = classify_canonical(row)
    assert b in ("promoters", "rejected_or_ambiguous")
    assert hint in ("promoter", "ambiguous")


def test_provenance_source_set_in_csv(tmp_path: Path, monkeypatch):
    root = tmp_path
    raw_dir = root / "data" / "raw_sources"
    cand_dir = root / "data" / "candidates"
    raw_dir.mkdir(parents=True)
    (raw_dir / "sweeps_lists_set1.md").write_text(SET1_MD, encoding="utf-8")

    monkeypatch.setattr("sweep_scout.intake_tables.repo_root", lambda: root)
    monkeypatch.setattr("sweep_scout.normalize_candidates.repo_root", lambda: root)
    monkeypatch.setattr("sweep_scout.dedupe_candidates.repo_root", lambda: root)
    monkeypatch.setattr("sweep_scout.bucket_candidates.repo_root", lambda: root)

    run_intake(raw_dir=raw_dir, out_path=cand_dir / "raw_intake_rows.json")
    run_normalize(intake_path=cand_dir / "raw_intake_rows.json", out_path=cand_dir / "normalized_candidate_rows.json")
    run_dedupe(
        normalized_path=cand_dir / "normalized_candidate_rows.json",
        out_path=cand_dir / "deduped_candidates.json",
    )
    run_bucket(deduped_path=cand_dir / "deduped_candidates.json", out_dir=cand_dir)

    ops = (cand_dir / "operators_candidates.csv").read_text(encoding="utf-8")
    assert "set1" in ops


def test_raw_intake_json_sorted_deterministic(tmp_path: Path, monkeypatch):
    raw_dir = tmp_path / "in"
    raw_dir.mkdir()
    (raw_dir / "sweeps_lists_set1.md").write_text(SET1_MD, encoding="utf-8")
    out = tmp_path / "raw.json"
    monkeypatch.setattr("sweep_scout.intake_tables.repo_root", lambda: tmp_path)
    run_intake(raw_dir=raw_dir, out_path=out)
    a = json.loads(out.read_text(encoding="utf-8"))
    run_intake(raw_dir=raw_dir, out_path=out)
    b = json.loads(out.read_text(encoding="utf-8"))
    assert a == b


def test_csv_columns_order():
    assert CSV_FIELDS[0] == "brand"
    assert CSV_FIELDS[-1] == "merge_notes"
