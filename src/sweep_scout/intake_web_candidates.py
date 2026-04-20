"""Ingest CSV of web-surfaced new/coming-soon candidate brands (not reviewed truth)."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from sweep_scout.normalize_candidates import normalize_domain
from sweep_scout.utils import deterministic_json_dumps, repo_root
from sweep_scout.web_candidate_domains import guess_domain_hosts

SOURCE_CSV = "sweeps_web_new_candidates_2026_04.csv"


def row_from_csv(r: dict[str, str], line_index: int) -> dict[str, Any]:
    brand = (r.get("brand") or "").strip()
    status_hint = (r.get("status_hint") or "").strip()
    discovery_source = (r.get("discovery_source") or "").strip()
    guesses = guess_domain_hosts(brand, max_guesses=4)
    return {
        "raw_line_index": line_index,
        "brand": brand,
        "status_hint": status_hint,
        "discovery_source": discovery_source,
        "candidate_domain_guesses": guesses,
        "notes": "web_new_candidates seed; not verified",
        "intake_channel": "web_new_candidates",
        "review_status": "needs_review",
        "source_file": SOURCE_CSV,
    }


def normalize_web_row(raw: dict[str, Any]) -> dict[str, Any]:
    guesses = [normalize_domain(h) or h for h in raw.get("candidate_domain_guesses") or []]
    guesses = [g for g in guesses if g]
    return {
        "brand": raw.get("brand", ""),
        "status_hint": raw.get("status_hint", ""),
        "discovery_source": raw.get("discovery_source", ""),
        "candidate_domain_guesses": guesses,
        "notes": raw.get("notes", ""),
        "intake_channel": "web_new_candidates",
        "review_status": raw.get("review_status", "needs_review"),
        "source_file": raw.get("source_file", SOURCE_CSV),
        "raw_line_index": raw.get("raw_line_index", 0),
    }


def run_intake_web(
    csv_path: Path | None = None,
    raw_out: Path | None = None,
    norm_out: Path | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    root = repo_root()
    csv_path = csv_path or (root / "data" / "raw_sources" / SOURCE_CSV)
    raw_out = raw_out or (root / "data" / "candidates" / "web_new_raw_rows.json")
    norm_out = norm_out or (root / "data" / "candidates" / "web_new_normalized_rows.json")

    raw_rows: list[dict[str, Any]] = []
    with csv_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if not (row.get("brand") or "").strip():
                continue
            raw_rows.append(row_from_csv(row, i))

    raw_rows.sort(key=lambda r: (str(r.get("brand", "")), int(r.get("raw_line_index", 0))))
    norm_rows = [normalize_web_row(r) for r in raw_rows]

    raw_out.parent.mkdir(parents=True, exist_ok=True)
    raw_out.write_text(deterministic_json_dumps(raw_rows), encoding="utf-8")
    norm_out.write_text(deterministic_json_dumps(norm_rows), encoding="utf-8")
    return raw_rows, norm_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest web new/coming-soon candidate CSV.")
    parser.add_argument("--csv", type=Path, default=None)
    args = parser.parse_args()
    root = repo_root()
    raw, norm = run_intake_web(csv_path=args.csv or (root / "data" / "raw_sources" / SOURCE_CSV))
    print(deterministic_json_dumps({"brands": len(raw), "output": "web_new_raw_rows.json + web_new_normalized_rows.json"}))


if __name__ == "__main__":
    main()
