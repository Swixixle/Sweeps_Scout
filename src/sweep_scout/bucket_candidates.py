from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

from sweep_scout.dedupe_candidates import brand_fold
from sweep_scout.utils import deterministic_json_dumps, repo_root


BUCKET_FILES: dict[str, Path] = {
    "operators": Path("operators_candidates.csv"),
    "promoters": Path("promoters_candidates.csv"),
    "corporate_entities": Path("corporate_entities_candidates.csv"),
    "redirects_rebrands": Path("redirects_rebrands_candidates.csv"),
    "rejected_or_ambiguous": Path("rejected_or_ambiguous_candidates.csv"),
}

CSV_FIELDS = [
    "brand",
    "primary_domain",
    "other_domains",
    "entity_type_hint",
    "category",
    "notes",
    "source_url",
    "source_set",
    "confidence",
    "review_status",
    "duplicate_group_id",
    "merge_notes",
]


def _haystack(row: dict[str, Any]) -> str:
    parts = [
        str(row.get("brand", "")),
        str(row.get("category", "")),
        str(row.get("notes", "")),
        str(row.get("normalized_primary_domain", "")),
        str(row.get("raw_other_domains", "")),
    ]
    return " ".join(parts).lower()


def _merge_group_size(merge_notes: str) -> int:
    if not merge_notes or "merged_ids=" not in merge_notes:
        return 1
    m = re.search(r"merged_ids=([^;]+)", merge_notes)
    if not m:
        return 1
    ids = [x for x in m.group(1).split(",") if x.strip()]
    return 1 + len(ids)


def classify_canonical(row: dict[str, Any]) -> tuple[str, str]:
    """Return (bucket_key, entity_type_hint). Heuristic only."""
    brand = str(row.get("brand", "")).strip()
    bf = brand_fold(brand)
    domain = str(row.get("normalized_primary_domain", "")).strip()
    hs = _haystack(row)
    merge_notes = str(row.get("merge_notes", "") or "")
    aliases = list(row.get("alias_candidates") or [])

    if not brand and not domain:
        return "rejected_or_ambiguous", "ambiguous"

    if bf == "vgw" or "virtual gaming worlds" in hs:
        return "corporate_entities", "corporate"

    if bf in {"aviagames", "papayagaming", "bigrunstudios"}:
        return "corporate_entities", "corporate"

    if re.search(r"\b(vgw|virtual gaming worlds)\b", hs) and not re.search(r"\bplay\b", hs):
        return "corporate_entities", "corporate"

    if re.search(
        r"\b(holding company|corporate parent|parent company|games ltd|inc\. subsidiary)\b",
        hs,
    ) and "sweep" not in bf:
        return "corporate_entities", "corporate"

    if "sweepstakes.com" in hs or re.search(r"\bsweepstakes\.com\b", domain):
        if "review" in hs or "aggregator" in hs or "giveaway" in hs or "social media" in hs:
            return "promoters", "promoter"
        return "promoters", "promoter"

    if re.search(r"\bsocial media giveaway\b", hs) or (
        "giveaway" in hs and "sweepstakes" in hs and len(domain) < 5
    ):
        return "rejected_or_ambiguous", "ambiguous"

    if re.search(r"\b(aggregator|affiliate|review site|listicle|compare sites)\b", hs):
        return "promoters", "promoter"

    mg = _merge_group_size(merge_notes)
    if mg > 1:
        return "redirects_rebrands", "redirect"
    if aliases and len(aliases) >= 2:
        return "redirects_rebrands", "redirect"
    if re.search(r"\b(rebrand|redirect|formerly|used to be|alias|mirror domain)\b", hs):
        return "redirects_rebrands", "redirect"

    if not domain:
        return "rejected_or_ambiguous", "ambiguous"

    if re.search(r"\b(papaya gaming|avia games|big run studios|aviagames|papayagaming)\b", hs):
        return "corporate_entities", "corporate"

    if re.search(r"\b(skill game|fish game|mobile app ecosystem)\b", hs):
        return "rejected_or_ambiguous", "ambiguous"

    return "operators", "operator"


def _confidence_csv_value(row: dict[str, Any]) -> str:
    """Format ``row['confidence']`` as a fixed-point string (normalized + deduped rows use numeric scores)."""
    c = row.get("confidence", 0.35)
    if isinstance(c, (int, float)):
        return f"{float(c):.4f}"
    s = str(c).strip().lower()
    if s in ("high",):
        return "0.7500"
    if s in ("medium", "med"):
        return "0.5500"
    if s in ("low",):
        return "0.3500"
    try:
        return f"{float(s):.4f}"
    except ValueError:
        return "0.3500"


def _row_to_csv_dict(row: dict[str, Any], entity_type_hint: str) -> dict[str, str]:
    other = row.get("alias_candidates") or row.get("other_domains") or []
    if isinstance(other, list):
        other_str = ", ".join(str(x) for x in other)
    else:
        other_str = str(other)
    return {
        "brand": str(row.get("brand", "")),
        "primary_domain": str(row.get("normalized_primary_domain", "")),
        "other_domains": other_str,
        "entity_type_hint": entity_type_hint,
        "category": str(row.get("category", "")),
        "notes": str(row.get("notes", "")),
        "source_url": str(row.get("source_url", "")),
        "source_set": str(row.get("source_set", "")),
        "confidence": _confidence_csv_value(row),
        "review_status": str(row.get("review_status", "needs_review")),
        "duplicate_group_id": str(row.get("duplicate_group_id", "")),
        "merge_notes": str(row.get("merge_notes", "")),
    }


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows.sort(key=lambda r: (r.get("primary_domain", ""), r.get("brand", "")))
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in CSV_FIELDS})


def run_bucket(
    deduped_path: Path | None = None,
    out_dir: Path | None = None,
) -> dict[str, int]:
    root = repo_root()
    deduped_path = deduped_path or (root / "data" / "candidates" / "deduped_candidates.json")
    out_dir = out_dir or (root / "data" / "candidates")

    rows: list[dict[str, Any]] = json.loads(deduped_path.read_text(encoding="utf-8"))
    canonical = [r for r in rows if r.get("duplicate_of") is None]
    buckets: dict[str, list[dict[str, str]]] = {k: [] for k in BUCKET_FILES}

    for r in sorted(canonical, key=lambda x: str(x.get("candidate_id", ""))):
        bucket_key, hint = classify_canonical(r)
        buckets[bucket_key].append(_row_to_csv_dict(r, hint))

    counts: dict[str, int] = {}
    for key, fname in BUCKET_FILES.items():
        p = out_dir / fname
        _write_csv(p, buckets[key])
        counts[key] = len(buckets[key])
    return counts


def run_full_pipeline() -> dict[str, Any]:
    from sweep_scout import dedupe_candidates, intake_tables, normalize_candidates

    intake_tables.run_intake()
    normalize_candidates.run_normalize()
    dedupe_candidates.run_dedupe()
    counts = run_bucket()
    return {"bucket_counts": counts}


def main() -> None:
    parser = argparse.ArgumentParser(description="Bucket deduped candidates into review CSVs.")
    parser.add_argument("--in", dest="in_path", type=Path, default=None)
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument(
        "--full-pipeline",
        action="store_true",
        help="Run intake → normalize → dedupe → bucket first",
    )
    args = parser.parse_args()
    root = repo_root()
    if args.full_pipeline:
        summary = run_full_pipeline()
        print(deterministic_json_dumps(summary))
        return
    counts = run_bucket(
        deduped_path=args.in_path or (root / "data" / "candidates" / "deduped_candidates.json"),
        out_dir=args.out_dir or (root / "data" / "candidates"),
    )
    print(deterministic_json_dumps(counts))


if __name__ == "__main__":
    main()
