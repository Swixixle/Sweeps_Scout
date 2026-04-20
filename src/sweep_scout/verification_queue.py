"""Build verification_queue.csv from deduped candidates (priority-sorted review queue)."""
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

from sweep_scout.bucket_candidates import classify_canonical
from sweep_scout.utils import deterministic_json_dumps, repo_root

_OPERATOR_TERMS = re.compile(
    r"\b(casino|sweeps|sweepstakes|slots|poker|sportsbook|bingo|social casino|fish|fish game|redeem|gold coins|sweeps coins|sweeps cash|coins)\b",
    re.I,
)
_GENERIC_BRANDS = re.compile(
    r"^(casino|slots|bet|play|win|game|online|best|top|new)\s*$",
    re.I,
)


def _looks_registrable_domain(host: str) -> bool:
    if not host or "." not in host:
        return False
    parts = host.split(".")
    if len(parts) < 2:
        return False
    tld = parts[-1]
    if len(tld) < 2 or not tld.isalpha():
        return False
    if host.endswith(".localhost") or host.endswith(".invalid"):
        return False
    return True


def _priority_score(row: dict[str, Any], entity_hint: str) -> tuple[float, str]:
    """Higher = verify first. Returns (score, why_flagged)."""
    reasons: list[str] = []
    score = 15.0
    brand = str(row.get("brand", ""))
    dom = str(row.get("normalized_primary_domain", ""))
    bm = row.get("bulk_metadata") or {}
    bm_noise = ""
    if isinstance(bm, dict):
        bm_noise = f" {bm.get('optional_bonus', '')} {bm.get('optional_rating', '')} {bm.get('optional_game_count', '')}"
    hay = f"{brand} {row.get('notes', '')} {row.get('category', '')}{bm_noise}".lower()
    repeat = int(bm.get("domain_repeat_count") or 0) if isinstance(bm, dict) else 0

    if _looks_registrable_domain(dom):
        score += 22.0
        reasons.append("registrable_domain")
    else:
        reasons.append("weak_domain")
        score -= 10.0

    if re.search(r"\.(com|us|net)\b", dom, re.I):
        score += 5.0
        reasons.append("tld_com_us_net")

    if _OPERATOR_TERMS.search(hay) or _OPERATOR_TERMS.search(dom):
        score += 18.0
        reasons.append("operator_terms")

    if repeat > 1:
        score += min(12.0, 4.0 * repeat)
        reasons.append(f"repeated_domain_x{repeat}")

    if entity_hint in ("corporate", "operator"):
        score += 8.0
        reasons.append(f"bucket_hint_{entity_hint}")

    if _GENERIC_BRANDS.match(brand.strip()) or len(brand.strip()) < 3:
        score -= 20.0
        reasons.append("generic_brand")

    if row.get("alias_candidates"):
        score += 4.0
        reasons.append("has_alias_candidates")

    if str(row.get("intake_channel", "")) == "markdown":
        score += 3.0
        reasons.append("curated_markdown")

    score = max(0.0, min(100.0, score))
    return score, ";".join(reasons)


def _needs_manual(row: dict[str, Any]) -> str:
    if row.get("alias_candidates"):
        return "yes"
    mg = str(row.get("merge_notes", ""))
    if mg and "merged_ids=" in mg:
        return "yes"
    if float(row.get("confidence", 0.35) or 0) < 0.42:
        return "maybe"
    return "no"


def run_build_verification_queue(
    deduped_path: Path | None = None,
    out_csv: Path | None = None,
) -> list[dict[str, str]]:
    root = repo_root()
    deduped_path = deduped_path or (root / "data" / "candidates" / "bulk_deduped_rows.json")
    out_csv = out_csv or (root / "data" / "candidates" / "verification_queue.csv")

    rows: list[dict[str, Any]] = json.loads(deduped_path.read_text(encoding="utf-8"))
    canonical = [r for r in rows if r.get("duplicate_of") is None]

    out_rows: list[dict[str, str]] = []
    for r in canonical:
        bucket, hint = classify_canonical(r)
        _ = bucket  # queue is cross-bucket
        pri, why = _priority_score(r, hint)
        bm = r.get("bulk_metadata") or {}
        if isinstance(bm, dict) and bm.get("source_file_id"):
            src = str(bm.get("source_file_id"))
        else:
            src = str(r.get("source_path", "") or r.get("source_set", "markdown"))

        out_rows.append(
            {
                "brand": str(r.get("brand", "")),
                "primary_domain": str(r.get("normalized_primary_domain", "")),
                "entity_type_hint": hint,
                "priority_score": f"{pri:.2f}",
                "why_flagged": why,
                "source_file": src,
                "duplicate_group_id": str(r.get("duplicate_group_id", "")),
                "needs_manual_verification": _needs_manual(r),
            }
        )

    out_rows.sort(key=lambda x: (-float(x["priority_score"]), x["primary_domain"], x["brand"]))

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "brand",
        "primary_domain",
        "entity_type_hint",
        "priority_score",
        "why_flagged",
        "source_file",
        "duplicate_group_id",
        "needs_manual_verification",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        for row in out_rows:
            w.writerow({k: row.get(k, "") for k in fields})

    return out_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build verification_queue.csv from bulk_deduped_rows.json")
    parser.add_argument("--in", dest="in_path", type=Path, default=None)
    parser.add_argument("--out", dest="out_path", type=Path, default=None)
    args = parser.parse_args()
    root = repo_root()
    n = run_build_verification_queue(
        deduped_path=args.in_path or (root / "data" / "candidates" / "bulk_deduped_rows.json"),
        out_csv=args.out_path or (root / "data" / "candidates" / "verification_queue.csv"),
    )
    print(deterministic_json_dumps({"verification_queue_rows": len(n)}))


if __name__ == "__main__":
    main()
