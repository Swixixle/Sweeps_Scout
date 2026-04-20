from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from sweep_scout.utils import deterministic_json_dumps, repo_root, sha256_text


def _get_field(row: dict[str, Any], *names: str) -> str:
    lower = {k.lower(): v for k, v in row.items() if isinstance(k, str)}
    for n in names:
        if n in row and row[n] is not None:
            return str(row[n]).strip()
        ln = n.lower()
        if ln in lower and lower[ln] is not None:
            return str(lower[ln]).strip()
    return ""


def confidence_label_from_score(score: float) -> str:
    """Bucket a numeric score in [0, 1] into high / medium / low (same thresholds as ``parse_confidence``)."""
    v = float(score)
    v = min(1.0, max(0.0, v))
    if v >= 0.7:
        return "high"
    if v >= 0.45:
        return "medium"
    return "low"


def parse_confidence(raw: str) -> tuple[str, float]:
    """Map optional source text to (label, score in [0,1]). Defaults are conservative."""
    s = (raw or "").strip()
    if not s:
        return "low", 0.35
    sl = s.lower()
    if sl in ("high", "hi"):
        return "high", 0.75
    if sl in ("medium", "med", "mid"):
        return "medium", 0.55
    if sl in ("low",):
        return "low", 0.35
    try:
        t = s.replace("%", "").strip()
        v = float(t)
        if "%" in raw or v > 1.0:
            v = min(1.0, max(0.0, v / 100.0))
        else:
            v = min(1.0, max(0.0, v))
        return confidence_label_from_score(v), round(v, 4)
    except ValueError:
        return "low", 0.35


def _split_other_domains(s: str) -> list[str]:
    if not s or not s.strip():
        return []
    parts = re.split(r"[,;|\n]+", s)
    return [p.strip() for p in parts if p.strip()]


def _normalize_host_fragment(raw: str) -> str:
    if not raw or not raw.strip():
        return ""
    s = raw.strip()
    if "://" in s:
        s = s.split("://", 1)[1]
    s = s.split("/", 1)[0]
    s = s.split("?", 1)[0]
    s = s.split("#", 1)[0]
    if "@" in s:
        s = s.split("@")[-1]
    if ":" in s and not s.startswith("["):
        maybe_port = s.rsplit(":", 1)[-1]
        if maybe_port.isdigit():
            s = s.rsplit(":", 1)[0]
    s = s.strip().lower().strip(".")
    if s.startswith("www."):
        s = s[4:]
    return s


def normalize_domain(raw: str) -> str:
    """Lowercase host, strip scheme/path/port; empty if invalid."""
    h = _normalize_host_fragment(raw)
    if not h:
        return ""
    if not re.match(r"^[a-z0-9.\-]+$", h):
        return ""
    return h


def _stable_candidate_id(parts: list[str]) -> str:
    key = "\u241f".join(parts)
    return "cand_" + sha256_text(key)[:16]


def normalize_intake_row(row: dict[str, Any], seq: int) -> dict[str, Any]:
    brand = _get_field(row, "brand", "Brand")
    raw_primary = _get_field(row, "primary_domain", "primary domain", "domain")
    raw_other = _get_field(row, "other_domains", "other domains", "aliases")
    category = _get_field(row, "category", "Category")
    notes = _get_field(row, "notes", "Notes")
    source_url = _get_field(row, "source_url", "source url", "url")
    source_set = _get_field(row, "source_set", "source set") or "unknown"
    source_path = _get_field(row, "source_path", "source path")
    intake_row_index = row.get("intake_row_index", seq)
    raw_conf = _get_field(row, "confidence", "Confidence")

    primary_norm = normalize_domain(raw_primary)
    raw_other_parts = _split_other_domains(raw_other)
    alias_candidates = sorted({normalize_domain(x) for x in raw_other_parts if normalize_domain(x)})

    confidence_label, confidence_score = parse_confidence(raw_conf)

    candidate_id = _stable_candidate_id(
        [
            source_set,
            source_path,
            str(intake_row_index),
            brand,
            raw_primary,
            raw_other,
        ]
    )

    return {
        "candidate_id": candidate_id,
        "review_status": "needs_review",
        "confidence": confidence_score,
        "confidence_label": confidence_label,
        "entity_type_hint": "unknown",
        "alias_candidates": alias_candidates,
        "brand": " ".join(brand.split()),
        "raw_primary_domain": raw_primary,
        "normalized_primary_domain": primary_norm,
        "raw_other_domains": raw_other,
        "other_domains": raw_other_parts,
        "category": category,
        "notes": notes,
        "source_url": source_url,
        "source_set": source_set,
        "source_path": source_path,
        "intake_row_index": int(intake_row_index) if str(intake_row_index).isdigit() else intake_row_index,
        "intake_channel": "markdown",
    }


def run_normalize(
    intake_path: Path | None = None,
    out_path: Path | None = None,
) -> list[dict[str, Any]]:
    root = repo_root()
    intake_path = intake_path or (root / "data" / "candidates" / "raw_intake_rows.json")
    out_path = out_path or (root / "data" / "candidates" / "normalized_candidate_rows.json")

    raw_rows = json.loads(intake_path.read_text(encoding="utf-8"))
    out: list[dict[str, Any]] = []
    for i, row in enumerate(raw_rows):
        out.append(normalize_intake_row(row, seq=i))

    out.sort(key=lambda r: str(r.get("candidate_id", "")))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(deterministic_json_dumps(out), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize raw intake rows.")
    parser.add_argument("--in", dest="in_path", type=Path, default=None)
    parser.add_argument("--out", dest="out_path", type=Path, default=None)
    args = parser.parse_args()
    root = repo_root()
    n = run_normalize(
        intake_path=args.in_path or (root / "data" / "candidates" / "raw_intake_rows.json"),
        out_path=args.out_path or (root / "data" / "candidates" / "normalized_candidate_rows.json"),
    )
    print(f"Wrote {len(n)} normalized rows")


if __name__ == "__main__":
    main()
