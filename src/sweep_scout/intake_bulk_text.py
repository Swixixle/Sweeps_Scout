"""Parse messy bulk text dumps into structured candidate rows (not reviewed truth)."""
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

from sweep_scout.dedupe_candidates import dedupe_normalized_rows
from sweep_scout.normalize_candidates import normalize_domain, parse_confidence
from sweep_scout.utils import deterministic_json_dumps, repo_root, sha256_text
from sweep_scout.verification_queue import run_build_verification_queue

# Decorative lines to skip
_SKIP_LINE = re.compile(
    r"^(---+|===+|\*{3,}|#+\s|source:|dump id:|generated:|rows?\s*:)\s*$",
    re.I,
)

_URL = re.compile(r"https?://[^\s\]|,;\"'<>]+", re.I)
# Host-like token (not exhaustive; intake-only)
_HOST_TOKEN = re.compile(
    r"\b((?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63})\b",
    re.I,
)
_BAD_TLD = frozenset(
    {
        "png",
        "jpg",
        "jpeg",
        "gif",
        "svg",
        "pdf",
        "css",
        "js",
        "json",
        "xml",
        "localhost",
    }
)


def _clean_brand(s: str) -> str:
    s = re.sub(r"^[\s\-–—|]+|[\s\-–—|]+$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _first_domain_in_text(text: str) -> str:
    for m in _HOST_TOKEN.finditer(text):
        host = m.group(1).lower().strip(".")
        parts = host.split(".")
        tld = parts[-1] if parts else ""
        if tld in _BAD_TLD or len(host) < 5:
            continue
        if "@" in text[max(0, m.start() - 1) : m.end() + 1]:
            continue
        return host
    return ""


def _extract_optional_fields(fragment: str) -> dict[str, str]:
    out: dict[str, str] = {}
    cm = re.search(
        r"\bconf(?:idence)?\s*[:=]\s*([0-9]+(?:\.[0-9]+)?\s*%?|high|medium|low|med|hi)\b",
        fragment,
        re.I,
    )
    if cm:
        out["optional_confidence"] = cm.group(1).strip()
    # rating: 4.5, 3/5, 88%
    rm = re.search(r"(?:rating|stars?)\s*[:=]\s*([0-9]+(?:\.[0-9]+)?(?:/10|/5)?|(?:[0-9]+%))", fragment, re.I)
    if rm:
        out["optional_rating"] = rm.group(1).strip()
    bm = re.search(
        r"(?:bonus|promo)\s*[:=]\s*([^\n|]{1,80})",
        fragment,
        re.I,
    )
    if bm:
        out["optional_bonus"] = bm.group(1).strip()
    ym = re.search(r"(?:launch|since|est\.?|founded)\s*[:=]?\s*([12][0-9]{3})", fragment, re.I)
    if ym:
        out["optional_launch_date"] = ym.group(1).strip()
    gm = re.search(r"(?:games?|titles?)\s*[:=]\s*([0-9,]+)", fragment, re.I)
    if gm:
        out["optional_game_count"] = gm.group(1).strip()
    return out


def _parse_structured_line(line: str) -> dict[str, str] | None:
    """Key: value patterns often found in dumps."""
    m = re.match(
        r"^(?:brand|name|site|operator)\s*[:=]\s*(.+?)\s+(?:domain|url|website)\s*[:=]\s*(\S+)",
        line,
        re.I,
    )
    if m:
        return {"brand": _clean_brand(m.group(1)), "domain": m.group(2).strip()}
    m = re.match(r"^(?:domain|url|website)\s*[:=]\s*(\S+)\s+(?:brand|name)\s*[:=]\s*(.+)$", line, re.I)
    if m:
        return {"brand": _clean_brand(m.group(2)), "domain": m.group(1).strip()}
    return None


def parse_bulk_line(line: str, line_index: int, source_file_id: str) -> dict[str, Any] | None:
    """Return one raw row dict or None if line should be skipped."""
    raw = line.rstrip("\n")
    stripped = raw.strip()
    if not stripped:
        return None
    if stripped.startswith("#"):
        return None
    if _SKIP_LINE.match(stripped):
        return None

    source_fragment = stripped[:500]
    brand = ""
    primary = ""
    category = ""
    notes = ""

    # Markdown table row
    if stripped.startswith("|") and "|" in stripped[1:]:
        parts = [p.strip() for p in stripped.split("|")]
        if parts and parts[0] == "":
            parts = parts[1:]
        if parts and parts[-1] == "":
            parts = parts[:-1]
        if len(parts) >= 2 and re.match(r"^:?-+:?$", parts[0] or "-"):
            return None
        head0 = (parts[0] if parts else "").lower()
        if head0 in ("brand", "domain", "name", "url", "primary_domain", "site"):
            return None
        if len(parts) >= 2:
            brand = _clean_brand(parts[0])
            rest_join = " | ".join(parts[1:])
            u = _URL.search(rest_join)
            if u:
                primary = u.group(0)
            else:
                primary = _first_domain_in_text(rest_join) or parts[1]
            if len(parts) > 2:
                notes = " | ".join(parts[2:])
        if not brand and len(parts) >= 1:
            brand = _clean_brand(parts[0])

    if not primary:
        kv = _parse_structured_line(stripped)
        if kv:
            brand = kv["brand"]
            primary = kv["domain"]

    if not primary:
        u = _URL.search(stripped)
        if u:
            primary = u.group(0)
            before = stripped[: u.start()].strip(" \t|–—-")
            after = stripped[u.end() :].strip(" \t|–—-")
            brand = _clean_brand(before) if before else _clean_brand(after)
            if not brand:
                brand = primary
        else:
            # Tab or pipe separated without URL
            if "\t" in stripped:
                cells = [c.strip() for c in stripped.split("\t") if c.strip()]
            elif "|" in stripped and not stripped.startswith("|"):
                cells = [c.strip() for c in re.split(r"\s*\|\s*", stripped) if c.strip()]
            else:
                cells = []
            if len(cells) >= 2:
                brand = _clean_brand(cells[0])
                maybe = cells[1]
                if _URL.search(maybe):
                    primary = _URL.search(maybe).group(0)
                else:
                    primary = maybe
                if len(cells) > 2:
                    notes = " ".join(cells[2:])
            elif len(cells) == 1:
                primary = _first_domain_in_text(cells[0]) or ""
                brand = cells[0] if not primary else _clean_brand(cells[0].replace(primary, "").strip(" -|"))

    if not primary:
        primary = _first_domain_in_text(stripped)
        if primary:
            brand = _clean_brand(stripped.replace(primary, "").strip(" \t|–—-,:;"))

    if not primary:
        return None
    if not brand:
        brand = primary

    opt = _extract_optional_fields(stripped)

    row: dict[str, Any] = {
        "raw_line_index": line_index,
        "brand": brand[:300],
        "primary_domain": primary[:500],
        "category": category,
        "notes": notes[:2000],
        "source_fragment": source_fragment,
        "source_file_id": source_file_id,
        "optional_bonus": opt.get("optional_bonus", ""),
        "optional_rating": opt.get("optional_rating", ""),
        "optional_launch_date": opt.get("optional_launch_date", ""),
        "optional_game_count": opt.get("optional_game_count", ""),
        "optional_confidence": opt.get("optional_confidence", ""),
    }
    return row


def parse_bulk_text(text: str, source_file_id: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    skipped_no_domain = 0
    for i, line in enumerate(text.splitlines()):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or (_SKIP_LINE.match(stripped) if stripped else False):
            continue
        rec = parse_bulk_line(line, i, source_file_id)
        if rec:
            rows.append(rec)
        elif len(stripped) > 1:
            skipped_no_domain += 1
    stats = {"skipped_lines_no_extractable_domain": skipped_no_domain, "parsed_rows": len(rows)}
    return rows, stats


def run_intake_bulk_text(
    path: Path | None = None,
    out_path: Path | None = None,
    source_file_id: str | None = None,
    stats_path: Path | None = None,
) -> list[dict[str, Any]]:
    root = repo_root()
    path = path or (root / "data" / "raw_sources" / "sweeps_bulk_dump_001.txt")
    out_path = out_path or (root / "data" / "candidates" / "bulk_raw_rows.json")
    stats_path = stats_path or (root / "data" / "candidates" / "bulk_intake_stats.json")
    source_file_id = source_file_id or path.stem
    source_filename = path.name

    text = path.read_text(encoding="utf-8", errors="replace")
    rows, stats = parse_bulk_text(text, source_file_id=source_file_id)
    stats["source_file"] = source_filename
    rows.sort(
        key=lambda r: (
            str(r.get("source_file_id", "")),
            int(r.get("raw_line_index", 0)),
            str(r.get("brand", "")),
        )
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(deterministic_json_dumps(rows), encoding="utf-8")
    stats_path.write_text(deterministic_json_dumps(stats), encoding="utf-8")
    return rows


def normalize_bulk_raw_row(
    raw: dict[str, Any],
    domain_repeat_count: int,
    *,
    source_filename: str = "sweeps_bulk_dump_001.txt",
) -> dict[str, Any]:
    """Map bulk raw row to the same normalized shape as markdown intake (+ bulk metadata)."""
    brand = str(raw.get("brand", "")).strip()
    raw_primary = str(raw.get("primary_domain", "")).strip()
    source_file_id = str(raw.get("source_file_id", "bulk"))
    raw_line_index = raw.get("raw_line_index", 0)
    notes = str(raw.get("notes", ""))
    cat = str(raw.get("category", ""))
    frag = str(raw.get("source_fragment", ""))[:800]
    primary_norm = normalize_domain(raw_primary)
    raw_conf = str(raw.get("optional_confidence", "")).strip()
    if raw_conf:
        confidence_label, conf = parse_confidence(raw_conf)
    else:
        confidence_label, conf = parse_confidence("")

    candidate_id = "cand_" + sha256_text(
        "\u241f".join(
            [
                "bulk",
                source_file_id,
                str(raw_line_index),
                brand,
                raw_primary,
            ]
        )
    )[:16]

    bulk_meta = {
        "source_file": source_filename,
        "source_file_id": source_file_id,
        "source_fragment": frag,
        "optional_bonus": raw.get("optional_bonus", ""),
        "optional_rating": raw.get("optional_rating", ""),
        "optional_launch_date": raw.get("optional_launch_date", ""),
        "optional_game_count": raw.get("optional_game_count", ""),
        "optional_confidence": raw_conf,
        "domain_repeat_count": domain_repeat_count,
    }

    return {
        "candidate_id": candidate_id,
        "review_status": "needs_review",
        "confidence": conf,
        "confidence_label": confidence_label,
        "entity_type_hint": "unknown",
        "alias_candidates": [],
        "brand": " ".join(brand.split()),
        "raw_primary_domain": raw_primary,
        "normalized_primary_domain": primary_norm,
        "raw_other_domains": "",
        "other_domains": [],
        "category": cat,
        "notes": notes,
        "source_url": "",
        "source_set": "bulk",
        "source_path": source_file_id,
        "intake_row_index": raw_line_index,
        "intake_channel": "bulk",
        "bulk_metadata": bulk_meta,
    }


def _domain_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in rows:
        d = normalize_domain(str(r.get("primary_domain", "")))
        if d:
            counts[d] = counts.get(d, 0) + 1
    return counts


def run_bulk_normalize(
    raw_path: Path | None = None,
    out_path: Path | None = None,
    *,
    source_filename: str = "sweeps_bulk_dump_001.txt",
) -> list[dict[str, Any]]:
    root = repo_root()
    raw_path = raw_path or (root / "data" / "candidates" / "bulk_raw_rows.json")
    out_path = out_path or (root / "data" / "candidates" / "bulk_normalized_rows.json")

    raw_rows: list[dict[str, Any]] = json.loads(raw_path.read_text(encoding="utf-8"))
    counts = _domain_counts(raw_rows)
    out: list[dict[str, Any]] = []
    dropped_invalid = 0
    for raw in raw_rows:
        d = normalize_domain(str(raw.get("primary_domain", "")))
        if not d:
            dropped_invalid += 1
            continue
        rc = counts.get(d, 1) if d else 0
        out.append(
            normalize_bulk_raw_row(
                raw,
                domain_repeat_count=rc,
                source_filename=source_filename,
            )
        )

    out.sort(key=lambda r: str(r.get("candidate_id", "")))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(deterministic_json_dumps(out), encoding="utf-8")
    stats_path = root / "data" / "candidates" / "bulk_intake_stats.json"
    if stats_path.is_file():
        st = json.loads(stats_path.read_text(encoding="utf-8"))
        st["dropped_invalid_domain_after_normalize"] = dropped_invalid
        st["rows_with_valid_domain"] = len(out)
        stats_path.write_text(deterministic_json_dumps(st), encoding="utf-8")
    return out


def run_bulk_dedupe_merge(
    bulk_normalized_path: Path | None = None,
    markdown_normalized_path: Path | None = None,
    out_path: Path | None = None,
    *,
    merge_markdown: bool = True,
) -> list[dict[str, Any]]:
    """Merge bulk + existing markdown-normalized rows and dedupe together."""
    root = repo_root()
    bulk_normalized_path = bulk_normalized_path or (root / "data" / "candidates" / "bulk_normalized_rows.json")
    markdown_normalized_path = markdown_normalized_path or (
        root / "data" / "candidates" / "normalized_candidate_rows.json"
    )
    out_path = out_path or (root / "data" / "candidates" / "bulk_deduped_rows.json")

    bulk_rows: list[dict[str, Any]] = json.loads(bulk_normalized_path.read_text(encoding="utf-8"))
    md_rows: list[dict[str, Any]] = []
    if merge_markdown and markdown_normalized_path.is_file():
        md_rows = json.loads(markdown_normalized_path.read_text(encoding="utf-8"))

    combined = bulk_rows + md_rows
    deduped = dedupe_normalized_rows(combined)
    deduped = _attach_merged_source_fragments(deduped)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(deterministic_json_dumps(deduped), encoding="utf-8")
    return deduped


def _attach_merged_source_fragments(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add merged_source_fragments on canonical rows from all group members (provenance)."""
    by_group: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        dg = str(r.get("duplicate_group_id", "") or "")
        if not dg:
            continue
        by_group.setdefault(dg, []).append(r)

    frags_by_canon: dict[str, list[str]] = {}
    for _dg, members in by_group.items():
        canon = next((m for m in members if m.get("duplicate_of") is None), None)
        if not canon:
            continue
        cid = str(canon.get("candidate_id", ""))
        seen: list[str] = []
        for m in sorted(members, key=lambda x: str(x.get("candidate_id", ""))):
            bm = m.get("bulk_metadata") if isinstance(m.get("bulk_metadata"), dict) else {}
            frag = str(bm.get("source_fragment", "") or "").strip()
            if frag and frag not in seen:
                seen.append(frag)
        if seen:
            frags_by_canon[cid] = seen

    out: list[dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        if rr.get("duplicate_of") is None:
            cid = str(rr.get("candidate_id", ""))
            if cid in frags_by_canon:
                rr["merged_source_fragments"] = frags_by_canon[cid]
        out.append(rr)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk text intake for sweeps candidate dumps.")
    parser.add_argument("--input", type=Path, default=None, help="Path to bulk .txt dump")
    parser.add_argument(
        "--bulk-only",
        action="store_true",
        help="Dedupe bulk rows only (do not merge normalized_candidate_rows.json)",
    )
    args = parser.parse_args()
    root = repo_root()
    inp = args.input or (root / "data" / "raw_sources" / "sweeps_bulk_dump_001.txt")
    run_intake_bulk_text(path=inp)
    run_bulk_normalize(source_filename=inp.name)
    run_bulk_dedupe_merge(merge_markdown=not args.bulk_only)
    run_build_verification_queue()
    print(deterministic_json_dumps({"status": "bulk_intake_complete", "input": str(inp)}))


if __name__ == "__main__":
    main()
