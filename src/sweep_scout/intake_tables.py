from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from sweep_scout.utils import deterministic_json_dumps, repo_root


def _source_set_from_filename(path: Path) -> str:
    name = path.stem.lower()
    if "set2" in name:
        return "set2"
    if "set1" in name:
        return "set1"
    return name


def _split_table_row(line: str) -> list[str]:
    line = line.rstrip("\n")
    if not line.strip().startswith("|"):
        return []
    parts = [p.strip() for p in line.split("|")]
    if parts and parts[0] == "":
        parts = parts[1:]
    if parts and parts[-1] == "":
        parts = parts[:-1]
    return parts


def _is_separator_row(cells: list[str]) -> bool:
    if not cells:
        return False
    return all(re.match(r"^:?-+:?$", c.strip()) for c in cells if c.strip())


def parse_markdown_tables(text: str, source_set: str, source_path: str) -> list[dict[str, Any]]:
    lines = text.splitlines()
    rows: list[dict[str, Any]] = []
    header: list[str] | None = None
    row_index = 0

    for line in lines:
        cells = _split_table_row(line)
        if len(cells) < 2:
            continue
        if header is None:
            header = [re.sub(r"\s+", "_", h.strip().lower()) for h in cells]
            continue
        if _is_separator_row(cells):
            continue
        if len(cells) != len(header):
            pad = len(header) - len(cells)
            if pad > 0:
                cells = cells + [""] * pad
            else:
                cells = cells[: len(header)]
        rec: dict[str, Any] = {
            "source_set": source_set,
            "source_path": source_path,
            "intake_row_index": row_index,
        }
        for key, val in zip(header, cells, strict=True):
            rec[key] = val.strip()
        rows.append(rec)
        row_index += 1

    return rows


def parse_raw_source_file(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    source_set = _source_set_from_filename(path)
    return parse_markdown_tables(text, source_set=source_set, source_path=str(path))


def run_intake(
    raw_dir: Path | None = None,
    out_path: Path | None = None,
) -> list[dict[str, Any]]:
    root = repo_root()
    raw_dir = raw_dir or (root / "data" / "raw_sources")
    out_path = out_path or (root / "data" / "candidates" / "raw_intake_rows.json")

    paths = sorted(raw_dir.glob("*.md"))
    all_rows: list[dict[str, Any]] = []
    for p in paths:
        all_rows.extend(parse_raw_source_file(p))

    all_rows.sort(
        key=lambda r: (
            str(r.get("source_set", "")),
            int(r.get("intake_row_index", 0)),
            str(r.get("brand", "")),
            str(r.get("primary_domain", "")),
        )
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(deterministic_json_dumps(all_rows), encoding="utf-8")
    return all_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse raw markdown sweeps list tables into JSON.")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="Directory containing sweeps_lists_set*.md (default: data/raw_sources)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output JSON path (default: data/candidates/raw_intake_rows.json)",
    )
    args = parser.parse_args()
    rows = run_intake(raw_dir=args.raw_dir, out_path=args.out)
    print(f"Wrote {len(rows)} intake rows to {args.out or (repo_root() / 'data/candidates/raw_intake_rows.json')}")


if __name__ == "__main__":
    main()
