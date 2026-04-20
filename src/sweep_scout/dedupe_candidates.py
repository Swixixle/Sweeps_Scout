from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from sweep_scout.normalize_candidates import confidence_label_from_score
from sweep_scout.utils import deterministic_json_dumps, repo_root, sha256_text


def brand_fold(brand: str) -> str:
    s = brand.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


class _UnionFind:
    def __init__(self, n: int) -> None:
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            if ra < rb:
                self.p[rb] = ra
            else:
                self.p[ra] = rb


def _should_link_by_brand_fold(a: dict[str, Any], b: dict[str, Any]) -> bool:
    ba = brand_fold(str(a.get("brand", "")))
    bb = brand_fold(str(b.get("brand", "")))
    if not ba or not bb or ba != bb:
        return False
    if len(ba) < 4:
        return False
    da = str(a.get("normalized_primary_domain", "")).strip()
    db = str(b.get("normalized_primary_domain", "")).strip()
    if da and db:
        return da == db
    return True


def _confidence_float(row: dict[str, Any]) -> float:
    c = row.get("confidence", 0.35)
    try:
        return float(c)
    except (TypeError, ValueError):
        return 0.35


def _max_confidence_for_group(rows: list[dict[str, Any]], idxs: list[int]) -> tuple[float, str]:
    scores = [_confidence_float(rows[k]) for k in idxs]
    m = max(scores) if scores else 0.35
    return m, confidence_label_from_score(m)


def dedupe_normalized_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Union-find duplicate detection on normalized candidate dicts (markdown and/or bulk)."""
    rows = sorted(rows, key=lambda r: str(r.get("candidate_id", "")))
    n = len(rows)
    uf = _UnionFind(n)

    by_domain: dict[str, list[int]] = {}
    for i, r in enumerate(rows):
        d = str(r.get("normalized_primary_domain", "")).strip()
        if not d:
            continue
        by_domain.setdefault(d, []).append(i)

    for _d, idxs in sorted(by_domain.items()):
        sorted_i = sorted(idxs)
        for a, b in zip(sorted_i, sorted_i[1:]):
            uf.union(a, b)

    for i in range(n):
        for j in range(i + 1, n):
            if _should_link_by_brand_fold(rows[i], rows[j]):
                uf.union(i, j)

    groups: dict[int, list[int]] = {}
    for i in range(n):
        r = uf.find(i)
        groups.setdefault(r, []).append(i)

    canon_by_root: dict[int, str] = {}
    group_id_by_root: dict[int, str] = {}
    for root_i, idxs in groups.items():
        ids = sorted(str(rows[k].get("candidate_id", "")) for k in idxs)
        canon = min(ids)
        canon_by_root[root_i] = canon
        group_id_by_root[root_i] = "dg_" + sha256_text(canon)[:12]

    out_rows: list[dict[str, Any]] = []
    for root_i, idxs in sorted(groups.items(), key=lambda x: canon_by_root[x[0]]):
        idxs_sorted = sorted(idxs, key=lambda k: str(rows[k].get("candidate_id", "")))
        canon_id = canon_by_root[root_i]
        dg = group_id_by_root[root_i]
        members = [str(rows[k].get("candidate_id", "")) for k in idxs_sorted]
        max_score, max_label = _max_confidence_for_group(rows, idxs_sorted)
        for k in idxs_sorted:
            r = dict(rows[k])
            cid = str(r.get("candidate_id", ""))
            if cid == canon_id:
                r["duplicate_of"] = None
                r["duplicate_group_id"] = dg
                if len(members) > 1:
                    r["confidence"] = round(max_score, 4)
                    r["confidence_label"] = max_label
                    others = [m for m in members if m != cid]
                    r["merge_notes"] = "canonical row; duplicate_group=" + dg + "; merged_ids=" + ",".join(others)
                else:
                    r["merge_notes"] = ""
            else:
                r["duplicate_of"] = canon_id
                r["duplicate_group_id"] = dg
                r["merge_notes"] = f"likely duplicate of {canon_id}; same group {dg}"
            out_rows.append(r)

    out_rows.sort(
        key=lambda r: (
            str(r.get("duplicate_group_id", "")),
            r.get("duplicate_of") is None,
            str(r.get("candidate_id", "")),
        )
    )
    return out_rows


def run_dedupe(
    normalized_path: Path | None = None,
    out_path: Path | None = None,
) -> list[dict[str, Any]]:
    root = repo_root()
    normalized_path = normalized_path or (root / "data" / "candidates" / "normalized_candidate_rows.json")
    out_path = out_path or (root / "data" / "candidates" / "deduped_candidates.json")

    rows: list[dict[str, Any]] = json.loads(normalized_path.read_text(encoding="utf-8"))
    out_rows = dedupe_normalized_rows(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(deterministic_json_dumps(out_rows), encoding="utf-8")
    return out_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Dedupe normalized candidates.")
    parser.add_argument("--in", dest="in_path", type=Path, default=None)
    parser.add_argument("--out", dest="out_path", type=Path, default=None)
    args = parser.parse_args()
    root = repo_root()
    d = run_dedupe(
        normalized_path=args.in_path or (root / "data" / "candidates" / "normalized_candidate_rows.json"),
        out_path=args.out_path or (root / "data" / "candidates" / "deduped_candidates.json"),
    )
    print(f"Wrote {len(d)} deduped rows")


if __name__ == "__main__":
    main()
