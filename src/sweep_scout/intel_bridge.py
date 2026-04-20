from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from sweep_scout.config import ensure_dirs, paths_for_repo, repo_root_from_args
from sweep_scout.utils import deterministic_json_dumps, normalize_host, utc_now_iso


def _load_snapshot(path: Path | None) -> dict[str, Any] | None:
    if not path or not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


def _domains_from_snapshot(snap: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for key in ("entities", "domains", "known_domains", "operators"):
        block = snap.get(key)
        if isinstance(block, list):
            for row in block:
                if isinstance(row, str):
                    out.add(normalize_host(row))
                elif isinstance(row, dict):
                    d = row.get("domain") or row.get("hostname") or row.get("normalized_domain")
                    if isinstance(d, str):
                        out.add(normalize_host(d))
        elif isinstance(block, dict):
            for k in block.keys():
                out.add(normalize_host(k))
    return {d for d in out if d}


def compare_domains(
    discovered_domains: list[dict[str, Any]],
    snapshot: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    known = _domains_from_snapshot(snapshot) if snapshot else set()
    rows: list[dict[str, Any]] = []
    for drow in discovered_domains:
        dom = drow.get("domain") or ""
        nd = normalize_host(dom)
        if not nd:
            continue
        status = "net_new"
        notes: list[str] = []
        if known and nd in known:
            status = "already_known"
        elif known:
            for k in known:
                if nd.endswith("." + k) or k.endswith("." + nd):
                    status = "possible_same_cluster"
                    notes.append(f"partial overlap with known {k}")
                    break
            if status == "net_new":
                rebr = drow.get("discovery_type") == "http_redirect" or False
                if rebr:
                    status = "possible_rebrand"
        rows.append(
            {
                "domain": nd,
                "bridge_status": status,
                "notes": notes,
                "source_urls": drow.get("source_urls") or [drow.get("source_url")],
            }
        )
    return rows


def run_intel_bridge(repo_root: Path, snapshot_path: Path | None = None) -> dict:
    ensure_dirs(repo_root)
    paths = paths_for_repo(repo_root)
    ts = utc_now_iso()

    env_path = os.environ.get("SWEEPS_INTEL_SNAPSHOT")
    sp = snapshot_path
    if sp is None and env_path:
        sp = Path(env_path)
    if sp is None:
        default = repo_root / "data" / "intel_snapshot.json"
        if default.is_file():
            sp = default

    snap = _load_snapshot(sp)
    discovered: list = []
    if paths["discovered_domains"].is_file():
        discovered = json.loads(paths["discovered_domains"].read_text(encoding="utf-8"))

    comparison = compare_domains(discovered, snap)
    out_path = paths["candidates"] / "intel_bridge.json"
    out_path.write_text(
        deterministic_json_dumps(
            {
                "run_at": ts,
                "snapshot_path": str(sp) if sp else None,
                "snapshot_loaded": snap is not None,
                "rows": comparison,
            }
        ),
        encoding="utf-8",
    )
    summary = {
        "run_at": ts,
        "snapshot_loaded": snap is not None,
        "compared": len(comparison),
    }
    return summary


def main_cli() -> None:
    p = argparse.ArgumentParser(description="Optional compare against Sweeps_Intel export")
    p.add_argument("--repo-root", default=".")
    p.add_argument("--intel-snapshot", default=None, help="Path to intel JSON export")
    args = p.parse_args()
    root = Path(args.repo_root).resolve()
    sp = Path(args.intel_snapshot).resolve() if args.intel_snapshot else None
    r = run_intel_bridge(root, snapshot_path=sp)
    print(deterministic_json_dumps(r))


if __name__ == "__main__":
    main_cli()
