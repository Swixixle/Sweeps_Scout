from __future__ import annotations

import argparse
import json
from pathlib import Path

from sweep_scout.config import ensure_dirs, paths_for_repo, repo_root_from_args
from sweep_scout.constants import REBRAND_PHRASES
from sweep_scout.utils import deterministic_json_dumps, domain_from_url, utc_now_iso


def normalize_redirect_records(repo_root: Path) -> dict:
    ensure_dirs(repo_root)
    paths = paths_for_repo(repo_root)
    ts = utc_now_iso()

    http_rows: list[dict] = []
    if paths["discovered_redirects"].is_file():
        try:
            http_rows = json.loads(paths["discovered_redirects"].read_text(encoding="utf-8"))
            if not isinstance(http_rows, list):
                http_rows = []
        except Exception:
            http_rows = []

    phrase_rows: list[dict] = []
    if paths["extracted_signals"].is_file():
        try:
            sigs = json.loads(paths["extracted_signals"].read_text(encoding="utf-8"))
        except Exception:
            sigs = []
        for row in sigs:
            hits = row.get("rebrand_phrase_hits") or []
            plain = (row.get("footer_legal_snippet") or "") + " " + (row.get("title") or "")
            url = row.get("final_url") or row.get("url")
            if hits and url:
                phrase_rows.append(
                    {
                        "from_url": url,
                        "to_url": url,
                        "domain_from": row.get("domain") or domain_from_url(str(url)),
                        "domain_to": row.get("domain") or domain_from_url(str(url)),
                        "discovery_type": "page_phrase_rebrand",
                        "phrases_matched": hits,
                        "source_url": url,
                        "first_seen": ts,
                        "notes": "text contains rebrand/redirect language; not a chain by itself",
                    }
                )

    merged: dict[tuple[str, str, str], dict] = {}
    for r in http_rows + phrase_rows:
        k = (r.get("from_url", ""), r.get("to_url", ""), r.get("discovery_type", ""))
        merged[k] = r

    out = sorted(merged.values(), key=lambda x: (x.get("from_url", ""), x.get("to_url", "")))
    paths["discovered_redirects"].write_text(deterministic_json_dumps(out), encoding="utf-8")

    norm_path = paths["candidates"] / "redirect_candidates.json"
    norm_path.write_text(deterministic_json_dumps(out), encoding="utf-8")

    report = {
        "run_at": ts,
        "total_redirect_records": len(out),
        "rebrand_phrases_tracked": list(REBRAND_PHRASES),
    }
    paths["reports_discovery"].mkdir(parents=True, exist_ok=True)
    return report


def main_cli() -> None:
    p = argparse.ArgumentParser(description="Normalize redirect / rebrand candidates")
    p.add_argument("--repo-root", default=".")
    args = p.parse_args()
    root = Path(args.repo_root).resolve()
    r = normalize_redirect_records(root)
    print(deterministic_json_dumps(r))


if __name__ == "__main__":
    main_cli()
