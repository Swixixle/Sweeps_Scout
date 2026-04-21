from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sweep_scout.classifier import run_classifier
from sweep_scout.config import ensure_dirs, paths_for_repo, repo_root_from_args
from sweep_scout.discover import run_discover
from sweep_scout.emit import run_emit
from sweep_scout.extract import run_extract
from sweep_scout.fingerprint import run_fingerprint
from sweep_scout.intel_bridge import run_intel_bridge
from sweep_scout.redirects import normalize_redirect_records
from sweep_scout.utils import deterministic_json_dumps, utc_now_iso


def run_monitor(
    repo_root: Path,
    *,
    discover_depth: int = 1,
    max_pages: int = 250,
    max_extract_urls: int = 120,
    max_fingerprint_domains: int = 500,
    sign: bool = False,
    private_key_path: Path | None = None,
    key_id: str = "scout-fingerprint-key-v1",
) -> dict:
    ensure_dirs(repo_root)
    paths = paths_for_repo(repo_root)
    ts = utc_now_iso()

    steps: dict = {}
    steps["discover"] = run_discover(repo_root, max_depth=discover_depth, max_pages=max_pages)
    steps["extract"] = run_extract(repo_root, from_discovered=True, max_urls=max_extract_urls)
    steps["fingerprint"] = run_fingerprint(
        repo_root,
        max_domains=max_fingerprint_domains,
        sign=sign,
        private_key_path=private_key_path,
        key_id=key_id,
    )
    steps["redirects"] = normalize_redirect_records(repo_root)
    steps["classifier"] = run_classifier(repo_root)
    steps["emit"] = run_emit(repo_root)
    steps["intel_bridge"] = run_intel_bridge(repo_root)

    report = {
        "run_at": ts,
        "repo_root": str(repo_root),
        "fingerprint_signed": sign,
        "fingerprint_key_id": key_id if sign else None,
        "steps": steps,
        "outputs": {
            "discovered_domains": str(paths["discovered_domains"]),
            "extracted_signals": str(paths["extracted_signals"]),
            "domain_fingerprints": str(paths["domain_fingerprints"]),
            "proposed_entities": str(paths["proposed_entities"]),
            "intel_bridge": str(paths["candidates"] / "intel_bridge.json"),
        },
    }
    rep_path = paths["reports_monitoring"] / f"monitor-{ts.replace(':', '-')}.json"
    rep_path.write_text(deterministic_json_dumps(report), encoding="utf-8")
    return report


def main_cli() -> None:
    p = argparse.ArgumentParser(description="Run full Sweeps_Scout pipeline")
    p.add_argument("--repo-root", default=".")
    p.add_argument("--depth", type=int, default=1)
    p.add_argument("--max-pages", type=int, default=250)
    p.add_argument("--max-extract-urls", type=int, default=120)
    p.add_argument("--max-fingerprint-domains", type=int, default=500)
    p.add_argument("--sign", action="store_true", help="Write signed envelope to domain_fingerprints.json")
    p.add_argument("--private-key", type=Path, default=None, help="PEM Ed25519 private key (required with --sign)")
    p.add_argument("--key-id", default="scout-fingerprint-key-v1", help="Key id for fingerprint signature envelope")
    args = p.parse_args()
    if args.sign and args.private_key is None:
        sys.exit("error: --private-key is required when --sign is set")
    root = Path(args.repo_root).resolve()
    r = run_monitor(
        root,
        discover_depth=args.depth,
        max_pages=args.max_pages,
        max_extract_urls=args.max_extract_urls,
        max_fingerprint_domains=args.max_fingerprint_domains,
        sign=args.sign,
        private_key_path=args.private_key,
        key_id=args.key_id,
    )
    print(
        deterministic_json_dumps(
            {
                "run_at": r["run_at"],
                "steps": r["steps"],
                "fingerprint_signed": r["fingerprint_signed"],
                "fingerprint_key_id": r["fingerprint_key_id"],
            }
        )
    )


if __name__ == "__main__":
    main_cli()
