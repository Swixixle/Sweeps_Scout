from __future__ import annotations

import argparse
from pathlib import Path


def repo_root_from_args(default: str = ".") -> Path:
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--repo-root", default=default, help="Repository root path")
    args, _ = p.parse_known_args()
    return Path(args.repo_root).resolve()


def paths_for_repo(repo_root: Path) -> dict[str, Path]:
    r = repo_root
    return {
        "seeds": r / "data" / "seeds",
        "cache": r / "data" / "cache",
        "candidates": r / "data" / "candidates",
        "reports_discovery": r / "reports" / "discovery",
        "reports_extraction": r / "reports" / "extraction",
        "reports_monitoring": r / "reports" / "monitoring",
        "seed_urls": r / "data" / "seeds" / "seed_urls.txt",
        "allow_domains": r / "data" / "seeds" / "allow_domains.txt",
        "deny_domains": r / "data" / "seeds" / "deny_domains.txt",
        "bootstrap_domains": r / "data" / "seeds" / "bootstrap_domains.txt",
        "discovered_domains": r / "data" / "candidates" / "discovered_domains.json",
        "discovered_pages": r / "data" / "candidates" / "discovered_pages.json",
        "discovered_redirects": r / "data" / "candidates" / "discovered_redirects.json",
        "extracted_signals": r / "data" / "candidates" / "extracted_signals.json",
        "proposed_entities": r / "data" / "candidates" / "proposed_entities.json",
        "proposed_relationships": r / "data" / "candidates" / "proposed_relationships.json",
    }


def ensure_dirs(repo_root: Path) -> None:
    p = paths_for_repo(repo_root)
    for key in ("cache", "candidates", "reports_discovery", "reports_extraction", "reports_monitoring"):
        p[key].mkdir(parents=True, exist_ok=True)
