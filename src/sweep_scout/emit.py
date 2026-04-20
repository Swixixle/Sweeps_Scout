from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sweep_scout.config import ensure_dirs, paths_for_repo, repo_root_from_args
from sweep_scout.utils import deterministic_json_dumps, normalize_host, utc_now_iso


def _candidate_id(domain: str) -> str:
    safe = normalize_host(domain).replace(".", "_")
    return f"candidate_{safe}"


def _evidence_snapshot(sig: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "title",
        "meta_description",
        "text_hits",
        "policy_links",
        "support_links",
        "provider_mentions",
        "rebrand_phrase_hits",
        "script_domains",
        "iframe_domains",
        "notes",
    )
    return {k: sig.get(k) for k in keys if k in sig}


def run_emit(repo_root: Path) -> dict:
    ensure_dirs(repo_root)
    paths = paths_for_repo(repo_root)
    ts = utc_now_iso()

    discovered: list[dict] = []
    if paths["discovered_domains"].is_file():
        discovered = json.loads(paths["discovered_domains"].read_text(encoding="utf-8"))

    signals: list[dict] = []
    if paths["extracted_signals"].is_file():
        signals = json.loads(paths["extracted_signals"].read_text(encoding="utf-8"))

    hints_path = paths["candidates"] / "classification_hints.json"
    hints: list[dict] = []
    if hints_path.is_file():
        hints = json.loads(hints_path.read_text(encoding="utf-8"))

    hint_by_domain = {h.get("domain"): h for h in hints if h.get("domain")}

    sig_by_domain = {}
    for s in signals:
        d = s.get("domain")
        if d:
            sig_by_domain[d] = s

    entities: list[dict] = []
    for drow in discovered:
        dom = drow.get("domain") or ""
        if not dom:
            continue
        hint = hint_by_domain.get(dom)
        sig = sig_by_domain.get(dom)
        likely = (hint or {}).get("label") or "unknown"
        conf = float((hint or {}).get("confidence_hint") or 0.0)
        reasoning = list((hint or {}).get("reasoning") or [])
        if not reasoning:
            reasoning = ["no classification hint; discovery-only record"]

        entities.append(
            {
                "candidate_id": _candidate_id(dom),
                "discovered_from": "sweeps_scout_pipeline",
                "source_urls": drow.get("source_urls") or [drow.get("source_url")],
                "normalized_domain": normalize_host(dom),
                "likely_type": likely,
                "confidence_hint": round(conf, 4),
                "reasoning": reasoning,
                "raw_evidence_snapshot": _evidence_snapshot(sig) if sig else {"note": "not extracted"},
            }
        )

    relationships: list[dict] = []
    for drow in discovered:
        dom = drow.get("domain") or ""
        for su in drow.get("source_urls") or []:
            p = urlparse(su)
            src_dom = p.hostname or ""
            if not src_dom or not dom:
                continue
            relationships.append(
                {
                    "relationship_id": f"rel_{normalize_host(src_dom)}__to__{normalize_host(dom)}",
                    "from_domain": normalize_host(src_dom),
                    "to_domain": normalize_host(dom),
                    "relationship_kind": "discovered_link",
                    "source_url": su,
                    "first_seen": ts,
                }
            )

    relationships = sorted(relationships, key=lambda r: r.get("relationship_id", ""))

    paths["proposed_entities"].write_text(deterministic_json_dumps(entities), encoding="utf-8")
    paths["proposed_relationships"].write_text(
        deterministic_json_dumps(relationships), encoding="utf-8"
    )

    report = {"run_at": ts, "entities": len(entities), "relationships": len(relationships)}
    return report


def main_cli() -> None:
    p = argparse.ArgumentParser(description="Emit candidate artifacts for Sweeps_Intel review")
    p.add_argument("--repo-root", default=".")
    args = p.parse_args()
    root = Path(args.repo_root).resolve()
    r = run_emit(root)
    print(deterministic_json_dumps(r))


if __name__ == "__main__":
    main_cli()
