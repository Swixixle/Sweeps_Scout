from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from sweep_scout.config import ensure_dirs, paths_for_repo, repo_root_from_args
from sweep_scout.constants import (
    PAYMENT_PATH_HINTS,
    PROMOTER_HINTS,
    PROVIDER_HINTS,
    SWEEPS_LANGUAGE,
)
from sweep_scout.utils import deterministic_json_dumps, domain_from_url, utc_now_iso


def _score_operator(title: str, meta: str, text_hits: list[str], policy_links: list[str]) -> float:
    s = 0.0
    blob = f"{title} {meta}".lower()
    if any(k in blob for k in ("social casino", "sweepstakes", "slots", "casino")):
        s += 0.25
    if any(k in blob for k in ("gold coins", "sweeps", "redeem")):
        s += 0.2
    for t in text_hits:
        if t.lower() in SWEEPS_LANGUAGE:
            s += 0.06
    if policy_links:
        s += 0.12
    return min(s, 1.0)


def _score_promoter(title: str, meta: str, support_links: list[str], policy_links: list[str]) -> float:
    s = 0.0
    blob = f"{title} {meta}".lower()
    for ph in PROMOTER_HINTS:
        if ph.lower() in blob:
            s += 0.2
    if len(support_links) > 8:
        s += 0.1
    if len(policy_links) < 2 and ("review" in blob or "compare" in blob or "best" in blob):
        s += 0.15
    return min(s, 1.0)


def _score_provider(provider_mentions: list[str]) -> float:
    if not provider_mentions:
        return 0.0
    return min(0.35 + 0.1 * min(len(provider_mentions), 4), 1.0)


def _score_payment(text_hits: list[str], title: str, meta: str) -> float:
    blob = f"{title} {meta}".lower()
    s = 0.0
    for t in text_hits:
        if t.lower() in PAYMENT_PATH_HINTS:
            s += 0.12
    for ph in PAYMENT_PATH_HINTS:
        if ph.lower() in blob:
            s += 0.1
    return min(s, 1.0)


def classify_signal(row: dict[str, Any]) -> dict[str, Any]:
    title = row.get("title") or ""
    meta = row.get("meta_description") or ""
    text_hits = list(row.get("text_hits") or [])
    policy_links = list(row.get("policy_links") or [])
    support_links = list(row.get("support_links") or [])
    provider_mentions = list(row.get("provider_mentions") or [])

    scores = {
        "likely_operator": _score_operator(title, meta, text_hits, policy_links),
        "likely_promoter": _score_promoter(title, meta, support_links, policy_links),
        "likely_provider": _score_provider(provider_mentions),
        "likely_payment_path": _score_payment(text_hits, title, meta),
    }

    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    best_label, best_score = ranked[0]
    if best_score < 0.15:
        label = "unknown"
        confidence = 0.1
        reasoning = ["insufficient keyword signals"]
    else:
        if best_label == "likely_provider" and scores["likely_operator"] > 0.35:
            label = "likely_operator"
            confidence = scores["likely_operator"]
            reasoning = [
                "operator signals outweighed provider-only mentions",
                f"likely_operator score={scores['likely_operator']:.2f}",
            ]
        else:
            label = best_label
            confidence = best_score
            reasoning = [f"highest rule score for {best_label} ({best_score:.2f})"]
            if scores["likely_operator"] > 0.2:
                reasoning.append(f"likely_operator={scores['likely_operator']:.2f}")
            if scores["likely_promoter"] > 0.2:
                reasoning.append(f"likely_promoter={scores['likely_promoter']:.2f}")
            if provider_mentions:
                reasoning.append(f"provider mentions: {', '.join(provider_mentions[:6])}")

    return {
        "domain": row.get("domain") or domain_from_url(str(row.get("final_url") or row.get("url"))),
        "url": row.get("final_url") or row.get("url"),
        "label": label,
        "confidence_hint": round(min(confidence, 1.0), 4),
        "scores": {k: round(v, 4) for k, v in scores.items()},
        "reasoning": reasoning,
    }


def run_classifier(repo_root: Path) -> dict:
    ensure_dirs(repo_root)
    paths = paths_for_repo(repo_root)
    ts = utc_now_iso()
    if not paths["extracted_signals"].is_file():
        paths["extracted_signals"].write_text("[]\n", encoding="utf-8")

    rows = json.loads(paths["extracted_signals"].read_text(encoding="utf-8"))
    out: list[dict] = []
    for row in rows:
        out.append(classify_signal(row))

    out_path = paths["candidates"] / "classification_hints.json"
    out_path.write_text(deterministic_json_dumps(out), encoding="utf-8")
    report = {"run_at": ts, "classified": len(out)}
    return report


def main_cli() -> None:
    p = argparse.ArgumentParser(description="Rule-based classifier hints")
    p.add_argument("--repo-root", default=".")
    args = p.parse_args()
    root = Path(args.repo_root).resolve()
    r = run_classifier(root)
    print(deterministic_json_dumps(r))


if __name__ == "__main__":
    main_cli()
