"""Tiered routing: who runs when, under budget caps (no Intel writes)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from sweep_scout.research_schema import ResearchJob, ResearchResult

BudgetLevel = Literal["low", "medium", "high"]


@dataclass
class ResearchPolicy:
    budget: BudgetLevel
    max_perplexity: int
    max_claude: int
    max_gemini: int


def policy_from_budget(budget: str) -> ResearchPolicy:
    b = (budget or "low").lower().strip()
    if b not in ("low", "medium", "high"):
        b = "low"
    caps = {
        "low": (8, 5, 3),
        "medium": (25, 15, 8),
        "high": (80, 50, 25),
    }
    mx = caps[b]
    return ResearchPolicy(budget=b, max_perplexity=mx[0], max_claude=mx[1], max_gemini=mx[2])


def priority_score(job: ResearchJob, det: ResearchResult) -> float:
    """Higher = more deserving of expensive passes."""
    row = job.source_provenance or {}
    sh = (row.get("status_hint") or "").lower()
    ds = (row.get("discovery_source") or "").lower()
    score = 0.0
    if sh in ("coming_soon", "newly_listed"):
        score += 3.0
    if "existing_family" in sh or "family" in ds:
        score += 2.5
    if ds.count("|") >= 2:
        score += 1.0
    ent = str(det.entity_type_guess)
    if ent == "unresolved":
        score += 2.0
    if ent == "likely_redirect_or_rebrand":
        score += 2.5
    if ent == "likely_noise":
        score += 0.5
    if det.redirect_hints:
        score += 1.5
    pr = (det.raw_payload or {}).get("page_risk") or {}
    if float(pr.get("risk_score") or 0) >= 35:
        score += 1.0
    return score


def eligible_perplexity(job: ResearchJob, det: ResearchResult) -> bool:
    """Broad recall for first expensive pass."""
    row = job.source_provenance or {}
    sh = (row.get("status_hint") or "").lower()
    ent = str(det.entity_type_guess)
    if ent in ("unresolved", "likely_redirect_or_rebrand", "likely_noise"):
        return True
    if sh in ("coming_soon", "newly_listed", "new_social"):
        return True
    if "family" in sh or "sweepskings" in (row.get("discovery_source") or "").lower():
        return True
    if det.redirect_hints:
        return True
    return False


def eligible_claude(
    job: ResearchJob,
    det: ResearchResult,
    perplexity: ResearchResult | None,
) -> bool:
    """Second pass: rows where Perplexity ran meaningfully or classification matters."""
    if perplexity is None:
        return False
    if perplexity.raw_payload.get("provider_status") == "unavailable":
        return False
    if perplexity.status not in ("ok", "error"):
        return False
    ent_p = str(perplexity.entity_type_guess)
    ent_d = str(det.entity_type_guess)
    if ent_p == "unresolved" or ent_p != ent_d:
        return True
    conf = float(perplexity.confidence_hint or 0)
    if conf < 0.55:
        return True
    return False


def eligible_gemini(
    job: ResearchJob,
    perplexity: ResearchResult | None,
    claude: ResearchResult | None,
    det: ResearchResult,
) -> bool:
    """Narrow: disagreements, family suspicion, competing domains."""
    row = job.source_provenance or {}
    if "family" in (row.get("status_hint") or "").lower():
        return True
    if (perplexity and claude) and str(perplexity.entity_type_guess) != str(claude.entity_type_guess):
        if perplexity.status == "ok" and claude.status == "ok":
            return True
    hints = (det.family_hints or []) + (perplexity.family_hints if perplexity else [])
    if hints:
        return True
    if perplexity and claude:
        dp = set(perplexity.extracted_domains or [])
        dc = set(claude.extracted_domains or [])
        if dp and dc and dp != dc:
            return True
    return False


def rank_jobs_for_perplexity(
    jobs: list[ResearchJob],
    deterministic_by_id: dict[str, ResearchResult],
) -> list[tuple[float, ResearchJob, ResearchResult]]:
    ranked: list[tuple[float, ResearchJob, ResearchResult]] = []
    for job in jobs:
        det = deterministic_by_id.get(job.candidate_id)
        if not det:
            continue
        if not eligible_perplexity(job, det):
            continue
        ranked.append((priority_score(job, det), job, det))
    ranked.sort(key=lambda x: -x[0])
    return ranked
