"""Stable JSON-serializable shapes for the research mesh (staging only; not Sweeps_Intel truth)."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

ProviderType = Literal["deep_research", "sorter", "verifier", "deterministic"]
ConsensusState = Literal[
    "likely_real_operator",
    "likely_promoter",
    "likely_corporate",
    "likely_redirect_or_rebrand",
    "likely_noise",
    "unresolved",
]


@dataclass
class ResearchJob:
    candidate_id: str
    brand: str
    candidate_domains: list[str]
    source_provenance: dict[str, Any]
    membrane_hosts: list[str] = field(default_factory=list)
    status: str = "queued"


@dataclass
class ResearchResult:
    candidate_id: str
    brand: str
    provider_name: str
    provider_type: ProviderType
    status: str
    citations: list[str]
    extracted_domains: list[str]
    entity_type_guess: ConsensusState | str
    family_hints: list[str]
    redirect_hints: list[str]
    confidence_hint: float
    notes: str
    disagreement_flags: list[str]
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchConsensus:
    candidate_id: str
    brand: str
    source_provenance: dict[str, Any]
    membrane_hosts: list[str]
    consensus: ConsensusState
    agreed_findings: dict[str, Any]
    contested_findings: dict[str, Any]
    unresolved_findings: dict[str, Any]
    merged_citations: list[str]
    merged_extracted_domains: list[str]
    provider_views: dict[str, Any]
    disagreement_flags: list[str]
    # Hints for downstream browser / enforcement layers (not blocking here)
    page_risk_score: float | None = None
    risky_keywords: list[str] = field(default_factory=list)
    likely_signup_surface: str | None = None
    likely_cashier_surface: str | None = None
    safe_paste_trigger_hint: str | None = None


@dataclass
class DisagreementRecord:
    candidate_id: str
    brand: str
    topic: str
    values_by_provider: dict[str, str]
    notes: str


def result_to_dict(r: ResearchResult) -> dict[str, Any]:
    d = asdict(r)
    return d


def consensus_to_dict(c: ResearchConsensus) -> dict[str, Any]:
    return asdict(c)


def disagreement_to_dict(d: DisagreementRecord) -> dict[str, Any]:
    return asdict(d)


def job_to_dict(j: ResearchJob) -> dict[str, Any]:
    return asdict(j)
