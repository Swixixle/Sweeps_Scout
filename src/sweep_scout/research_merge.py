"""Merge multi-provider research outputs without collapsing disagreement."""
from __future__ import annotations

from typing import Any

from sweep_scout.research_schema import (
    DisagreementRecord,
    ResearchConsensus,
    ResearchJob,
    ResearchResult,
    consensus_to_dict,
)


def _entity_key(r: ResearchResult) -> str:
    return str(r.entity_type_guess)


def _meaningful_results(results: list[ResearchResult]) -> list[ResearchResult]:
    """Exclude unavailable/stub external providers from voting (deterministic always kept)."""
    out: list[ResearchResult] = []
    for r in results:
        if r.provider_name == "deterministic_local":
            out.append(r)
            continue
        st = (r.raw_payload or {}).get("provider_status")
        if st == "unavailable":
            continue
        if r.status == "stub":
            continue
        if (r.raw_payload or {}).get("stub"):
            continue
        out.append(r)
    return out


def _enrichment_from_deterministic(det: ResearchResult | None) -> dict[str, Any]:
    if not det:
        return {}
    pr = (det.raw_payload or {}).get("page_risk") or {}
    hits = list(pr.get("keyword_hits") or [])
    score = pr.get("risk_score")
    try:
        page_risk_score = float(score) if score is not None else None
    except (TypeError, ValueError):
        page_risk_score = None
    signup = None
    if "register" in hits and "login" in hits:
        signup = "elevated_register_login_language"
    cashier = None
    if any(x in hits for x in ("cashier", "deposit", "wallet", "card number")):
        cashier = "elevated_payment_cashier_language"
    hint_parts: list[str] = []
    if page_risk_score and page_risk_score >= 45:
        hint_parts.append("high_page_risk_score")
    if det.redirect_hints:
        hint_parts.append("redirect_observed")
    if "verify identity" in hits:
        hint_parts.append("identity_verification_language")
    return {
        "page_risk_score": page_risk_score,
        "risky_keywords": hits,
        "likely_signup_surface": signup,
        "likely_cashier_surface": cashier,
        "safe_paste_trigger_hint": "; ".join(hint_parts) if hint_parts else None,
    }


def merge_job(
    job: ResearchJob,
    results: list[ResearchResult],
    *,
    deterministic_result: ResearchResult | None = None,
) -> tuple[ResearchConsensus, list[DisagreementRecord]]:
    """
    Build consensus record + optional disagreement rows when entity labels or
    domain sets conflict across meaningful (non-stub) providers.
    """
    det = deterministic_result or next(
        (r for r in results if r.provider_name == "deterministic_local"),
        None,
    )
    meaningful = _meaningful_results(results)
    by_provider = {r.provider_name: r for r in results}
    provider_views: dict[str, Any] = {
        name: {
            "entity_type_guess": str(res.entity_type_guess),
            "status": res.status,
            "confidence_hint": res.confidence_hint,
            "extracted_domains": res.extracted_domains,
            "citations": res.citations,
            "notes": res.notes,
            "disagreement_flags": res.disagreement_flags,
            "provider_status": (res.raw_payload or {}).get("provider_status"),
        }
        for name, res in by_provider.items()
    }

    det_m = next((r for r in meaningful if r.provider_name == "deterministic_local"), None)
    ext_non_unres = [
        _entity_key(r)
        for r in meaningful
        if r.provider_type != "deterministic" and _entity_key(r) != "unresolved"
    ]
    all_entity_keys = [_entity_key(r) for r in meaningful]
    uniq_entities = sorted(set(all_entity_keys))

    agreed: dict[str, Any] = {}
    contested: dict[str, Any] = {}
    unresolved: dict[str, Any] = {}

    all_domains: list[str] = []
    for r in results:
        if (r.raw_payload or {}).get("stub"):
            continue
        all_domains.extend(r.extracted_domains)
    merged_domains = sorted(set(all_domains))

    all_cites: list[str] = []
    for r in results:
        if (r.raw_payload or {}).get("stub"):
            continue
        all_cites.extend(r.citations)
    merged_citations = sorted(set(all_cites))

    consensus_label = "unresolved"
    if ext_non_unres and len(set(ext_non_unres)) == 1:
        consensus_label = ext_non_unres[0]
    elif det_m and not ext_non_unres:
        consensus_label = str(det_m.entity_type_guess)
    elif det_m and ext_non_unres:
        consensus_label = "unresolved"
        contested["entity_type_guess"] = {r.provider_name: _entity_key(r) for r in meaningful}
    elif det_m:
        consensus_label = str(det_m.entity_type_guess)

    if len(uniq_entities) > 1:
        consensus_label = "unresolved"
        contested["entity_type_guess"] = {r.provider_name: _entity_key(r) for r in meaningful}

    agreed["extracted_domains"] = merged_domains
    if consensus_label != "unresolved" and not contested.get("entity_type_guess"):
        agreed["entity_type_guess"] = consensus_label
    else:
        unresolved["entity_type_candidates"] = uniq_entities
        unresolved["extracted_domains"] = merged_domains

    flags: list[str] = []
    disagreements: list[DisagreementRecord] = []

    if len(meaningful) >= 2 and len(set(_entity_key(r) for r in meaningful)) > 1:
        flags.append("entity_type_mismatch")
        disagreements.append(
            DisagreementRecord(
                candidate_id=job.candidate_id,
                brand=job.brand,
                topic="entity_type_guess",
                values_by_provider={r.provider_name: _entity_key(r) for r in meaningful},
                notes="Providers proposed different entity labels; keep staged.",
            )
        )

    dom_sets = [set(r.extracted_domains) for r in meaningful if r.extracted_domains]
    if len(dom_sets) >= 2:
        inter = set.intersection(*dom_sets) if dom_sets else set()
        union = set.union(*dom_sets) if dom_sets else set()
        if union - inter:
            flags.append("extracted_domains_differ")
            disagreements.append(
                DisagreementRecord(
                    candidate_id=job.candidate_id,
                    brand=job.brand,
                    topic="extracted_domains",
                    values_by_provider={
                        r.provider_name: ",".join(sorted(r.extracted_domains)) for r in meaningful
                    },
                    notes="Domain lists differ between providers.",
                )
            )

    enrich = _enrichment_from_deterministic(det)

    return (
        ResearchConsensus(
            candidate_id=job.candidate_id,
            brand=job.brand,
            source_provenance=job.source_provenance,
            membrane_hosts=list(job.membrane_hosts),
            consensus=consensus_label if consensus_label in (
                "likely_real_operator",
                "likely_promoter",
                "likely_corporate",
                "likely_redirect_or_rebrand",
                "likely_noise",
                "unresolved",
            ) else "unresolved",
            agreed_findings=agreed,
            contested_findings=contested,
            unresolved_findings=unresolved,
            merged_citations=merged_citations,
            merged_extracted_domains=merged_domains,
            provider_views=provider_views,
            disagreement_flags=sorted(set(flags)),
            page_risk_score=enrich.get("page_risk_score"),
            risky_keywords=list(enrich.get("risky_keywords") or []),
            likely_signup_surface=enrich.get("likely_signup_surface"),
            likely_cashier_surface=enrich.get("likely_cashier_surface"),
            safe_paste_trigger_hint=enrich.get("safe_paste_trigger_hint"),
        ),
        disagreements,
    )


def merge_all(
    jobs: list[ResearchJob],
    results_by_candidate: dict[str, list[ResearchResult]],
    *,
    deterministic_by_id: dict[str, ResearchResult] | None = None,
) -> tuple[list[ResearchConsensus], list[DisagreementRecord]]:
    consensus_list: list[ResearchConsensus] = []
    all_dis: list[DisagreementRecord] = []
    job_by_id = {j.candidate_id: j for j in jobs}
    det_map = deterministic_by_id or {}
    for cid, job in job_by_id.items():
        res = results_by_candidate.get(cid, [])
        if not res:
            continue
        det = det_map.get(cid) or next(
            (r for r in res if r.provider_name == "deterministic_local"),
            None,
        )
        cons, dis = merge_job(job, res, deterministic_result=det)
        consensus_list.append(cons)
        all_dis.extend(dis)
    consensus_list.sort(key=lambda c: (c.brand, c.candidate_id))
    return consensus_list, all_dis


def consensus_records_to_jsonable(consensus_list: list[ResearchConsensus]) -> list[dict[str, Any]]:
    return [consensus_to_dict(c) for c in consensus_list]
