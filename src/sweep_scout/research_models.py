"""Provider adapters: deterministic (real) + external APIs when keys + budget allow."""
from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from sweep_scout.fetch import fetch_url
from sweep_scout.page_risk_signals import compute_page_risk
from sweep_scout.research_cache import ResearchCache
from sweep_scout.research_providers_api import (
    build_user_prompt_claude,
    build_user_prompt_gemini,
    build_user_prompt_perplexity,
    call_anthropic_messages,
    call_gemini_generate,
    call_perplexity_chat,
    deterministic_summary_for_prompt,
    parsed_to_result_fields,
)
from sweep_scout.research_schema import ConsensusState, ResearchJob, ResearchResult
from sweep_scout.utils import repo_root, utc_now_iso
from sweep_scout.verify_candidates import verify_domain


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


class ResearchProvider(ABC):
    name: str
    provider_type: str

    @abstractmethod
    def run(
        self,
        job: ResearchJob,
        *,
        cache_dir: Path | None = None,
        research_cache: ResearchCache | None = None,
        refresh_cache: bool = False,
        prior_context: dict[str, Any] | None = None,
    ) -> ResearchResult:
        raise NotImplementedError


def _infer_entity_from_deterministic(
    checks: list[dict[str, Any]],
    page_risk: dict[str, Any],
) -> ConsensusState:
    statuses = [c.get("verification_status") for c in checks]
    if any(s == "redirected" for s in statuses):
        return "likely_redirect_or_rebrand"
    reachable = any(s == "reachable" for s in statuses)
    hits = page_risk.get("keyword_hits") or []
    if reachable and any(k in hits for k in ("sweeps", "sweepstakes", "gold coins", "sweeps coins")):
        return "likely_real_operator"
    if reachable and ("deposit" in hits or "cashier" in hits):
        return "likely_real_operator"
    if not any(s in ("reachable", "redirected", "unclear") for s in statuses):
        return "unresolved"
    if page_risk.get("risk_score", 0) >= 40 and not reachable:
        return "likely_noise"
    return "unresolved"


class DeterministicLocalProvider(ResearchProvider):
    name = "deterministic_local"
    provider_type = "deterministic"

    def run(
        self,
        job: ResearchJob,
        *,
        cache_dir: Path | None = None,
        research_cache: ResearchCache | None = None,
        refresh_cache: bool = False,
        prior_context: dict[str, Any] | None = None,
    ) -> ResearchResult:
        checks: list[dict[str, Any]] = []
        cache_dir = cache_dir or (repo_root() / "data" / "cache" / "research_deterministic")
        for host in job.membrane_hosts[:8]:
            vr = verify_domain(host, cache_dir=cache_dir)
            checks.append({"host": host, **vr})

        body_for_risk: bytes | None = None
        for c in checks:
            st = c.get("verification_status")
            if st in ("reachable", "redirected", "unclear"):
                fu = c.get("final_url") or ""
                if fu:
                    fr = fetch_url(
                        fu if fu.startswith("http") else f"https://{c.get('host')}/",
                        timeout=18.0,
                        cache_dir=cache_dir,
                        retries=0,
                    )
                    if fr.body:
                        body_for_risk = fr.body
                        break
                elif c.get("host"):
                    fr = fetch_url(f"https://{c['host']}/", timeout=18.0, cache_dir=cache_dir, retries=0)
                    if fr.body:
                        body_for_risk = fr.body
                        break

        page_risk = compute_page_risk(body_for_risk)
        entity = _infer_entity_from_deterministic(checks, page_risk)
        extracted = [c["host"] for c in checks if c.get("dns_ok")]
        flags: list[str] = []
        if len({c.get("verification_status") for c in checks}) > 1:
            flags.append("mixed_verification_status_across_hosts")

        return ResearchResult(
            candidate_id=job.candidate_id,
            brand=job.brand,
            provider_name=self.name,
            provider_type="deterministic",
            status="ok",
            citations=[],
            extracted_domains=extracted,
            entity_type_guess=entity,
            family_hints=[],
            redirect_hints=[
                str(c.get("final_url"))
                for c in checks
                if c.get("verification_status") == "redirected" and c.get("final_url")
            ],
            confidence_hint=0.55 if entity != "unresolved" else 0.35,
            notes="deterministic DNS/HTTP/policy/keyword pass; heuristic entity label only",
            disagreement_flags=flags,
            raw_payload={
                "deterministic_checks": checks,
                "page_risk": page_risk,
                "provider_status": "ok",
                "generated_at": utc_now_iso(),
            },
        )


def _result_from_api(
    job: ResearchJob,
    provider_name: str,
    provider_type: str,
    fields: dict[str, Any],
    *,
    status: str,
    notes: str,
    raw_extra: dict[str, Any],
) -> ResearchResult:
    return ResearchResult(
        candidate_id=job.candidate_id,
        brand=job.brand,
        provider_name=provider_name,
        provider_type=provider_type,
        status=status,
        citations=fields.get("citations") or [],
        extracted_domains=fields.get("extracted_domains") or [],
        entity_type_guess=fields.get("entity_type_guess") or "unresolved",
        family_hints=fields.get("family_hints") or [],
        redirect_hints=fields.get("redirect_hints") or [],
        confidence_hint=float(fields.get("confidence_hint") or 0.45),
        notes=notes[:2000],
        disagreement_flags=[],
        raw_payload={**raw_extra, "provider_status": raw_extra.get("provider_status", "ok")},
    )


class PerplexityDeepResearchProvider(ResearchProvider):
    name = "perplexity_deep_research"
    provider_type = "deep_research"

    def run(
        self,
        job: ResearchJob,
        *,
        cache_dir: Path | None = None,
        research_cache: ResearchCache | None = None,
        refresh_cache: bool = False,
        prior_context: dict[str, Any] | None = None,
    ) -> ResearchResult:
        if not _env("PERPLEXITY_API_KEY"):
            return _result_from_api(
                job,
                self.name,
                "deep_research",
                {"entity_type_guess": "unresolved", "confidence_hint": 0.0},
                status="unavailable",
                notes="PERPLEXITY_API_KEY not set",
                raw_extra={"provider_status": "unavailable"},
            )
        rc = research_cache
        if rc and not refresh_cache:
            cached = rc.get(job.candidate_id, self.name, job.brand, job.candidate_domains)
            if cached and cached.get("payload"):
                p = cached["payload"]
                fields = p.get("fields") or {}
                return _result_from_api(
                    job,
                    self.name,
                    "deep_research",
                    fields,
                    status="ok",
                    notes=str(fields.get("notes") or "cached"),
                    raw_extra={**p.get("raw", {}), "cached": True, "provider_status": "ok"},
                )

        det = (prior_context or {}).get("deterministic_result")
        if det is None:
            det_summary = "(deterministic not passed)"
        else:
            det_summary = deterministic_summary_for_prompt(det)
        prompt = build_user_prompt_perplexity(
            job.brand,
            job.candidate_domains,
            job.membrane_hosts,
            det_summary,
        )
        parsed, err = call_perplexity_chat(prompt)
        if err and parsed is None:
            return _result_from_api(
                job,
                self.name,
                "deep_research",
                {"entity_type_guess": "unresolved"},
                status="error",
                notes=err[:1500],
                raw_extra={"provider_status": "error", "error": err[:2000]},
            )
        if not parsed:
            return _result_from_api(
                job,
                self.name,
                "deep_research",
                {"entity_type_guess": "unresolved"},
                status="error",
                notes="empty_parse",
                raw_extra={"provider_status": "error"},
            )
        fields = parsed_to_result_fields(parsed)
        raw_extra = {"model_json": parsed, "provider_status": "ok"}
        if rc:
            rc.set(
                job.candidate_id,
                self.name,
                job.brand,
                job.candidate_domains,
                {"fields": fields, "raw": raw_extra},
            )
        return _result_from_api(
            job,
            self.name,
            "deep_research",
            fields,
            status="ok",
            notes=fields.get("notes") or "",
            raw_extra=raw_extra,
        )


class ClaudeSorterProvider(ResearchProvider):
    name = "claude_sorter"
    provider_type = "sorter"

    def run(
        self,
        job: ResearchJob,
        *,
        cache_dir: Path | None = None,
        research_cache: ResearchCache | None = None,
        refresh_cache: bool = False,
        prior_context: dict[str, Any] | None = None,
    ) -> ResearchResult:
        if not _env("ANTHROPIC_API_KEY"):
            return _result_from_api(
                job,
                self.name,
                "sorter",
                {"entity_type_guess": "unresolved"},
                status="unavailable",
                notes="ANTHROPIC_API_KEY not set",
                raw_extra={"provider_status": "unavailable"},
            )
        rc = research_cache
        if rc and not refresh_cache:
            cached = rc.get(job.candidate_id, self.name, job.brand, job.candidate_domains)
            if cached and cached.get("payload"):
                p = cached["payload"]
                fields = p.get("fields") or {}
                return _result_from_api(
                    job,
                    self.name,
                    "sorter",
                    fields,
                    status="ok",
                    notes=str(fields.get("notes") or "cached"),
                    raw_extra={**p.get("raw", {}), "cached": True, "provider_status": "ok"},
                )

        det = (prior_context or {}).get("deterministic_result")
        p_res = (prior_context or {}).get("perplexity_result")
        det_summary = deterministic_summary_for_prompt(det) if det else ""
        p_json = json.dumps(result_to_minimal_dict(p_res), ensure_ascii=True) if p_res else "{}"
        prompt = build_user_prompt_claude(job.brand, job.candidate_domains, det_summary, p_json)
        parsed, err = call_anthropic_messages(prompt)
        if err is None and parsed is None:
            return _result_from_api(
                job,
                self.name,
                "sorter",
                {"entity_type_guess": "unresolved"},
                status="unavailable",
                notes="anthropic empty",
                raw_extra={"provider_status": "unavailable"},
            )
        if err and parsed is None:
            return _result_from_api(
                job,
                self.name,
                "sorter",
                {"entity_type_guess": "unresolved"},
                status="error",
                notes=err[:1500],
                raw_extra={"provider_status": "error", "error": err[:2000]},
            )
        if not parsed:
            return _result_from_api(
                job,
                self.name,
                "sorter",
                {"entity_type_guess": "unresolved"},
                status="error",
                notes="empty_parse",
                raw_extra={"provider_status": "error"},
            )
        fields = parsed_to_result_fields(parsed)
        fields["agrees_with_perplexity"] = parsed.get("agrees_with_perplexity")
        fields["weak_evidence_flags"] = parsed.get("weak_evidence_flags") or []
        raw_extra = {"model_json": parsed, "provider_status": "ok"}
        if rc:
            rc.set(
                job.candidate_id,
                self.name,
                job.brand,
                job.candidate_domains,
                {"fields": fields, "raw": raw_extra},
            )
        return _result_from_api(
            job,
            self.name,
            "sorter",
            fields,
            status="ok",
            notes=fields.get("notes") or "",
            raw_extra=raw_extra,
        )


class GeminiVerifierProvider(ResearchProvider):
    name = "gemini_verifier"
    provider_type = "verifier"

    def run(
        self,
        job: ResearchJob,
        *,
        cache_dir: Path | None = None,
        research_cache: ResearchCache | None = None,
        refresh_cache: bool = False,
        prior_context: dict[str, Any] | None = None,
    ) -> ResearchResult:
        if not _env("GEMINI_API_KEY") and not _env("GOOGLE_API_KEY"):
            return _result_from_api(
                job,
                self.name,
                "verifier",
                {"entity_type_guess": "unresolved"},
                status="unavailable",
                notes="GEMINI_API_KEY/GOOGLE_API_KEY not set",
                raw_extra={"provider_status": "unavailable"},
            )
        rc = research_cache
        if rc and not refresh_cache:
            cached = rc.get(job.candidate_id, self.name, job.brand, job.candidate_domains)
            if cached and cached.get("payload"):
                p = cached["payload"]
                fields = p.get("fields") or {}
                return _result_from_api(
                    job,
                    self.name,
                    "verifier",
                    fields,
                    status="ok",
                    notes=str(fields.get("notes") or "cached"),
                    raw_extra={**p.get("raw", {}), "cached": True, "provider_status": "ok"},
                )

        det = (prior_context or {}).get("deterministic_result")
        p_res = (prior_context or {}).get("perplexity_result")
        c_res = (prior_context or {}).get("claude_result")
        det_summary = deterministic_summary_for_prompt(det) if det else ""
        prompt = build_user_prompt_gemini(
            job.brand,
            job.candidate_domains,
            det_summary,
            json.dumps(result_to_minimal_dict(p_res), ensure_ascii=True) if p_res else "{}",
            json.dumps(result_to_minimal_dict(c_res), ensure_ascii=True) if c_res else "{}",
        )
        parsed, err = call_gemini_generate(prompt)
        if err is None and parsed is None:
            return _result_from_api(
                job,
                self.name,
                "verifier",
                {"entity_type_guess": "unresolved"},
                status="unavailable",
                notes="gemini empty",
                raw_extra={"provider_status": "unavailable"},
            )
        if err and parsed is None:
            return _result_from_api(
                job,
                self.name,
                "verifier",
                {"entity_type_guess": "unresolved"},
                status="error",
                notes=err[:1500],
                raw_extra={"provider_status": "error", "error": err[:2000]},
            )
        if not parsed:
            return _result_from_api(
                job,
                self.name,
                "verifier",
                {"entity_type_guess": "unresolved"},
                status="error",
                notes="empty_parse",
                raw_extra={"provider_status": "error"},
            )
        fields = parsed_to_result_fields(parsed)
        if parsed.get("footer_or_company_phrases"):
            fields["family_hints"] = list(fields.get("family_hints") or []) + [
                str(x) for x in (parsed.get("footer_or_company_phrases") or [])[:10]
            ]
        extra_notes = parsed.get("same_backend_or_legal_language_hint")
        notes = fields.get("notes") or ""
        if extra_notes:
            notes = f"{notes} | {extra_notes}"[:2000]
        raw_extra = {"model_json": parsed, "provider_status": "ok"}
        if rc:
            rc.set(
                job.candidate_id,
                self.name,
                job.brand,
                job.candidate_domains,
                {"fields": fields, "raw": raw_extra},
            )
        return _result_from_api(
            job,
            self.name,
            "verifier",
            fields,
            status="ok",
            notes=notes,
            raw_extra=raw_extra,
        )


def result_to_minimal_dict(r: ResearchResult | None) -> dict[str, Any]:
    if r is None:
        return {}
    return {
        "entity_type_guess": str(r.entity_type_guess),
        "extracted_domains": r.extracted_domains,
        "citations": r.citations[:10],
        "notes": r.notes[:500],
        "confidence_hint": r.confidence_hint,
    }


PROVIDER_REGISTRY: dict[str, type[ResearchProvider]] = {
    "perplexity": PerplexityDeepResearchProvider,
    "claude": ClaudeSorterProvider,
    "gemini": GeminiVerifierProvider,
    "deterministic": DeterministicLocalProvider,
}


def get_provider(name: str) -> ResearchProvider:
    key = name.lower().strip()
    cls = PROVIDER_REGISTRY.get(key)
    if not cls:
        raise KeyError(f"unknown provider: {name}")
    return cls()
