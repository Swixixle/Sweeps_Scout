"""Tiered research mesh: deterministic → Perplexity → Claude → Gemini → merge (staging only)."""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

from sweep_scout.domain_membrane import generate_membrane_hosts
from sweep_scout.research_cache import DEFAULT_PROMPT_VERSION, ResearchCache, cache_root_default
from sweep_scout.research_merge import merge_all
from sweep_scout.research_models import (
    ClaudeSorterProvider,
    DeterministicLocalProvider,
    GeminiVerifierProvider,
    PerplexityDeepResearchProvider,
)
from sweep_scout.research_policy import (
    eligible_claude,
    eligible_gemini,
    policy_from_budget,
    priority_score,
    rank_jobs_for_perplexity,
)
from sweep_scout.research_schema import (
    ResearchJob,
    ResearchResult,
    consensus_to_dict,
    disagreement_to_dict,
    job_to_dict,
    result_to_dict,
)
from sweep_scout.utils import deterministic_json_dumps, repo_root, sha256_text


def _candidate_id(brand: str, discovery: str) -> str:
    return sha256_text(f"{brand.strip()}|{discovery.strip()}")[:16]


def _resolve_input_path(p: Path) -> Path:
    if p.is_file():
        return p
    root = repo_root()
    cand = root / "data" / "candidates" / p.name
    if cand.is_file():
        return cand
    if p.name == "verification_queue.csv":
        alt = root / "data" / "candidates" / "web_new_verification_queue.csv"
        if alt.is_file():
            return alt
    return root / p


def load_queue_rows(csv_path: Path, *, limit: int | None) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with csv_path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append({k: (v or "").strip() for k, v in row.items()})
            if limit is not None and len(rows) >= limit:
                break
    return rows


def build_jobs(rows: list[dict[str, str]]) -> list[ResearchJob]:
    jobs: list[ResearchJob] = []
    for row in rows:
        brand = row.get("brand") or row.get("Brand") or ""
        if not brand:
            continue
        disc = row.get("discovery_source") or ""
        cid = _candidate_id(brand, disc)
        cand_dom = row.get("verified_domain_candidate") or row.get("primary_domain") or ""
        doms = [d.strip() for d in cand_dom.split("|") if d.strip()]
        membrane = generate_membrane_hosts(brand, max_hosts=6)
        prov: dict[str, Any] = dict(row)
        prov["input_csv_row"] = True
        jobs.append(
            ResearchJob(
                candidate_id=cid,
                brand=brand,
                candidate_domains=doms,
                source_provenance=prov,
                membrane_hosts=membrane,
                status="ready",
            )
        )
    jobs.sort(key=lambda j: (j.brand, j.candidate_id))
    return jobs


def _stats_from_external_results(results: list[ResearchResult]) -> dict[str, Any]:
    by: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in results:
        if r.provider_name == "deterministic_local":
            continue
        pn = r.provider_name
        rp = r.raw_payload or {}
        if rp.get("cached"):
            by[pn]["cache_hits"] += 1
        elif rp.get("provider_status") == "unavailable":
            by[pn]["unavailable"] += 1
        elif r.status == "error" or rp.get("provider_status") == "error":
            by[pn]["errors"] += 1
        else:
            by[pn]["live_calls"] += 1
    return {k: dict(v) for k, v in sorted(by.items())}


def _consensus_label_counts(consensus_list: list[Any]) -> dict[str, int]:
    from collections import Counter

    return dict(Counter(getattr(c, "consensus", None) or c.get("consensus") for c in consensus_list))


def run_research_mesh(
    *,
    input_csv: Path,
    out_dir: Path,
    limit: int | None = None,
    deterministic_only: bool = False,
    with_external: bool = False,
    providers: list[str] | None = None,
    cache_dir: Path | None = None,
    budget: str = "low",
    refresh_cache: bool = False,
) -> dict[str, Any]:
    """
    Staged execution: deterministic on all rows; external providers only when
    ``with_external`` or explicit ``providers`` (non-deterministic), subject to caps.
    """
    csv_path = _resolve_input_path(input_csv)
    if not csv_path.is_file():
        raise FileNotFoundError(f"queue CSV not found: {csv_path}")

    policy = policy_from_budget(budget)
    rows = load_queue_rows(csv_path, limit=limit)
    jobs = build_jobs(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    fetch_cache = cache_dir or (repo_root() / "data" / "cache" / "research_mesh")
    research_cache = ResearchCache(prompt_version=DEFAULT_PROMPT_VERSION)

    job_by_id = {j.candidate_id: j for j in jobs}
    results_by_cid: dict[str, list[ResearchResult]] = {j.candidate_id: [] for j in jobs}
    all_flat: list[ResearchResult] = []

    det_prov = DeterministicLocalProvider()
    deterministic_by_id: dict[str, ResearchResult] = {}
    for job in jobs:
        r = det_prov.run(
            job,
            cache_dir=fetch_cache,
            research_cache=research_cache,
            refresh_cache=False,
        )
        deterministic_by_id[job.candidate_id] = r
        results_by_cid[job.candidate_id].append(r)
        all_flat.append(r)

    run_external = bool(
        with_external or (providers and any(p.lower().strip() != "deterministic" for p in providers))
    )
    external_set = {"perplexity", "claude", "gemini"}
    if providers:
        want = {p.lower().strip() for p in providers} & external_set
    elif with_external:
        want = set(external_set)
    else:
        want = set()

    perplexity_results: dict[str, ResearchResult] = {}
    claude_results: dict[str, ResearchResult] = {}
    gemini_results: dict[str, ResearchResult] = {}

    if not deterministic_only and run_external and want:
        if "perplexity" in want:
            ranked = rank_jobs_for_perplexity(jobs, deterministic_by_id)
            pplx = PerplexityDeepResearchProvider()
            for _sc, job, det in ranked[: policy.max_perplexity]:
                ctx = {"deterministic_result": det}
                r = pplx.run(
                    job,
                    cache_dir=fetch_cache,
                    research_cache=research_cache,
                    refresh_cache=refresh_cache,
                    prior_context=ctx,
                )
                perplexity_results[job.candidate_id] = r
                results_by_cid[job.candidate_id].append(r)
                all_flat.append(r)

        if "claude" in want and perplexity_results:
            claude_candidates: list[tuple[float, ResearchJob, ResearchResult, ResearchResult]] = []
            for cid, pr in perplexity_results.items():
                job = job_by_id[cid]
                det = deterministic_by_id[cid]
                if eligible_claude(job, det, pr):
                    claude_candidates.append((priority_score(job, det), job, det, pr))
            claude_candidates.sort(key=lambda x: -x[0])
            cl = ClaudeSorterProvider()
            for _sc, job, det, pr in claude_candidates[: policy.max_claude]:
                ctx = {"deterministic_result": det, "perplexity_result": pr}
                r = cl.run(
                    job,
                    cache_dir=fetch_cache,
                    research_cache=research_cache,
                    refresh_cache=refresh_cache,
                    prior_context=ctx,
                )
                claude_results[job.candidate_id] = r
                results_by_cid[job.candidate_id].append(r)
                all_flat.append(r)

        if "gemini" in want:
            gem_candidates: list[tuple[float, ResearchJob, ResearchResult, ResearchResult | None, ResearchResult | None]] = []
            for job in jobs:
                cid = job.candidate_id
                det = deterministic_by_id[cid]
                pr = perplexity_results.get(cid)
                cr = claude_results.get(cid)
                if eligible_gemini(job, pr, cr, det):
                    pr2 = pr or ResearchResult(
                        candidate_id=cid,
                        brand=job.brand,
                        provider_name="perplexity_deep_research",
                        provider_type="deep_research",
                        status="unavailable",
                        citations=[],
                        extracted_domains=[],
                        entity_type_guess="unresolved",
                        family_hints=[],
                        redirect_hints=[],
                        confidence_hint=0.0,
                        notes="",
                        disagreement_flags=[],
                        raw_payload={"provider_status": "unavailable"},
                    )
                    gem_candidates.append((priority_score(job, det), job, det, pr2, cr))
            gem_candidates.sort(key=lambda x: -x[0])
            gm = GeminiVerifierProvider()
            for _sc, job, det, pr, cr in gem_candidates[: policy.max_gemini]:
                ctx = {
                    "deterministic_result": det,
                    "perplexity_result": pr,
                    "claude_result": cr,
                }
                r = gm.run(
                    job,
                    cache_dir=fetch_cache,
                    research_cache=research_cache,
                    refresh_cache=refresh_cache,
                    prior_context=ctx,
                )
                gemini_results[job.candidate_id] = r
                results_by_cid[job.candidate_id].append(r)
                all_flat.append(r)

    consensus_list, disagreements = merge_all(
        jobs,
        results_by_cid,
        deterministic_by_id=deterministic_by_id,
    )

    membrane_payload = [
        {
            "candidate_id": j.candidate_id,
            "brand": j.brand,
            "membrane_hosts": j.membrane_hosts,
            "source_provenance": {"discovery_source": j.source_provenance.get("discovery_source", "")},
        }
        for j in jobs
    ]

    out_jobs = out_dir / "research_jobs.json"
    out_results = out_dir / "research_results.json"
    out_consensus = out_dir / "research_consensus.json"
    out_dis = out_dir / "disagreement_queue.json"
    out_membrane = out_dir / "membrane_candidates.json"

    out_jobs.write_text(
        deterministic_json_dumps([job_to_dict(j) for j in jobs]),
        encoding="utf-8",
    )
    out_results.write_text(
        deterministic_json_dumps([result_to_dict(r) for r in all_flat]),
        encoding="utf-8",
    )
    out_consensus.write_text(
        deterministic_json_dumps([consensus_to_dict(c) for c in consensus_list]),
        encoding="utf-8",
    )
    out_dis.write_text(
        deterministic_json_dumps([disagreement_to_dict(d) for d in disagreements]),
        encoding="utf-8",
    )
    out_membrane.write_text(deterministic_json_dumps(membrane_payload), encoding="utf-8")

    label_counts = _consensus_label_counts(consensus_list)
    ext_stats = _stats_from_external_results(all_flat)

    return {
        "jobs": len(jobs),
        "result_rows": len(all_flat),
        "consensus_rows": len(consensus_list),
        "disagreement_rows": len(disagreements),
        "consensus_label_counts": label_counts,
        "provider_stats": ext_stats,
        "deterministic_runs": len(jobs),
        "research_cache_dir": str(cache_root_default()),
        "budget": policy.budget,
        "policy_caps": {
            "max_perplexity": policy.max_perplexity,
            "max_claude": policy.max_claude,
            "max_gemini": policy.max_gemini,
        },
        "outputs": {
            "research_jobs": str(out_jobs),
            "research_results": str(out_results),
            "research_consensus": str(out_consensus),
            "disagreement_queue": str(out_dis),
            "membrane_candidates": str(out_membrane),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Tiered research mesh orchestrator (staging only).")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("verification_queue.csv"),
        help="CSV path or filename under data/candidates/",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--deterministic-only", action="store_true")
    parser.add_argument("--with-external", action="store_true")
    parser.add_argument(
        "--budget",
        choices=("low", "medium", "high"),
        default="low",
        help="Caps for external provider rows per stage",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Ignore provider JSON cache reads (still writes new results)",
    )
    parser.add_argument(
        "--provider",
        action="append",
        dest="providers",
        metavar="NAME",
        help="perplexity | claude | gemini (repeatable). Implies external tier for listed providers.",
    )
    args = parser.parse_args()

    out_dir = args.out_dir or (repo_root() / "data" / "candidates")
    summary = run_research_mesh(
        input_csv=args.input,
        out_dir=out_dir,
        limit=args.limit,
        deterministic_only=args.deterministic_only,
        with_external=args.with_external,
        providers=args.providers,
        budget=args.budget,
        refresh_cache=args.refresh_cache,
    )
    print(deterministic_json_dumps(summary))


if __name__ == "__main__":
    main()
