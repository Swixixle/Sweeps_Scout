from __future__ import annotations

import json
from pathlib import Path

from sweep_scout.domain_membrane import generate_membrane_hosts
from sweep_scout.page_risk_signals import compute_page_risk
from sweep_scout.research_merge import merge_job
from sweep_scout.research_cache import ResearchCache
from sweep_scout.research_models import DeterministicLocalProvider, PerplexityDeepResearchProvider
from sweep_scout.research_policy import policy_from_budget
from sweep_scout.research_orchestrator import build_jobs, run_research_mesh
from sweep_scout.research_schema import ResearchJob, ResearchResult


def test_membrane_max_six_and_tlds():
    h = generate_membrane_hosts("Bang Coins", max_hosts=6)
    assert len(h) <= 6
    assert "bangcoins.com" in h
    assert "bangcoins.net" in h


def test_membrane_fortune_winz_variants():
    h = generate_membrane_hosts("Fortune Winz", max_hosts=6)
    assert any(x.startswith("fortunewinz.") for x in h)
    assert any(x.startswith("fortunewins.") for x in h)


def test_membrane_explicit_domain():
    h = generate_membrane_hosts("Play at Rake.us today", max_hosts=6)
    assert "rake.us" in h


def test_page_risk_keywords():
    body = b"<html>Sweepstakes social casino redeem gold coins card number</html>"
    r = compute_page_risk(body)
    assert r["risk_score"] > 0
    assert "sweepstakes" in r["keyword_hits"]


def test_merge_disagreement():
    job = ResearchJob(
        candidate_id="abc",
        brand="X",
        candidate_domains=["x.com"],
        source_provenance={},
        membrane_hosts=["x.com"],
    )
    r1 = ResearchResult(
        candidate_id="abc",
        brand="X",
        provider_name="deterministic_local",
        provider_type="deterministic",
        status="ok",
        citations=[],
        extracted_domains=["x.com"],
        entity_type_guess="likely_real_operator",
        family_hints=[],
        redirect_hints=[],
        confidence_hint=0.5,
        notes="",
        disagreement_flags=[],
    )
    r2 = ResearchResult(
        candidate_id="abc",
        brand="X",
        provider_name="claude_sorter",
        provider_type="sorter",
        status="ok",
        citations=[],
        extracted_domains=["y.com"],
        entity_type_guess="likely_promoter",
        family_hints=[],
        redirect_hints=[],
        confidence_hint=0.2,
        notes="claude view",
        disagreement_flags=[],
        raw_payload={"provider_status": "ok"},
    )
    cons, dis = merge_job(job, [r1, r2])
    assert cons.consensus == "unresolved"
    assert cons.contested_findings
    assert len(dis) >= 1


def test_orchestrator_graceful_stub_external(tmp_path: Path, monkeypatch):
    csv_in = tmp_path / "q.csv"
    csv_in.write_text(
        "brand,status_hint,discovery_source,verified_domain_candidate\n"
        "ZedTest,newly_listed,x,zedtest.com\n",
        encoding="utf-8",
    )

    def fake_verify(host, **kw):
        return {
            "primary_domain": host,
            "verification_status": "unreachable",
            "verification_score": 0.0,
            "verification_notes": "",
            "dns_ok": False,
            "http_status": None,
            "final_url": "",
            "keyword_hits": [],
            "policy_links_found": [],
        }

    monkeypatch.setattr("sweep_scout.research_models.verify_domain", fake_verify)
    out = tmp_path / "out"
    summary = run_research_mesh(
        input_csv=csv_in,
        out_dir=out,
        limit=1,
        deterministic_only=False,
        with_external=True,
        cache_dir=tmp_path / "cache",
    )
    assert summary["jobs"] == 1
    assert summary["result_rows"] == 2  # deterministic + perplexity (unavailable without API key)
    rj = (out / "research_results.json").read_text(encoding="utf-8")
    assert "PERPLEXITY_API_KEY" in rj or "unavailable" in rj


def test_consensus_output_shape(tmp_path: Path, monkeypatch):
    csv_in = tmp_path / "q.csv"
    csv_in.write_text(
        "brand,status_hint,discovery_source,verified_domain_candidate\n"
        "AbcCo,newly_listed,x,abcco.com\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sweep_scout.research_models.verify_domain",
        lambda host, **kw: {
            "primary_domain": host,
            "verification_status": "unreachable",
            "verification_score": 0.0,
            "verification_notes": "",
            "dns_ok": False,
            "http_status": None,
            "final_url": "",
            "keyword_hits": [],
            "policy_links_found": [],
        },
    )
    run_research_mesh(
        input_csv=csv_in,
        out_dir=tmp_path / "o",
        limit=1,
        deterministic_only=True,
        cache_dir=tmp_path / "c",
    )
    cons = json.loads((tmp_path / "o" / "research_consensus.json").read_text(encoding="utf-8"))
    assert len(cons) == 1
    assert "consensus" in cons[0]
    assert "agreed_findings" in cons[0]
    assert "provider_views" in cons[0]
    assert "page_risk_score" in cons[0]
    assert "safe_paste_trigger_hint" in cons[0]


def test_research_cache_roundtrip(tmp_path: Path):
    rc = ResearchCache(root=tmp_path / "rc")
    rc.set("cid1", "perplexity_deep_research", "BrandX", ["a.com"], {"fields": {"entity_type_guess": "unresolved"}})
    g = rc.get("cid1", "perplexity_deep_research", "BrandX", ["a.com"])
    assert g is not None
    assert g["payload"]["fields"]["entity_type_guess"] == "unresolved"


def test_policy_budget_caps():
    assert policy_from_budget("low").max_perplexity == 8
    assert policy_from_budget("high").max_gemini == 25


def test_perplexity_http_error_not_crash(monkeypatch, tmp_path: Path):
    job = ResearchJob(
        candidate_id="c1",
        brand="ErrBrand",
        candidate_domains=["e.com"],
        source_provenance={},
        membrane_hosts=["e.com"],
    )
    monkeypatch.setenv("PERPLEXITY_API_KEY", "test-key")
    monkeypatch.setattr(
        "sweep_scout.research_models.call_perplexity_chat",
        lambda prompt: (None, "perplexity_http_500:bad"),
    )
    r = PerplexityDeepResearchProvider().run(
        job,
        research_cache=ResearchCache(root=tmp_path / "rc"),
        prior_context={"deterministic_result": None},
    )
    assert r.status == "error"
    assert (r.raw_payload or {}).get("provider_status") == "error"


def test_deterministic_provider_no_network(monkeypatch, tmp_path: Path):
    job = ResearchJob(
        candidate_id="id1",
        brand="NoNet",
        candidate_domains=[],
        source_provenance={},
        membrane_hosts=["invalid.invalid"],
    )

    def fake_verify(host, **kw):
        return {
            "primary_domain": host,
            "verification_status": "unreachable",
            "verification_score": 0.0,
            "verification_notes": "",
            "dns_ok": False,
            "http_status": None,
            "final_url": "",
            "keyword_hits": [],
            "policy_links_found": [],
        }

    monkeypatch.setattr("sweep_scout.research_models.verify_domain", fake_verify)
    p = DeterministicLocalProvider()
    r = p.run(job, cache_dir=tmp_path / "cache")
    assert r.status == "ok"
    assert r.provider_name == "deterministic_local"
