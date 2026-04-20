from __future__ import annotations

import json

from sweep_scout.verify_web_candidates import (
    _queue_priority,
    build_redirect_hints_json,
    host_matches_known_family,
    pick_best_guess,
    run_verify_web,
)
from sweep_scout.web_candidate_domains import extract_embedded_domain, fold_brand_slug, guess_domain_hosts


def test_extract_embedded_acebet_rake_score():
    assert extract_embedded_domain("Acebet.cc") == "acebet.cc"
    assert extract_embedded_domain("Rake.us") == "rake.us"
    assert extract_embedded_domain("Score.us") == "score.us"


def test_guess_plain_brand_conservative():
    g = guess_domain_hosts("Dorados", max_guesses=4)
    assert "dorados.com" in g
    assert len(g) <= 4


def test_guess_bang_coins_compact_slug():
    g = guess_domain_hosts("Bang Coins", max_guesses=4)
    assert fold_brand_slug("Bang Coins") == "bangcoins"
    assert "bangcoins.com" in g
    assert "bangcoins.net" in g


def test_guess_cider_casino_extra_casino_host():
    g = guess_domain_hosts("Cider Casino", max_guesses=4)
    assert "cidercasino.com" in g
    assert "cidercasino.net" in g
    assert any(h.endswith("casino.com") for h in g)


def test_pick_best_prefers_reachable():
    a = {
        "guess_host": "a.com",
        "verification_status": "unreachable",
        "verification_score": 0.9,
        "keyword_hits": [],
        "policy_links_found": [],
        "final_url": "",
    }
    b = {
        "guess_host": "b.com",
        "verification_status": "reachable",
        "verification_score": 0.3,
        "keyword_hits": ["sweeps"],
        "policy_links_found": ["https://x.com/terms"],
        "final_url": "https://b.com/",
    }
    assert pick_best_guess("Zed", [a, b])["verification_status"] == "reachable"


def test_pick_best_prefers_embedded_domain_in_brand():
    hi = {
        "guess_host": "other.com",
        "verification_status": "reachable",
        "verification_score": 0.9,
        "keyword_hits": [],
        "policy_links_found": [],
        "final_url": "https://other.com/",
    }
    emb = {
        "guess_host": "acebet.cc",
        "verification_status": "reachable",
        "verification_score": 0.2,
        "keyword_hits": [],
        "policy_links_found": [],
        "final_url": "https://acebet.cc/",
    }
    assert pick_best_guess("Acebet.cc", [hi, emb])["guess_host"] == "acebet.cc"


def test_queue_priority_orders_reachable_higher():
    s_hi, _ = _queue_priority(
        "Dorados",
        "newly_listed",
        "a|b|c",
        {
            "verification_status": "reachable",
            "verification_score": 0.5,
            "keyword_hits": ["sweeps", "casino"],
            "policy_links_found": ["https://x.com/terms"],
            "redirect_target_if_any": "",
            "redirects_to_known_operator_family": False,
        },
    )
    s_lo, _ = _queue_priority(
        "Dorados",
        "newly_listed",
        "a",
        {
            "verification_status": "unreachable",
            "verification_score": 0.0,
            "keyword_hits": [],
            "policy_links_found": [],
            "redirect_target_if_any": "",
            "redirects_to_known_operator_family": False,
        },
    )
    assert s_hi > s_lo


def test_redirect_hints_include_review_notes():
    hints = build_redirect_hints_json(
        [
            {
                "brand": "Luck Party",
                "status_hint": "x",
                "discovery_source": "sweepskings",
                "verified_domain_candidate": "",
                "redirect_target_if_any": "",
                "redirects_to_known_operator_family": False,
                "known_operator_host_match": "",
                "verification_status": "unclear",
                "ambiguity": False,
            }
        ]
    )
    assert "Luck Party" in hints["review_site_unverified_family_notes"]
    assert "Blazesoft" in hints["review_site_unverified_family_notes"]["Luck Party"] or "Zula" in hints["review_site_unverified_family_notes"]["Luck Party"]


def test_known_family_match():
    assert host_matches_known_family("www.chumba.com") == "chumba.com"
    assert host_matches_known_family("unknown.example.com") is None


def test_verify_web_no_crash(monkeypatch, tmp_path):
    norm = tmp_path / "web_new_normalized_rows.json"
    norm.write_text(
        '[{"brand":"Zed","status_hint":"x","discovery_source":"a|b","candidate_domain_guesses":["invalid.invalid"],"notes":""}]',
        encoding="utf-8",
    )

    def fake_verify(host, **kw):
        return {
            "primary_domain": host,
            "verification_status": "unreachable",
            "verification_score": 0.0,
            "verification_notes": "test",
            "dns_ok": False,
            "http_status": None,
            "final_url": "",
            "keyword_hits": [],
            "policy_links_found": [],
        }

    monkeypatch.setattr("sweep_scout.verify_web_candidates.verify_domain", fake_verify)
    out_json = tmp_path / "v.json"
    out, queue = run_verify_web(
        normalized_path=norm,
        out_json=out_json,
        out_queue=tmp_path / "q.csv",
        cache_dir=tmp_path / "c",
    )
    assert len(out) == 1
    assert len(queue) == 1
    hints_path = out_json.with_name("web_new_redirect_hints.json")
    assert hints_path.is_file()
    hints = json.loads(hints_path.read_text(encoding="utf-8"))
    assert "review_site_unverified_family_notes" in hints


def test_redirect_to_known_operator_in_summary(monkeypatch, tmp_path):
    norm = tmp_path / "web_new_normalized_rows.json"
    norm.write_text(
        '[{"brand":"Proxy","status_hint":"x","discovery_source":"x","candidate_domain_guesses":["redirect.me"],"notes":""}]',
        encoding="utf-8",
    )

    def fake_verify(host, **kw):
        return {
            "primary_domain": host,
            "verification_status": "redirected",
            "verification_score": 0.5,
            "verification_notes": "test",
            "dns_ok": True,
            "http_status": 302,
            "final_url": "https://www.chumba.com/lobby",
            "keyword_hits": ["casino"],
            "policy_links_found": [],
        }

    monkeypatch.setattr("sweep_scout.verify_web_candidates.verify_domain", fake_verify)
    out, queue = run_verify_web(
        normalized_path=norm,
        out_json=tmp_path / "v.json",
        out_queue=tmp_path / "q.csv",
        cache_dir=tmp_path / "c",
    )
    assert out[0].get("redirects_to_known_operator_family") is True
    assert out[0].get("known_operator_host_match") == "chumba.com"
    assert queue[0].get("redirect_target_if_any", "").startswith("https://")
