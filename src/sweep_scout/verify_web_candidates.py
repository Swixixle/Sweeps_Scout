"""Live verification for web_new candidate domain guesses (discovery only; not truth)."""
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sweep_scout.utils import deterministic_json_dumps, normalize_host, repo_root
from sweep_scout.verify_candidates import verify_domain
from sweep_scout.web_candidate_domains import extract_embedded_domain, fold_brand_slug

# Hosts of established sweeps/social-casino operators (redirect-family hints only).
KNOWN_OPERATOR_HOSTS = frozenset(
    {
        "chumba.com",
        "luckylandslots.com",
        "stake.us",
        "wowvegas.com",
        "mcluck.com",
        "pulsz.com",
        "funzcity.com",
        "fliff.com",
        "nolimitcoins.com",
        "sidepot.us",
        "modo.us",
        "novig.com",
        "crowncoins.com",
        "yaycasino.com",
        "globalpoker.com",
        "high5casino.com",
        "fortunecoins.com",
        "sweepstakes.mobi",
        "chanced.com",
        "legendz.com",
    }
)

# Unverified copy from public review/comparison pages — not production truth.
REVIEW_SITE_FAMILY_NOTES: dict[str, str] = {
    "Luck Party": (
        "Third-party listings (e.g. SweepsKings-style pages) sometimes group Luck Party with Zula and "
        "Fortune Wins under a shared platform/vendor label. Treat as an unverified bibliographic hint; "
        "confirm ownership/terms only on live sites."
    ),
    "Zula": (
        "Often appears near Luck Party / Fortune Wins on comparison pages—possible same-family cluster "
        "per reviews; not verified here."
    ),
    "Fortune Wins": (
        "Often appears near Luck Party / Zula on comparison pages—possible same-family cluster "
        "per reviews; not verified here."
    ),
}


def host_matches_known_family(host: str) -> str | None:
    h = normalize_host(host or "")
    if not h:
        return None
    for k in KNOWN_OPERATOR_HOSTS:
        if h == k or h.endswith("." + k):
            return k
    return None


def _status_rank(st: str) -> int:
    return {"reachable": 4, "redirected": 3, "unclear": 2, "unreachable": 1}.get(st or "", 0)


def compute_redirect_target_if_any(rec: dict[str, Any]) -> str:
    """Non-empty when final URL host differs from the guessed primary, or status is redirected."""
    pu = normalize_host(str(rec.get("primary_domain", "")))
    fu = str(rec.get("final_url") or "").strip()
    st = str(rec.get("verification_status", ""))
    if not fu:
        return ""
    try:
        fh = normalize_host(urlparse(fu).netloc.split("@")[-1])
    except Exception:
        return fu[:500]
    if st == "redirected":
        return fu[:500]
    if fh and pu and fh != pu:
        return fu[:500]
    return ""


def _final_resembles_brand(brand: str, rec: dict[str, Any]) -> int:
    slug = fold_brand_slug(brand)
    if len(slug) < 4:
        return 0
    fu = str(rec.get("final_url") or "")
    try:
        h = normalize_host(urlparse(fu).netloc.split("@")[-1])
    except Exception:
        return 0
    compact = h.replace(".", "")
    if slug[: min(10, len(slug))] in compact:
        return 1
    return 0


def pick_best_guess(brand: str, guess_results: list[dict[str, Any]]) -> dict[str, Any]:
    if not guess_results:
        return {}
    emb = extract_embedded_domain(brand)

    def key(r: dict[str, Any]) -> tuple[Any, ...]:
        gh = str(r.get("guess_host") or r.get("primary_domain") or "")
        embed_pref = 1 if emb and gh == emb else 0
        return (
            embed_pref,
            _status_rank(str(r.get("verification_status", ""))),
            _final_resembles_brand(brand, r),
            float(r.get("verification_score", 0)),
            len(r.get("keyword_hits") or []),
            len(r.get("policy_links_found") or []),
        )

    return max(guess_results, key=key)


def _annotate_known_redirect(rec: dict[str, Any]) -> dict[str, Any]:
    out = dict(rec)
    final = rec.get("final_url") or ""
    matched = None
    fh = ""
    try:
        fu = urlparse(str(final))
        fh = normalize_host(fu.netloc.split("@")[-1])
        matched = host_matches_known_family(fh) if fh else None
    except Exception:
        pass
    out["redirects_to_known_operator_family"] = bool(matched)
    out["known_operator_host_match"] = matched or ""
    out["redirect_target_if_any"] = compute_redirect_target_if_any(out)
    if matched:
        n = str(out.get("verification_notes", ""))
        out["verification_notes"] = (n + "; known_operator_family:" + matched)[:500]
    return out


def verify_brand_guesses(
    brand: str,
    guesses: list[str],
    *,
    cache_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for host in guesses:
        if not host:
            continue
        vr = verify_domain(host, cache_dir=cache_dir)
        vr["guess_host"] = host
        results.append(_annotate_known_redirect(vr))

    if not results:
        return [], {
            "verified_domain_candidate": "",
            "verification_status": "unreachable",
            "verification_score": 0.0,
            "verification_notes": "no_domain_guesses_generated",
            "final_url": "",
            "keyword_hits": [],
            "policy_links_found": [],
            "redirects_to_known_operator_family": False,
            "known_operator_host_match": "",
            "redirect_target_if_any": "",
        }

    best = pick_best_guess(brand, results)
    best = _annotate_known_redirect(best)
    summary = {
        "verified_domain_candidate": str(best.get("primary_domain", "")),
        "verification_status": str(best.get("verification_status", "unclear")),
        "verification_score": float(best.get("verification_score", 0)),
        "verification_notes": str(best.get("verification_notes", "")),
        "final_url": str(best.get("final_url", "") or ""),
        "keyword_hits": list(best.get("keyword_hits") or []),
        "policy_links_found": list(best.get("policy_links_found") or []),
        "redirects_to_known_operator_family": bool(best.get("redirects_to_known_operator_family")),
        "known_operator_host_match": str(best.get("known_operator_host_match") or ""),
        "redirect_target_if_any": str(best.get("redirect_target_if_any") or ""),
        "http_status": best.get("http_status"),
        "dns_ok": best.get("dns_ok"),
    }
    return results, summary


def _queue_priority(
    brand: str,
    status_hint: str,
    discovery_source: str,
    summary: dict[str, Any],
) -> tuple[float, str]:
    reasons: list[str] = []
    score = 5.0
    st = str(summary.get("verification_status", ""))
    if st == "reachable":
        score += 80.0
        reasons.append("reachable")
    elif st == "redirected":
        score += 55.0
        reasons.append("redirected")
    elif st == "unclear":
        score += 25.0
        reasons.append("unclear")
    else:
        reasons.append("unreachable")

    score += float(summary.get("verification_score", 0)) * 35.0
    score += len(summary.get("keyword_hits") or []) * 6.0
    score += len(summary.get("policy_links_found") or []) * 5.0

    if summary.get("redirect_target_if_any"):
        score += 12.0
        reasons.append("has_redirect_target")

    if summary.get("redirects_to_known_operator_family"):
        score += 40.0
        reasons.append("redirect_to_known_operator_family")

    if extract_embedded_domain(brand):
        score += 18.0
        reasons.append("explicit_domain_in_brand")

    ds = discovery_source or ""
    score += ds.count("|") * 4.0
    if ds.count("|") >= 2:
        reasons.append("multi_source_mention")

    if status_hint == "newly_listed":
        score += 6.0
        reasons.append("newly_listed")

    if re.search(r"\.(com|us|cc|net)\b", brand, re.I):
        score += 10.0
        reasons.append("brand_looks_like_domain")

    score = max(0.0, min(200.0, score))
    return score, ";".join(reasons)


def _needs_manual(summary: dict[str, Any], guess_count: int) -> str:
    if summary.get("redirects_to_known_operator_family"):
        return "yes"
    if summary.get("redirect_target_if_any"):
        return "yes"
    if guess_count > 1 and str(summary.get("verification_status")) in ("unclear", "redirected"):
        return "yes"
    if str(summary.get("verification_status")) == "unreachable":
        return "maybe"
    return "maybe"


def build_redirect_hints_json(verified_rows: list[dict[str, Any]]) -> dict[str, Any]:
    live: list[dict[str, Any]] = []
    for v in verified_rows:
        brand = str(v.get("brand", ""))
        live.append(
            {
                "brand": brand,
                "status_hint": v.get("status_hint"),
                "discovery_source": v.get("discovery_source"),
                "verified_domain_candidate": v.get("verified_domain_candidate"),
                "redirect_target_if_any": v.get("redirect_target_if_any", ""),
                "redirects_to_known_operator_family": v.get("redirects_to_known_operator_family"),
                "known_operator_host_match": v.get("known_operator_host_match", ""),
                "verification_status": v.get("verification_status"),
                "ambiguity": v.get("ambiguity"),
            }
        )
    return {
        "review_site_unverified_family_notes": dict(sorted(REVIEW_SITE_FAMILY_NOTES.items())),
        "live_verification_redirects": sorted(live, key=lambda x: str(x.get("brand", ""))),
    }


def run_verify_web(
    normalized_path: Path | None = None,
    out_json: Path | None = None,
    out_queue: Path | None = None,
    out_hints: Path | None = None,
    *,
    cache_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    root = repo_root()
    normalized_path = normalized_path or (root / "data" / "candidates" / "web_new_normalized_rows.json")
    out_json = out_json or (root / "data" / "candidates" / "web_new_verified_candidates.json")
    out_queue = out_queue or (root / "data" / "candidates" / "web_new_verification_queue.csv")
    if out_hints is None:
        out_hints = out_json.with_name("web_new_redirect_hints.json")
    cache_dir = cache_dir or (root / "data" / "cache" / "web_verify")

    rows: list[dict[str, Any]] = json.loads(normalized_path.read_text(encoding="utf-8"))
    verified_out: list[dict[str, Any]] = []
    queue_rows: list[dict[str, str]] = []

    for r in sorted(rows, key=lambda x: str(x.get("brand", ""))):
        brand = str(r.get("brand", ""))
        guesses = list(r.get("candidate_domain_guesses") or [])
        status_hint = str(r.get("status_hint", ""))
        discovery_source = str(r.get("discovery_source", ""))

        guess_results, summary = verify_brand_guesses(brand, guesses, cache_dir=cache_dir)
        family_note = REVIEW_SITE_FAMILY_NOTES.get(brand, "")
        rec = {
            "brand": brand,
            "status_hint": status_hint,
            "discovery_source": discovery_source,
            "candidate_domain_guesses": guesses,
            "guess_results": guess_results,
            **summary,
            "review_site_family_note_unverified": family_note,
            "ambiguity": len([g for g in guess_results if _status_rank(str(g.get("verification_status"))) >= 2])
            > 1,
        }
        verified_out.append(rec)

        pri, why = _queue_priority(brand, status_hint, discovery_source, summary)
        queue_rows.append(
            {
                "brand": brand,
                "status_hint": status_hint,
                "discovery_source": discovery_source,
                "verified_domain_candidate": str(summary.get("verified_domain_candidate", "")),
                "verification_status": str(summary.get("verification_status", "")),
                "verification_score": f"{float(summary.get('verification_score', 0)):.4f}",
                "keyword_hits_count": str(len(summary.get("keyword_hits") or [])),
                "policy_links_found": "|".join(summary.get("policy_links_found") or [])[:800],
                "why_flagged": why,
                "needs_manual_verification": _needs_manual(summary, len(guesses)),
                "redirect_target_if_any": str(summary.get("redirect_target_if_any", ""))[:800],
                "_pri": pri,
            }
        )

    queue_rows.sort(key=lambda x: (-float(x.get("_pri", 0)), x.get("brand", "")))
    for q in queue_rows:
        q.pop("_pri", None)

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(deterministic_json_dumps(verified_out), encoding="utf-8")

    hints_obj = build_redirect_hints_json(verified_out)
    out_hints.write_text(deterministic_json_dumps(hints_obj), encoding="utf-8")

    fields = [
        "brand",
        "status_hint",
        "discovery_source",
        "verified_domain_candidate",
        "verification_status",
        "verification_score",
        "keyword_hits_count",
        "policy_links_found",
        "why_flagged",
        "needs_manual_verification",
        "redirect_target_if_any",
    ]
    with out_queue.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        for row in queue_rows:
            w.writerow({k: row.get(k, "") for k in fields})

    return verified_out, queue_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify web_new candidate domain guesses.")
    args = parser.parse_args()
    verified, queue = run_verify_web()
    from collections import Counter

    c = Counter(str(r.get("verification_status", "")) for r in verified)
    print(
        deterministic_json_dumps(
            {
                "brands": len(verified),
                "queue_rows": len(queue),
                "by_status": dict(sorted(c.items())),
                "redirect_hints": str(repo_root() / "data" / "candidates" / "web_new_redirect_hints.json"),
            }
        )
    )


if __name__ == "__main__":
    main()
