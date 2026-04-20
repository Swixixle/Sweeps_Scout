from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from urllib.parse import urlparse

from sweep_scout.config import ensure_dirs, paths_for_repo, repo_root_from_args
from sweep_scout.constants import (
    PAYMENT_PATH_HINTS,
    POLICY_PATH_HINTS,
    PROVIDER_HINTS,
    REBRAND_PHRASES,
    SUPPORT_PATH_HINTS,
    SWEEPS_LANGUAGE,
)
from sweep_scout.fetch import fetch_url
from sweep_scout.html_sniff import parse_signals
from sweep_scout.utils import deterministic_json_dumps, domain_from_url, utc_now_iso


def _lower(s: str) -> str:
    return s.lower()


def _hits(text: str, terms: tuple[str, ...]) -> list[str]:
    t = _lower(text)
    out: list[str] = []
    for term in terms:
        if term.lower() in t:
            out.append(term)
    return sorted(set(out))


def _path_hints(url: str, hints: tuple[str, ...]) -> bool:
    p = _lower(urlparse(url).path)
    for h in hints:
        if h in p:
            return True
    return False


def _categorize_links(links: list[tuple[str, str]]) -> tuple[list[str], list[str], list[str]]:
    policy: list[str] = []
    support: list[str] = []
    other: list[str] = []
    for url, _rel in links:
        lu = _lower(url)
        if any(h in lu for h in POLICY_PATH_HINTS):
            policy.append(url)
        elif any(h in lu for h in SUPPORT_PATH_HINTS):
            support.append(url)
        else:
            other.append(url)
    return sorted(set(policy)), sorted(set(support)), other


def _domains_from_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        d = domain_from_url(u)
        if d and d not in seen:
            seen.add(d)
            out.append(d)
    return sorted(out)


def _footer_snippet(text: str) -> str:
    low = text[-4000:] if len(text) > 4000 else text
    return low.strip()[:1200]


def extract_record_for_url(
    url: str,
    *,
    cache_dir: Path,
    max_body: int = 2_000_000,
) -> dict:
    fr = fetch_url(url, cache_dir=cache_dir)
    final = fr.final_url or url
    dom = domain_from_url(final)
    base = final
    notes: list[str] = []
    if fr.error:
        notes.append(f"fetch_error: {fr.error}")

    title = ""
    meta_description = ""
    script_domains: list[str] = []
    iframe_domains: list[str] = []
    asset_domains: list[str] = []
    support_links: list[str] = []
    policy_links: list[str] = []
    contact_emails: list[str] = []
    text_hits: list[str] = []
    provider_mentions: list[str] = []
    rebrand_hits: list[str] = []

    ctype = (fr.content_type or "").lower()
    plain = ""
    if fr.body and "html" in ctype and fr.status and 200 <= fr.status < 400:
        sig = parse_signals(fr.body[:max_body], base)
        title = sig["title"]
        meta_description = sig["meta_description"]
        plain = sig["plain_text_sample"]
        script_domains = _domains_from_urls(sig["script_src"])
        iframe_domains = _domains_from_urls(sig["iframe_src"])
        asset_domains = _domains_from_urls(
            [u for u, _ in sig["links"] if re.search(r"\.(css|js|png|jpg|jpeg|webp|svg)(\?|$)", u, re.I)]
        )
        pol, sup, _ = _categorize_links(sig["links"])
        policy_links = pol
        support_links = sup
        contact_emails = sorted(set(sig["emails"]))

        text_hits.extend(_hits(plain, SWEEPS_LANGUAGE))
        text_hits.extend(_hits(plain, PAYMENT_PATH_HINTS))
        text_hits = sorted(set(text_hits))

        for prov in PROVIDER_HINTS:
            if prov.lower() in plain.lower():
                provider_mentions.append(prov)

        rebrand_hits = _hits(plain, REBRAND_PHRASES)

        if _path_hints(base, POLICY_PATH_HINTS):
            notes.append("policy_like_path")
        if _path_hints(base, SUPPORT_PATH_HINTS):
            notes.append("support_like_path")

    elif fr.body and fr.status and 200 <= fr.status < 400:
        notes.append("non_html_skipped")

    return {
        "url": url,
        "final_url": final,
        "domain": dom,
        "status": fr.status,
        "content_type": fr.content_type,
        "title": title,
        "meta_description": meta_description,
        "footer_legal_snippet": _footer_snippet(plain) if plain else "",
        "script_domains": script_domains,
        "iframe_domains": iframe_domains,
        "asset_domains": sorted(set(asset_domains))[:50],
        "support_links": support_links[:80],
        "contact_emails": contact_emails[:40],
        "policy_links": policy_links[:80],
        "text_hits": text_hits,
        "provider_mentions": sorted(set(provider_mentions)),
        "rebrand_phrase_hits": rebrand_hits,
        "notes": sorted(set(notes)),
        "fetched_at": fr.fetched_at,
    }


def run_extract(
    repo_root: Path,
    *,
    from_discovered: bool = True,
    max_urls: int = 120,
) -> dict:
    ensure_dirs(repo_root)
    paths = paths_for_repo(repo_root)
    cache_dir = paths["cache"]

    urls: list[str] = []
    if from_discovered and paths["discovered_domains"].is_file():
        data = json.loads(paths["discovered_domains"].read_text(encoding="utf-8"))
        for row in data:
            u = row.get("discovered_url") or row.get("final_url")
            if u:
                urls.append(u)
    if not urls and paths["discovered_pages"].is_file():
        pdata = json.loads(paths["discovered_pages"].read_text(encoding="utf-8"))
        for row in pdata:
            u = row.get("final_url") or row.get("requested_url")
            if u:
                urls.append(u)

    seen: set[str] = set()
    uniq: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
        if len(uniq) >= max_urls:
            break

    ts = utc_now_iso()
    records: list[dict] = []
    for u in uniq:
        try:
            records.append(extract_record_for_url(u, cache_dir=cache_dir))
        except Exception as e:
            records.append(
                {
                    "url": u,
                    "final_url": u,
                    "domain": domain_from_url(u),
                    "status": None,
                    "content_type": None,
                    "title": "",
                    "meta_description": "",
                    "footer_legal_snippet": "",
                    "script_domains": [],
                    "iframe_domains": [],
                    "asset_domains": [],
                    "support_links": [],
                    "contact_emails": [],
                    "policy_links": [],
                    "text_hits": [],
                    "provider_mentions": [],
                    "rebrand_phrase_hits": [],
                    "notes": [f"exception:{e}"],
                    "fetched_at": ts,
                }
            )

    paths["extracted_signals"].write_text(deterministic_json_dumps(records), encoding="utf-8")
    report = {
        "run_at": ts,
        "urls_processed": len(records),
        "from_discovered": from_discovered,
    }
    paths["reports_extraction"].mkdir(parents=True, exist_ok=True)
    rep_path = paths["reports_extraction"] / f"extract-{ts.replace(':', '-')}.json"
    rep_path.write_text(deterministic_json_dumps(report), encoding="utf-8")
    return report


def main_cli() -> None:
    p = argparse.ArgumentParser(description="Extract lightweight signals from discovered URLs")
    p.add_argument("--repo-root", default=".")
    p.add_argument("--from-discovered", action="store_true", default=True)
    p.add_argument("--no-from-discovered", action="store_true")
    p.add_argument("--max-urls", type=int, default=120)
    args = p.parse_args()
    root = Path(args.repo_root).resolve()
    fd = not args.no_from_discovered
    r = run_extract(root, from_discovered=fd, max_urls=args.max_urls)
    print(deterministic_json_dumps(r))


if __name__ == "__main__":
    main_cli()
