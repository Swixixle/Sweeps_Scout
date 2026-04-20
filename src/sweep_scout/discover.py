from __future__ import annotations

import argparse
from collections import deque
from pathlib import Path

from sweep_scout.config import ensure_dirs, paths_for_repo, repo_root_from_args
from sweep_scout.fetch import fetch_url
from sweep_scout.html_sniff import extract_links_from_html
from sweep_scout.utils import (
    deterministic_json_dumps,
    domain_from_url,
    host_in_denylist,
    host_matches_allowlist,
    normalize_url,
    read_lines_file,
    utc_now_iso,
)


def _should_crawl_url(url: str, allow: list[str], deny: list[str]) -> bool:
    d = domain_from_url(url)
    if not d:
        return False
    if host_in_denylist(d, deny):
        return False
    return host_matches_allowlist(d, allow)


def run_discover(
    repo_root: Path,
    *,
    max_depth: int = 1,
    max_pages: int = 250,
) -> dict:
    ensure_dirs(repo_root)
    paths = paths_for_repo(repo_root)
    seeds = read_lines_file(str(paths["seed_urls"]))
    allow = read_lines_file(str(paths["allow_domains"]))
    deny = read_lines_file(str(paths["deny_domains"]))
    bootstrap = read_lines_file(str(paths["bootstrap_domains"]))

    for b in bootstrap:
        b = b.strip().lower().strip(".")
        if b and not b.startswith("http"):
            seeds.append(f"https://{b}/")

    ts = utc_now_iso()
    cache_dir = paths["cache"]

    pages: list[dict] = []
    redirects: list[dict] = []
    domain_map: dict[str, dict] = {}
    seen_fetch: set[str] = set()

    queue: deque[tuple[str, int, str, str]] = deque()
    for s in seeds:
        nu = normalize_url(s)
        if nu:
            queue.append((nu, 0, nu, "seed"))

    while queue and len(pages) < max_pages:
        url, depth, source_url, how = queue.popleft()
        if url in seen_fetch:
            continue
        seen_fetch.add(url)

        fr = fetch_url(url, cache_dir=cache_dir)
        final = fr.final_url or url
        status = fr.status

        page_id = f"p{len(pages)}"
        pages.append(
            {
                "page_id": page_id,
                "requested_url": url,
                "final_url": final,
                "status": status,
                "content_type": fr.content_type,
                "bytes": len(fr.body),
                "error": fr.error,
                "fetched_at": fr.fetched_at,
                "depth": depth,
                "source_url": source_url,
            }
        )

        if final.rstrip("/") != url.rstrip("/"):
            redirects.append(
                {
                    "from_url": url,
                    "to_url": final,
                    "domain_from": domain_from_url(url),
                    "domain_to": domain_from_url(final),
                    "discovery_type": "http_redirect",
                    "source_url": source_url,
                    "first_seen": ts,
                }
            )

        base = final
        ctype = (fr.content_type or "").lower()
        if status and 200 <= status < 400 and "html" in ctype and fr.body:
            links = extract_links_from_html(fr.body, base)
            page_source = final
            for link in links:
                dom = domain_from_url(link)
                if not dom:
                    continue
                key = dom
                rec = {
                    "domain": dom,
                    "source_url": page_source,
                    "discovered_url": link,
                    "final_url": link,
                    "first_seen": ts,
                    "discovery_type": "outbound_link",
                }
                if key not in domain_map:
                    domain_map[key] = rec
                else:
                    ex = domain_map[key]
                    if "source_urls" not in ex:
                        ex["source_urls"] = [ex["source_url"]]
                    if page_source not in ex["source_urls"]:
                        ex["source_urls"].append(page_source)

                if depth < max_depth and _should_crawl_url(link, allow, deny):
                    if link not in seen_fetch:
                        queue.append((link, depth + 1, page_source, "crawl"))

    discovered_domains = list(domain_map.values())
    for r in discovered_domains:
        if "source_urls" not in r:
            r["source_urls"] = [r["source_url"]]
        r["source_urls"] = sorted(set(r["source_urls"]))
        r["source_url"] = r["source_urls"][0] if r["source_urls"] else ""

    paths["candidates"].mkdir(parents=True, exist_ok=True)
    paths["reports_discovery"].mkdir(parents=True, exist_ok=True)

    paths["discovered_pages"].write_text(
        deterministic_json_dumps(pages), encoding="utf-8"
    )
    paths["discovered_domains"].write_text(
        deterministic_json_dumps(discovered_domains), encoding="utf-8"
    )

    existing_redir: list = []
    if paths["discovered_redirects"].is_file():
        try:
            import json

            existing_redir = json.loads(paths["discovered_redirects"].read_text(encoding="utf-8"))
            if not isinstance(existing_redir, list):
                existing_redir = []
        except Exception:
            existing_redir = []

    merged_redir = existing_redir + redirects
    by_key: dict[tuple[str, str], dict] = {}
    for r in merged_redir:
        k = (r.get("from_url", ""), r.get("to_url", ""))
        by_key[k] = r
    all_redir = sorted(by_key.values(), key=lambda x: (x.get("from_url", ""), x.get("to_url", "")))
    paths["discovered_redirects"].write_text(
        deterministic_json_dumps(all_redir), encoding="utf-8"
    )

    report = {
        "run_at": ts,
        "max_depth": max_depth,
        "max_pages": max_pages,
        "pages_fetched": len(pages),
        "unique_domains": len(discovered_domains),
        "redirects_recorded": len(redirects),
    }
    rep_path = paths["reports_discovery"] / f"discover-{ts.replace(':', '-')}.json"
    rep_path.write_text(deterministic_json_dumps(report), encoding="utf-8")
    return report


def main_cli() -> None:
    p = argparse.ArgumentParser(description="Discover domains and pages from seeds")
    p.add_argument("--repo-root", default=".")
    p.add_argument("--depth", type=int, default=1)
    p.add_argument("--max-pages", type=int, default=250)
    args = p.parse_args()
    root = Path(args.repo_root).resolve()
    r = run_discover(root, max_depth=args.depth, max_pages=args.max_pages)
    print(deterministic_json_dumps(r))


if __name__ == "__main__":
    main_cli()
