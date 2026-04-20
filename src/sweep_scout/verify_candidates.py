"""Low-cost HTTP/DNS verification pass for candidate primary domains (discovery only)."""
from __future__ import annotations

import argparse
import csv
import json
import re
import socket
from pathlib import Path
from typing import Any
from sweep_scout.fetch import fetch_url
from sweep_scout.html_sniff import extract_links_from_html
from sweep_scout.utils import deterministic_json_dumps, domain_from_url, normalize_host, repo_root

_KEYWORDS = [
    "sweeps",
    "sweepstakes",
    "social casino",
    "casino",
    "gold coins",
    "sweeps coins",
    "sweeps cash",
    "redeem",
    "no purchase necessary",
    "sportsbook",
    "poker",
    "bingo",
    "fish game",
    "amusement",
    "no purchase",
]
_POLICY_HINTS = re.compile(
    r"(terms|privacy|rules|sweepstakes rules|user agreement|responsible gaming)",
    re.I,
)


def _dns_resolves(host: str) -> bool:
    if not host:
        return False
    try:
        socket.getaddrinfo(host, None, socket.AF_UNSPEC)
        return True
    except OSError:
        return False


def _html_to_text(body: bytes) -> str:
    try:
        s = body.decode("utf-8", errors="replace")
    except Exception:
        s = str(body)
    s = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", s)
    s = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    return s.lower()


def _keyword_hits(text: str) -> list[str]:
    hits: list[str] = []
    for kw in _KEYWORDS:
        if kw in text:
            hits.append(kw)
    return sorted(set(hits))


def _policy_links(links: list[str]) -> list[str]:
    out: list[str] = []
    for u in links:
        lu = u.lower()
        if _POLICY_HINTS.search(lu):
            out.append(u)
    return sorted(set(out))[:20]


def verify_domain(
    primary_domain: str,
    *,
    cache_dir: Path | None = None,
    timeout: float = 18.0,
) -> dict[str, Any]:
    host = normalize_host(primary_domain)
    dns_ok = _dns_resolves(host)
    url = f"https://{host}/"
    fr = fetch_url(url, timeout=timeout, cache_dir=cache_dir, retries=1)
    final_host = normalize_host(domain_from_url(fr.final_url)) if fr.final_url else ""
    redirected = bool(host and final_host and host != final_host)

    text = _html_to_text(fr.body) if fr.body else ""
    hits = _keyword_hits(text)
    links = extract_links_from_html(fr.body, fr.final_url or url) if fr.body else []
    policies = _policy_links(links)

    score = 0.0
    if dns_ok:
        score += 0.15
    if fr.status and 200 <= fr.status < 400:
        score += 0.25
    if redirected:
        score += 0.05
    score += min(0.35, 0.07 * len(hits))
    score += min(0.2, 0.04 * len(policies))
    if fr.error and not fr.body:
        score *= 0.5
    score = round(min(1.0, max(0.0, score)), 4)

    if not dns_ok:
        status = "unreachable"
    elif redirected:
        status = "redirected"
    elif fr.status and 200 <= fr.status < 400 and fr.body:
        status = "reachable"
    elif fr.status and fr.body:
        status = "unclear"
    else:
        status = "unclear"

    notes: list[str] = []
    if fr.error:
        notes.append(f"fetch:{fr.error[:120]}")
    if redirected:
        notes.append(f"redirect->{final_host}")
    notes.append(f"keywords:{len(hits)}")
    notes.append(f"policy_links:{len(policies)}")

    return {
        "primary_domain": host,
        "verification_status": status,
        "verification_score": score,
        "verification_notes": "; ".join(notes),
        "dns_ok": dns_ok,
        "http_status": fr.status,
        "final_url": fr.final_url,
        "keyword_hits": hits,
        "policy_links_found": policies,
    }


def write_verification_queue_verified(
    queue_csv: Path,
    verified_records: list[dict[str, Any]],
    out_csv: Path,
) -> None:
    """Merge verification fields into a copy of the queue (non-verified rows get empty verify cols)."""
    by_dom = {str(r.get("primary_domain", "")).strip(): r for r in verified_records if r.get("primary_domain")}
    extra = [
        "verification_status",
        "verification_score",
        "verification_notes",
        "dns_ok",
        "http_status",
        "final_url",
        "keyword_hits",
    ]
    with queue_csv.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        base = list(reader.fieldnames or [])
        for c in extra:
            if c not in base:
                base.append(c)
        fieldnames = base
        rows_out: list[dict[str, str]] = []
        for row in reader:
            row = dict(row)
            dom = (row.get("primary_domain") or "").strip()
            vr = by_dom.get(dom, {})
            for c in extra:
                if c == "keyword_hits":
                    row[c] = ",".join(vr.get("keyword_hits") or [])
                elif c in vr:
                    v = vr.get(c)
                    row[c] = "" if v is None else str(v)
                else:
                    row[c] = ""
            rows_out.append(row)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        w.writeheader()
        for row in rows_out:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def run_verify_from_queue(
    queue_csv: Path | None = None,
    out_json: Path | None = None,
    *,
    limit: int | None = 50,
    cache_dir: Path | None = None,
    reviewed_queue_csv: Path | None = None,
) -> list[dict[str, Any]]:
    root = repo_root()
    queue_csv = queue_csv or (root / "data" / "candidates" / "verification_queue.csv")
    out_json = out_json or (root / "data" / "candidates" / "verified_candidates.json")
    reviewed_queue_csv = reviewed_queue_csv or (root / "data" / "candidates" / "verification_queue_verified.csv")
    cache_dir = cache_dir or (root / "data" / "cache" / "verify")

    if not queue_csv.is_file():
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(deterministic_json_dumps([]), encoding="utf-8")
        return []

    results: list[dict[str, Any]] = []
    with queue_csv.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)

    rows.sort(key=lambda x: (-float(x.get("priority_score") or 0), x.get("primary_domain", "")))

    for i, row in enumerate(rows):
        if limit is not None and i >= limit:
            break
        dom = (row.get("primary_domain") or "").strip()
        if not dom:
            continue
        rec = verify_domain(dom, cache_dir=cache_dir)
        rec["brand"] = row.get("brand", "")
        rec["priority_score"] = row.get("priority_score", "")
        results.append(rec)

    results.sort(key=lambda x: (-x.get("verification_score", 0), x.get("primary_domain", "")))
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(deterministic_json_dumps(results), encoding="utf-8")
    write_verification_queue_verified(queue_csv, results, reviewed_queue_csv)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify candidate domains (DNS + HTTP + keyword sniff).")
    parser.add_argument("--queue", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--no-limit", action="store_true")
    args = parser.parse_args()
    root = repo_root()
    lim = None if args.no_limit else args.limit
    n = run_verify_from_queue(
        queue_csv=args.queue or (root / "data" / "candidates" / "verification_queue.csv"),
        out_json=args.out or (root / "data" / "candidates" / "verified_candidates.json"),
        limit=lim,
    )
    from collections import Counter

    c = Counter(str(r.get("verification_status", "")) for r in n)
    print(
        deterministic_json_dumps(
            {
                "verified": len(n),
                "by_status": dict(sorted(c.items())),
            }
        )
    )


if __name__ == "__main__":
    main()
