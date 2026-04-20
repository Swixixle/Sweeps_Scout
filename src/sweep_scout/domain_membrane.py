"""Small conservative domain membrane around a brand string (discovery only)."""
from __future__ import annotations

from sweep_scout.normalize_candidates import normalize_domain
from sweep_scout.web_candidate_domains import extract_embedded_domain, fold_brand_slug

_ALLOWED_TLDS = ("com", "net", "org", "us")


def _brand_suggests_operator_context(brand: str) -> bool:
    b = brand.lower()
    return any(
        k in b
        for k in (
            "casino",
            "sweeps",
            "sweep",
            "slots",
            "gold",
            "social",
            "fish",
            "poker",
            "bingo",
        )
    )


def _slug_variants(slug: str) -> list[str]:
    """At most two compact roots: primary + optional winz/wins swap."""
    if not slug:
        return []
    out = [slug]
    if "winz" in slug:
        alt = slug.replace("winz", "wins", 1)
        if alt != slug:
            out.append(alt)
    elif "wins" in slug and "casino" not in slug:
        alt = slug.replace("wins", "winz", 1)
        if alt != slug and alt not in out:
            out.append(alt)
    return out[:2]


def generate_membrane_hosts(brand: str, *, max_hosts: int = 6) -> list[str]:
    """
    Up to ``max_hosts`` hosts using .com / .net / .org / .us only,
    plus optional ``{slug}casino.com`` / ``{slug}sweeps.com`` when the brand
    strongly suggests operator context. No large combinatorics.
    """
    brand = brand.strip()
    out: list[str] = []
    seen: set[str] = set()

    def add(h: str) -> None:
        host = normalize_domain(h)
        if not host or host in seen or len(out) >= max_hosts:
            return
        seen.add(host)
        out.append(host)

    emb = extract_embedded_domain(brand)
    if emb:
        add(emb)

    slug = fold_brand_slug(brand)
    if not slug:
        return out[:max_hosts]

    op = _brand_suggests_operator_context(brand)
    variants = _slug_variants(slug)

    for v in variants:
        for tld in _ALLOWED_TLDS:
            add(f"{v}.{tld}")
            if len(out) >= max_hosts:
                return out[:max_hosts]

    if op and len(out) < max_hosts:
        v0 = variants[0]
        if not v0.endswith("casino"):
            add(f"{v0}casino.com")
        if len(out) < max_hosts:
            add(f"{v0}sweeps.com")

    return out[:max_hosts]
