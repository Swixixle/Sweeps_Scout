"""Conservative domain guesses for web-surfaced brand strings (candidate intake only, not truth)."""
from __future__ import annotations

import re

from sweep_scout.normalize_candidates import normalize_domain


def fold_brand_slug(brand: str) -> str:
    """Lowercase, strip punctuation/spaces to a compact token for host guesses."""
    return re.sub(r"[^a-z0-9]+", "", brand.lower().strip())[:48]


def extract_embedded_domain(brand: str) -> str | None:
    """Return normalized host if the brand embeds a plausible domain (e.g. Acebet.cc, Rake.us)."""
    for m in re.finditer(r"\b((?:[a-z0-9][a-z0-9-]*\.)+[a-z]{2,63})\b", brand.strip(), re.I):
        raw = m.group(1).lower()
        if raw.count(".") < 1:
            continue
        h = normalize_domain(raw)
        if h:
            return h
    return None


def guess_domain_hosts(brand: str, *, max_guesses: int = 4) -> list[str]:
    """
    At most ``max_guesses`` hosts: embedded domain first, then .com / .net, optional
    ``{slug}casino.com`` when \"casino\" appears in the brand but slug lacks that suffix,
    then ``play{slug}.com``. No large combinatorics.
    """
    brand = brand.strip()
    out: list[str] = []
    seen: set[str] = set()

    def add(h: str) -> None:
        h = normalize_domain(h)
        if not h or h in seen:
            return
        seen.add(h)
        out.append(h)

    emb = extract_embedded_domain(brand)
    if emb:
        add(emb)

    slug = fold_brand_slug(brand)
    if not slug:
        return out[:max_guesses]

    sequence: list[str] = [f"{slug}.com", f"{slug}.net"]
    if "casino" in brand.lower() and not slug.endswith("casino"):
        sequence.append(f"{slug}casino.com")
    sequence.append(f"play{slug}.com")

    for host in sequence:
        if len(out) >= max_guesses:
            break
        add(host)

    return out[:max_guesses]
