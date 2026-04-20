"""Deterministic page-risk profiling from fetched HTML/text (defense-oriented signals)."""
from __future__ import annotations

import re
from typing import Any

# Order: higher-index keywords can add more weight in scoring.
_RISK_KEYWORDS: tuple[tuple[str, float], ...] = (
    ("sweeps", 1.0),
    ("sweepstakes", 1.2),
    ("social casino", 1.4),
    ("casino", 1.0),
    ("no purchase necessary", 1.3),
    ("redeem", 1.1),
    ("sweeps coins", 1.2),
    ("gold coins", 1.0),
    ("deposit", 1.5),
    ("promotions", 1.0),
    ("bonus", 1.0),
    ("register", 1.2),
    ("login", 1.0),
    ("verify identity", 1.8),
    ("card number", 2.0),
    ("cashier", 1.6),
    ("wallet", 1.2),
    ("poker", 1.0),
    ("sportsbook", 1.0),
    ("bingo", 1.0),
    ("fish game", 1.2),
)


def _html_to_text(body: bytes) -> str:
    try:
        s = body.decode("utf-8", errors="replace")
    except Exception:
        s = str(body)
    s = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", s)
    s = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    return s.lower()


def keyword_hits(text_lower: str) -> list[str]:
    hits: list[str] = []
    for kw, _w in _RISK_KEYWORDS:
        if kw in text_lower:
            hits.append(kw)
    return sorted(set(hits))


def compute_page_risk(
    body: bytes | None,
    *,
    field_context_hints: list[str] | None = None,
) -> dict[str, Any]:
    """
    Return keyword hits, optional field-context hints, risk_score (0–100), and risk_reasons.
    """
    text = _html_to_text(body) if body else ""
    hits = keyword_hits(text)
    score = 0.0
    reasons: list[str] = []
    for kw in hits:
        w = dict(_RISK_KEYWORDS).get(kw, 1.0)
        score += w * 4.0
        reasons.append(f"keyword:{kw}")
    # Cap contribution from keywords
    score = min(85.0, score)
    if field_context_hints:
        for h in field_context_hints:
            reasons.append(f"field:{h}")
            score += 5.0
    score = min(100.0, round(score, 2))
    if not hits and not field_context_hints:
        reasons.append("no_risk_keywords")
    return {
        "keyword_hits": hits,
        "field_context_hints": list(field_context_hints or []),
        "risk_score": score,
        "risk_reasons": reasons[:40],
    }
