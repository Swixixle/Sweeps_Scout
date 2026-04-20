"""HTTP calls to external research APIs + JSON extraction (stdlib only)."""
from __future__ import annotations

import json
import os
import re
import ssl
import urllib.error
import urllib.request
from typing import Any

DEFAULT_TIMEOUT = 90.0


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def extract_json_object(text: str) -> dict[str, Any] | None:
    """Best-effort JSON object from model output."""
    if not text:
        return None
    t = text.strip()
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", t, re.DOTALL | re.I)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    start = t.find("{")
    if start < 0:
        return None
    depth = 0
    for i in range(start, len(t)):
        if t[i] == "{":
            depth += 1
        elif t[i] == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(t[start : i + 1])
                except json.JSONDecodeError:
                    return None
    return None


def _post_json(url: str, headers: dict[str, str], body: dict[str, Any], timeout: float) -> tuple[int, str]:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if e.fp else ""
        return e.code, raw
    except Exception as e:
        return -1, str(e)


def call_perplexity_chat(user_prompt: str, *, timeout: float = DEFAULT_TIMEOUT) -> tuple[dict[str, Any] | None, str | None]:
    key = _env("PERPLEXITY_API_KEY")
    if not key:
        return None, None
    url = "https://api.perplexity.ai/chat/completions"
    body = {
        "model": "sonar",
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a careful research assistant for sweepstakes/social-casino brand due diligence. "
                    "Respond with ONLY valid JSON, no markdown outside the JSON object."
                ),
            },
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    code, raw = _post_json(url, headers, body, timeout)
    if code != 200:
        return None, f"perplexity_http_{code}:{raw[:500]}"
    try:
        outer = json.loads(raw)
        content = outer["choices"][0]["message"]["content"]
    except (KeyError, json.JSONDecodeError, IndexError) as e:
        return None, f"perplexity_parse:{e}"
    parsed = extract_json_object(content)
    if not parsed:
        return None, f"perplexity_no_json:{content[:400]}"
    return parsed, None


def call_anthropic_messages(user_prompt: str, *, timeout: float = DEFAULT_TIMEOUT) -> tuple[dict[str, Any] | None, str | None]:
    key = _env("ANTHROPIC_API_KEY")
    if not key:
        return None, None
    url = "https://api.anthropic.com/v1/messages"
    body = {
        "model": "claude-3-5-haiku-20241022",
        "max_tokens": 2048,
        "messages": [{"role": "user", "content": user_prompt}],
    }
    headers = {
        "x-api-key": key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    code, raw = _post_json(url, headers, body, timeout)
    if code != 200:
        return None, f"anthropic_http_{code}:{raw[:500]}"
    try:
        outer = json.loads(raw)
        blocks = outer.get("content") or []
        text = "".join(b.get("text", "") for b in blocks if isinstance(b, dict))
    except (json.JSONDecodeError, KeyError) as e:
        return None, f"anthropic_parse:{e}"
    parsed = extract_json_object(text)
    if not parsed:
        return None, f"anthropic_no_json:{text[:400]}"
    return parsed, None


def call_gemini_generate(user_prompt: str, *, timeout: float = DEFAULT_TIMEOUT) -> tuple[dict[str, Any] | None, str | None]:
    key = _env("GEMINI_API_KEY") or _env("GOOGLE_API_KEY")
    if not key:
        return None, None
    model = "gemini-1.5-flash"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    body = {
        "contents": [{"parts": [{"text": user_prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2048},
    }
    headers = {"Content-Type": "application/json"}
    code, raw = _post_json(url, headers, body, timeout)
    if code != 200:
        return None, f"gemini_http_{code}:{raw[:500]}"
    try:
        outer = json.loads(raw)
        parts = outer["candidates"][0]["content"]["parts"]
        text = "".join(p.get("text", "") for p in parts)
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        return None, f"gemini_parse:{e}"
    parsed = extract_json_object(text)
    if not parsed:
        return None, f"gemini_no_json:{text[:400]}"
    return parsed, None


def parsed_to_result_fields(parsed: dict[str, Any]) -> dict[str, Any]:
    """Normalize keys from LLM JSON into ResearchResult-friendly fields."""
    ent = str(parsed.get("entity_type_guess") or "unresolved").strip()
    valid = {
        "likely_real_operator",
        "likely_promoter",
        "likely_corporate",
        "likely_redirect_or_rebrand",
        "likely_noise",
        "unresolved",
    }
    if ent not in valid:
        ent = "unresolved"
    cites = parsed.get("citations") or []
    if isinstance(cites, str):
        cites = [cites]
    doms = parsed.get("extracted_domains") or []
    if isinstance(doms, str):
        doms = [doms]
    fam = parsed.get("family_hints") or []
    if isinstance(fam, str):
        fam = [fam]
    red = parsed.get("redirect_hints") or []
    if isinstance(red, str):
        red = [red]
    conf = parsed.get("confidence_hint")
    try:
        conf_f = float(conf) if conf is not None else 0.45
    except (TypeError, ValueError):
        conf_f = 0.45
    conf_f = max(0.0, min(1.0, conf_f))
    notes = str(parsed.get("notes") or "")[:2000]
    return {
        "citations": [str(c) for c in cites if c][:30],
        "extracted_domains": [str(d).lower().strip() for d in doms if d][:30],
        "entity_type_guess": ent,
        "family_hints": [str(f) for f in fam if f][:20],
        "redirect_hints": [str(r) for r in red if r][:20],
        "confidence_hint": conf_f,
        "notes": notes,
    }


def build_user_prompt_perplexity(job_brand: str, domains: list[str], membrane: list[str], det_summary: str) -> str:
    return f"""Research the sweepstakes / social-casino style brand "{job_brand}" for US-facing context.

Known domain candidates from our scan (not verified as official): {domains}
Membrane guesses: {membrane[:8]}

Deterministic local scan summary (heuristic, may be wrong):
{det_summary}

Return ONLY a JSON object with keys:
citations: string[] (URLs you relied on, if any)
extracted_domains: string[] (likely official or important hostnames, lowercase)
entity_type_guess: one of likely_real_operator | likely_promoter | likely_corporate | likely_redirect_or_rebrand | likely_noise | unresolved
launch_status_hint: string (live | upcoming | dead | affiliate_only | unknown)
family_hints: string[] (possible sibling/parent brands, unverified)
confidence_hint: number 0-1
notes: string (short)
"""


def build_user_prompt_claude(
    job_brand: str,
    domains: list[str],
    det_summary: str,
    perplexity_json: str,
) -> str:
    return f"""You audit another research pass for brand "{job_brand}".

Domains: {domains}

Deterministic summary:
{det_summary}

Perplexity JSON output:
{perplexity_json}

Return ONLY JSON with keys:
citations: string[]
extracted_domains: string[]
entity_type_guess: likely_real_operator | likely_promoter | likely_corporate | likely_redirect_or_rebrand | likely_noise | unresolved
agrees_with_perplexity: boolean
weak_evidence_flags: string[]
family_hints: string[]
confidence_hint: number 0-1
notes: string
"""


def build_user_prompt_gemini(
    job_brand: str,
    domains: list[str],
    det_summary: str,
    p_json: str,
    c_json: str,
) -> str:
    return f"""Second opinion for "{job_brand}". Compare sources; do not assert legal ownership.

Domains: {det_summary}

Perplexity: {p_json}
Claude: {c_json}

Return ONLY JSON with keys:
citations: string[]
extracted_domains: string[] (alternates if useful)
entity_type_guess: likely_real_operator | likely_promoter | likely_corporate | likely_redirect_or_rebrand | likely_noise | unresolved
family_hints: string[]
footer_or_company_phrases: string[]
same_backend_or_legal_language_hint: string
confidence_hint: number 0-1
notes: string
"""


def deterministic_summary_for_prompt(det: Any) -> str:
    raw = det.raw_payload if hasattr(det, "raw_payload") else {}
    checks = raw.get("deterministic_checks") or []
    lines = []
    for c in checks[:6]:
        lines.append(
            f"{c.get('host')}: status={c.get('verification_status')} score={c.get('verification_score')}"
        )
    pr = raw.get("page_risk") or {}
    lines.append(f"page_risk_score={pr.get('risk_score')} hits={pr.get('keyword_hits')}")
    return "\n".join(lines)[:4000]
