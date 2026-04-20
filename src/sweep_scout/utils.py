from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse


def repo_root() -> Path:
    """Repository root (parent of ``src``)."""
    return Path(__file__).resolve().parents[2]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def deterministic_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def strip_www(host: str) -> str:
    h = host.lower().strip(".")
    if h.startswith("www."):
        return h[4:]
    return h


def normalize_host(host: str) -> str:
    if not host:
        return ""
    host = host.strip().lower().strip(".")
    if "@" in host:
        return ""
    if ":" in host:
        host = host.split(":")[0]
    return strip_www(host)


def normalize_url(url: str, base: str | None = None) -> str | None:
    if not url or not isinstance(url, str):
        return None
    u = url.strip()
    if not u or u.startswith("#") or u.startswith("javascript:") or u.startswith("mailto:"):
        return None
    if u.startswith("//"):
        u = "https:" + u
    try:
        if base:
            u = urljoin(base, u)
        parsed = urlparse(u)
        if parsed.scheme not in ("http", "https"):
            return None
        host = parsed.hostname
        if not host:
            return None
        host = normalize_host(host)
        port = parsed.port
        if port in (80, 443) or port is None:
            netloc = host
        else:
            netloc = f"{host}:{port}"
        scheme = "https" if (port == 443 or (port is None and parsed.scheme == "https")) else "http"
        path = parsed.path or "/"
        query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        query_pairs.sort(key=lambda x: (x[0], x[1]))
        query = urlencode(query_pairs) if query_pairs else ""
        return urlunparse((scheme, netloc, path, "", query, ""))
    except Exception:
        return None


def domain_from_url(url: str) -> str:
    try:
        p = urlparse(url if "://" in url else f"https://{url}")
        return normalize_host(p.netloc.split("@")[-1])
    except Exception:
        return ""


def read_lines_file(path: str) -> list[str]:
    out: list[str] = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                out.append(line)
    except FileNotFoundError:
        pass
    return out


def host_matches_allowlist(host: str, allow: list[str]) -> bool:
    h = normalize_host(host)
    if not allow:
        return True
    for a in allow:
        a = normalize_host(a)
        if not a:
            continue
        if h == a or h.endswith("." + a):
            return True
    return False


def host_in_denylist(host: str, deny: list[str]) -> bool:
    h = normalize_host(host)
    for d in deny:
        d = normalize_host(d)
        if not d:
            continue
        if h == d or h.endswith("." + d):
            return True
    return False


EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)
