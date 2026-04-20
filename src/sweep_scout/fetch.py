from __future__ import annotations

import argparse
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sweep_scout.utils import sha256_text, utc_now_iso


DEFAULT_UA = (
    "SweepsScout/0.1 (+https://github.com/example/sweeps-scout; discovery research bot)"
)


@dataclass
class FetchResult:
    url: str
    status: int | None
    final_url: str
    content_type: str | None
    body: bytes
    error: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    fetched_at: str = field(default_factory=utc_now_iso)


def _read_cache(cache_path: Path) -> FetchResult | None:
    if not cache_path.is_file():
        return None
    try:
        raw = cache_path.read_bytes()
        if len(raw) < 8:
            return None
        status = int.from_bytes(raw[:4], "big")
        ct_len = int.from_bytes(raw[4:8], "big")
        ct = raw[8 : 8 + ct_len].decode("utf-8", errors="replace")
        rest = raw[8 + ct_len :]
        final_len = int.from_bytes(rest[:4], "big")
        final_url = rest[4 : 4 + final_len].decode("utf-8", errors="replace")
        body = rest[4 + final_len :]
        return FetchResult(
            url=final_url,
            status=status,
            final_url=final_url,
            content_type=ct or None,
            body=body,
            error=None,
        )
    except Exception:
        return None


def _write_cache(cache_path: Path, result: FetchResult) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    ct = (result.content_type or "").encode("utf-8")
    fu = result.final_url.encode("utf-8")
    payload = (
        (result.status if result.status is not None else 0).to_bytes(4, "big")
        + len(ct).to_bytes(4, "big")
        + ct
        + len(fu).to_bytes(4, "big")
        + fu
        + result.body
    )
    cache_path.write_bytes(payload)


def fetch_url(
    url: str,
    *,
    timeout: float = 20.0,
    max_redirects: int = 10,
    user_agent: str = DEFAULT_UA,
    cache_dir: Path | None = None,
    retries: int = 2,
) -> FetchResult:
    ssl_ctx = ssl.create_default_context()
    cache_path: Path | None = None
    if cache_dir is not None:
        h = sha256_text(url)
        cache_path = cache_dir / f"{h}.bin"
        cached = _read_cache(cache_path)
        if cached is not None:
            cached.url = url
            return cached

    last_err: str | None = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": user_agent, "Accept": "*/*"},
                method="GET",
            )
            class _LimitedRedirects(urllib.request.HTTPRedirectHandler):
                max_redirections = max_redirects

            https_handler = urllib.request.HTTPSHandler(context=ssl_ctx)
            opener = urllib.request.build_opener(_LimitedRedirects(), https_handler)
            with opener.open(req, timeout=timeout) as resp:
                final = resp.geturl()
                status = resp.getcode()
                ct = resp.headers.get("Content-Type")
                body = resp.read(2_000_000)
                headers = {k.lower(): v for k, v in resp.headers.items()}
                out = FetchResult(
                    url=url,
                    status=status,
                    final_url=final or url,
                    content_type=ct,
                    body=body,
                    error=None,
                    headers=headers,
                )
                if cache_path:
                    _write_cache(cache_path, out)
                return out
        except urllib.error.HTTPError as e:
            final = e.url or url
            try:
                body = e.read(2_000_000)
            except Exception:
                body = b""
            out = FetchResult(
                url=url,
                status=e.code,
                final_url=final,
                content_type=e.headers.get("Content-Type") if e.headers else None,
                body=body,
                error=str(e),
                headers={k.lower(): v for k, v in (e.headers or {}).items()},
            )
            if cache_path and out.status is not None:
                _write_cache(cache_path, out)
            return out
        except Exception as e:
            last_err = str(e)
            if attempt < retries:
                time.sleep(0.4 * (attempt + 1))
                continue
            parsed = urlparse(url)
            return FetchResult(
                url=url,
                status=None,
                final_url=url,
                content_type=None,
                body=b"",
                error=last_err,
            )
    return FetchResult(
        url=url,
        status=None,
        final_url=url,
        content_type=None,
        body=b"",
        error=last_err or "unknown",
    )


def main_cli() -> None:
    p = argparse.ArgumentParser(description="Fetch a URL (debug)")
    p.add_argument("url")
    p.add_argument("--repo-root", default=".")
    args = p.parse_args()
    root = Path(args.repo_root).resolve()
    cache = root / "data" / "cache"
    r = fetch_url(args.url, cache_dir=cache)
    print(r.status, r.final_url, r.content_type, r.error, len(r.body))


if __name__ == "__main__":
    main_cli()
