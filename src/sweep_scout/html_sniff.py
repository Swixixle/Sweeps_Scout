"""Lightweight HTML parsing (stdlib html.parser)."""
from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

from sweep_scout.utils import EMAIL_RE, normalize_url


class LinkCollectingParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        ad = dict((k.lower(), v or "") for k, v in attrs)
        if tag == "a" and ad.get("href"):
            nu = normalize_url(ad["href"], self.base_url)
            if nu:
                self.links.append(nu)
        if tag == "area" and ad.get("href"):
            nu = normalize_url(ad["href"], self.base_url)
            if nu:
                self.links.append(nu)


class SignalParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.title_parts: list[str] = []
        self.in_title = False
        self.meta_description = ""
        self.links: list[tuple[str, str]] = []
        self.script_src: list[str] = []
        self.iframe_src: list[str] = []
        self.text_chunks: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        ad = dict((k.lower(), v or "") for k, v in attrs)
        t = tag.lower()
        if t in ("script", "style"):
            self._skip_depth += 1
        if t == "title":
            self.in_title = True
        if t == "meta":
            if ad.get("name", "").lower() == "description" and ad.get("content"):
                self.meta_description = ad["content"].strip()
            if ad.get("property", "").lower() in ("og:description",) and ad.get("content"):
                if not self.meta_description:
                    self.meta_description = ad["content"].strip()
        if t == "a" and ad.get("href"):
            nu = normalize_url(ad["href"], self.base_url)
            if nu:
                self.links.append((nu, ad.get("rel", "")))
        if t == "link" and ad.get("href"):
            nu = normalize_url(ad["href"], self.base_url)
            if nu:
                self.links.append((nu, ad.get("rel", "")))
        if t == "script" and ad.get("src"):
            nu = normalize_url(ad["src"], self.base_url)
            if nu:
                self.script_src.append(nu)
        if t == "iframe" and ad.get("src"):
            nu = normalize_url(ad["src"], self.base_url)
            if nu:
                self.iframe_src.append(nu)

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in ("script", "style") and self._skip_depth > 0:
            self._skip_depth -= 1
        if t == "title":
            self.in_title = False

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        if self.in_title:
            self.title_parts.append(data)
        if data.strip():
            self.text_chunks.append(data)

    def plain_text(self) -> str:
        return re.sub(r"\s+", " ", "".join(self.text_chunks)).strip()

    def title(self) -> str:
        return re.sub(r"\s+", " ", "".join(self.title_parts)).strip()


def extract_links_from_html(html: bytes | str, base_url: str) -> list[str]:
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    p = LinkCollectingParser(base_url)
    try:
        p.feed(text)
        p.close()
    except Exception:
        pass
    seen: set[str] = set()
    out: list[str] = []
    for u in p.links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def parse_signals(html: bytes | str, base_url: str) -> dict[str, Any]:
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    p = SignalParser(base_url)
    try:
        p.feed(text)
        p.close()
    except Exception:
        pass
    plain = p.plain_text()
    emails = sorted(set(EMAIL_RE.findall(plain)))
    return {
        "title": p.title(),
        "meta_description": p.meta_description,
        "plain_text_sample": plain[:8000],
        "links": p.links,
        "script_src": p.script_src,
        "iframe_src": p.iframe_src,
        "emails": emails,
    }
