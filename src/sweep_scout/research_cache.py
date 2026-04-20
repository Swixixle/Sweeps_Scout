"""Local JSON cache for provider outputs (cheap repeat runs)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sweep_scout.utils import repo_root, sha256_text, utc_now_iso


DEFAULT_PROMPT_VERSION = "v1"


def cache_root_default() -> Path:
    return repo_root() / "data" / "candidates" / "research_cache"


@dataclass
class CacheEntry:
    candidate_id: str
    provider_name: str
    brand_signature: str
    prompt_version: str
    created_at: str
    payload: dict[str, Any]


class ResearchCache:
    """
    One file per (candidate_id, provider, prompt_version, brand) signature.
    """

    def __init__(self, root: Path | None = None, *, prompt_version: str = DEFAULT_PROMPT_VERSION):
        self.root = root or cache_root_default()
        self.prompt_version = prompt_version
        self.root.mkdir(parents=True, exist_ok=True)

    def _signature(self, brand: str, candidate_domains: list[str]) -> str:
        dom = "|".join(sorted(candidate_domains))[:200]
        return f"{brand.strip()}|{dom}"

    def _file_stem(self, candidate_id: str, provider_name: str, brand_signature: str) -> str:
        h = sha256_text(f"{candidate_id}|{provider_name}|{self.prompt_version}|{brand_signature}")[:32]
        safe_provider = "".join(c if c.isalnum() else "_" for c in provider_name)[:40]
        return f"{candidate_id}_{safe_provider}_{h}"

    def path_for(
        self,
        candidate_id: str,
        provider_name: str,
        brand: str,
        candidate_domains: list[str],
    ) -> Path:
        sig = self._signature(brand, candidate_domains)
        return self.root / f"{self._file_stem(candidate_id, provider_name, sig)}.json"

    def get(
        self,
        candidate_id: str,
        provider_name: str,
        brand: str,
        candidate_domains: list[str],
    ) -> dict[str, Any] | None:
        p = self.path_for(candidate_id, provider_name, brand, candidate_domains)
        if not p.is_file():
            return None
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
        if raw.get("prompt_version") != self.prompt_version:
            return None
        return raw

    def set(
        self,
        candidate_id: str,
        provider_name: str,
        brand: str,
        candidate_domains: list[str],
        payload: dict[str, Any],
    ) -> Path:
        p = self.path_for(candidate_id, provider_name, brand, candidate_domains)
        sig = self._signature(brand, candidate_domains)
        entry = {
            "candidate_id": candidate_id,
            "provider_name": provider_name,
            "brand_signature": sig,
            "prompt_version": self.prompt_version,
            "created_at": utc_now_iso(),
            "payload": payload,
        }
        p.write_text(json.dumps(entry, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return p

    def invalidate(
        self,
        candidate_id: str,
        provider_name: str,
        brand: str,
        candidate_domains: list[str],
    ) -> bool:
        p = self.path_for(candidate_id, provider_name, brand, candidate_domains)
        if p.is_file():
            try:
                p.unlink()
                return True
            except OSError:
                return False
        return False
