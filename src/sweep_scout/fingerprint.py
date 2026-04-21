"""Per-domain DNS + TLS fingerprinting (network code uses stdlib; optional Ed25519 signing via cryptography)."""
from __future__ import annotations

import argparse
import json
import logging
import socket
import ssl
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sweep_scout._dns import query_mx, query_ns, resolve_a_aaaa
from sweep_scout._signing import (
    SigningKeyError,
    generate_keypair,
    load_private_key_pem,
    sign_envelope,
)
from sweep_scout._tls import fetch_peer_cert_meta
from sweep_scout.config import ensure_dirs, paths_for_repo
from sweep_scout.utils import deterministic_json_dumps, normalize_host, utc_now_iso

logger = logging.getLogger(__name__)


def _fingerprint_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _generated_at_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _empty_tls() -> dict[str, Any]:
    return {
        "subject_cn": None,
        "issuer_cn": None,
        "san": [],
        "not_before": None,
        "not_after": None,
    }


def fingerprint_domain(
    domain: str,
    *,
    dns_timeout: float = 5.0,
    tls_timeout: float = 5.0,
) -> dict[str, Any]:
    """Fingerprint one domain; always returns a dict (graceful degradation)."""
    host = normalize_host(domain)
    ts = _fingerprint_ts()
    errors: dict[str, str | None] = {
        "dns_a": None,
        "dns_aaaa": None,
        "dns_ns": None,
        "dns_mx": None,
        "tls": None,
    }
    dns_block: dict[str, Any] = {"a": [], "aaaa": [], "ns": [], "mx": []}
    tls_block: dict[str, Any] = _empty_tls()
    partial = False

    if not host:
        errors["dns_a"] = "empty domain"
        errors["dns_aaaa"] = "empty domain"
        errors["dns_ns"] = "empty domain"
        errors["dns_mx"] = "empty domain"
        errors["tls"] = "empty domain"
        return {
            "domain": host,
            "fingerprinted_at": ts,
            "dns": dns_block,
            "tls": tls_block,
            "errors": errors,
            "partial": True,
        }

    try:
        a_rec, aaaa_rec, err_a, err_aaaa = resolve_a_aaaa(host, timeout=dns_timeout)
        dns_block["a"] = a_rec
        dns_block["aaaa"] = aaaa_rec
        if err_a:
            errors["dns_a"] = err_a
            partial = True
        if err_aaaa:
            errors["dns_aaaa"] = err_aaaa
            partial = True

        ns_rec, err_ns = query_ns(host, timeout=dns_timeout)
        dns_block["ns"] = ns_rec
        if err_ns:
            errors["dns_ns"] = err_ns
            partial = True

        mx_rec, err_mx = query_mx(host, timeout=dns_timeout)
        dns_block["mx"] = mx_rec
        if err_mx:
            errors["dns_mx"] = err_mx
            partial = True

        tls_meta, err_tls = fetch_peer_cert_meta(host, timeout=tls_timeout)
        if err_tls:
            errors["tls"] = err_tls
            partial = True
        elif tls_meta:
            tls_block = tls_meta
    except socket.timeout as e:
        logger.warning("fingerprint timeout for %s: %s", host, e)
        errors["dns_a"] = errors["dns_a"] or str(e)
        errors["dns_aaaa"] = errors["dns_aaaa"] or str(e)
        partial = True
    except socket.gaierror as e:
        logger.warning("fingerprint gaierror for %s: %s", host, e)
        errors["dns_a"] = errors["dns_a"] or str(e)
        errors["dns_aaaa"] = errors["dns_aaaa"] or str(e)
        partial = True
    except ssl.SSLError as e:
        logger.warning("fingerprint ssl error for %s: %s", host, e)
        errors["tls"] = errors["tls"] or str(e)
        partial = True
    except OSError as e:
        logger.warning("fingerprint oserror for %s: %s", host, e)
        errors["dns_a"] = errors["dns_a"] or str(e)
        partial = True
    except Exception:
        logger.exception("fingerprint unexpected error for %s", host)
        partial = True
        for k in errors:
            if errors[k] is None:
                errors[k] = "unexpected error"

    return {
        "domain": host,
        "fingerprinted_at": ts,
        "dns": dns_block,
        "tls": tls_block,
        "errors": errors,
        "partial": partial,
    }


def run_fingerprint(
    repo_root: Path,
    *,
    max_domains: int = 500,
    sign: bool = False,
    private_key_path: Path | None = None,
    key_id: str = "scout-fingerprint-key-v1",
) -> dict[str, Any]:
    if sign and private_key_path is None:
        raise SigningKeyError("private_key_path is required when sign=True")

    ensure_dirs(repo_root)
    paths = paths_for_repo(repo_root)
    ts = utc_now_iso()

    rows: list[dict[str, Any]] = []
    if paths["discovered_domains"].is_file():
        rows = json.loads(paths["discovered_domains"].read_text(encoding="utf-8"))

    seen: set[str] = set()
    domains: list[str] = []
    for row in rows:
        d = row.get("domain") or ""
        h = normalize_host(str(d))
        if h and h not in seen:
            seen.add(h)
            domains.append(h)
        if len(domains) >= max_domains:
            break

    domains.sort()

    records: list[dict[str, Any]] = []
    partial_n = 0
    for d in domains:
        rec = fingerprint_domain(d)
        records.append(rec)
        if rec.get("partial"):
            partial_n += 1

    if sign:
        assert private_key_path is not None
        private_key = load_private_key_pem(private_key_path.resolve())
        payload: dict[str, Any] = {
            "artifact_type": "domain_fingerprints",
            "generated_at": _generated_at_z(),
            "fingerprints": {rec["domain"]: rec for rec in records},
        }
        envelope = sign_envelope(payload, private_key, key_id)
        paths["domain_fingerprints"].write_text(deterministic_json_dumps(envelope), encoding="utf-8")
    else:
        paths["domain_fingerprints"].write_text(deterministic_json_dumps(records), encoding="utf-8")

    paths["reports_fingerprinting"].mkdir(parents=True, exist_ok=True)
    rep: dict[str, Any] = {
        "run_at": ts,
        "domains_processed": len(records),
        "partial_count": partial_n,
        "max_domains": max_domains,
        "output": str(paths["domain_fingerprints"]),
    }
    if sign:
        rep["signed"] = True
        rep["key_id"] = key_id
    rep_path = paths["reports_fingerprinting"] / f"fingerprint-{ts.replace(':', '-')}.json"
    rep_path.write_text(deterministic_json_dumps(rep), encoding="utf-8")

    return rep


def main_cli() -> None:
    p = argparse.ArgumentParser(description="Fingerprint domains (DNS + TLS)")
    p.add_argument("--repo-root", default=".")
    p.add_argument("--max-domains", type=int, default=500)
    p.add_argument("--sign", action="store_true", help="Write signed envelope to domain_fingerprints.json")
    p.add_argument("--private-key", type=Path, default=None, help="PEM Ed25519 private key (required with --sign)")
    p.add_argument("--key-id", default="scout-fingerprint-key-v1", help="Key id for signature envelope")
    p.add_argument(
        "--generate-keypair",
        type=Path,
        default=None,
        metavar="DIR",
        help="Write new Ed25519 private.pem and public.pem in DIR and exit",
    )
    args = p.parse_args()
    if args.generate_keypair is not None:
        priv, pub = generate_keypair(args.generate_keypair.resolve())
        print(
            deterministic_json_dumps(
                {"ok": True, "private_key": str(priv), "public_key": str(pub), "key_id": args.key_id}
            )
        )
        return
    if args.sign and args.private_key is None:
        logger.error("--private-key is required when --sign is set")
        sys.exit("error: --private-key is required when --sign is set")
    root = Path(args.repo_root).resolve()
    try:
        r = run_fingerprint(
            root,
            max_domains=args.max_domains,
            sign=args.sign,
            private_key_path=args.private_key,
            key_id=args.key_id,
        )
    except SigningKeyError as e:
        logger.error("signing failed: %s", e)
        sys.exit(f"error: {e}")
    print(deterministic_json_dumps(r))


if __name__ == "__main__":
    main_cli()
