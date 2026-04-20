"""TLS certificate peer metadata via stdlib ssl (+ openssl CLI for PEM parsing when needed)."""
from __future__ import annotations

import logging
import re
import shutil
import socket
import ssl
import subprocess
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _cn_from_subject_tuple(subject: Any) -> str | None:
    if not subject:
        return None
    try:
        for rdn in subject:
            for attr, val in rdn:
                if attr == "commonName":
                    return str(val)
    except (TypeError, ValueError) as e:
        logger.warning("tls subject cn parse: %s", e)
    return None


def _san_list_from_cert_dict(cert: dict[str, Any]) -> list[str]:
    raw = cert.get("subjectAltName")
    if not raw:
        return []
    names: list[str] = []
    try:
        for kind, val in raw:
            if kind == "DNS":
                names.append(str(val).lower())
    except (TypeError, ValueError) as e:
        logger.warning("tls san parse: %s", e)
    return sorted(set(names))


def _cert_time_to_iso(value: str | None) -> str | None:
    if not value:
        return None
    s = " ".join(value.split())
    try:
        dt = datetime.strptime(s, "%b %d %H:%M:%S %Y GMT").replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    logger.warning("tls cert time unparseable: %r", value)
    return None


def _cn_from_openssl_dn(dn_line: str) -> str | None:
    # subject=CN = example.com  OR  subject=C=US, O=Org, CN=example.com
    m = re.findall(r"CN\s*=\s*([^,]+?)(?=,|$)", dn_line, flags=re.I)
    if not m:
        return None
    return m[-1].strip()


def _parse_openssl_text(
    subject_out: str,
    issuer_out: str,
    dates_out: str,
    san_out: str,
) -> dict[str, Any]:
    subj_line = next((ln for ln in subject_out.splitlines() if ln.lower().startswith("subject=")), "")
    iss_line = next((ln for ln in issuer_out.splitlines() if ln.lower().startswith("issuer=")), "")
    nb_m = re.search(r"notBefore=(.+)", dates_out)
    na_m = re.search(r"notAfter=(.+)", dates_out)
    nb_raw = nb_m.group(1).strip() if nb_m else None
    na_raw = na_m.group(1).strip() if na_m else None
    sans = sorted(set(re.findall(r"DNS:([^,\s]+)", san_out, flags=re.I)))
    return {
        "subject_cn": _cn_from_openssl_dn(subj_line),
        "issuer_cn": _cn_from_openssl_dn(iss_line),
        "san": [s.lower() for s in sans],
        "not_before": _cert_time_to_iso(nb_raw),
        "not_after": _cert_time_to_iso(na_raw),
    }


def _meta_from_pem_openssl(pem: str, *, timeout: float) -> tuple[dict[str, Any] | None, str | None]:
    if not shutil.which("openssl"):
        return None, "openssl binary not found (needed for PEM parsing on this Python)"
    pem_b = pem.encode("ascii") if isinstance(pem, str) else pem
    try:
        sub = subprocess.run(
            ["openssl", "x509", "-inform", "PEM", "-noout", "-subject"],
            input=pem_b,
            capture_output=True,
            timeout=timeout,
            check=True,
        )
        iss = subprocess.run(
            ["openssl", "x509", "-inform", "PEM", "-noout", "-issuer"],
            input=pem_b,
            capture_output=True,
            timeout=timeout,
            check=True,
        )
        dates = subprocess.run(
            ["openssl", "x509", "-inform", "PEM", "-noout", "-dates"],
            input=pem_b,
            capture_output=True,
            timeout=timeout,
            check=True,
        )
        san = subprocess.run(
            ["openssl", "x509", "-inform", "PEM", "-noout", "-ext", "subjectAltName"],
            input=pem_b,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError:
        return None, "openssl binary not found"
    except subprocess.TimeoutExpired:
        return None, f"timeout after {timeout}s"
    except subprocess.CalledProcessError as e:
        return None, f"openssl error: {(e.stderr or b'').decode('utf-8', errors='replace')[:200]}"

    san_text = (san.stdout + san.stderr).decode("utf-8", errors="replace")
    meta = _parse_openssl_text(
        sub.stdout.decode("utf-8", errors="replace"),
        iss.stdout.decode("utf-8", errors="replace"),
        dates.stdout.decode("utf-8", errors="replace"),
        san_text,
    )
    return meta, None


def fetch_peer_cert_meta(
    host: str,
    *,
    timeout: float,
) -> tuple[dict[str, Any] | None, str | None]:
    """
    Return TLS metadata dict matching fingerprint schema tls block, or (None, error).
    """
    pem: str | None = None
    try:
        pem = ssl.get_server_certificate((host, 443), timeout=timeout)
    except socket.timeout:
        return None, f"timeout after {timeout}s"
    except socket.gaierror as e:
        return None, str(e)
    except ssl.SSLError as e:
        return None, str(e)
    except OSError as e:
        return None, str(e)

    if not pem:
        return None, "no peer certificate"

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        with socket.create_connection((host, 443), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                cert = ssock.getpeercert()
    except socket.timeout:
        return None, f"timeout after {timeout}s"
    except socket.gaierror as e:
        return None, str(e)
    except ssl.SSLError as e:
        return None, str(e)
    except OSError as e:
        return None, str(e)

    if cert:
        out: dict[str, Any] = {
            "subject_cn": _cn_from_subject_tuple(cert.get("subject")),
            "issuer_cn": _cn_from_subject_tuple(cert.get("issuer")),
            "san": _san_list_from_cert_dict(cert),
            "not_before": _cert_time_to_iso(cert.get("notBefore")),
            "not_after": _cert_time_to_iso(cert.get("notAfter")),
        }
        return out, None

    meta, err = _meta_from_pem_openssl(pem, timeout=timeout)
    if meta:
        return meta, None
    return None, err or "no peer certificate metadata (empty cert dict and openssl parse failed)"
