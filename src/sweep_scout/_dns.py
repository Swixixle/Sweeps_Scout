"""Minimal stdlib DNS client (UDP) for A/AAAA via getaddrinfo and NS/MX via wire format."""
from __future__ import annotations

import logging
import random
import socket
import struct
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any

logger = logging.getLogger(__name__)

DNS_TYPE_NS = 2
DNS_TYPE_MX = 15
DNS_CLASS_IN = 1

_DEFAULT_SERVER = ("8.8.8.8", 53)


def _encode_hostname(fqdn: str) -> bytes:
    fqdn = fqdn.rstrip(".").lower()
    if not fqdn:
        return b"\x00"
    out = bytearray()
    for label in fqdn.split("."):
        enc = label.encode("idna")
        if len(enc) > 63:
            raise ValueError("dns label too long")
        out.append(len(enc))
        out.extend(enc)
    out.append(0)
    return bytes(out)


def _build_query(qname: str, qtype: int, qid: int | None = None) -> tuple[bytes, int]:
    if qid is None:
        qid = random.randint(1, 65535)
    flags = 0x0100  # RD
    header = struct.pack("!HHHHHH", qid, flags, 1, 0, 0, 0)
    body = _encode_hostname(qname) + struct.pack("!HH", qtype, DNS_CLASS_IN)
    return header + body, qid


def _read_label_list(msg: bytes, off: int, depth: int = 0) -> tuple[list[str], int]:
    if depth > 32:
        raise ValueError("dns name loop or depth exceeded")
    labels: list[str] = []
    while True:
        if off >= len(msg):
            raise ValueError("dns truncated message")
        n = msg[off]
        if n == 0:
            return labels, off + 1
        if (n & 0xC0) == 0xC0:
            if off + 1 >= len(msg):
                raise ValueError("dns truncated pointer")
            ptr = ((n & 0x3F) << 8) | msg[off + 1]
            if ptr >= len(msg):
                raise ValueError("dns bad compression pointer")
            sub, _ = _read_label_list(msg, ptr, depth + 1)
            return labels + sub, off + 2
        off += 1
        if off + n > len(msg):
            raise ValueError("dns truncated label")
        labels.append(msg[off : off + n].decode("ascii", errors="replace"))
        off += n


def _decode_dns_name(msg: bytes, off: int, depth: int = 0) -> tuple[str, int]:
    parts, end = _read_label_list(msg, off, depth)
    return ".".join(parts), end


def _skip_question(msg: bytes, off: int) -> int:
    _, off = _decode_dns_name(msg, off)
    return off + 4


def _parse_rr(
    msg: bytes,
    off: int,
    want_type: int,
) -> tuple[list[Any], int]:
    """Parse one resource record starting at off; return (extracted payloads, next offset)."""
    _, off = _decode_dns_name(msg, off)
    if off + 10 > len(msg):
        raise ValueError("dns truncated rr header")
    rtype, _rclass, _ttl, rdlength = struct.unpack_from("!HHIH", msg, off)
    off += 10
    if off + rdlength > len(msg):
        raise ValueError("dns truncated rdata")
    rdata = msg[off : off + rdlength]
    off += rdlength
    out: list[Any] = []
    if rtype == want_type:
        if want_type == DNS_TYPE_NS:
            name, _ = _decode_dns_name(msg, off - rdlength)
            out.append(name)
        elif want_type == DNS_TYPE_MX:
            if rdlength < 3:
                raise ValueError("dns mx rdata too short")
            pref = struct.unpack_from("!H", rdata, 0)[0]
            host, _ = _decode_dns_name(msg, off - rdlength + 2)
            if not host:
                host = "."
            out.append({"priority": int(pref), "host": host})
    return out, off


def _collect_rr_of_type(
    msg: bytes,
    off: int,
    count: int,
    want_type: int,
) -> tuple[list[Any], int]:
    acc: list[Any] = []
    for _ in range(count):
        batch, off = _parse_rr(msg, off, want_type)
        acc.extend(batch)
    return acc, off


def _dns_exchange(
    qname: str,
    qtype: int,
    *,
    timeout: float,
    server: tuple[str, int] = _DEFAULT_SERVER,
) -> tuple[bytes | None, str | None]:
    pkt, qid = _build_query(qname, qtype)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(timeout)
        sock.sendto(pkt, server)
        data, _ = sock.recvfrom(65535)
    except socket.timeout:
        return None, "udp timeout"
    except OSError as e:
        logger.warning("dns udp error for %s type %s: %s", qname, qtype, e)
        return None, str(e)
    finally:
        sock.close()

    if len(data) < 12:
        return None, "dns response too short"
    rid, flags, _qdcount, _ancount, _nscount, _arcount = struct.unpack("!HHHHHH", data[:12])
    if rid != qid:
        return None, "dns transaction id mismatch"
    rcode = flags & 0x000F
    if rcode != 0:
        return None, f"dns rcode {rcode}"
    if flags & 0x0200:
        logger.warning("dns response truncated (TC) for %s type %s", qname, qtype)
    return data, None


def _merge_mx(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[int, str]] = set()
    out: list[dict[str, Any]] = []
    for r in records:
        host = str(r["host"]).lower()
        if host != ".":
            host = host.rstrip(".")
        key = (int(r["priority"]), host)
        if key not in seen:
            seen.add(key)
            out.append({"priority": key[0], "host": host})
    out.sort(key=lambda x: (x["priority"], x["host"]))
    return out


def _merge_ns(records: list[str]) -> list[str]:
    norm: set[str] = set()
    for h in records:
        if not h:
            continue
        x = h.lower().rstrip(".")
        norm.add(x if x else ".")
    return sorted(norm)


def _gai_family(host: str, family: int, timeout: float) -> tuple[list[Any] | None, str | None]:
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(socket.getaddrinfo, host, None, family, socket.SOCK_STREAM)
            try:
                return fut.result(timeout=timeout), None
            except FuturesTimeoutError:
                return None, "timeout"
    except socket.gaierror as e:
        return None, str(e)
    except OSError as e:
        logger.warning("getaddrinfo OSError for %s family %s: %s", host, family, e)
        return None, str(e)


def resolve_a_aaaa(
    host: str,
    *,
    timeout: float,
) -> tuple[list[str], list[str], str | None, str | None]:
    """Return (A list, AAAA list, error_a, error_aaaa)."""
    a_list: list[str] = []
    aaaa_list: list[str] = []

    infos4, err4 = _gai_family(host, socket.AF_INET, timeout)
    if infos4:
        for info in infos4:
            if info[0] == socket.AF_INET:
                addr = info[4][0]
                if addr not in a_list:
                    a_list.append(addr)

    infos6, err6 = _gai_family(host, socket.AF_INET6, timeout)
    if infos6:
        for info in infos6:
            if info[0] == socket.AF_INET6:
                addr = info[4][0]
                if addr not in aaaa_list:
                    aaaa_list.append(addr)

    err_a = err4 if not a_list else None
    if not aaaa_list:
        # IPv4-only names often return an error for AF_INET6; treat as non-failure.
        err_aaaa = None if a_list else err6
    else:
        err_aaaa = None

    a_list.sort()
    aaaa_list.sort()
    return a_list, aaaa_list, err_a, err_aaaa


def query_ns(
    fqdn: str,
    *,
    timeout: float,
    server: tuple[str, int] = _DEFAULT_SERVER,
) -> tuple[list[str], str | None]:
    raw, err = _dns_exchange(fqdn, DNS_TYPE_NS, timeout=timeout, server=server)
    if err or raw is None:
        return [], err or "dns exchange failed"
    _rid, flags, qdcount, ancount, nscount, arcount = struct.unpack("!HHHHHH", raw[:12])
    _ = flags
    off = 12
    try:
        for _ in range(qdcount):
            off = _skip_question(raw, off)
        acc, off = _collect_rr_of_type(raw, off, ancount, DNS_TYPE_NS)
        acc2, off = _collect_rr_of_type(raw, off, nscount, DNS_TYPE_NS)
        acc3, _ = _collect_rr_of_type(raw, off, arcount, DNS_TYPE_NS)
    except ValueError as e:
        logger.warning("dns ns parse %s: %s", fqdn, e)
        return [], str(e)
    hosts = [str(x) for x in acc + acc2 + acc3]
    return _merge_ns(hosts), None


def query_mx(
    fqdn: str,
    *,
    timeout: float,
    server: tuple[str, int] = _DEFAULT_SERVER,
) -> tuple[list[dict[str, Any]], str | None]:
    raw, err = _dns_exchange(fqdn, DNS_TYPE_MX, timeout=timeout, server=server)
    if err or raw is None:
        return [], err or "dns exchange failed"
    _rid, flags, qdcount, ancount, nscount, arcount = struct.unpack("!HHHHHH", raw[:12])
    _ = flags
    off = 12
    try:
        for _ in range(qdcount):
            off = _skip_question(raw, off)
        acc, off = _collect_rr_of_type(raw, off, ancount, DNS_TYPE_MX)
        acc2, off = _collect_rr_of_type(raw, off, nscount, DNS_TYPE_MX)
        acc3, _ = _collect_rr_of_type(raw, off, arcount, DNS_TYPE_MX)
    except ValueError as e:
        logger.warning("dns mx parse %s: %s", fqdn, e)
        return [], str(e)
    rows = [x for x in acc + acc2 + acc3 if isinstance(x, dict)]
    return _merge_mx(rows), None
