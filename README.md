# Sweeps_Scout

Sweeps_Scout is the discovery-side component of a three-repo sweepstakes and social-casino accountability pipeline. It crawls seed URLs, extracts page-level signals, fingerprints domains with DNS and TLS metadata, and emits candidate entities and relationships for review in **Sweeps_Intel**. The codebase is **stdlib-only at runtime** (`dependencies = []` in `pyproject.toml`), local-first, and writes **deterministic JSON** artifacts.

Scout does not ship reviewed truth, blocklists, or enforcement—those live in Intel and Relief.

```mermaid
flowchart LR
  discover --> extract --> fingerprint --> redirects --> classifier --> emit --> intel_bridge
```

## Pipeline stages (monitor)

The default `monitor` run executes these steps in order:

| Step | What it does |
|:-----|:-------------|
| **discover** | Fetches seed URLs and bounded outbound links; records pages, HTTP redirects, and unique domains under allow/deny rules. |
| **extract** | Re-fetches representative URLs per domain, parses HTML for titles, links, keyword hints, and related domains (scripts/iframes). |
| **fingerprint** | Per **normalized domain** (not per URL): resolves A/AAAA via `getaddrinfo`, NS/MX via a small UDP DNS client to `8.8.8.8`, and TLS subject/issuer/SAN/validity via `ssl` (with optional **`openssl` CLI** parsing when `getpeercert()` returns an empty dict, e.g. Python 3.14). All network code is stdlib. |
| **redirects** | Merges HTTP and phrase-based redirect hints into normalized redirect records. |
| **classifier** | Rule-based type hints from extracted and discovery data. |
| **emit** | Builds `proposed_entities.json` and `proposed_relationships.json` for Intel review. |
| **intel_bridge** | Compares discovered domains to an optional Intel snapshot (`SWEEPS_INTEL_SNAPSHOT` or `data/intel_snapshot.json`); does not write Intel production data. |

## Status

| Area | Status | Notes |
|:-----|:------:|:------|
| Monitor pipeline (steps above) | 🟢 | Primary local workflow; sequential HTTP, bounded depth. |
| Fingerprint stage | 🟢 | `domain_fingerprints.json`; graceful `errors` + `partial` per domain. |
| Research orchestrator (`research_orchestrator`) | 🟡 | Optional LLM mesh; **not** invoked by `monitor`. |
| Standalone stage CLIs | 🟢 | `discover`, `extract`, `fingerprint`, `redirects`, `classifier`, `emit`, `intel_bridge`, `monitor`. |
| Intake / verify / dedupe CLIs | 🟡 | `intake_*`, `normalize_candidates`, `dedupe_candidates`, `bucket_candidates`, `verify_*`, `verification_queue`, etc.—separate bulk and web workflows. |
| Debug fetch | ⚪ | `python -m sweep_scout.fetch` for one-off URL inspection. |

## Install

Requires Python 3.10 or newer.

```bash
git clone https://github.com/Swixixle/Sweeps_Scout.git
cd Sweeps_Scout
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest
```

Runtime **`dependencies = []`** is intentional: new features that touch the network should stay stdlib (see `src/sweep_scout/_dns.py`, `src/sweep_scout/_tls.py`). Dev-only tools use `[project.optional-dependencies] dev` (e.g. pytest).

## Running the pipeline

Configure seeds under `data/seeds/` (`seed_urls.txt`; optional `allow_domains.txt`, `deny_domains.txt`, `bootstrap_domains.txt`).

**Full monitor run** (all stages; writes per-step summaries and a top-level report):

```bash
python -m sweep_scout.monitor --repo-root . --depth 1 --max-pages 100
```

- Artifacts: `data/candidates/*.json` (domains, signals, fingerprints, proposals, …).
- Reports: `reports/monitoring/monitor-<timestamp>.json`, plus `reports/discovery/`, `reports/extraction/`, `reports/fingerprinting/` as each stage runs.

**Single stage (example: extract only):**

```bash
python -m sweep_scout.extract --repo-root .
```

Outputs include `data/candidates/extracted_signals.json` and `reports/extraction/extract-<timestamp>.json`.

**Fingerprint only:**

```bash
python -m sweep_scout.fingerprint --repo-root . --max-domains 500
```

Writes `data/candidates/domain_fingerprints.json` and `reports/fingerprinting/fingerprint-<timestamp>.json`.

## Outputs — what Intel consumes

Scout’s canonical outputs live under **`data/candidates/`** in this repo. For **Sweeps_Intel**, treat **`data/research_candidates/scout_import/`** (in the **Intel** repository) as the canonical drop for imports: copy or sync the JSON (and any CSV your importer expects) there. Typical handoffs:

| Scout artifact | Intel use (conceptual) |
|:---------------|:------------------------|
| `domain_fingerprints.json` | Operator graph / clustering (shared NS, SAN overlap, MX patterns). |
| `discovered_domains.json` | Candidate domain import with provenance. |
| `proposed_entities.json`, `proposed_relationships.json` | Review queue and graph proposals. |

Intel remains the review gate; Scout never writes Intel’s production truth files.

## Research orchestrator (separate plane)

`python -m sweep_scout.research_orchestrator` runs a tiered research mesh (deterministic local pass plus optional external providers). External calls use **Perplexity**, **Anthropic**, and/or **Gemini** and require API keys in the environment (`PERPLEXITY_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY` or `GOOGLE_API_KEY`). This path is **opt-in** and separate from the default `monitor` pipeline.

## Architecture

- **[Sweeps_Intel](https://github.com/Swixixle/Sweeps_Intel)** — Reviews Scout candidates, clusters sibling brands, maintains curated intelligence and the signed blocklist.
- **[Sweeps_Relief](https://github.com/Swixixle/Sweeps_Relief)** — Enforcement client that consumes Intel’s signed artifacts.

## Development notes

- **Zero runtime deps** — keep new I/O in the standard library unless the project explicitly changes policy.
- **Logging** — module-level `logging.getLogger(__name__)`; narrow catches (`socket.timeout`, `gaierror`, `SSLError`, `OSError`) where appropriate; avoid bare `except`.
- **Determinism** — JSON via `deterministic_json_dumps` (`sort_keys`, stable formatting) for reproducible artifacts.
- **History** — prefer small, scoped commits; messages should read as an audit trail.

## Limitations

- Crawl **depth and page caps** are conservative by design (default depth 1)—this is not a general-purpose site crawler.
- **Fingerprint TLS** may depend on a system **`openssl`** binary when Python’s `getpeercert()` dict is empty; failures surface in `errors.tls`.
- **No async HTTP** under the zero-dep rule; fetches are sequential.
- **Discovery is best-effort**—unknown operators stay invisible until their domains appear in seeds, lists, or future intake sources.

## License

See the LICENSE file at the repo root.
