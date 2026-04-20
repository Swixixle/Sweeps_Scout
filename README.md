# Sweeps_Scout

Sweeps_Scout is the **active discovery and monitoring** tool in the sweeps ecosystem. It hunts sites, crawls a bounded scope, extracts lightweight signals, and **proposes candidate intelligence** for human review elsewhere.

It is **not** reviewed truth, **not** a duplicate of Sweeps_Intel normalized schemas as source of truth, and **not** a blocking or enforcement layer.

| Repo | Role |
|------|------|
| **Sweeps_Scout** (this repo) | Hunter / crawler / monitor — candidates and evidence |
| **Sweeps_Intel** | Reviewed intelligence / curated truth / affiliations |
| **Sweeps_Relief** | Enforcement / blocking / on-device receipts |

## Quick start

From the repository root (after installing the package):

```bash
pip install -e ".[dev]"
python3 -m sweep_scout.monitor --repo-root .
```

Edit `data/seeds/seed_urls.txt` with real seed URLs (one per line). Optional: `allow_domains.txt`, `deny_domains.txt`, `bootstrap_domains.txt`.

## CLI

| Command | Purpose |
|---------|---------|
| `python3 -m sweep_scout.discover --repo-root .` | Crawl seeds, discover domains/pages/redirects |
| `python3 -m sweep_scout.extract --repo-root .` | Fetch discovered URLs and extract signals |
| `python3 -m sweep_scout.redirects --repo-root .` | Merge HTTP + phrase-based redirect candidates |
| `python3 -m sweep_scout.classifier --repo-root .` | Rule-based type hints |
| `python3 -m sweep_scout.emit --repo-root .` | Emit `proposed_entities` / `proposed_relationships` |
| `python3 -m sweep_scout.monitor --repo-root .` | Run discover → extract → redirects → classifier → emit (+ optional intel bridge) |

Optional: set `SWEEPS_INTEL_SNAPSHOT` or place `data/intel_snapshot.json` to compare discoveries to a Sweeps_Intel export.

## Outputs (candidates, not truth)

Under `data/candidates/` and `reports/`:

- `discovered_domains.json`, `discovered_pages.json`, `discovered_redirects.json`
- `extracted_signals.json`, `classification_hints.json`, `intel_bridge.json`
- `proposed_entities.json`, `proposed_relationships.json`
- Timestamped reports under `reports/discovery/`, `reports/extraction/`, `reports/monitoring/`

**Warning:** All JSON outputs are **candidate intelligence** and **proposals**. Sweeps_Intel remains the review gate; Sweeps_Scout does not write into Sweeps_Intel production seeds or truth files.

## Design

- Conservative crawling, bounded depth (default 1), deterministic JSON
- Full provenance (source URLs preserved)
- Stdlib-first Python 3; no ML, no CT logs in v1, no browser automation unless you add it behind an explicit flag later
