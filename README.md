# wb-api-tools

[![tests](https://github.com/jpazvd/wb-api-repo/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/jpazvd/wb-api-repo/actions/workflows/tests.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://badge.fury.io/py/wb-api-tools.svg)](https://pypi.org/project/wb-api-tools/)
[![Stata 16+](https://img.shields.io/badge/Stata-16+-1a5276.svg)](https://www.stata.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Package: **`wb-api-tools`** on PyPI — repo: [`jpazvd/wb-api-repo`](https://github.com/jpazvd/wb-api-repo).

World Bank Open Data helpers in **Python** (library + CLI) and **Stata**
(`wbopendata` ado package). Two surfaces over the same WB API v2, with a
shared YAML metadata cache so discovery commands stay fast and offline-safe.

Python package: **[`wb-api-tools` on PyPI](https://pypi.org/project/wb-api-tools/)** —
`pip install wb-api-tools` (live release version shown in the PyPI badge above).
Parallel v0.x track to the upstream [Stata `wbopendata`](https://github.com/jpazvd/wbopendata)
package (Stata Journal lineage, v18.x).

## What's here

| Surface | Entry point | Reference |
| --- | --- | --- |
| Python library | `wb_api_tools.{discovery,data,text}` (re-exported at package root) | [docs/PYTHON_USER_GUIDE.md](docs/PYTHON_USER_GUIDE.md) |
| Python CLI | `wb-api-tools <subcmd>` (after install) or `python -m wb_api_tools <subcmd>` | `--help` on every subcommand |
| Stata package | `src/w/wbopendata.ado` (v17.4.0) | `help wbopendata` in Stata, or `src/w/wbopendata.sthlp` |
| YAML metadata cache | `~/.cache/wbopendata/_wbopendata_{indicators,sources,topics}.yaml` (XDG-aware) | populated by `wb-api-tools sync` |

## Install

From PyPI (once published — see `pyproject.toml` for the canonical name):

```bash
pip install wb-api-tools
```

From a git checkout (dev mode):

```bash
git clone https://github.com/jpazvd/wb-api-repo.git
cd wb-api-repo
pip install -e ".[test]"
```

Requires Python 3.11+. The Stata package is loaded by adding `src/w/` and
`src/_/` to Stata's adopath (or installed via `net install` once an SSC
release lands).

## Quick start

The repo ships with a runnable 7-section walkthrough that exercises the
whole Python surface (discovery, live `describe`, `get_data` flag matrix,
`enrich_country_context`, `wb_text` formats):

```bash
PYTHONIOENCODING=utf-8 python examples/demo_pr_b_c.py
```

Captured transcript: [docs/PYTHON_DEMO.md](docs/PYTHON_DEMO.md).

## Python CLI

After `pip install`, use the `wb-api-tools` console script (or
`python -m wb_api_tools` if PATH doesn't include scripts). Each subcommand
has `--help` for full flag descriptions.

| Subcommand | Purpose |
| --- | --- |
| `countries` | Fetch country metadata |
| `indicators` | Fetch indicator metadata (legacy CSV/parquet/yaml dump) |
| `data` | Fetch indicator data; `--no-basic` skips country-context auto-merge, `--geo` adds capital/lat/lon, `--language es` switches the API path |
| `sources` | List WB data sources (`--all` for the full set) |
| `alltopics` | List all WB topic categories |
| `info <id>` | Show full metadata for one indicator (from YAML cache) |
| `describe <id>` | Fetch fresh metadata for one indicator (live API; `--language` supported) |
| `search [term]` | Paginated indicator search; `--source`, `--topic`, `--field`, `--exact` |
| `sync` | Populate / refresh the YAML metadata cache from the live WB API |

Example:

```bash
wb-api-tools data \
    --indicators SP.POP.TOTL,NY.GDP.MKTP.CD \
    --countries "BRA;USA;IND" \
    --date 2010:2020 \
    --geo --long --out _data/wb/pop_gdp_long.csv
```

Output is written to `--out` — six file formats supported by extension:

| Extension | Format | Notes |
| --- | --- | --- |
| `.csv` | Comma-separated | Default fallback for unknown extensions too |
| `.parquet` | Apache Parquet | Columnar; small + fast for analytics |
| `.json` | JSON records, pretty-printed | `[{...}, {...}]` indent=2 |
| `.jsonl` / `.ndjson` | Line-delimited JSON | Streaming-friendly for `jq`, Spark, BigQuery |
| `.yaml` / `.yml` | YAML records | Stata-friendly |

Plus two stdout modes:

- **`--out -`** → full CSV streamed to stdout (pipeable into other tools)
- **`--out` omitted** → 20-row preview to stdout (head only, not parseable)

## Python library

After `pip install`, the package is importable directly — no `sys.path`
hacks needed:

```python
import wb_api_tools as wb

wb.search("poverty headcount", limit=5)
df = wb.get_data(["SP.POP.TOTL"], "BRA;USA;IND", date="2020", geo=True)
wb.wrap("long indicator title ...", width=60, fmt="stack")   # for Stata graph title()
```

Full reference: [docs/PYTHON_USER_GUIDE.md](docs/PYTHON_USER_GUIDE.md)
(library + CLI + Stata-parity table).

## Stata package

`src/w/wbopendata.ado` is the v17.4.0 dispatcher; current Phase-0-through-6
surface mirrors the Python library:

- `wbopendata, sources / allsources / alltopics / info / search / describe`
  discovery commands
- `wbopendata, indicator(X) clear` data fetch with `noBASIC`, `geo`,
  `language(es)`, `cache(days)`, `sync`
- `linewrap(W) maxlength(N) linewrapformat(stack|newline|lines|smcl)`
  for graph-title and SMCL formatting

Open `src/w/wbopendata.sthlp` in Stata's viewer or run `help wbopendata`
once the package is on the adopath. The Python-side
[docs/PYTHON_USER_GUIDE.md](docs/PYTHON_USER_GUIDE.md) §5 has a row-by-row
Stata ↔ Python parity table.

## YAML metadata cache

The offline metadata cache lives in a per-user XDG-aware directory
(typically `~/.cache/wbopendata/` on POSIX or `~/AppData/Local/wbopendata/`
on Windows; override with `$WBOPENDATA_YAML_DIR`):

- `_wbopendata_indicators.yaml` — 29,511 indicators (~18 MB)
- `_wbopendata_sources.yaml` — 71 sources
- `_wbopendata_topics.yaml` — 21 topics

Discovery commands (`info`, `search`, `sources`, `alltopics`) read from
this cache for microsecond lookups. After `pip install`, populate it once:

```bash
wb-api-tools sync                # download + write all three YAMLs (~10 min first time)
wb-api-tools sync --commit --tag # git-commit + tag (dev mode only)
```

A semi-monthly GitHub Action (`.github/workflows/wb_metadata_nightly.yml`
— file name is historical; cron runs on the 1st and 15th of every month
at 02:17 UTC, so 14–17 days apart depending on month length) keeps the
repo-committed cache fresh. Manually triggerable via `workflow_dispatch`.

## Documentation

- [docs/PYTHON_USER_GUIDE.md](docs/PYTHON_USER_GUIDE.md) — Python library + CLI reference (Stata `.sthlp` equivalent)
- [docs/PYTHON_DEMO.md](docs/PYTHON_DEMO.md) — captured live-API transcript from the 7-section walkthrough
- [docs/EXAMPLES.md](docs/EXAMPLES.md) — end-to-end workflows (API, Stata, Python)
- [docs/AGE_BANDS.md](docs/AGE_BANDS.md) — standard 5-year age band codes for population indicators
- [examples/](examples/) — runnable Python examples
- [CHANGELOG.md](CHANGELOG.md) — per-release change log
- [doc/VERSIONING_POLICY.md](doc/VERSIONING_POLICY.md) — semver policy + component-level `.ado` version headers

## Development

```bash
PYTHONIOENCODING=utf-8 python -m pytest tests/   # 62 cases across discovery, wb_text, wb_api_tools
```

Useful Makefile targets:

```bash
make wb-update-metadata   # refresh YAML cache (v0.1.0 pipeline)
make wb-metadata          # legacy YAML builder (pre-Phase-0)
make wb-metadata-csv      # legacy CSV builder
make wb-config            # batch data pulls from config.yaml
```

Branch model: feature work on `develop`; releases tag from `main`. See the
v0.1.0 release notes for the full PR list.

## Integration

The Python CLI and library plug into:

- **Makefiles / pipelines** (`make wb-update-metadata`, cron, GitHub Actions)
- **Stata workflows** (export CSV → `import delimited`, or use the Stata package directly)
- **R workflows** (`readr::read_csv` or `arrow::read_parquet`)
- **Jupyter notebooks** for ad-hoc analysis

## License

See [LICENSE.md](LICENSE.md). Developed to bridge **Stata `wbopendata`
workflows** with modern Python pipelines for reproducible UNICEF / World
Bank style analytics.
