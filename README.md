# wb-api-repo

World Bank Open Data helpers in **Python** (library + CLI) and **Stata**
(`wbopendata` ado package). Two surfaces over the same WB API v2, with a
shared YAML metadata cache so discovery commands stay fast and offline-safe.

Current release: **[v0.1.0](https://github.com/jpazvd/wb-api-repo/releases/tag/v0.1.0)** (2026-05-23).
Parallel v0.x track to the upstream [`wbopendata-dev`](https://github.com/jpazvd/wbopendata-dev)
Stata Journal lineage (v18.x).

## What's here

| Surface | Entry point | Reference |
| --- | --- | --- |
| Python library | `src/py/wb_discovery.py`, `src/py/wb_api_tools.py`, `src/py/wb_text.py` | [docs/PYTHON_USER_GUIDE.md](docs/PYTHON_USER_GUIDE.md) |
| Python CLI | `python src/py/wb_api_tools.py <subcmd>` | `--help` on every subcommand |
| Stata package | `src/w/wbopendata.ado` (v17.4.0) | `help wbopendata` in Stata, or `src/w/wbopendata.sthlp` |
| YAML metadata cache | `src/_/_wbopendata_{indicators,sources,topics}.yaml` | refreshed by `python src/py/wb_api_tools.py sync` |

## Install

```bash
git clone https://github.com/jpazvd/wb-api-repo.git
cd wb-api-repo
pip install -r requirements.txt
```

Requires Python 3.11+. The Stata package is loaded by adding `src/w/` and
`src/_/` to Stata's adopath (or installed via `net install` once an SSC
release lands).

## Quick start

The repo ships with a runnable 7-section walkthrough that exercises the
whole Python surface (discovery, live `describe`, `get_data` flag matrix,
`enrich_country_context`, `wb_text` formats):

```bash
PYTHONIOENCODING=utf-8 python src/py/examples/demo_pr_b_c.py
```

Captured transcript: [docs/PYTHON_DEMO.md](docs/PYTHON_DEMO.md).

## Python CLI

`python src/py/wb_api_tools.py <subcommand>` — run any subcommand with
`--help` for full flag descriptions.

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
| `sync` | Refresh the YAML metadata cache from the live WB API |

Example:

```bash
python src/py/wb_api_tools.py data \
    --indicators SP.POP.TOTL,NY.GDP.MKTP.CD \
    --countries "BRA;USA;IND" \
    --date 2010:2020 \
    --geo --long --out _data/wb/pop_gdp_long.csv
```

Output is written to `--out` (`.csv` / `.parquet` / `.yaml` / `.yml`) or
printed as a preview if `--out` is omitted.

## Python library

After putting `src/py/` on `sys.path` (the tests do this), the library is
importable directly:

```python
import sys; sys.path.insert(0, "src/py")
import wb_discovery as wd
from wb_api_tools import get_data, enrich_country_context
import wb_text as wt

wd.search("poverty headcount", limit=5)
df = get_data(["SP.POP.TOTL"], "BRA;USA;IND", date="2020", geo=True)
wt.wrap("long indicator title ...", width=60, fmt="stack")   # for Stata graph title()
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

`src/_/_wbopendata_*.yaml` is the offline metadata cache populated from the
live WB API:

- `_wbopendata_indicators.yaml` — 29,511 indicators (~18 MB)
- `_wbopendata_sources.yaml` — 71 sources
- `_wbopendata_topics.yaml` — 21 topics

Discovery commands (`info`, `search`, `sources`, `alltopics`) read from this
cache for microsecond lookups. Refresh with:

```bash
python src/py/wb_api_tools.py sync                # in-place refresh
python src/py/wb_api_tools.py sync --commit --tag # also git-commit + tag
```

A weekly GitHub Action (`.github/workflows/wb_metadata_nightly.yml` — file
name is historical; cron runs every Monday at 02:17 UTC) keeps the cache
fresh.

## Documentation

- [docs/PYTHON_USER_GUIDE.md](docs/PYTHON_USER_GUIDE.md) — Python library + CLI reference (Stata `.sthlp` equivalent)
- [docs/PYTHON_DEMO.md](docs/PYTHON_DEMO.md) — captured live-API transcript from the 7-section walkthrough
- [docs/EXAMPLES.md](docs/EXAMPLES.md) — end-to-end workflows (API, Stata, Python)
- [docs/AGE_BANDS.md](docs/AGE_BANDS.md) — standard 5-year age band codes for population indicators
- [src/py/examples/](src/py/examples/) — runnable Python examples
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
