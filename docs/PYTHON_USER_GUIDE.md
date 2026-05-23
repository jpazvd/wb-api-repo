# Python User Guide

The Python equivalent of the Stata `wbopendata.sthlp` help file. Documents
the full Python surface that ships in `src/py/`, organised by intent.

For the WB-API CLI itself, see also [README.md](../README.md) and the
captured demo transcript in [PYTHON_DEMO.md](PYTHON_DEMO.md).

## At a glance

| What you want                  | Where it lives                                    |
| ------------------------------ | ------------------------------------------------- |
| Browse what's available        | `wb_discovery.sources / alltopics / search`       |
| Look up one indicator          | `wb_discovery.info` (cache) or `describe` (live)  |
| Pull tidy data                 | `wb_api_tools.get_data(...)`                      |
| Add country context to a frame | `wb_api_tools.enrich_country_context(df, ...)`    |
| Wrap text for Stata graphs     | `wb_text.wrap / wrap_lines / truncate`            |
| Refresh the YAML cache         | `wb_discovery.sync(...)` or `wb_api_tools sync`   |

All public functions have docstrings; this guide is the high-level map.

---

## 1. Library API

### `wb_discovery` — read from the YAML cache

```python
import src.py.wb_discovery as wd

wd.sources(limit=20)         # first 20 sources, sorted by id
wd.allsources()              # uncapped (71 today)
wd.alltopics()               # all 21 topics
wd.info("SP.POP.TOTL")       # dict: code, name, source_name, topic_names, ...
wd.info("sp.pop.totl")       # case-insensitive fallback
wd.search("poverty headcount", limit=10)              # substring across name+desc
wd.search(topic="3", limit=10)                        # browse-mode by topic
wd.search("GDP", source="2", topic="3", limit=10)     # combined filters
wd.search("population", page=2, limit=20)             # pagination
```

Notes:
- The YAML cache lives in `src/_/_wbopendata_{indicators,sources,topics}.yaml`.
- Override the cache location with `WBOPENDATA_YAML_DIR` (used by tests).
- A module-level `_SECTION_CACHE` avoids re-parsing the 18 MB indicators
  YAML on sequential calls. Call `wd.clear_cache()` to force a fresh read.
- `sync()` refreshes the cache by hitting the live WB API and invalidates
  `_SECTION_CACHE` automatically on success.

### `wb_discovery.describe` — live metadata

```python
wd.describe("SP.POP.TOTL")                    # English (default endpoint)
wd.describe("SP.POP.TOTL", language="es")     # /v2/es/indicator/...
```

Returns the same key set as `info()` (verified by
`test_describe_and_info_have_same_keys`).

### `wb_api_tools.get_data` — indicator data

```python
from src.py.wb_api_tools import get_data

# DEFAULT — auto-merges 8 country-context fields (region, income, ...)
df = get_data(["SP.POP.TOTL"], "BRA;USA;IND", date="2020")

# LEAN — skip the auto-merge
df = get_data(["SP.POP.TOTL"], "BRA;USA;IND", date="2020", no_basic=True)

# +geo — also merge capital + latitude + longitude
df = get_data(["SP.POP.TOTL"], "BRA;USA;IND", date="2020", geo=True)

# Geo-only (no basic context, just the 3 geo fields)
df = get_data(["SP.POP.TOTL"], "BRA;USA;IND", date="2020",
              no_basic=True, geo=True)

# Multilingual
df = get_data(["SP.POP.TOTL"], "BRA", date="2020", language="es")
```

| Flags                       | Columns added vs. raw API CSV   |
| --------------------------- | ------------------------------- |
| (default)                   | +8 basic context fields         |
| `no_basic=True`             | +0                              |
| `geo=True`                  | +8 basic +3 geo                 |
| `no_basic=True, geo=True`   | +3 geo only                     |

The 8 basic fields are: `region`, `regionname`, `adminregion`,
`adminregionname`, `incomelevel`, `incomelevelname`, `lendingtype`,
`lendingtypename`. The 3 geo fields are: `capital`, `latitude`,
`longitude`.

### `wb_api_tools.enrich_country_context` — Stata `match()` for pandas

Attach the same 8 (or 8+3) context columns to any user DataFrame keyed
by ISO3:

```python
from src.py.wb_api_tools import enrich_country_context
import pandas as pd

user_df = pd.DataFrame({"iso3": ["BRA","USA","IND","DEU","JPN"],
                        "my_metric": [1.2, 3.4, 5.6, 7.8, 9.0]})

enriched = enrich_country_context(user_df, iso_col="iso3")            # +basic
geo_too  = enrich_country_context(user_df, iso_col="iso3", geo=True)  # +basic +geo
```

The input frame is **not** mutated (verified by
`test_enrich_country_context_input_not_mutated`). Raises `KeyError`
if `iso_col` is missing.

### `wb_text` — text wrapping for publication graphs

```python
import src.py.wb_text as wt

s = "GDP per capita (current US$) — Gross domestic product divided ..."
wt.wrap(s, width=60, fmt="stack")    # '"line1" "line2" ...' (graph title())
wt.wrap(s, width=60, fmt="newline")  # 'line1\nline2\n...'   (SMCL note)
wt.wrap(s, width=60, fmt="lines")    # ['line1', 'line2', ...]
wt.wrap(s, width=60, fmt="smcl")     # 'line1{break}line2{break}...'
wt.wrap(s, width=60, fmt="all")      # 5-key dict combining the above
wt.wrap_lines(s, width=60)           # alias for fmt="lines"
wt.truncate(s, width=80, suffix="...")
```

`width` is floored at 1; bad `fmt` raises `ValueError`; `truncate`
clamps the suffix when `width <= len(suffix)`.

---

## 2. CLI reference

The library above is mirrored by `src/py/wb_api_tools.py` as a CLI.
Run `python src/py/wb_api_tools.py --help` for the live tree.

```text
python src/py/wb_api_tools.py <subcommand> [options]

Subcommands:
  countries    Fetch country metadata
  indicators   Fetch indicator metadata
  data         Fetch indicator data (with --no-basic, --geo, --language)
  sources      List WB data sources (--limit / --all)
  alltopics    List all WB topic categories
  info         Show full metadata for one indicator (YAML cache)
  describe     Fetch fresh metadata for one indicator (live; --language)
  search       Paginated indicator search (--page --limit --source --topic --field --exact)
  sync         Refresh YAML cache (--save-raw --no-validate --skip-diff --commit --tag)
```

Every flag has a `help=` string; `python src/py/wb_api_tools.py <cmd> --help`
on any subcommand is the source of truth.

### Common patterns

```bash
# Discovery
python src/py/wb_api_tools.py sources --limit 5
python src/py/wb_api_tools.py alltopics
python src/py/wb_api_tools.py info SP.POP.TOTL
python src/py/wb_api_tools.py describe SP.POP.TOTL --language es
python src/py/wb_api_tools.py search "poverty headcount" --limit 5

# Data with auto-merge variants
python src/py/wb_api_tools.py data --indicators SP.POP.TOTL --countries "BRA;USA;IND" --date 2020 --out pop.csv
python src/py/wb_api_tools.py data --indicators SP.POP.TOTL --countries BRA --date 2020 --no-basic --out pop_lean.csv
python src/py/wb_api_tools.py data --indicators SP.POP.TOTL --countries BRA --date 2020 --geo --out pop_geo.csv

# Refresh metadata cache
python src/py/wb_api_tools.py sync
python src/py/wb_api_tools.py sync --commit --tag         # also git-commit + tag
```

---

## 3. Verification

### Pytest

```bash
PYTHONIOENCODING=utf-8 python -m pytest tests/ -v
```

62 tests across three files:
- `tests/test_wb_discovery.py` — 31 cases (sources, allsources, alltopics,
  info, search filters/pagination, describe live + multilingual)
- `tests/test_wb_text.py` — 17 cases (all 4 wrap formats + truncate)
- `tests/test_wb_api_tools.py` — 14 cases (`get_data` + `enrich_country_context`
  flag matrix, language passthrough)

### Live demo

```bash
PYTHONIOENCODING=utf-8 python src/py/examples/demo_pr_b_c.py
```

Runs all six surfaces against the real YAML cache and the live API in
one ~15 s run. The captured transcript lives in
[PYTHON_DEMO.md](PYTHON_DEMO.md).

---

## 4. Where things live

```
src/py/
├── wb_api_client.py     # WBAPIClient (low-level HTTP, retries, language URL)
├── wb_api_tools.py      # get_data, enrich_country_context, CLI entrypoint
├── wb_discovery.py      # sources, alltopics, info, search, describe, sync
├── wb_text.py           # wrap, wrap_lines, truncate
├── yaml_generator.py    # YAML cache writer (indicators/sources/topics)
├── schema_validator.py  # JSON-Schema validation of generated YAML
├── update_metadata.py   # orchestrator behind `sync()`
├── diff_analyzer.py     # before/after YAML diff for sync runs
├── git_manager.py       # stage/commit/tag helpers for sync --commit --tag
├── run_from_config.py   # batch data pulls from config.yaml
├── make_wb_metadata_*.py# legacy YAML/CSV builders (pre-PR-B)
└── examples/
    ├── demo_pr_b_c.py            # 7-section walkthrough (live + cache)
    └── generate_age_sex_codes.py # population indicator code generator

src/_/                                       # YAML metadata cache
├── _wbopendata_indicators.yaml  (18 MB, 29,511 indicators)
├── _wbopendata_sources.yaml     (12 KB, 71 sources)
└── _wbopendata_topics.yaml      (15 KB, 21 topics)

config/
├── config_update.yaml           # sync pipeline settings
└── schema_yaml_v2.json          # JSON-Schema for generated YAML
```

---

## 5. Parity with Stata `wbopendata`

| Stata surface                         | Python equivalent                          |
| ------------------------------------- | ------------------------------------------ |
| `wbopendata, indicator(X) clear`      | `get_data([X], ...)`                       |
| `wbopendata, sources`                 | `wd.sources()` / CLI `sources`             |
| `wbopendata, allsources`              | `wd.allsources()` / CLI `sources --all`    |
| `wbopendata, alltopics`               | `wd.alltopics()` / CLI `alltopics`         |
| `wbopendata, search("term")`          | `wd.search("term", ...)` / CLI `search`    |
| `wbopendata, indicator(X) describe`   | `wd.describe(X, language=...)`             |
| `wbopendata, ... language(es)`        | `language="es"` on `describe` / `get_data` |
| `wbopendata, ... noBASIC`             | `no_basic=True` on `get_data`              |
| Country-context merge (Phase 5)       | default in `get_data`; `enrich_country_context` for arbitrary frames |
| `linewrap(width)` for graph titles    | `wt.wrap(s, width=W, fmt="stack")`         |
| `__wbod_yaml_sync` cache refresh      | `wd.sync(argv)` / CLI `sync`               |

---

*This file is hand-maintained. Update it when adding a new public function
or CLI subcommand; the demo transcript and pytest harness stay in sync
automatically.*
