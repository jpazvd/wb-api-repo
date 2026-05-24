# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This repo starts its **package version** at `v0.1.0` on a parallel track to
`wbopendata-dev` (Stata Journal lineage, v18.x). Component-level `.ado` headers
retain their upstream lineage versions (see `doc/VERSIONING_POLICY.md`).

## [Unreleased]

### Added (Unreleased)

- **CLI: `--out -` for full CSV to stdout.** All subcommands that
  accept `--out` (data / countries / indicators / sources / alltopics /
  search) now route to `sys.stdout` as CSV when the path is a single
  dash, conventional Unix style. Lets you pipe output into other
  tools without the round-trip through disk:

  ```bash
  wb-api-tools data --indicators SP.POP.TOTL --countries BRA \
      --date 2010:2020 --long --out - | csvkit ...
  ```

  Omitting `--out` still emits a 20-row preview (head-only, not
  parseable) — distinct from `--out -` which emits the full DataFrame.
- **CLI: JSON + JSONL output formats.** Two new file-extension routes
  in the same `--out` dispatcher:
  - `--out file.json` — pretty-printed records orient
    (`[{...}, {...}]`, indent=2). Web / JS / notebook friendly.
  - `--out file.jsonl` or `--out file.ndjson` — line-delimited
    records, one JSON object per line. Streaming-friendly for `jq`,
    log pipelines, Spark / BigQuery ingest.

  Both use `pd.DataFrame.to_json(..., force_ascii=False)` so non-ASCII
  characters in country names + descriptions round-trip cleanly.

  Deliberately NOT added: XML (`pandas.to_xml()` works but use case
  is narrow — defer until requested), SDMX (the right path is a
  separate fetch mode hitting WB's native SDMX endpoint, not a
  pandas-to-SDMX serializer — planned for v0.3.0).
- **`tests/test_cli.py`** — 9 cases covering all `_save_df` output
  paths: dash-to-stdout, no-out preview, .csv (+ stderr-status
  assertion), .yaml, unknown-ext fallback, .json records, .jsonl
  lines, .ndjson alias parity with .jsonl, dash-mode-no-status.
  Suite now **71/71**.

### Changed (Unreleased)

- **CLI status lines now route to stderr** (Unix convention: stdout =
  data, stderr = diagnostics). Affects:
  - `_save_df()` "Wrote: ..." line on file output
  - `search` subcommand's `total=N page=M/N limit=L` summary

  Fixes a real bug Copilot caught on PR #33: `wb-api-tools search
  --out -` previously contaminated the piped CSV with the summary
  line glued to the top. With the fix, `--out -` is a clean
  parseable stream across all six subcommands; status info is still
  human-visible (unredirected stderr).

### Removed (Unreleased)

- **Stata badge removed from README**. The PyPI package
  (`wb-api-tools`) is Python-only — `pip install wb-api-tools` gives
  the Python library + CLI, not the Stata `wbopendata.ado` files. The
  Stata badge added in v0.2.1 was misleading on the PyPI project page
  (suggested `pip install` would deliver Stata content). The repo
  still ships both surfaces; the Stata package is documented in its
  own section of the README + via `help wbopendata` in Stata.

### Documentation (Unreleased)

- **README restructured for PyPI-first audience.**
  - Reordered the badge block — PyPI version badge first (highest
    install-decision signal), tests + Python + new pepy.tech Downloads
    badge + License after.
  - Moved `pip install wb-api-tools` above the fold (was buried after
    a "What's here" table).
  - Added a **Quick-start with 5 worked examples**, three of which
    embed inline PNG figures pulled from `docs/figures/` via absolute
    `raw.githubusercontent.com` URLs (work on both GitHub and PyPI):
    - Example 1 — population time-series for BRA/USA/IND, 2000-2023
      (line chart)
    - Example 2 — G7 GDP per capita PPP cross-section, 2022 (bar chart)
    - Example 3 — poverty vs GDP per capita scatter, 2019 (mirrors
      Stata `wbopendata_examples.ado` example 04)
    - Example 4 — discovery workflow (`search` → `info`)
    - Example 5 — `enrich_country_context` user-DataFrame match
      (mirrors Stata example 05)
  - Added **What's new in v0.2.1** section right after the examples,
    visible at-a-glance so the README doesn't feel undated.
  - Added a **Common indicators** starter table — 15 high-traffic
    codes across Population / Economy / Poverty / Education / Health /
    Environment categories — lowers activation cost for first-time
    users (the full universe is 29,511).
  - Added **Troubleshooting** section covering YAML cache missing,
    cache-dir resolution order, corporate proxy, Windows
    `UnicodeEncodeError`, and "is `sync` stuck?" FAQ.
  - Added **Citation** section with BibTeX for both `wb-api-tools` and
    the upstream Stata `wbopendata` (SSC RePEc).
  - Moved the **Project surfaces** table (renamed from "What's here")
    below the Quick-start so PyPI visitors see runnable code before
    architectural framing.

- **New `examples/readme_examples.py`** — runnable Python script that
  reproduces the five Quick-start examples and writes the three PNG +
  SVG figure twins to `docs/figures/`. Mirrors the Stata
  `wbopendata_examples.ado` numbering and theme.

- **New `examples/readme_examples.ipynb`** — paired Jupyter notebook
  (18 cells, outputs captured) for the same five examples. GitHub
  renders it inline — DataFrame tables + figures — without anyone
  having to clone or install.

- **New `examples/_build_readme_notebook.py`** — internal builder that
  constructs the notebook from a single Python source via `nbformat`
  and executes it via `nbconvert --execute --inplace`. Keeps the
  `.py` + `.ipynb` pair in sync without jupytext.

- **New `docs/figures/` directory** — committed PNG + SVG assets
  embedded in the README + reproducible from the script above.

- **Example 3 fitted-curve overlay (selected via R²)**. Three
  candidate functional forms tried — linear-in-log(GDP),
  quadratic-in-log(GDP), logistic 4PL — and the best fit overlaid in
  black. On the 78-country 2019 cross-section the logistic 4PL wins
  (R²=0.834 vs 0.775 for quadratic vs 0.503 for linear); it's also
  the principled choice since poverty headcount is bounded [0, 100%]
  and a sigmoid respects both asymptotes. `scipy.optimize.curve_fit`
  for the logistic; `numpy.polyfit` for the polynomial baselines.

- **`[examples]` optional-dependencies group** added to
  `pyproject.toml`. Pulls in matplotlib, scipy, nbformat, jupyter,
  nbconvert — needed to regenerate the README's figures + notebook,
  NOT needed at runtime. `pip install -e ".[examples]"` for dev work
  on the README.

### Fixed (Unreleased)

- Test fixture in `tests/test_wb_discovery.py` for `SI.POV.DDAY`
  described the indicator as `"Poverty at $2.15/day"` (the pre-2025
  WB methodology). Updated to `"Poverty at $3.00/day (2021 PPP)"`
  to match the current YAML cache definition + the README's Common
  Indicators table. Cosmetic stub-text change; test logic unchanged.
- `examples/readme_examples.py` guarded the `sys.stdout.reconfigure`
  call with `hasattr` + try/except (matches the existing pattern in
  `examples/demo_pr_b_c.py`). Avoids `AttributeError` in environments
  where stdout/stderr don't expose `.reconfigure` (some IDE consoles,
  test runners that replace streams).

### Deferred to release-prep (Unreleased)

- **Re-pin README image URLs from `main` to the release tag**
  (`raw.githubusercontent.com/jpazvd/wb-api-repo/<tag>/docs/figures/...`)
  during the v0.3.0 release-prep PR. Today the URLs point at `main`,
  which means PyPI's frozen v0.3.0 README would show whatever
  `docs/figures/` looks like in HEAD — could drift if a contributor
  regenerates the script + figures change pixel-wise. Risk is low
  (figures only change when example code changes; we control both
  ends), but pinning eliminates the drift entirely. Tracked in the
  v0.3.0 release checklist.

## [0.2.1] — 2026-05-24

**PATCH release — PyPI badge rendering fix.** Switches the README
badge block from dynamic shields.io endpoints (`pypi/pyversions/...`,
`pypi/l/...`, `pypi/dm/...`) to static badges + `badge.fury.io` for
the PyPI version. Dynamic shields.io endpoints scrape PyPI and have
caching/indexing delay on brand-new packages — the v0.2.0 page was
showing alt-text instead of the Python-version and License badges for
hours after publish. Static badges render instantly and never break.

No code changes; no API impact.

### Changed (v0.2.1)

- **README badges** aligned with the [`unicefData`](https://github.com/unicef-drp/unicefData)
  badge convention:
  - `pypi/pyversions/wb-api-tools` → static `python-3.11+` shield
  - `pypi/l/wb-api-tools` → static `License-MIT` shield
  - `pypi/v/wb-api-tools` → `badge.fury.io/py/wb-api-tools`
  - Added a new **Stata 14+** badge to reflect that the repo also
    ships the Stata `wbopendata` ado package (was invisible on the
    PyPI page).
  - Dropped the **Downloads** badge — `pypi/dm/...` returned
    "package not found" for the first 24h after publish anyway, and
    download stats for a small-audience scientific package are noise.

## [0.2.0] — 2026-05-23

**Stable release.** Promotes [0.2.0rc1] after a clean PyPI publish +
clean-venv smoke test: `pip install --pre wb-api-tools`, then
`wb_api_tools.__version__` returned `0.2.0rc1` and
`len(wb_api_tools.__all__)` returned `17`. Identical code to rc1;
only the version string is bumped + the README cosmetic fixes from
PR #26 land in this tag.

`pip install wb-api-tools` (no `--pre`) now resolves to this release.

See the `[0.2.0rc1]` section below for the full scope of what 0.2.0
delivers (PyPI packaging, OIDC publish workflow, XDG-aware cache,
PROJECT_ROOT removed, bundled config+schema).

### Added (v0.2.0)

- README H1 renamed `wb-api-repo` → `wb-api-tools` so the PyPI page
  header and the README's first heading agree (PR #26).
- One-line callout under the badges making the repo↔package
  relationship explicit.

### Changed (v0.2.0)

- User-facing references to the upstream Stata package now point at
  [`jpazvd/wbopendata`](https://github.com/jpazvd/wbopendata) (public
  repo) instead of the dev fork (PR #26).
- `doc/VERSIONING_POLICY.md` and `doc/roadmap/README.md` aligned with
  the same naming convention.

## [0.2.0rc1] — 2026-05-23

**First PyPI release candidate.** Packages the Python library + CLI as
`wb-api-tools` on PyPI; ships via OIDC trusted publishing. Pre-release
tag — installable only with `pip install --pre wb-api-tools`; plain
`pip install wb-api-tools` skips it. If smoke-test passes, bump to
stable `0.2.0`; if broken, bump to `rc2`.

### Added (v0.2.0rc1)

- **`wb-api-tools` package on PyPI** (PR #23) — the Python code,
  previously a flat `src/py/*.py` script tree requiring `sys.path`
  hacks, now ships as an installable distribution. `pip install
  wb-api-tools` makes the library + `wb-api-tools` console script
  available; `import wb_api_tools as wb` works without any
  bootstrapping.
- **`pyproject.toml`** with hatchling build backend; deps
  (`requests`, `pandas`, `pyyaml`, `jsonschema`, `gitpython`); `[test]`
  extra (`pytest`, `pytest-mock`, `requests-mock`); console-script
  entry point `wb-api-tools = wb_api_tools.cli:main`.
- **XDG-aware cache** (`src/wb_api_tools/cache.py`) — discovery reads
  from `$WBOPENDATA_YAML_DIR` > `$XDG_CACHE_HOME/wbopendata` >
  `$LOCALAPPDATA/wbopendata` > `~/.cache/wbopendata`. No more
  repo-relative paths.
- **Bundled config + schema** (`src/wb_api_tools/_resources/`) ride
  along in the wheel so `wb-api-tools sync` works out of the box for
  pip-installed users (writes to `get_cache_dir()`).
- **`.github/workflows/publish.yml`** (PR #24) — OIDC-authenticated
  publish on `v*` tag push. Two jobs: `build` (`python -m build` +
  smoke install) and `publish` (`pypa/gh-action-pypi-publish@release/v1`
  via OIDC trusted publisher, `environment: pypi`). Publish job is
  gated by `if: startsWith(github.ref, 'refs/tags/v')` so
  `workflow_dispatch` from a branch builds but doesn't publish.
  Version-guard step asserts the built wheel version matches the git
  tag and rejects `.dev` builds.
- **PyPI badges** in README (version / pyversions / license / tests /
  downloads).

### Changed (v0.2.0rc1)

- **Module renames during packaging** (no public API break):
  - `wb_discovery.py` → `wb_api_tools.discovery`
  - `wb_text.py` → `wb_api_tools.text`
  - `wb_api_client.py` → `wb_api_tools.api_client`
  - `wb_api_tools.py` split into `wb_api_tools.data` (library) +
    `wb_api_tools.cli` (CLI entry point + `_save_df`).
- **YAML cache location**: was `<repo>/src/_/_wbopendata_*.yaml`;
  now `~/.cache/wbopendata/_wbopendata_*.yaml` (per-user, XDG).
  The repo-committed cache at `src/_/` stays for the Stata side +
  dev convenience.
- **Makefile**: targets switched from `python src/py/foo.py` to
  `python -m wb_api_tools.foo`. Added `install`, `test`, `demo`
  targets.
- **Stata `__wbod_sync.ado`**: dropped the 4-path candidate search
  for the Python pipeline; now shells `python -m
  wb_api_tools.update_metadata` (assumes `pip install wb-api-tools`).

### Removed (v0.2.0rc1)

- `src/py/` directory (replaced by `src/wb_api_tools/`).
- `src/py/examples/` (moved to repo-root `examples/`, not shipped in
  the wheel).

## [0.1.1] — 2026-05-23

PATCH release. Docs + CI infrastructure follow-ups after v0.1.0; no
changes to the Python or Stata public surface. 3 PRs since v0.1.0.

### Added (v0.1.1)

- **CI: pytest workflow** (PR #18) — `.github/workflows/tests.yml` runs
  the 62-case pytest suite on push to `develop`/`main` and on PRs
  targeting either. Python 3.11 + pip cache; uses `python -m pytest`
  and `python -m pip` consistently for interpreter clarity.
- **README refresh** (PR #17) — rewrote `README.md` for the v0.1.0
  surface: single H1 (was two concatenated READMEs), all 9 CLI
  subcommands documented (was 3), Python library import pattern, Stata
  package section, YAML cache section, documentation index. Cross-links
  to PYTHON_USER_GUIDE / PYTHON_DEMO / CHANGELOG / VERSIONING_POLICY.

### Changed (v0.1.1)

- **Metadata refresh cadence weekly → semi-monthly** (PR #19) —
  `wb_metadata_nightly.yml` cron `'17 2 * * 1'` → `'17 2 1,15 * *'`
  (1st + 15th of every month at 02:17 UTC). Workflow display name
  cadence-agnostic ("World Bank Data Refresh"); auto-commit messages
  dropped the stale "Nightly" prefix. Filename kept to preserve
  workflow ID + run history.

### Fixed (v0.1.1)

- **Silent cron-parse bug** (PR #17) — `wb_metadata_nightly.yml` cron
  was `'17 2 * * 1''` with a stray trailing quote; the YAML parser
  either rejected the schedule or fell through to an unintended cadence.
  Every push to develop since PR #12 had been showing 0-second failures
  on this workflow as a result. Now parses cleanly.

## [0.1.0] — 2026-05-23

First tagged release on the parallel v0.x track. Closes the Stata-side
discovery, cache-sync, country-context, and multilingual surface
(Phases 0-7), and the Python-side parity catch-up (PR A debt cleanup,
PR B discovery API, PR C country-context / multilingual / linewrap,
live YAML cache population, demo, and user guide). 14 PRs total since
the cloned baseline.

### Added

- **Python validation pass + docs audit** (closes the post-demo cleanup):
  - **`docs/PYTHON_USER_GUIDE.md`** — comprehensive Python-surface reference (library + CLI + verification + Stata-parity table). Hand-maintained companion to the auto-captured `docs/PYTHON_DEMO.md` transcript.
  - **CLI `--help` completeness**: every flag on every `wb_api_tools.py` subcommand now has a `help=` string. `countries`, `indicators`, `data`, `alltopics`, `sources`, `search`, `sync` `--out`/`--codes`/`--search`/`--date`/`--long`/`--page`/`--limit`/`--save-raw`/`--no-validate`/`--skip-diff`/`--commit`/`--tag` previously had no descriptions.
  - **Removed dead `--per-page` flag** from `data` subcommand. `get_data()` uses CSV downloads (which don't paginate), so the value was always ignored. Documented as a no-op by Copilot; removing rather than wiring or hiding.
  - **Docstrings** added to `wb_api_tools.build_parser()` and `wb_api_tools.main()` (the two public CLI helpers were undocumented).
  - **Validation pass results** (no code changes, recorded for traceability):
    - pytest: 62/62 green (31 discovery + 17 wb_text + 14 wb_api_tools).
    - Live demo: `examples/demo_pr_b_c.py` runs end-to-end against the live API + 18 MB YAML cache without errors.
    - Module docstring coverage: 9/15 modules (legacy `make_wb_metadata_*` builders and one-off examples remain undocumented by design).
    - Public function docstring coverage: 34/64 (53%); 100% on the PR B + PR C surface (`wb_discovery`, `wb_text`, `yaml_generator`, `update_metadata`, `schema_validator`).
- **Python PR C — Country-context flags + multilingual + linewrap** (closes the remaining Phase 5-6 Python deferrals):
  - `wb_api_tools.get_data(geo=True)` — supplementary 3-field geographic merge (capital / latitude / longitude); combinable with `no_basic` for any of 4 flag matrices. Phase 5 `geo` parity.
  - `wb_api_tools.enrich_country_context(df, iso_col, *, basic=True, geo=False)` — standalone helper that merges country context into a user-supplied DataFrame on any ISO3 column. Python equivalent of Stata `wbopendata, match(varname) [basic geo]`. Left-join semantics; defensive `.copy()`; raises `KeyError` if `iso_col` missing.
  - **New module `src/py/wb_text.py`** — Stata Phase-6 linewrap parity:
    - `wb_text.wrap(text, *, width=80, fmt="stack"|"newline"|"lines"|"smcl"|"all")` — text wrapping with 4 output formats matching Stata's `linewrapformat`.
    - `wb_text.wrap_lines(text, *, width=80)` — convenience shorthand for `wrap(fmt="lines")`.
    - `wb_text.truncate(text, *, width=80, suffix="...")` — single-line cap with ellipsis (Stata `maxlength` without `linewrap`).
  - **`language=` kwarg** added to `wb_discovery.describe()`, `wb_api_tools.get_data()`, and `WBAPIClient.fetch_indicator_metadata()`. Inserts `/{lang}/` into the WB API URL path when non-English (`/v2/es/indicator/...`); `None` / `'en'` / `''` / `'EN'` all use the un-prefixed default endpoint. Phase 6 `language()` parity (live-API surface). Note: YAML-cache readers (`info` / `sources` / `alltopics` / `search`) remain English-only — multilingual YAML generation is a future-phase pipeline change.
  - **CLI flags** added to `wb_api_tools.py`: `data --geo --language LANG` and `describe --language LANG`.
  - **`tests/test_wb_text.py`** — 14 new pytest cases covering all 4 wrap formats, edge cases (empty / None / `width<=0` / bad fmt), and the `truncate` helper.
  - **Extended `tests/test_wb_discovery.py`** — 5 new tests for `describe(language=)` passthrough + `WBAPIClient` URL construction (parametrised across None/empty/en/EN English variants).
  - **Extended `tests/test_wb_api_tools.py`** — 9 new tests for `get_data(geo=True)`, `enrich_country_context` (6 paths incl. custom iso_col + input-not-mutated invariant), and `get_data(language=)` URL prefix.
  - Full suite: **59 passed** (28 pre-PR-C + 31 new).
- **Python PR B — Discovery API** (Python parity with the Stata `wbopendata` discovery + sync surface ported in Phases 3-6):
  - **New module `src/py/wb_discovery.py`** (~270 LOC) exposing:
    - `sources(limit=20)` / `allsources()` — read `_wbopendata_sources.yaml`, sorted by numeric ID.
    - `alltopics()` — read `_wbopendata_topics.yaml`.
    - `info(indicator_id)` — single-indicator metadata lookup from YAML cache (case-insensitive; falls back to uppercase for canonical WB codes).
    - `search(term, *, page=1, limit=20, source=None, topic=None, field="name+description", exact=False)` — paginated full-text search with source/topic/field filters; supports "browse mode" (empty term + filter).
    - `describe(indicator_id)` — fresh metadata fetch via WB API (live counterpart to `info()`; same dict shape so callers can swap).
    - `sync(argv=None)` — in-process wrapper around `update_metadata.main()` (Phase 1 pipeline).
    - `_transform_api_indicator(raw)` — maps raw WB-API record → YAML schema v2.0 keys.
    - `_load_yaml_section()` — graceful degradation: returns `{}` + `logger.warning` when YAML cache missing, instead of raising. (User-facing functions then surface this as `[]` / `None`.)
    - `WBOPENDATA_YAML_DIR` env-var override for test / alternative deployments.
  - **`WBAPIClient.fetch_indicator_metadata(code)`** added to `src/py/wb_api_client.py` for the live `describe()` path.
  - **`get_data()` auto-merge**: now joins 8 basic country-context fields (region / regionname / adminregion / adminregionname / incomelevel / incomelevelname / lendingtype / lendingtypename) via `countryiso3code` by default; opt-out with `no_basic=True` (or `--no-basic` CLI flag). Mirrors Phase 5 Stata semantics. Cached at module level so repeated calls don't refetch `/country`.
  - **CLI subcommands** added to `src/py/wb_api_tools.py`: `sources`, `alltopics`, `info <id>`, `describe <id>`, `search <term> [--page N --limit N --source N --topic N --field FIELD --exact --out FILE]`, `sync [--save-raw --no-validate --skip-diff --commit --tag]`, plus `data --no-basic`.
  - **`tests/test_wb_discovery.py`** — 24 new pytest tests covering sources / alltopics / info / search (10 paths) / describe (mock-based, 5 paths) / drop-in shape parity between `describe()` and `info()`. Full suite now 26/26 green.

### Fixed

- **Python P1 (Phase 1 debt cleanup)** — addressed all 5 Copilot inline findings from PR #2 that were deferred at the time:
  - `yaml_generator.py` — wired `config_update.yaml`'s `yaml_output.{indicators,sources,topics}_file` overrides through `YAMLGenerator(filenames=...)`; previously the hardcoded basenames silently ignored config (`0bda2f3`).
  - `wb_api_client.py` — wired `config_update.yaml`'s `wb_api.{base_url,retry_count,retry_delay}` through `WBAPIClient(base_url=..., max_retries=..., retry_delay=...)`; previously the client read class constants regardless of config (`ece4890`).
  - `yaml_generator.py` — `total_sources` / `total_topics` set AFTER the empty-`id` filter so the metadata count matches what's written; logger warning when records dropped. Mirrors the existing `generate_indicators_yaml` pattern (`08453e0`).
  - `wb_api_client.py` — pages/total coercion via `max(1, int(... or 1))` + try/except; handles all 11 edge cases incl. Python-truthy `'0'`. Previously could `TypeError` on string responses (`71c1c35`).
  - `yaml_generator.py` — hoisted `_wrap_long_text` + `_str_representer` to module scope; `yaml.add_representer` now runs once at import instead of twice per `generate_indicators_yaml` call. Removes per-method global-state mutation (`71c1c35`).
- `pytest tests/` — 2/2 pass post-change. End-to-end YAMLGenerator round-trip verified (filename overrides + folded scalars).

### Added (Stata phases 2-7)

- **Phase 7** — 92-test QA suite ported from `wbopendata-dev/qa/` to `wb-api-repo/qa/`:
  - `qa/run_tests.do` (2 851 LOC, v 3.0.0) — main harness covering 92 tests across 15 categories (ENV / DL / FMT / CTRY / REG / LW / UPD / TOPIC+LANG / Advanced / Cache+Sync / Discovery / Char / ERR / EXT / DET).
  - `qa/run_tests_dev.do` — dev-mode runner with `profile.do` integration.
  - 4 standalone tests (`smoke_search_aliases.do`, `test_ctry04_fix.do`, `test_v1850_pagination.do`, `run_test_v1850.do`) — focused regressions for Phase 3 surface.
  - `qa/scripts/` — 4 small helpers (`benchmark_parsers`, `check_yaml_vars`, `debug_yaml_read`, `test_yaml_check`).
  - `qa/fixtures/` — 22 files (~1.8 MB): 12 CSV reference snapshots for offline comparison + 6 small XML/JSON API probes + manifest + decompress/generate helpers. Intentionally skipped: `fixtures.tar.gz`/`.zip` (compressed dupes) and `fixtures/api/indicators_default.xml` (13.7 MB — regenerable via `generate_test_fixtures.py`).
  - `qa/README.md`, `qa/TESTING_GUIDE.md`, `qa/test_protocol.md` — usage docs, methodology, and current protocol (replaces the Phase 0 placeholder README).
  - **Expected gaps when run against v17.4.0** (this distribution):
    - CHAR-01..06 (6 tests): `noCHAR` enforcement is deferred to Phase 6.1.
    - LANG-01 (1 test): `language()` end-to-end wiring not yet verified.
    - Some UPD-* tests: `__wbod_update_indicators` / `__wbod_update_regionmetadata` (admin paths) not ported; planned for Phase 8.
    - Anything requiring populated YAML in `src/_/` (most DL/CTRY/DISC tests): blocked until `make wb-update-metadata` is run at least once.
  - `qa/` files are NOT enumerated in `src/wbopendata.pkg` — development-only, not SSC distributable (mirrors `wbopendata-dev` convention).
- **Phase 6** — `describe` (metadata-only) + linewrap publication features in `src/w/wbopendata.ado` (v17.3.0 → v17.4.0):
  - `wbopendata, describe indicator(<id>)` — fetch indicator metadata only, no data download; routes to new `__wbod_query_metadata`.
  - `linewrap(<mode>)` / `maxlength(<n>)` / `linewrapformat(stack|all|lines|newline|smcl)` — wrap long metadata strings for publication graphs.
  - 3 new helpers (~934 LOC):
    - `__wbod_linewrap.ado` (323 LOC, v 2.1) — Mead Over + Joao Pedro Azevedo string-wrapping engine.
    - `__wbod_metadata_linewrap.ado` (199 LOC, v 1.1) — per-field metadata wrapper; returns `_stack` / `_newline` / `_nlines` / `_line1..._lineN`.
    - `__wbod_query_metadata.ado` (412 LOC, v 16.8) — v18 linewrap-aware metadata fetch.
  - `src/wbopendata.pkg` extended with 3 new entries.
  - Coexists with legacy `_query_metadata.ado` (Phase 0) — still called by the v17 data-fetch path; v18 helper only called from the new dispatcher `describe` block.
  - **Deferred to Phase 6.1**: `noCHAR` enforcement (the actual `char define wbopendata_*` writes inside the v17 data-fetch path — surgical risk) and `language()` wiring verification (probably works via v17 `_query.ado`; needs end-to-end test against multilingual API endpoint).
- **Phase 5** — Basic country context on-by-default in `src/w/wbopendata.ado` (v17.2.0 → v17.3.0):
  - 8-field basic context (region / regionname / adminregion / adminregionname / incomelevel / incomelevelname / lendingtype / lendingtypename) is now auto-merged into every data-fetch output unless the user passes `noBASIC`.
  - `noCHAR` option added (the actual `char define wbopendata_*` write block lands with Phase 5.1 / Phase 6 — currently the flag is parsed and defaulted ON, but not yet enforced).
  - Both country-context call sites — the `match()` flow and the default data-fetch flow — retargeted from legacy `_countrymetadata` to new `__wbod_countrymetadata` (v18, 175 LOC), which adds the `basic` and `geo` flag passthroughs.
  - Legacy `_countrymetadata.ado` retained for the `_update_countrymetadata` / `_update_regionmetadata` admin paths.
  - `src/wbopendata.pkg` extended with 1 new entry (`_/__wbod_countrymetadata.ado`).
- **Phase 4** — Cache management + `sync replace` (apply) path in `src/w/wbopendata.ado` (v17.1.0 → v17.2.0):
  - `wbopendata, sync replace [force]` — full apply flow: preview → snapshot → `__wbod_sync` (Python-preferred / Stata-fallback) → stats history → diff display.
  - `wbopendata, clearcache` / `, cacheinfo` / `, checkupdate` — metadata-cache subcommands.
  - `wbopendata, cleardatacache` / `, resetdatacache` — data-cache subcommands.
  - `cachedays(integer 7)`, `nocache`, `forcestata`, `forcepython` options.
  - Deprecated aliases: `syncforce` / `syncpreview` / `syncdryrun` (one-line deprecation notice + canonical-modifier substitution).
  - 6 new `__wbod_*` helpers (~1957 LOC):
    - `__wbod_cache.ado` (283 LOC) — cache backend with 5 inline subcommands (clear/info/checkversion/cleardatacache/resetdatacache).
    - `__wbod_sync.ado` (293 LOC) — sync orchestrator with 6 inline programs (check_python/check_staleness/download_yaml/run_python/update_sync_history/write_cache_meta).
    - `__wbod_sync_diff.ado` (182 LOC) — before/after snapshot diff.
    - `__wbod_refresh_yaml.ado` (685 LOC) — Stata-native YAML regeneration fallback.
    - `__wbod_api_read_indicators.ado` (358 LOC) — bulk WB-API fetcher feeding refresh_yaml.
    - `__wbod_write_stats_history.ado` (156 LOC) — post-sync stats logger.
  - `src/wbopendata.pkg` extended with 6 new entries.
- **Phase 3** — Stata discovery commands wired in `src/w/wbopendata.ado` (v17.0 → v17.1.0):
  - `wbopendata, sources` / `, allsources` — list 71 WB data sources (limit-aware).
  - `wbopendata, alltopics` — list 21 topic categories.
  - `wbopendata, info(<id>)` — full metadata for one indicator (description, source, topics, unit, notes, clickable URL conversion).
  - `wbopendata, search(<term>)` — paginated full-text indicator search with `searchsource()`, `searchtopic()`, `searchfield()`, `exact`, `page()`, `limit()` filters.
  - `wbopendata, sync` — dryrun preview of YAML-vs-API diff (the `replace` apply path is deferred to Phase 4).
  - 17 new `__wbod_*` / `__wbopendata_*` helpers under `src/_/` (~4 100 LOC): 5 entry points (sources/topics/info/search/sync_preview) + 5 Tier-2 shared infrastructure (get_yaml_path/parse_yaml_ind_v2/search_cache/search/api_read) + 3 Tier-3 leaf helpers (search_aliases/search_pagenav/check_yaml) + 4 transitive deps (check_version/get_source_name/get_topic_name/website).
  - `src/wbopendata.pkg` extended with 17 new entries.
  - **Runtime prerequisite**: discovery commands need `_wbopendata_{indicators,sources,topics}.yaml` in `src/_/`. Run `make wb-update-metadata` (Phase 1) before using.
- **Phase 2** — `yaml` Stata frame library (ported verbatim from `wbopendata-dev`):
  - `src/y/yaml.ado` (v 1.9.2) — dispatcher; subcommands `read`/`write`/`describe`/`list`/`get`/`validate`/`frames`/`clear`/`dir`.
  - `src/y/yaml_*.ado` — 9 subcommand implementations (~2 000 LOC).
  - `src/y/yaml{,_examples,_whatsnew}.sthlp` — main help + example gallery + release notes.
  - `src/y/README.md` — architecture diagram, subcommand reference, supported YAML-feature matrix.
  - `src/_/__yaml_{collapse,fastread,mataread,tokenize_line}.ado` — Mata-accelerated foundation helpers (988 LOC).
  - `src/wbopendata.pkg` extended with 17 new file entries (10 `.ado` in `y/`, 3 `.sthlp` in `y/`, 4 `.ado` in `_/`).
- **Phase 1** — YAML metadata pipeline (ported verbatim from `wbopendata-dev`):
  - `src/py/wb_api_client.py` — HTTP client with retries + pagination; context-manager `WBAPIClient`.
  - `src/py/yaml_generator.py` — transforms WB API JSON → YAML schema v2.0 with SHA256 checksum.
  - `src/py/schema_validator.py` + `config/schema_yaml_v2.json` — 7-variant JSON Schema validation.
  - `src/py/diff_analyzer.py` — key-set diffs for before/after metadata-refresh summaries.
  - `src/py/git_manager.py` — stage/commit/tag helpers for optional `--commit`/`--tag` flow.
  - `src/py/update_metadata.py` + `config/config_update.yaml` — 5-stage orchestrator CLI.
  - `make wb-update-metadata` target.
- Dependencies: `jsonschema`, `gitpython` added to `requirements.txt`.

### Changed

- Repository layout reorganised to mirror `wbopendata-dev`:
  - `_wbopendata/w/*` → `src/w/`
  - `_wbopendata/_/*` → `src/_/`
  - `_wbopendata/{c,i}/*` → `src/{c,i}/`
  - `_programs/*.py` → `src/py/`
- Adopted Semantic Versioning + `CHANGELOG.md` + `CITATION.cff` + `doc/VERSIONING_POLICY.md`.
- Added SSC packaging scaffolding (`src/wbopendata.pkg`, `src/stata.toc`).
- Added placeholder `src/y/`, `qa/`, `doc/{architecture,user-guide,roadmap}/` directories
  for upcoming phases.

### Fixed (Stata Phase 0 drift)

- `.gitignore`: retargeted three rules from `_tests/` to `tests/` to track the Phase 0 directory rename (oversight from Phase 0).
- `src/y/README.md`: corrected to state that YAML metadata files live in `src/_/` per `wbopendata-dev`'s `src/wbopendata.pkg` convention; `src/y/` is reserved for the `yaml.ado` Stata library landing in Phase 2.

### Added (planned, subsequent phases)

- Phase 6.1 cleanup: `noCHAR` enforcement (inline `char define wbopendata_*` writes inside data-fetch path) + `language()` wiring verification
- Layered documentation parity (Phase 8)
- CI/CD + SSC packaging workflows (Phase 9)

## [0.0.0] — 2026-04-19 (pre-restructure baseline)

Initial public state of `wb-api-repo` before alignment work began.

### Existing

- Stata `wbopendata.ado` v17.0 (24Jan2023) under `_wbopendata/`
- Python CLI `wb_api_tools.py` with `countries` / `indicators` / `data` subcommands
- `config.yaml` batch runner via `run_from_config.py`
- Weekly metadata refresh workflow under `.github/workflows/`
- pytest harness under `_tests/`
