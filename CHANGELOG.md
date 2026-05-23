# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This repo starts its **package version** at `v0.1.0` on a parallel track to
`wbopendata-dev` (Stata Journal lineage, v18.x). Component-level `.ado` headers
retain their upstream lineage versions (see `doc/VERSIONING_POLICY.md`).

## [Unreleased]

### Added

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

### Fixed

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
