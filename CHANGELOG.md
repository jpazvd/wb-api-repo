# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This repo starts its **package version** at `v0.1.0` on a parallel track to
`wbopendata-dev` (Stata Journal lineage, v18.x). Component-level `.ado` headers
retain their upstream lineage versions (see `doc/VERSIONING_POLICY.md`).

## [Unreleased]

### Added

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

- 7-day TTL HTTP cache + `sync replace` (apply) path (Phase 4)
- Country-context auto-merge + multilingual + publication features (Phases 5–6)
- 92-test QA suite parity (Phase 7)
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
