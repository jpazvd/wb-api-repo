# wb-api-tools Versioning Policy

Adapted from the [Stata `wbopendata`](https://github.com/jpazvd/wbopendata)
versioning policy. Two coordinated tracks:

## Track 1 — Package version (canonical)

Files that MUST move together at every release:
- `CITATION.cff`
- `CHANGELOG.md`
- `src/wbopendata.pkg`
- `src/stata.toc`

Initial package version: **v0.1.0** (parallel track; independent of the Stata [`wbopendata`](https://github.com/jpazvd/wbopendata) v18.x).

Bump rules ([SemVer](https://semver.org)):
- **PATCH** — bugfix, doc-only change, internal refactor with no observable behaviour change.
- **MINOR** — backward-compatible feature addition (new option, new subcommand).
- **MAJOR** — breaking change (removed option, changed default, renamed command).

## Track 2 — Component-level (per-file)

Every `.ado` keeps a `*!` header. Two sub-rules:

1. **Upstream lineage preserved.** Files imported from the original `wbopendata` lineage
   keep their pre-existing header version (e.g. `*! v 17.0 24Jan2023`). Don't mass-rewrite.
2. **On edit, bump.** Any `.ado` modified in a release MUST have its header bumped
   per the SemVer rules above. Normalise legacy `v 10` → `v 10.0.0` only when touching.

## Workflow checklist

Before opening a PR:
- [ ] Bump headers for every modified `.ado`.
- [ ] Add a `CHANGELOG.md` entry under `## [Unreleased]`.
- [ ] If the PR is a release, move `[Unreleased]` → `[X.Y.Z] — YYYY-MM-DD`,
      bump `CITATION.cff` and `src/wbopendata.pkg`, tag with `git tag -a vX.Y.Z`.

## Coordination with the Stata `wbopendata` package

This repo's package version (v0.x) is **independent** of the Stata
[`wbopendata`](https://github.com/jpazvd/wbopendata) v18.x lineage.
Component-level headers may coincidentally match when an `.ado` was
copied unchanged; that's expected, not load-bearing.

Per-axis alignment of features tracked in `CHANGELOG.md` under each phase.

## Author

João Pedro Azevedo
