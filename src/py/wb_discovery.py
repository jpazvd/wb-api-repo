"""
World Bank Open Data discovery API (Python).

Python equivalents of the Stata `wbopendata` discovery subcommands
ported in Phases 3-6 to `src/w/wbopendata.ado`:

  Stata                              | Python
  ---------------------------------- | ---------------------------------
  wbopendata, sources                | wb_discovery.sources(limit=20)
  wbopendata, allsources             | wb_discovery.allsources()
  wbopendata, alltopics              | wb_discovery.alltopics()
  wbopendata, info(<id>)             | wb_discovery.info(id)           [PR B C2]
  wbopendata, search(<term>)         | wb_discovery.search(term, ...)  [PR B C3]
  wbopendata, describe indicator(<id>) | wb_discovery.describe(id)     [PR B C4]
  wbopendata, sync                   | wb_discovery.sync()             [PR B C6]

Reads from the YAML metadata cache at `src/_/_wbopendata_*.yaml` produced
by `update_metadata.py` (Phase 1 pipeline). Override the cache directory
via the WBOPENDATA_YAML_DIR environment variable for tests / alternative
deployments.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

DEFAULT_YAML_DIR = Path(__file__).resolve().parents[2] / "src" / "_"

SOURCES_YAML = "_wbopendata_sources.yaml"
TOPICS_YAML = "_wbopendata_topics.yaml"
INDICATORS_YAML = "_wbopendata_indicators.yaml"


# Module-level cache for parsed YAML sections.
# The indicators file is ~18 MB; PyYAML's safe_load peaks at ~200-400 MB
# of transient memory per call. Without this cache, two sequential
# wd.info() / wd.search() calls reliably OOM on a 16 GB machine
# (regression caught by examples/demo_pr_b_c.py).
# Keyed by (resolved abs path, section) so multiple yaml dirs
# (env-var overrides in tests) don't cross-contaminate.
_SECTION_CACHE: Dict[tuple, Dict[str, Dict]] = {}


def clear_cache() -> None:
    """Drop the in-process YAML section cache.

    Call after `sync()` to force the next discovery call to re-read the
    refreshed YAML. Also useful in long-running notebooks / services
    where you want to reclaim the ~200 MB used by the indicators cache.
    """
    _SECTION_CACHE.clear()


def _yaml_dir() -> Path:
    """Resolve the YAML metadata directory.

    Precedence: WBOPENDATA_YAML_DIR env var > DEFAULT_YAML_DIR (repo path).
    """
    env = os.environ.get("WBOPENDATA_YAML_DIR")
    return Path(env) if env else DEFAULT_YAML_DIR


def _load_yaml_section(filename: str, section: str) -> Dict[str, Dict]:
    """Load a single section ('sources' / 'topics' / 'indicators') from a metadata YAML.

    Returns empty dict + warning if the file is missing — lets callers
    degrade gracefully when the user hasn't run `make wb-update-metadata`
    yet, instead of raising.

    Cached at module level — re-parsing the 18 MB indicators file each
    call peaks at ~200 MB of transient memory and reliably OOMs after
    a few sequential discovery operations. Cache is keyed by resolved
    abs path so test env-var overrides don't cross-contaminate. Drop
    via `clear_cache()` after `sync()` or to reclaim memory.
    """
    path = _yaml_dir() / filename
    cache_key = (str(path.resolve()), section)
    if cache_key in _SECTION_CACHE:
        return _SECTION_CACHE[cache_key]
    if not path.exists():
        logger.warning(
            "YAML metadata not found: %s. Run `make wb-update-metadata` first to populate.",
            path,
        )
        # Cache the empty result too — avoids repeated stat() on a missing file
        _SECTION_CACHE[cache_key] = {}
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    section_dict = payload.get(section, {}) or {}
    _SECTION_CACHE[cache_key] = section_dict
    return section_dict


def sources(limit: Optional[int] = 20) -> List[Dict]:
    """List World Bank data sources from the YAML metadata cache.

    Args:
        limit: Max number of sources to return; None or <=0 means no cap.
               Default 20 mirrors the Stata `wbopendata, sources` default.

    Returns:
        List of source records (each a dict with code/name/description/url/etc.),
        sorted by integer source ID for stable output.
    """
    src_dict = _load_yaml_section(SOURCES_YAML, "sources")
    if not src_dict:
        return []
    # Sort by numeric ID for stable, intuitive ordering (matches Stata behaviour)
    records = sorted(src_dict.values(), key=lambda r: int(r.get("code", "0") or 0))
    if limit is not None and limit > 0:
        records = records[:limit]
    return records


def allsources() -> List[Dict]:
    """List all World Bank data sources (no limit).

    Equivalent to `wbopendata, allsources` — convenience wrapper over
    `sources(limit=None)` for symmetry with the Stata API.
    """
    return sources(limit=None)


def alltopics() -> List[Dict]:
    """List all WB topic categories from the YAML metadata cache.

    Returns:
        List of topic records (each a dict with code/name/description),
        sorted by integer topic ID.
    """
    top_dict = _load_yaml_section(TOPICS_YAML, "topics")
    if not top_dict:
        return []
    return sorted(top_dict.values(), key=lambda r: int(r.get("code", "0") or 0))


def search(
    term: str = "",
    *,
    page: int = 1,
    limit: int = 20,
    source: Optional[str] = None,
    topic: Optional[str] = None,
    field: str = "name+description",
    exact: bool = False,
) -> Dict:
    """Paginated full-text indicator search (Python equivalent of
    `wbopendata, search(<term>) [searchsource searchtopic searchfield exact page limit]`).

    Args:
        term:   substring (or exact code with `exact=True`) to look for.
                Empty string + a source/topic filter = "browse" mode.
        page:   1-indexed page number.
        limit:  results per page (default 20, same as Stata).
        source: source-ID filter (string; matched against indicator's source_id).
        topic:  topic-ID filter (string; matched against any element of topic_ids).
        field:  which fields to search in 'term'. One of:
                  "name"  | "description" | "note" | "code"
                  "name+description"  (default — matches Stata default)
                  "all"   (name + description + note + code)
        exact:  exact code match instead of substring; applies to `code` field.

    Returns:
        Dict with:
          term:    echoed input
          total:   total matches (pre-pagination)
          page:    current page (1-indexed)
          pages:   total pages
          limit:   per-page count
          results: list of indicator dicts on this page
    """
    ind_dict = _load_yaml_section(INDICATORS_YAML, "indicators")
    if not ind_dict:
        return {"term": term, "total": 0, "page": page, "pages": 0, "limit": limit, "results": []}

    needle = (term or "").strip().lower()
    field_set = _expand_search_fields(field)

    matches: List[Dict] = []
    for rec in ind_dict.values():
        # Source filter
        if source and str(rec.get("source_id", "")) != str(source):
            continue
        # Topic filter — topic_ids is a list of strings; no semicolon parsing
        # needed (the Stata implementation has to deal with semicolon-joined
        # strings + the leading-zero edge case).
        if topic and str(topic) not in {str(t) for t in (rec.get("topic_ids") or [])}:
            continue
        # Text match
        if needle:
            if exact and "code" in field_set:
                if rec.get("code", "").lower() == needle:
                    matches.append(rec)
                continue
            # Substring across the selected fields
            haystack_parts = [str(rec.get(f, "") or "") for f in field_set]
            if any(needle in h.lower() for h in haystack_parts):
                matches.append(rec)
        else:
            # No term — must rely on source/topic filter
            if source or topic:
                matches.append(rec)

    # Sort matches by code for stable pagination
    matches.sort(key=lambda r: r.get("code", ""))

    total = len(matches)
    per_page = max(1, int(limit))
    n_pages = max(1, (total + per_page - 1) // per_page) if total else 0
    page_idx = max(1, int(page))
    if total and page_idx > n_pages:
        page_idx = n_pages
    start = (page_idx - 1) * per_page
    end = start + per_page
    return {
        "term": term,
        "total": total,
        "page": page_idx,
        "pages": n_pages,
        "limit": per_page,
        "results": matches[start:end] if total else [],
    }


def _expand_search_fields(field: str) -> List[str]:
    """Map the user-friendly `field` arg to a list of indicator-dict keys."""
    f = (field or "name+description").lower()
    if f == "all":
        return ["name", "description", "note", "code"]
    if "+" in f:
        return [p.strip() for p in f.split("+") if p.strip()]
    return [f]


def sync(argv: Optional[List[str]] = None) -> int:
    """Run the Phase-1 metadata-refresh pipeline (Python equivalent of
    `wbopendata, sync replace` — full apply path).

    Thin wrapper around update_metadata.main() so callers can trigger
    a YAML refresh in-process without shelling out. Accepts the same
    argv list as the CLI:
      sync(["--save-raw", "--no-validate"])

    Returns the orchestrator's exit code (0 success, non-zero failure).
    """
    import sys as _sys
    from pathlib import Path as _Path
    # Make sure src/py/ is on sys.path so update_metadata's local
    # `from diff_analyzer import ...` peers resolve when called from
    # outside that directory.
    here = _Path(__file__).resolve().parent
    if str(here) not in _sys.path:
        _sys.path.insert(0, str(here))
    saved_argv = _sys.argv
    try:
        _sys.argv = ["update_metadata"] + list(argv or [])
        from update_metadata import main as _main
        return _main() or 0
    finally:
        _sys.argv = saved_argv


def describe(indicator_id: str, language: Optional[str] = None) -> Optional[Dict]:
    """Fetch FRESH metadata for one indicator from the WB API
    (Python equivalent of Stata `wbopendata, describe indicator(<id>)
    [language(es|fr)]`).

    Unlike info() (which reads from the local YAML cache and may be stale),
    describe() always hits api.worldbank.org for the latest record.
    Returns the same dict shape as info() so callers can swap between them.

    Args:
        indicator_id: WB indicator code (e.g. 'SP.POP.TOTL').
        language:     ISO-639-1 code: 'en' (default), 'es', 'fr'. Non-English
                      values trigger the localised API endpoint
                      (/v2/{language}/indicator/...).

    Returns:
        Indicator metadata dict (YAML schema v2.0 shape) or None on
        unknown code / API error.
    """
    if not indicator_id:
        return None
    # Normalise to upper-case for case-insensitive parity with info()
    code = indicator_id.upper()
    from wb_api_client import WBAPIClient  # local import to avoid cycle on import

    try:
        with WBAPIClient() as client:
            raw = client.fetch_indicator_metadata(code, language=language)
    except Exception as exc:  # network errors / bad response
        logger.error("describe(%r, language=%r) failed: %s", indicator_id, language, exc)
        return None
    if not raw:
        return None
    return _transform_api_indicator(raw)


def _transform_api_indicator(raw: Dict) -> Dict:
    """Map a raw WB-API indicator record to the YAML schema v2.0 shape
    used by info(). Keeps describe()'s output drop-in compatible.
    """
    source = raw.get("source") or {}
    topics = raw.get("topics") or []
    return {
        "code": raw.get("id", ""),
        "name": raw.get("name", ""),
        "source_id": str(source.get("id", "")) if isinstance(source, dict) else "",
        "source_name": source.get("value", "") if isinstance(source, dict) else "",
        "topic_ids": [str(t.get("id", "")) for t in topics if isinstance(t, dict)],
        "topic_names": [
            (t.get("value") or "").strip() for t in topics if isinstance(t, dict)
        ],
        "description": (raw.get("sourceNote") or "").strip(),
        "unit": raw.get("unit", "") or "",
        "source_org": raw.get("sourceOrganization", "") or "",
        "note": raw.get("note", "") or "",
        "limited_data": False,  # API doesn't expose; YAML schema default
    }


def info(indicator_id: str) -> Optional[Dict]:
    """Return full metadata for one indicator (Python equivalent of
    `wbopendata, info(<id>)`).

    Args:
        indicator_id: WB indicator code (case-insensitive lookup).
                      Common forms accepted: 'SP.POP.TOTL', 'sp.pop.totl'.

    Returns:
        Single indicator dict — keys per the YAML schema v2.0:
            code, name, source_id, source_name, topic_ids, topic_names,
            description, unit, source_org, note, limited_data
        OR None if the indicator is not in the YAML cache.

    The YAML keys are case-sensitive (typically uppercase); we try the
    raw input first then the upper-cased form.
    """
    if not indicator_id:
        return None
    ind_dict = _load_yaml_section(INDICATORS_YAML, "indicators")
    if not ind_dict:
        return None
    # Direct hit first (preserves exact case if the YAML used a lowercase key)
    if indicator_id in ind_dict:
        return ind_dict[indicator_id]
    # Fall back to upper-case (WB indicator codes are canonically uppercase)
    upper = indicator_id.upper()
    if upper in ind_dict:
        return ind_dict[upper]
    logger.info("Indicator %r not found in YAML cache (tried %r and %r).",
                indicator_id, indicator_id, upper)
    return None
