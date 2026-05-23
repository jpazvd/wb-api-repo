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
  wbopendata, sync                   | wb_discovery.sync()             [PR B C5]

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
    """
    path = _yaml_dir() / filename
    if not path.exists():
        logger.warning(
            "YAML metadata not found: %s. Run `make wb-update-metadata` first to populate.",
            path,
        )
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return payload.get(section, {}) or {}


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
