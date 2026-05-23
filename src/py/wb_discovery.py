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
