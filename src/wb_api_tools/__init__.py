"""World Bank Open Data helpers — Python library + CLI.

Mirrors the Stata ``wbopendata`` surface (discovery, data fetch,
country-context auto-merge, multilingual descriptions, linewrap text
helpers) with a YAML metadata cache for fast offline discovery.

Quick start:

    import wb_api_tools as wb
    wb.sources(limit=5)                         # browse data sources (cached)
    wb.search("poverty", limit=10)              # search indicator catalogue
    df = wb.get_data(["SP.POP.TOTL"], "BRA;USA;IND", date="2020")

The cache (``~/.cache/wbopendata/`` or ``$XDG_CACHE_HOME/wbopendata/``)
is populated by the ``sync()`` function or the ``wb-api-tools sync``
CLI subcommand. Discovery functions degrade gracefully (log + return
empty) when the cache is missing.

Full reference: https://github.com/jpazvd/wb-api-repo/blob/main/docs/PYTHON_USER_GUIDE.md
"""

from __future__ import annotations

__version__ = "0.2.1"

from .api_client import WBAPIClient
from .data import (
    get_data,
    enrich_country_context,
    get_country_metadata,
    get_indicator_metadata,
)
from .discovery import (
    sources,
    allsources,
    alltopics,
    info,
    search,
    describe,
    sync,
    clear_cache,
)
from .text import wrap, wrap_lines, truncate

__all__ = [
    "__version__",
    "WBAPIClient",
    "get_data",
    "enrich_country_context",
    "get_country_metadata",
    "get_indicator_metadata",
    "sources",
    "allsources",
    "alltopics",
    "info",
    "search",
    "describe",
    "sync",
    "clear_cache",
    "wrap",
    "wrap_lines",
    "truncate",
]
