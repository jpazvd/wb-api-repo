"""Cache directory resolution for the YAML metadata cache.

Used by :mod:`wb_api_tools.discovery` to locate the cached
``_wbopendata_{sources,topics,indicators}.yaml`` files at runtime.

Resolution precedence (highest first):

1. ``$WBOPENDATA_YAML_DIR`` — explicit override, honored verbatim
   (used by tests + alternative deployments).
2. ``$XDG_CACHE_HOME/wbopendata`` (POSIX) / ``$LOCALAPPDATA/wbopendata``
   (Windows) — standard per-user cache location.
3. ``~/.cache/wbopendata`` (POSIX) / ``~/AppData/Local/wbopendata``
   (Windows) — fallback when the env var above is unset.

The cache directory is *not* created here; it's the caller's job to
``mkdir(parents=True, exist_ok=True)`` before writing. Discovery
functions degrade gracefully (log warning + return empty) when the
directory or its YAMLs are missing — see
:func:`wb_api_tools.discovery._load_yaml_section`.
"""

from __future__ import annotations

import os
from pathlib import Path


def get_cache_dir() -> Path:
    """Resolve the cache directory for YAML metadata files.

    See module docstring for the resolution precedence.
    """
    override = os.environ.get("WBOPENDATA_YAML_DIR")
    if override:
        return Path(override)

    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
    else:
        base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")

    return Path(base) / "wbopendata"
