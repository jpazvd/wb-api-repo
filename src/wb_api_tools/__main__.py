"""Entry point for ``python -m wb_api_tools``."""
from __future__ import annotations

import sys

from .cli import main

sys.exit(main() or 0)
