"""
Text-wrapping helpers for WB metadata strings (Python equivalent of
Stata Phase-6 linewrap / maxlength / linewrapformat options).

Used to break long indicator descriptions / source notes / units into
publication-friendly multi-line text for graph titles, table cells,
SMCL output, etc.

Quick reference (Stata → Python):

  Stata                                      | Python
  ------------------------------------------ | ------------------------------------
  linewrap(stack)    maxlength(80)           | wb_text.wrap(s, width=80, fmt="stack")
  linewrap(newline)  maxlength(80)           | wb_text.wrap(s, width=80, fmt="newline")
  linewrap(lines)    maxlength(80)           | wb_text.wrap_lines(s, width=80)
  linewrapformat(smcl)                       | wb_text.wrap(s, width=80, fmt="smcl")
  (truncation w/ ellipsis)                   | wb_text.truncate(s, width=80)

Format choices (mirror Stata __wbod_metadata_linewrap.ado):
  "stack"   space-separated double-quoted segments — for graph title()
              e.g. `"line1" "line2" "line3"`
  "newline" newline-joined single string — for SMCL note/caption display
              e.g. `"line1\\nline2\\nline3"`
  "lines"   returns List[str] — one element per wrapped line
  "smcl"    Stata SMCL output: lines joined with the `{break}` tag
              e.g. `"line1{break}line2"` — renders as line break in Results
  "all"     returns dict with {stack, newline, smcl, lines, nlines}
"""

from __future__ import annotations

import textwrap
from typing import Dict, List, Union


_VALID_FORMATS = {"stack", "newline", "lines", "smcl", "all"}


def wrap(
    text: str,
    *,
    width: int = 80,
    fmt: str = "stack",
    break_long_words: bool = False,
    break_on_hyphens: bool = False,
) -> Union[str, List[str], Dict[str, object]]:
    """Wrap `text` at `width` characters using one of several output formats.

    Args:
        text:              input string (None / empty -> empty result in
                            the chosen format).
        width:             target line width (default 80; max enforced at 1).
        fmt:               output format (see module docstring).
        break_long_words:  textwrap default — keep words intact across lines.
        break_on_hyphens:  textwrap default — don't break on hyphens.

    Returns:
        - str when fmt in {"stack", "newline", "smcl"}
        - List[str] when fmt == "lines"
        - Dict[str, object] when fmt == "all" — keys: stack, newline, smcl, lines, nlines
    """
    if fmt not in _VALID_FORMATS:
        raise ValueError(f"fmt must be one of {sorted(_VALID_FORMATS)}, got {fmt!r}")
    width = max(1, int(width))

    if not text:
        empty_map = {
            "stack":   "",
            "newline": "",
            "lines":   [],
            "smcl":    "",
            "all":     {"stack": "", "newline": "", "smcl": "", "lines": [], "nlines": 0},
        }
        return empty_map[fmt]

    lines = textwrap.wrap(
        text,
        width=width,
        break_long_words=break_long_words,
        break_on_hyphens=break_on_hyphens,
    )

    if fmt == "stack":
        # Stata `linewrap(stack)`: quoted segments joined by spaces, suitable
        # for direct paste into `graph ... , title(...)`. Mirrors
        # __wbod_metadata_linewrap.ado lines 106-114.
        return " ".join(f'"{line}"' for line in lines)
    if fmt == "newline":
        return "\n".join(lines)
    if fmt == "lines":
        return lines
    if fmt == "smcl":
        # Stata `linewrap`/`linewrapformat(smcl)`: lines separated by the
        # {break} SMCL tag for the Results window. Mirrors
        # __wbod_metadata_linewrap.ado lines 129-137.
        return "{break}".join(lines)
    # fmt == "all"
    return {
        "stack":   " ".join(f'"{line}"' for line in lines),
        "newline": "\n".join(lines),
        "smcl":    "{break}".join(lines),
        "lines":   lines,
        "nlines":  len(lines),
    }


def wrap_lines(text: str, *, width: int = 80, **kwargs) -> List[str]:
    """Convenience: shorthand for `wrap(text, fmt='lines', width=width)`."""
    return wrap(text, width=width, fmt="lines", **kwargs)


def truncate(text: str, *, width: int = 80, suffix: str = "...") -> str:
    """Truncate `text` to fit within `width` characters, appending `suffix`
    if truncated.

    Equivalent to Stata `maxlength()` when used without linewrap — return
    a single line capped at the limit.

    Args:
        text:   input string (None / empty -> "")
        width:  max output length INCLUDING suffix
        suffix: appended on truncation (default "..."). When
                `len(suffix) >= width` the suffix is also dropped —
                truncation degrades to a hard text cut so the result
                NEVER exceeds `width`.
    """
    if not text:
        return ""
    width = max(1, int(width))
    if len(text) <= width:
        return text
    # Defensive: if the suffix alone won't fit, do a clean truncation
    # without it. Otherwise the result would exceed the width contract.
    if len(suffix) >= width:
        return text[:width]
    cut = width - len(suffix)
    return text[:cut] + suffix
