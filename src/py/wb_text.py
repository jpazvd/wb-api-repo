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

Format choices:
  "stack"   single string with \\n between hard-wrapped lines
  "newline" alias for stack (kept for Stata-name fidelity)
  "lines"   returns List[str] — one element per wrapped line
  "smcl"    Stata SMCL output: lines joined with `" `" `" `"' newline marker
  "all"     returns dict with all three: {stack, lines, nlines}
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
        - Dict[str, object] when fmt == "all" — keys: stack, lines, nlines
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
            "all":     {"stack": "", "lines": [], "nlines": 0},
        }
        return empty_map[fmt]

    lines = textwrap.wrap(
        text,
        width=width,
        break_long_words=break_long_words,
        break_on_hyphens=break_on_hyphens,
    )

    if fmt in ("stack", "newline"):
        return "\n".join(lines)
    if fmt == "lines":
        return lines
    if fmt == "smcl":
        # Stata SMCL convention: each line wrapped in compound quotes,
        # separated by a literal `" `"' line break marker.
        return " `\"`\"' ".join(lines)
    # fmt == "all"
    return {"stack": "\n".join(lines), "lines": lines, "nlines": len(lines)}


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
        suffix: appended on truncation (default "...")
    """
    if not text:
        return ""
    width = max(1, int(width))
    if len(text) <= width:
        return text
    cut = max(0, width - len(suffix))
    return text[:cut] + suffix
