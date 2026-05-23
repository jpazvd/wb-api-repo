"""Tests for src/py/wb_text.py — Python equivalent of Stata Phase-6
linewrap / maxlength / linewrapformat options."""

from __future__ import annotations

import os
import sys
from typing import List

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src", "py")))


@pytest.fixture
def long_text() -> str:
    return "a long string " * 10  # ~140 chars


# --- wrap() formats ---------------------------------------------------

def test_wrap_stack_default(long_text: str) -> None:
    import wb_text as wt
    out = wt.wrap(long_text, width=30)
    assert isinstance(out, str) and "\n" in out


def test_wrap_newline_alias_matches_stack(long_text: str) -> None:
    import wb_text as wt
    assert wt.wrap(long_text, width=30, fmt="newline") == wt.wrap(long_text, width=30, fmt="stack")


def test_wrap_lines_returns_list(long_text: str) -> None:
    import wb_text as wt
    out: List[str] = wt.wrap(long_text, width=30, fmt="lines")
    assert isinstance(out, list) and len(out) > 1
    assert all(isinstance(line, str) for line in out)


def test_wrap_smcl_uses_smcl_markup(long_text: str) -> None:
    import wb_text as wt
    out = wt.wrap(long_text, width=30, fmt="smcl")
    assert isinstance(out, str)
    # SMCL compound-quote separator contains a backtick
    assert "`" in out


def test_wrap_all_returns_dict_with_three_keys(long_text: str) -> None:
    import wb_text as wt
    out = wt.wrap(long_text, width=30, fmt="all")
    assert isinstance(out, dict) and set(out) == {"stack", "lines", "nlines"}
    assert out["nlines"] == len(out["lines"]) > 1
    assert out["stack"] == "\n".join(out["lines"])


# --- wrap() edge cases ------------------------------------------------

def test_wrap_empty_string_returns_empty_per_fmt() -> None:
    import wb_text as wt
    assert wt.wrap("", fmt="stack") == ""
    assert wt.wrap("", fmt="lines") == []
    assert wt.wrap("", fmt="smcl") == ""
    assert wt.wrap("", fmt="all") == {"stack": "", "lines": [], "nlines": 0}


def test_wrap_none_input_returns_empty_per_fmt() -> None:
    import wb_text as wt
    assert wt.wrap(None, fmt="stack") == ""
    assert wt.wrap(None, fmt="lines") == []


def test_wrap_width_floored_at_one() -> None:
    import wb_text as wt
    # Negative / zero widths floor to 1 — still produces a non-empty result
    out = wt.wrap("hello world", width=0, fmt="lines")
    assert isinstance(out, list) and out
    out2 = wt.wrap("hello world", width=-5, fmt="lines")
    assert isinstance(out2, list) and out2


def test_wrap_bad_fmt_raises() -> None:
    import wb_text as wt
    with pytest.raises(ValueError, match="fmt must be"):
        wt.wrap("x", fmt="bogus")


# --- wrap_lines + truncate -------------------------------------------

def test_wrap_lines_convenience(long_text: str) -> None:
    import wb_text as wt
    assert wt.wrap_lines(long_text, width=30) == wt.wrap(long_text, width=30, fmt="lines")


def test_truncate_no_op_when_fit() -> None:
    import wb_text as wt
    assert wt.truncate("short", width=80) == "short"


def test_truncate_appends_ellipsis() -> None:
    import wb_text as wt
    result = wt.truncate("a" * 100, width=10)
    assert result == "aaaaaaa..." and len(result) == 10


def test_truncate_custom_suffix() -> None:
    import wb_text as wt
    result = wt.truncate("a" * 100, width=10, suffix="…")
    assert result.endswith("…") and len(result) == 10


def test_truncate_empty_or_none_returns_empty() -> None:
    import wb_text as wt
    assert wt.truncate("", width=10) == ""
    assert wt.truncate(None, width=10) == ""
