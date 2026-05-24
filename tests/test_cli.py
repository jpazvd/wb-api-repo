"""Tests for wb_api_tools.cli._save_df output routing."""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd
import pytest

from wb_api_tools.cli import _save_df


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "countryiso3code": ["BRA", "USA", "IND"],
        "year": [2020, 2020, 2020],
        "value": [212.6, 331.5, 1402.6],
    })


def test_save_df_dash_writes_full_csv_to_stdout(sample_df: pd.DataFrame, capsys) -> None:
    """`--out -` must emit FULL CSV (not head-only preview) to stdout."""
    _save_df(sample_df, "-")
    captured = capsys.readouterr()
    # Expect header + all 3 rows (CSV row count = 4 with header, plus trailing newline)
    lines = [ln for ln in captured.out.splitlines() if ln.strip()]
    assert lines[0] == "countryiso3code,year,value"
    assert len(lines) == 1 + len(sample_df)
    # BRA / USA / IND all present (i.e. not truncated to a preview)
    body = "\n".join(lines[1:])
    for code in ("BRA", "USA", "IND"):
        assert code in body


def test_save_df_none_prints_preview_only(sample_df: pd.DataFrame, capsys) -> None:
    """`--out` omitted -> head(20) preview to stdout, NOT a CSV. Distinct from --out -."""
    _save_df(sample_df, None)
    captured = capsys.readouterr()
    # Preview uses to_string, which does NOT produce comma-separated header
    assert "," not in captured.out.split("\n")[0], \
        "preview should NOT be CSV (commas) — use --out - for CSV-to-stdout"


def test_save_df_csv_writes_file(sample_df: pd.DataFrame, tmp_path: Path, capsys) -> None:
    out = tmp_path / "data.csv"
    _save_df(sample_df, str(out))
    assert out.exists()
    contents = out.read_text(encoding="utf-8")
    assert contents.startswith("countryiso3code,year,value")
    # "Wrote: ..." status line printed to stdout
    assert "Wrote:" in capsys.readouterr().out


def test_save_df_yaml_writes_file(sample_df: pd.DataFrame, tmp_path: Path) -> None:
    out = tmp_path / "data.yaml"
    _save_df(sample_df, str(out))
    assert out.exists()
    import yaml
    records = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert isinstance(records, list) and len(records) == 3
    assert records[0]["countryiso3code"] == "BRA"


def test_save_df_unknown_extension_falls_back_to_csv(sample_df: pd.DataFrame, tmp_path: Path) -> None:
    out = tmp_path / "data.weird"
    _save_df(sample_df, str(out))
    assert out.exists()
    assert out.read_text(encoding="utf-8").startswith("countryiso3code,year,value")


def test_save_df_json_writes_records_orient(sample_df: pd.DataFrame, tmp_path: Path) -> None:
    """`.json` -> records orient, pretty-indented."""
    out = tmp_path / "data.json"
    _save_df(sample_df, str(out))
    import json
    records = json.loads(out.read_text(encoding="utf-8"))
    assert isinstance(records, list) and len(records) == 3
    assert records[0] == {"countryiso3code": "BRA", "year": 2020, "value": 212.6}
    # Confirm pretty-printed (indent=2) — not a single-line dump
    assert "\n" in out.read_text(encoding="utf-8")


def test_save_df_jsonl_writes_lines_orient(sample_df: pd.DataFrame, tmp_path: Path) -> None:
    """`.jsonl` -> one record per line (streaming-friendly)."""
    out = tmp_path / "data.jsonl"
    _save_df(sample_df, str(out))
    import json
    lines = [ln for ln in out.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 3, "each row should be on its own line"
    parsed = [json.loads(ln) for ln in lines]
    assert parsed[0]["countryiso3code"] == "BRA"
    assert parsed[2]["countryiso3code"] == "IND"


def test_save_df_ndjson_alias_same_as_jsonl(sample_df: pd.DataFrame, tmp_path: Path) -> None:
    """`.ndjson` is the same wire format as `.jsonl` (common alias)."""
    out_jsonl = tmp_path / "data.jsonl"
    out_ndjson = tmp_path / "data.ndjson"
    _save_df(sample_df, str(out_jsonl))
    _save_df(sample_df, str(out_ndjson))
    assert out_jsonl.read_text(encoding="utf-8") == out_ndjson.read_text(encoding="utf-8")
