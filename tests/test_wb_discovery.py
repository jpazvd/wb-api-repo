"""Tests for src/py/wb_discovery.py — the Python discovery API
(PR B; Python equivalent of the Stata wbopendata discovery subcommands).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict
from unittest import mock

import pytest
import yaml

# Make src/py importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src", "py")))


@pytest.fixture
def yaml_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a tiny YAML metadata cache in a tmp dir; point
    WBOPENDATA_YAML_DIR at it. wb_discovery reads the env var at
    call-time (not import-time), so no reload needed."""
    sources = {
        "_metadata": {"total_sources": 3},
        "sources": {
            "2":  {"code": "2",  "name": "WDI",  "description": "World Dev Indicators"},
            "11": {"code": "11", "name": "GFI",  "description": "Global Findex"},
            "1":  {"code": "1",  "name": "OLD",  "description": "Legacy"},
        },
    }
    topics = {
        "_metadata": {"total_topics": 2},
        "topics": {
            "3": {"code": "3", "name": "Economy", "description": ""},
            "1": {"code": "1", "name": "Agri",    "description": ""},
        },
    }
    # Full 11-key shape per yaml_generator schema v2.0 so info() and
    # describe() outputs are key-set-equal (covered by
    # test_describe_and_info_have_same_keys).
    def _ind(code, name, source_id, topic_ids, description, topic_names=None):
        return {
            "code": code, "name": name,
            "source_id": source_id, "source_name": "WDI" if source_id == "2" else "GFI",
            "topic_ids": topic_ids,
            "topic_names": topic_names or ["TopicName"] * len(topic_ids),
            "description": description, "unit": "",
            "source_org": "WB", "note": "", "limited_data": False,
        }
    indicators = {
        "_metadata": {"total_indicators": 5},
        "indicators": {
            "SP.POP.TOTL":    _ind("SP.POP.TOTL",    "Population, total",         "2",  ["8"],  "Total population"),
            "SP.POP.0014":    _ind("SP.POP.0014",    "Population ages 0-14",      "2",  ["8"],  "Population aged 0-14"),
            "NY.GDP.MKTP.CD": _ind("NY.GDP.MKTP.CD", "GDP current USD",           "2",  ["3"],  "Gross domestic product"),
            "SI.POV.DDAY":    _ind("SI.POV.DDAY",    "Poverty headcount",         "2",  ["11"], "Poverty at $2.15/day"),
            "EG.USE.ELEC":    _ind("EG.USE.ELEC",    "Electric power consumption","11", ["5"],  "Energy use"),
        },
    }
    for fname, payload in [
        ("_wbopendata_sources.yaml",    sources),
        ("_wbopendata_topics.yaml",     topics),
        ("_wbopendata_indicators.yaml", indicators),
    ]:
        (tmp_path / fname).write_text(yaml.safe_dump(payload), encoding="utf-8")
    monkeypatch.setenv("WBOPENDATA_YAML_DIR", str(tmp_path))
    return tmp_path


# --- sources / allsources / alltopics ---------------------------------

def test_sources_default_returns_sorted(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.sources()
    assert [x["code"] for x in r] == ["1", "2", "11"]


def test_sources_limit_caps(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.sources(limit=2)
    assert len(r) == 2 and r[0]["code"] == "1"


def test_allsources_no_cap(yaml_dir: Path) -> None:
    import wb_discovery as wd
    assert len(wd.allsources()) == 3


def test_alltopics_sorted(yaml_dir: Path) -> None:
    import wb_discovery as wd
    assert [x["code"] for x in wd.alltopics()] == ["1", "3"]


def test_missing_yaml_returns_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WBOPENDATA_YAML_DIR", str(tmp_path / "nope"))
    import wb_discovery as wd
    assert wd.sources() == []
    assert wd.alltopics() == []


# --- info -------------------------------------------------------------

def test_info_exact_case(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.info("SP.POP.TOTL")
    assert r is not None and r["name"] == "Population, total"


def test_info_lowercase_falls_back_to_upper(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.info("sp.pop.totl")
    assert r is not None and r["code"] == "SP.POP.TOTL"


def test_info_unknown_returns_none(yaml_dir: Path) -> None:
    import wb_discovery as wd
    assert wd.info("NOPE.NOT.HERE") is None


def test_info_empty_short_circuit(yaml_dir: Path) -> None:
    import wb_discovery as wd
    assert wd.info("") is None


# --- search -----------------------------------------------------------

def test_search_substring_across_name_and_description(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.search("population")
    assert r["total"] == 2
    assert [x["code"] for x in r["results"]] == ["SP.POP.0014", "SP.POP.TOTL"]


def test_search_topic_filter_browse_mode(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.search(topic="8")
    assert r["total"] == 2
    assert all("8" in x["topic_ids"] for x in r["results"])


def test_search_source_filter(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.search(source="11")
    assert r["total"] == 1
    assert r["results"][0]["code"] == "EG.USE.ELEC"


def test_search_term_plus_topic_intersection(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.search("total", topic="8")
    assert r["total"] == 1
    assert r["results"][0]["code"] == "SP.POP.TOTL"


def test_search_pagination(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.search(topic="8", limit=1, page=2)
    assert r["pages"] == 2 and r["page"] == 2 and len(r["results"]) == 1
    assert r["results"][0]["code"] == "SP.POP.TOTL"


def test_search_exact_code(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.search("SP.POP.TOTL", field="code", exact=True)
    assert r["total"] == 1


def test_search_page_overflow_clamps_to_last(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.search("population", page=999)
    assert r["page"] == 1


def test_search_no_results(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.search("nonexistent_term_xyz")
    assert r["total"] == 0 and r["pages"] == 0 and r["results"] == []


def test_search_field_description_only(yaml_dir: Path) -> None:
    import wb_discovery as wd
    r = wd.search("Gross", field="description")
    assert r["total"] == 1 and r["results"][0]["code"] == "NY.GDP.MKTP.CD"


# --- describe + transformer ------------------------------------------

@pytest.fixture
def api_record() -> Dict:
    return {
        "id": "SP.POP.TOTL",
        "name": "Population, total",
        "unit": "",
        "source": {"id": "2", "value": "WDI"},
        "sourceNote": "  Total population based on de facto definition.  ",
        "sourceOrganization": "World Bank",
        "topics": [{"id": "8", "value": " Health "}, {"id": "19", "value": "Climate"}],
    }


def test_transform_api_indicator_shape(api_record: Dict) -> None:
    import wb_discovery as wd
    out = wd._transform_api_indicator(api_record)
    assert out["code"] == "SP.POP.TOTL"
    assert out["topic_ids"] == ["8", "19"]
    assert out["topic_names"] == ["Health", "Climate"]
    assert out["description"] == "Total population based on de facto definition."


def test_describe_happy_path(api_record: Dict) -> None:
    import wb_discovery as wd
    with mock.patch("wb_api_client.WBAPIClient.fetch_indicator_metadata", return_value=api_record):
        r = wd.describe("SP.POP.TOTL")
    assert r is not None and r["code"] == "SP.POP.TOTL"


def test_describe_empty_short_circuit() -> None:
    import wb_discovery as wd
    assert wd.describe("") is None


def test_describe_unknown_code_returns_none() -> None:
    import wb_discovery as wd
    with mock.patch("wb_api_client.WBAPIClient.fetch_indicator_metadata", return_value=None):
        assert wd.describe("NOPE") is None


def test_describe_network_error_returns_none() -> None:
    import wb_discovery as wd
    with mock.patch("wb_api_client.WBAPIClient.fetch_indicator_metadata",
                    side_effect=RuntimeError("boom")):
        assert wd.describe("SP.POP.TOTL") is None


def test_describe_and_info_have_same_keys(yaml_dir: Path, api_record: Dict) -> None:
    """The whole point of describe(): drop-in compatible with info()."""
    import wb_discovery as wd
    info_keys = set(wd.info("SP.POP.TOTL").keys())
    describe_keys = set(wd._transform_api_indicator(api_record).keys())
    assert info_keys == describe_keys
