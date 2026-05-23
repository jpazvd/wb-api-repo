#!/usr/bin/env python3
"""
End-to-end walkthrough of the Python library catch-up arc
(Python PR A debt cleanup + PR B discovery API + PR C
country-context/multilingual/linewrap).

Run from the repo root:

    PYTHONIOENCODING=utf-8 python src/py/examples/demo_pr_b_c.py

What it exercises (and where it lives):

  Discovery (PR B)              src/py/wb_discovery.py
    sources / allsources / alltopics / info / search / describe / sync
  Data fetch enrichment (PR C)  src/py/wb_api_tools.py
    get_data(..., no_basic, geo, language)
    enrich_country_context(df, iso_col, basic, geo)
  Text wrap (PR C)              src/py/wb_text.py
    wrap(stack/newline/lines/smcl/all) / wrap_lines / truncate
  Multilingual (PR C)
    describe(language='es')
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Make the in-repo modules importable when run from repo root
HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))  # src/py/

import pandas as pd  # noqa: E402

import wb_api_tools as t  # noqa: E402
import wb_discovery as wd  # noqa: E402
import wb_text as wt  # noqa: E402


# ----------------------------------------------------------------------
# Pretty-print helpers
# ----------------------------------------------------------------------

def section(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def sub(title: str) -> None:
    print()
    print(f"--- {title} ---")


# ----------------------------------------------------------------------
# 0. Sanity: confirm the YAML cache exists (chore PR #12 populated it)
# ----------------------------------------------------------------------

def demo_0_cache_check() -> None:
    section("0. YAML cache health-check (committed by PR #12)")
    yaml_dir = Path(__file__).resolve().parents[2] / "_"
    for name in ("indicators", "sources", "topics"):
        p = yaml_dir / f"_wbopendata_{name}.yaml"
        if p.exists():
            print(f"  OK  {p.name:35s} {p.stat().st_size:>12,} bytes")
        else:
            print(f"  MISSING  {p.name} — run `python src/py/wb_api_tools.py sync` first")


# ----------------------------------------------------------------------
# 1. Discovery — read from the YAML cache (PR B C1-C3)
# ----------------------------------------------------------------------

def demo_1_discovery_yaml() -> None:
    section("1. Discovery — YAML cache reads (PR B)")

    sub("wd.sources(limit=5)")
    for s in wd.sources(limit=5):
        print(f"  [{s['code']:>3}] {s['name']}")

    sub("wd.allsources() — total count")
    all_src = wd.allsources()
    print(f"  total sources: {len(all_src)}")

    sub("wd.alltopics() — first 5 of 21")
    for tp in wd.alltopics()[:5]:
        print(f"  [{tp['code']:>2}] {tp['name']}")

    sub("wd.info('SP.POP.TOTL')")
    info = wd.info("SP.POP.TOTL")
    if info:
        for k in ("code", "name", "source_name", "topic_names", "unit"):
            print(f"  {k:14s} {info[k]}")

    sub("wd.info('sp.pop.totl') — case-insensitive fallback")
    info_lower = wd.info("sp.pop.totl")
    print(f"  matched: {info_lower is not None}, code={info_lower['code'] if info_lower else None!r}")

    sub("wd.info('NOPE.NOT.HERE') — unknown returns None")
    print(f"  result: {wd.info('NOPE.NOT.HERE')!r}")


# ----------------------------------------------------------------------
# 2. Discovery — search with filters + pagination (PR B C3)
# ----------------------------------------------------------------------

def demo_2_search() -> None:
    section("2. Search — filters + pagination (PR B C3)")

    sub("wd.search('poverty headcount', limit=3) — substring across name+desc")
    r = wd.search("poverty headcount", limit=3)
    print(f"  total={r['total']}  page={r['page']}/{r['pages']}  limit={r['limit']}")
    for hit in r["results"]:
        print(f"  [{hit['code']:<24}] {hit['name'][:60]}")

    sub("wd.search(topic='3', limit=3) — Economy & Growth (browse mode)")
    r2 = wd.search(topic="3", limit=3)
    print(f"  total={r2['total']} indicators tagged with topic 3")
    for hit in r2["results"]:
        print(f"  [{hit['code']:<24}] {hit['name'][:60]}")

    sub("wd.search('GDP', source='2', topic='3', limit=3) — combined filters")
    r3 = wd.search("GDP", source="2", topic="3", limit=3)
    print(f"  total={r3['total']} (substring 'GDP' AND source=2/WDI AND topic=3/Economy)")
    for hit in r3["results"]:
        print(f"  [{hit['code']:<24}] {hit['name'][:60]}")

    sub("wd.search('population', limit=2, page=2) — pagination")
    r4 = wd.search("population", limit=2, page=2)
    print(f"  total={r4['total']}  page={r4['page']}/{r4['pages']}  (showing page 2)")
    for hit in r4["results"]:
        print(f"  [{hit['code']:<24}] {hit['name'][:60]}")


# ----------------------------------------------------------------------
# 3. Live API — describe + multilingual (PR B C4 + PR C C4)
# ----------------------------------------------------------------------

def demo_3_describe_live() -> None:
    section("3. Live API — describe() + language= (PR B C4 + PR C C4)")

    sub("wd.describe('SP.POP.TOTL') — English (default)")
    d_en = wd.describe("SP.POP.TOTL")
    if d_en:
        print(f"  name: {d_en['name']}")
        print(f"  desc: {(d_en['description'] or '')[:140]}...")

    sub("wd.describe('SP.POP.TOTL', language='es') — Spanish")
    d_es = wd.describe("SP.POP.TOTL", language="es")
    if d_es:
        print(f"  name: {d_es['name']}")
        print(f"  desc: {(d_es['description'] or '')[:140]}...")

    sub("Verification: describe() and info() return identical key sets")
    info_keys = set(wd.info("SP.POP.TOTL").keys())
    describe_keys = set(d_en.keys()) if d_en else set()
    print(f"  info keys      : {sorted(info_keys)}")
    print(f"  describe keys  : {sorted(describe_keys)}")
    print(f"  identical?     : {info_keys == describe_keys}")


# ----------------------------------------------------------------------
# 4. Data fetch with country-context auto-merge (PR B C5 + PR C C1)
# ----------------------------------------------------------------------

def demo_4_data_context() -> None:
    section("4. get_data() — country-context auto-merge (PR B C5 + PR C C1)")

    sub("get_data(['SP.POP.TOTL'], 'BRA;USA;IND', date='2020') — DEFAULT (basic merge ON)")
    df = t.get_data(
        indicators=["SP.POP.TOTL"], countries="BRA;USA;IND", date="2020", long=True,
    )
    print(f"  shape: {df.shape}    columns: {list(df.columns)}")
    if not df.empty:
        for _, row in df.iterrows():
            print(f"  {row['countryiso3code']}  {int(row['value']):>14,}  "
                  f"region={row.get('region')}  income={row.get('incomelevel')}")

    sub("get_data(..., no_basic=True) — LEAN (no merge)")
    df2 = t.get_data(
        indicators=["SP.POP.TOTL"], countries="BRA;USA;IND", date="2020",
        long=True, no_basic=True,
    )
    print(f"  shape: {df2.shape}    columns: {list(df2.columns)}")
    print(f"  no_basic suppressed {len(df.columns) - len(df2.columns)} context cols")

    sub("get_data(..., geo=True) — basic + 3 geo cols")
    df3 = t.get_data(
        indicators=["SP.POP.TOTL"], countries="BRA;USA;IND", date="2020",
        long=True, geo=True,
    )
    extras = set(df3.columns) - set(df2.columns)
    print(f"  shape: {df3.shape}    columns added vs lean: {sorted(extras)}")

    sub("get_data(..., no_basic=True, geo=True) — geo only")
    df4 = t.get_data(
        indicators=["SP.POP.TOTL"], countries="BRA;USA;IND", date="2020",
        long=True, no_basic=True, geo=True,
    )
    print(f"  shape: {df4.shape}    columns: {list(df4.columns)}")


# ----------------------------------------------------------------------
# 5. enrich_country_context() — Stata match() for pandas (PR C C2)
# ----------------------------------------------------------------------

def demo_5_enrich_external_df() -> None:
    section("5. enrich_country_context() — Stata `match()` for pandas (PR C C2)")

    sub("User DataFrame with custom ISO column name")
    user_df = pd.DataFrame({
        "iso3":    ["BRA", "USA", "IND", "DEU", "JPN"],
        "my_metric": [1.2, 3.4, 5.6, 7.8, 9.0],
    })
    print("  Input:")
    print(user_df.to_string(index=False))

    sub("enrich_country_context(user_df, iso_col='iso3')")
    enriched = t.enrich_country_context(user_df, iso_col="iso3")
    cols_to_show = ["iso3", "my_metric", "region", "regionname", "incomelevelname"]
    print(enriched[cols_to_show].to_string(index=False))

    sub("Same with geo=True — adds 3 geographic fields")
    enriched_geo = t.enrich_country_context(user_df, iso_col="iso3", geo=True)
    geo_cols = [c for c in enriched_geo.columns if c not in user_df.columns and c not in enriched.columns]
    print(f"  Added by geo=True: {geo_cols}")
    print(enriched_geo[["iso3", "capital", "latitude", "longitude"]].to_string(index=False))


# ----------------------------------------------------------------------
# 6. wb_text — wrap / truncate (PR C C3)
# ----------------------------------------------------------------------

def demo_6_wb_text() -> None:
    section("6. wb_text — text wrapping for publication graphs (PR C C3)")

    long_str = (
        "GDP per capita (current US$) — Gross domestic product divided by midyear "
        "population. GDP is the sum of gross value added by all resident producers in "
        "the economy plus any product taxes and minus any subsidies not included in "
        "the value of the products."
    )

    sub(f"Input ({len(long_str)} chars)")
    print(f"  {long_str[:100]}...")

    sub('wt.wrap(s, width=60, fmt="stack") — for Stata `graph ..., title(...)`')
    print(f"  {wt.wrap(long_str, width=60, fmt='stack')}")

    sub('wt.wrap(s, width=60, fmt="newline") — for SMCL note/caption')
    for line in wt.wrap(long_str, width=60, fmt="newline").split("\n"):
        print(f"  |{line}")

    sub('wt.wrap(s, width=60, fmt="lines") — List[str]')
    for i, line in enumerate(wt.wrap_lines(long_str, width=60), 1):
        print(f"  [{i}] {line}")

    sub('wt.wrap(s, width=60, fmt="smcl") — Stata SMCL with {break} tag')
    print(f"  {wt.wrap(long_str, width=60, fmt='smcl')}")

    sub('wt.truncate(s, width=80, suffix="...") — single-line cap')
    print(f"  {wt.truncate(long_str, width=80)}")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main() -> int:
    # Force UTF-8 on Windows console so the ✅ in update_metadata logs etc.
    # doesn't crash subsequent prints (per memory feedback_python_io_encoding_utf8_windows).
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

    print("=" * 72)
    print("  wb-api-repo — Python library catch-up demo")
    print("  Exercises PR A (Phase 1 fixes) + PR B (discovery API)")
    print("                                 + PR C (geo / enrich / wb_text / language)")
    print("=" * 72)

    demo_0_cache_check()
    demo_1_discovery_yaml()
    demo_2_search()
    demo_3_describe_live()
    demo_4_data_context()
    demo_5_enrich_external_df()
    demo_6_wb_text()

    print()
    print("=" * 72)
    print("  Demo complete.")
    print("  See `tests/test_wb_discovery.py`, `tests/test_wb_text.py`, and")
    print("  `tests/test_wb_api_tools.py` for the 62-case pytest harness.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
