#!/usr/bin/env python3
"""
Generate QA test fixtures for wbopendata.

Downloads frozen API response snapshots from the World Bank API and saves
them as CSV and XML files for deterministic offline testing.

Two categories of fixtures are generated:

  1. Data query fixtures (CSV) — used by DET/EXT tests in run_tests.do
  2. API metadata fixtures (XML/JSON) — used by UPD tests for offline
     metadata update pipeline validation

Usage:
    python generate_test_fixtures.py [--data-only | --api-only] [--verbose]

Output:
    qa/fixtures/*.csv           (data query fixtures)
    qa/fixtures/api/*.xml|json  (API metadata fixtures)
    qa/fixtures/manifest.json   (data fixture index)
    qa/fixtures/api/manifest.json (API fixture index)

Author: JP Azevedo / Claude Code
Date: 2026-02-10
License: MIT
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FIXTURES_DIR = Path(__file__).parent
API_DIR = FIXTURES_DIR / "api"

WB_API_BASE = "https://api.worldbank.org/v2"

# Rate limiting
REQUEST_DELAY = 0.5  # seconds between requests

# ---------------------------------------------------------------------------
# Data query fixtures (CSV)
# ---------------------------------------------------------------------------
DATA_FIXTURES = [
    {
        "filename": "SP_POP_TOTL_USA.csv",
        "url": f"{WB_API_BASE}/en/countries/USA/Indicators/SP.POP.TOTL?downloadformat=CSV&HREQ=N&filetype=data",
        "description": "DET-01: Population total, USA (basic parsing)",
    },
    {
        "filename": "SP_POP_TOTL_USA_BRA_IND.csv",
        "url": f"{WB_API_BASE}/en/countries/USA;BRA;IND/Indicators/SP.POP.TOTL?downloadformat=CSV&HREQ=N&filetype=data",
        "description": "DET-02: Population total, USA+BRA+IND (wide reshape)",
    },
    {
        "filename": "SP_POP_TOTL_all.csv",
        "url": f"{WB_API_BASE}/en/countries/all/Indicators/SP.POP.TOTL?downloadformat=CSV&HREQ=N&filetype=data",
        "description": "DET-06: Population total, all countries (long format)",
    },
    {
        "filename": "SP_POP_TOTL_USA_2020.csv",
        "url": f"{WB_API_BASE}/en/countries/USA/Indicators/SP.POP.TOTL?downloadformat=CSV&HREQ=N&filetype=data&date=2020",
        "description": "DET-07: Population total, USA, 2020 (latest test)",
    },
    {
        "filename": "NY_GDP_MKTP_CD_USA.csv",
        "url": f"{WB_API_BASE}/en/countries/USA/Indicators/NY.GDP.MKTP.CD?downloadformat=CSV&HREQ=N&filetype=data",
        "description": "DET-09/10: GDP current USD, USA (char / nochar test)",
    },
    {
        "filename": "SP_DYN_LE00_IN_all.csv",
        "url": f"{WB_API_BASE}/en/countries/all/Indicators/SP.DYN.LE00.IN?downloadformat=CSV&HREQ=N&filetype=data",
        "description": "EXT-02: Life expectancy, all countries (extreme: many countries)",
    },
    {
        "filename": "SI_POV_DDAY_all.csv",
        "url": f"{WB_API_BASE}/en/countries/all/Indicators/SI.POV.DDAY?downloadformat=CSV&HREQ=N&filetype=data",
        "description": "EXT-03a: Poverty headcount, all countries",
    },
    {
        "filename": "SL_UEM_TOTL_ZS_all.csv",
        "url": f"{WB_API_BASE}/en/countries/all/Indicators/SL.UEM.TOTL.ZS?downloadformat=CSV&HREQ=N&filetype=data",
        "description": "EXT-03b: Unemployment rate, all countries",
    },
    {
        "filename": "country_USA.csv",
        "url": f"{WB_API_BASE}/en/Countries/USA/?downloadformat=CSV&HREQ=N&filetype=data",
        "description": "Country metadata query: USA",
    },
    {
        "filename": "SP_POP_TOTL_USA_2010_2020.csv",
        "url": f"{WB_API_BASE}/en/countries/USA/Indicators/SP.POP.TOTL?downloadformat=CSV&HREQ=N&filetype=data&date=2010:2020",
        "description": "Year range: Population total, USA, 2010-2020",
    },
    {
        "filename": "SP_POP_TOTL_USA_source2.csv",
        "url": f"{WB_API_BASE}/en/countries/USA/Indicators/SP.POP.TOTL?source=2&downloadformat=CSV&HREQ=N&filetype=data",
        "description": "Source filter: Population total, USA, WDI (source=2)",
    },
    {
        "filename": "SP_POP_TOTL_USA_2020_pin.csv",
        "url": f"{WB_API_BASE}/en/countries/USA/Indicators/SP.POP.TOTL?downloadformat=CSV&HREQ=N&filetype=data&date=2020",
        "description": "Value-pin: Population total, USA, 2020",
    },
    {
        "filename": "DEPRECATED_INDICATOR_all.csv",
        "url": None,
        "description": "ERR: Deprecated/invalid indicator (manually created, empty fixture)",
        "manual": True,
    },
]

# ---------------------------------------------------------------------------
# API metadata fixtures (XML/JSON)
# ---------------------------------------------------------------------------
API_FIXTURES = [
    {
        "filename": "indicators_count.xml",
        "url": f"{WB_API_BASE}/indicators/?per_page=1&page=1",
        "description": "UPD-01: Indicator count (orchestrator check)",
    },
    {
        "filename": "countries_count.xml",
        "url": f"{WB_API_BASE}/countries/?per_page=1&page=1",
        "description": "UPD-02: Country count (orchestrator check)",
    },
    {
        "filename": "indicators_page1.xml",
        "url": f"{WB_API_BASE}/indicators?per_page=10000&page=1",
        "description": "UPD-03: Indicator catalogue page 1/3",
    },
    {
        "filename": "indicators_page2.xml",
        "url": f"{WB_API_BASE}/indicators?per_page=10000&page=2",
        "description": "UPD-04: Indicator catalogue page 2/3",
    },
    {
        "filename": "indicators_page3.xml",
        "url": f"{WB_API_BASE}/indicators?per_page=10000&page=3",
        "description": "UPD-05: Indicator catalogue page 3/3",
    },
    {
        "filename": "countries_full.xml",
        "url": f"{WB_API_BASE}/countries/?per_page=500&page=1",
        "description": "UPD-06: Country metadata (full, page 1)",
    },
    {
        "filename": "region_full.xml",
        "url": f"{WB_API_BASE}/region/?per_page=500&page=1",
        "description": "UPD-08: Region metadata (full, page 1)",
    },
    {
        "filename": "indicators_default.xml",
        "url": f"{WB_API_BASE}/indicators/?per_page=25000&page=1",
        "description": "Default _api_read query (indicator list with per_page=25000)",
    },
    {
        "filename": "countries_page2.xml",
        "url": f"{WB_API_BASE}/countries/?per_page=500&page=2",
        "description": "Country metadata page 2 (if >500 entries)",
    },
    {
        "filename": "indicators_count.json",
        "url": f"{WB_API_BASE}/indicators/?per_page=1&page=1&format=json",
        "description": "Indicator count (JSON format for validation)",
    },
    {
        "filename": "countries_count.json",
        "url": f"{WB_API_BASE}/countries/?per_page=1&page=1&format=json",
        "description": "Country count (JSON format for validation)",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fetch_url(url, timeout=60):
    """Fetch URL with retry logic."""
    for attempt in range(3):
        try:
            req = Request(url, headers={"User-Agent": "wbopendata-fixtures/1.0"})
            with urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (URLError, HTTPError) as e:
            if attempt < 2:
                print(f"    [retry {attempt + 1}] {e}")
                time.sleep(2 * (attempt + 1))
            else:
                raise


def download_fixture(fixture, dest_dir, verbose=False):
    """Download a single fixture file."""
    filepath = dest_dir / fixture["filename"]

    if fixture.get("manual"):
        # Manual fixtures are not downloaded — create if missing
        if not filepath.exists():
            filepath.write_text(
                "countryiso3code,indicator_id,indicator_name,date,value\n",
                encoding="utf-8",
            )
            if verbose:
                print(f"  [CREATED] {fixture['filename']} (empty fixture)")
        else:
            if verbose:
                print(f"  [EXISTS]  {fixture['filename']} (manual)")
        return True

    url = fixture["url"]
    if verbose:
        print(f"  Downloading {fixture['filename']}...", end=" ", flush=True)

    try:
        data = fetch_url(url)
        filepath.write_bytes(data)
        size_kb = len(data) / 1024
        if verbose:
            print(f"{size_kb:.1f} KB")
        return True
    except Exception as e:
        print(f"\n    [ERROR] {fixture['filename']}: {e}")
        return False


def write_manifest(fixtures, dest_dir, label):
    """Write manifest.json for a set of fixtures."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entries = []

    for f in fixtures:
        filepath = dest_dir / f["filename"]
        entry = {
            "filename": f["filename"],
            "url": f.get("url", "(manual)"),
            "description": f["description"],
            "exists": filepath.exists(),
        }
        if filepath.exists():
            stat = filepath.stat()
            entry["size_bytes"] = stat.st_size
            entry["modified"] = datetime.fromtimestamp(stat.st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        entries.append(entry)

    manifest = {
        "label": label,
        "generated_at": timestamp,
        "generator": "generate_test_fixtures.py",
        "fixtures": entries,
    }

    manifest_path = dest_dir / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as fp:
        json.dump(manifest, fp, indent=2)

    return manifest_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Generate QA test fixtures for wbopendata"
    )
    parser.add_argument("--data-only", action="store_true", help="Only data CSVs")
    parser.add_argument("--api-only", action="store_true", help="Only API XML/JSON")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    print("=" * 60)
    print("wbopendata fixture generator")
    print("=" * 60)

    os.makedirs(API_DIR, exist_ok=True)

    success = 0
    failed = 0

    # --- Data fixtures ---
    if not args.api_only:
        print(f"\n[Data fixtures] -> {FIXTURES_DIR}")
        for f in DATA_FIXTURES:
            ok = download_fixture(f, FIXTURES_DIR, args.verbose)
            if ok:
                success += 1
            else:
                failed += 1
            time.sleep(REQUEST_DELAY)

        mpath = write_manifest(DATA_FIXTURES, FIXTURES_DIR, "wbopendata data query fixtures")
        print(f"  Manifest: {mpath}")

    # --- API fixtures ---
    if not args.data_only:
        print(f"\n[API fixtures] -> {API_DIR}")
        for f in API_FIXTURES:
            ok = download_fixture(f, API_DIR, args.verbose)
            if ok:
                success += 1
            else:
                failed += 1
            time.sleep(REQUEST_DELAY)

        mpath = write_manifest(API_FIXTURES, API_DIR, "wbopendata update pipeline fixtures")
        print(f"  Manifest: {mpath}")

    # --- Summary ---
    print(f"\n{'=' * 60}")
    print(f"Done: {success} succeeded, {failed} failed")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
