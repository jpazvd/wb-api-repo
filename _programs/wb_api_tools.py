#!/usr/bin/env python3
"""
wb_api_tools.py — World Bank API helper

Features
- Fetch country metadata (code, name, region, income, lending type, coords, etc.)
- Fetch indicator metadata (code, name, source, unit, topic IDs, etc.)
- Fetch indicator data (values, dates) for lists of indicators/countries with paging
- Write outputs to CSV/Parquet when desired

API Docs: https://api.worldbank.org/

Usage examples
--------------
# 1) Country metadata
python wb_api_tools.py countries --out countries.csv

# 2) Indicator metadata (all indicators)
python wb_api_tools.py indicators --out indicators.csv

# 3) Indicator metadata (filter by codes or search term in name)
python wb_api_tools.py indicators --codes SI.POV.DDAY,NY.GDP.PCAP.PP.KD --out ind_meta.csv
python wb_api_tools.py indicators --search "poverty" --out poverty_inds.csv

# 4) Indicator data (wide or long); comma-separated lists allowed
python wb_api_tools.py data --indicators SI.POV.DDAY,NY.GDP.PCAP.PP.KD --countries all --date 2000:2023 --long --out data_long.csv

# 5) Save to Parquet
python wb_api_tools.py data --indicators SI.POV.DDAY --countries BRA,IND,ZAF --date 2010: --out data.parquet

Notes
-----
- The World Bank API paginates results. This script automatically iterates all pages.
- Use "--countries all" for all economies, or a comma-separated list of ISO3 codes / WB codes.
- "--date" accepts ranges like "2000:2023" or open ranges like "2010:" or a single year "2020".
- Output format is inferred from file extension (.csv or .parquet). If omitted, prints a sample to stdout.
"""

from __future__ import annotations
import sys, time, math, argparse, json
from typing import Iterable, List, Dict, Any, Optional, Tuple
import requests
import pandas as pd

BASE = "https://api.worldbank.org/v2"
JSON = {"format": "json"}
SESSION = requests.Session()
DEFAULT_PER_PAGE = 1000
RETRIES = 4
BACKOFF = 0.8

def _request(url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Perform a GET with retries. Returns (header, data_list)."""
    params = dict(params or {})
    if "format" not in params:
        params["format"] = "json"
    last_err = None
    for i in range(RETRIES):
        try:
            resp = SESSION.get(url, params=params, timeout=60)
            if resp.status_code >= 500:
                raise requests.HTTPError(f"{resp.status_code} {resp.text[:200]}")
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list) or len(payload) < 2:
                # some endpoints may return a dict error
                raise ValueError(f"Unexpected payload: {str(payload)[:200]}")
            header, data = payload[0], payload[1]
            return header, data
        except Exception as e:
            last_err = e
            time.sleep(BACKOFF * (2 ** i))
    raise RuntimeError(f"Request failed after {RETRIES} attempts: {last_err}")

def _paged(url: str, params: Optional[Dict[str, Any]] = None, per_page: int = DEFAULT_PER_PAGE) -> Iterable[Dict[str, Any]]:
    """Iterate all pages for an endpoint that returns [header, data]."""
    params = dict(params or {})
    params.update({"per_page": per_page, "page": 1, "format": "json"})
    header, data = _request(url, params)
    if not data:
        return
    yield from data
    total = int(header.get("total", 0))
    pages = int(header.get("pages", 1))
    for p in range(2, pages + 1):
        params["page"] = p
        _, data = _request(url, params)
        for row in data:
            yield row

# ---------- Metadata ----------

def get_country_metadata(per_page: int = DEFAULT_PER_PAGE) -> pd.DataFrame:
    url = f"{BASE}/country"
    rows = list(_paged(url, {}, per_page=per_page))
    # Normalize nested fields
    def _get(obj, *keys):
        cur = obj
        for k in keys:
            cur = (cur or {}).get(k) if isinstance(cur, dict) else None
        return cur
    recs = []
    for r in rows:
        recs.append({
            "id": r.get("id"),
            "iso2Code": r.get("iso2Code"),
            "name": r.get("name"),
            "region_id": _get(r, "region", "id"),
            "region": _get(r, "region", "value"),
            "adminregion_id": _get(r, "adminregion", "id"),
            "adminregion": _get(r, "adminregion", "value"),
            "incomeLevel_id": _get(r, "incomeLevel", "id"),
            "incomeLevel": _get(r, "incomeLevel", "value"),
            "lendingType_id": _get(r, "lendingType", "id"),
            "lendingType": _get(r, "lendingType", "value"),
            "capitalCity": r.get("capitalCity"),
            "longitude": r.get("longitude"),
            "latitude": r.get("latitude"),
        })
    return pd.DataFrame.from_records(recs)

def get_indicator_metadata(codes: Optional[List[str]] = None,
                           search: Optional[str] = None,
                           per_page: int = DEFAULT_PER_PAGE) -> pd.DataFrame:
    """
    If 'codes' is provided, fetch metadata for those codes specifically.
    Else, fetch all indicators (optionally filtered by 'search' term in the name).
    """
    if codes:
        # Fetch individually because the /indicator endpoint doesn't filter by multiple codes in one go.
        recs = []
        for code in codes:
            url = f"{BASE}/indicator/{code}"
            _, data = _request(url, params={"format":"json"})
            for r in data:
                recs.append(_normalize_indicator_meta(r))
        return pd.DataFrame.from_records(recs)

    # All indicators (optionally search by ?search=poverty — WB API supports 'per_page' and 'page' but not 'search' formally;
    # however, there is 'source/2/indicator' and 'topic' endpoints. We'll apply client-side filter on name/code.)
    url = f"{BASE}/indicator"
    rows = list(_paged(url, {}, per_page=per_page))
    df = pd.DataFrame.from_records([_normalize_indicator_meta(r) for r in rows])
    if search:
        s = search.lower()
        mask = df["id"].str.lower().str.contains(s) | df["name"].str.lower().str.contains(s)
        df = df.loc[mask].copy()
    return df

def _normalize_indicator_meta(r: Dict[str, Any]) -> Dict[str, Any]:
    def _get(obj, *keys):
        cur = obj
        for k in keys:
            cur = (cur or {}).get(k) if isinstance(cur, dict) else None
        return cur
    topics = r.get("topics") or []
    topic_ids = [t.get("id") for t in topics if isinstance(t, dict)]
    topic_values = [t.get("value") for t in topics if isinstance(t, dict)]
    source = r.get("source") or {}
    return {
        "id": r.get("id"),
        "name": r.get("name"),
        "unit": r.get("unit"),
        "source_id": source.get("id"),
        "source": source.get("value"),
        "source_note": r.get("sourceNote"),
        "source_organization": r.get("sourceOrganization"),
        "topics": ";".join([t for t in topic_values if t]),
        "topic_ids": ";".join([t for t in topic_ids if t]),
    }

# ---------- Data ----------

def get_data(indicators: List[str],
             countries: str = "all",
             date: Optional[str] = None,
             per_page: int = DEFAULT_PER_PAGE,
             long: bool = False) -> pd.DataFrame:
    """
    Fetch indicator data for given indicator codes and countries.

    indicators: list of WDI codes, e.g., ["SI.POV.DDAY","NY.GDP.PCAP.PP.KD"]
    countries: "all" (default) or comma-separated codes (e.g., "BRA,IND,ZAF")
    date: 'YYYY' or 'YYYY:YYYY' or 'YYYY:' for open-ended to latest
    long: if True returns columns: [countryiso3code, country, indicator, date, value]
          else returns wide format with one column per indicator.
    """
    if isinstance(indicators, str):
        indicators = [c.strip() for c in indicators.split(",") if c.strip()]
    indicators = list(dict.fromkeys(indicators))  # dedupe preserve order

    frames = []
    for ind in indicators:
        url = f"{BASE}/country/{countries}/indicator/{ind}"
        params = {}
        if date:
            params["date"] = date
        rows = list(_paged(url, params, per_page=per_page))
        if not rows:
            continue
        # Normalize into DataFrame
        recs = []
        for r in rows:
            countryiso3 = r.get("countryiso3code")
            country = (r.get("country") or {}).get("value") if isinstance(r.get("country"), dict) else None
            recs.append({
                "countryiso3code": countryiso3,
                "country": country,
                "indicator": ind,
                "date": r.get("date"),
                "value": r.get("value"),
            })
        frames.append(pd.DataFrame.from_records(recs))

    if not frames:
        return pd.DataFrame(columns=["countryiso3code","country","indicator","date","value"])

    df_long = pd.concat(frames, ignore_index=True)
    # Ensure proper dtypes/sorting
    with pd.option_context("mode.copy_on_write", True):
        df_long["date"] = pd.to_numeric(df_long["date"], errors="coerce")
        df_long.sort_values(["countryiso3code", "indicator", "date"], inplace=True)

    if long:
        return df_long
    # Wide format pivot
    wide = df_long.pivot_table(index=["countryiso3code","country","date"],
                               columns="indicator",
                               values="value",
                               aggfunc="first").reset_index()
    # Flatten columns
    wide.columns = [c if isinstance(c, str) else c[1] for c in wide.columns.values]
    return wide

# ---------- IO helpers ----------

def _save_df(df: pd.DataFrame, out: Optional[str]) -> None:
    if not out:
        # Print preview
        print(df.head(20).to_string(index=False))
        return
    out = out.strip()
    lower = out.lower()
    if lower.endswith(".csv"):
        df.to_csv(out, index=False)
    elif lower.endswith(".parquet"):
        df.to_parquet(out, index=False)
    elif lower.endswith(".yaml") or lower.endswith(".yml"):
        try:
            import yaml  # type: ignore
        except Exception as e:
            raise SystemExit("YAML output requested but PyYAML is not installed. Install with: pip install pyyaml") from e
        # Convert DataFrame to list-of-dicts for readable YAML
        records = df.to_dict(orient="records")
        with open(out, "w", encoding="utf-8") as f:
            yaml.safe_dump(records, f, sort_keys=False, allow_unicode=True)
    else:
        # default CSV
        df.to_csv(out, index=False)
    print(f"Wrote: {out}  (rows={len(df):,}, cols={len(df.columns)})")

# ---------- CLI ----------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="World Bank API helper")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_c = sub.add_parser("countries", help="Fetch country metadata")
    p_c.add_argument("--out", help="Output file (.csv or .parquet)")

    p_i = sub.add_parser("indicators", help="Fetch indicator metadata")
    p_i.add_argument("--codes", help="Comma-separated indicator codes")
    p_i.add_argument("--search", help="Filter by substring in code or name (client-side)")
    p_i.add_argument("--out", help="Output file (.csv or .parquet)")

    p_d = sub.add_parser("data", help="Fetch indicator data")
    p_d.add_argument("--indicators", required=True, help="Comma-separated indicator codes")
    p_d.add_argument("--countries", default="all", help='"all" or comma-separated ISO3 codes (e.g., BRA,IND)')
    p_d.add_argument("--date", help='Year or range "YYYY:YYYY" or open "YYYY:"')
    p_d.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE, help="Rows per page (default 1000)")
    p_d.add_argument("--long", action="store_true", help="Return long/stacked format")
    p_d.add_argument("--out", help="Output file (.csv or .parquet)")

    return p

def main(argv=None):
    argv = argv or sys.argv[1:]
    args = build_parser().parse_args(argv)
    if args.cmd == "countries":
        df = get_country_metadata()
        _save_df(df, args.out)
    elif args.cmd == "indicators":
        codes = None
        if args.codes:
            codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        df = get_indicator_metadata(codes=codes, search=args.search)
        _save_df(df, args.out)
    elif args.cmd == "data":
        df = get_data(indicators=args.indicators,
                      countries=args.countries,
                      date=args.date,
                      per_page=args.per_page,
                      long=args.long)
        _save_df(df, args.out)

if __name__ == "__main__":
    main()
