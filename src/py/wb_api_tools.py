#!/usr/bin/env python3
from __future__ import annotations
"""
wb_api_tools.py — World Bank API helper
- Country metadata
- Indicator metadata
- Indicator data (long/wide)
- CSV/Parquet/YAML output
"""
import sys, time, argparse
from typing import Dict, Any, Optional, Iterable, List, Tuple
import requests, pandas as pd

# Global verbose flag
VERBOSE = False

BASE = "https://api.worldbank.org/v2"
SESSION = requests.Session()
DEFAULT_PER_PAGE = 1000
RETRIES = 4
BACKOFF = 0.8

def _request(url: str, params: Optional[Dict[str, Any]] = None, format_type: str = "json") -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Make API request with support for both JSON and CSV formats
    Following Stata wbopendata approach for CSV downloads
    """
    params = dict(params or {})
    params.setdefault("format", format_type)
    last = None
    for i in range(RETRIES):
        try:
            r = SESSION.get(url, params=params, timeout=60)
            if r.status_code >= 500:
                raise requests.HTTPError(f"{r.status_code} {r.text[:200]}")
            r.raise_for_status()

            if format_type == "csv":
                # Handle CSV response directly
                import io
                # Strip BOM if present
                text = r.text
                if text.startswith('\ufeff'):
                    text = text[1:]
                df = pd.read_csv(io.StringIO(text))
                # Convert DataFrame to expected format
                data = df.to_dict('records')
                return {}, data
            else:
                # Handle JSON response (for metadata)
                payload = r.json()
                if isinstance(payload, list) and len(payload) >= 2:
                    return payload[0], payload[1]
                raise ValueError("Unexpected JSON payload structure")

        except Exception as e:
            last = e
            time.sleep(BACKOFF * (2**i))
    raise RuntimeError(f"Request failed after {RETRIES} attempts: {last}")

def _paged(url: str, params: Optional[Dict[str, Any]] = None, per_page: int = DEFAULT_PER_PAGE, format_type: str = "json") -> Iterable[Dict[str, Any]]:
    """
    Handle paginated requests with support for both JSON and CSV formats
    For CSV format, pagination is handled differently since all data comes in one response
    """
    params = dict(params or {})
    params.update({"format": format_type, "per_page": per_page, "page": 1})

    if format_type == "csv":
        # For CSV, we can get all data in one request
        # World Bank CSV API supports large downloads without pagination
        _, data = _request(url, params, format_type="csv")
        if data:
            for row in data:
                yield row
    else:
        # JSON pagination (for metadata)
        hdr, data = _request(url, params, format_type="json")
        if data:
            for row in data:
                yield row
        pages = int((hdr or {}).get("pages", 1) or 1)
        for p in range(2, pages+1):
            params["page"] = p
            _, data = _request(url, params, format_type="json")
            for row in data or []:
                yield row

def get_country_metadata(per_page: int = DEFAULT_PER_PAGE) -> pd.DataFrame:
    """Fetch country metadata using JSON (more reliable for structured data)"""
    url = f"{BASE}/country"
    rows = list(_paged(url, {}, per_page=per_page, format_type="json"))
    def g(obj, *ks):
        cur = obj
        for k in ks:
            cur = (cur or {}).get(k) if isinstance(cur, dict) else None
        return cur
    recs = [{
        "id": r.get("id"),
        "iso2Code": r.get("iso2Code"),
        "name": r.get("name"),
        "region_id": g(r,"region","id"),
        "region": g(r,"region","value"),
        "adminregion_id": g(r,"adminregion","id"),
        "adminregion": g(r,"adminregion","value"),
        "incomeLevel_id": g(r,"incomeLevel","id"),
        "incomeLevel": g(r,"incomeLevel","value"),
        "lendingType_id": g(r,"lendingType","id"),
        "lendingType": g(r,"lendingType","value"),
        "capitalCity": r.get("capitalCity"),
        "longitude": r.get("longitude"),
        "latitude": r.get("latitude"),
    } for r in rows]
    return pd.DataFrame.from_records(recs)

def _normalize_indicator_meta(r: Dict[str, Any]) -> Dict[str, Any]:
    topics = r.get("topics") or []
    topic_ids = [t.get("id") for t in topics if isinstance(t, dict)]
    topic_vals = [t.get("value") for t in topics if isinstance(t, dict)]
    src = r.get("source") or {}
    return {
        "id": r.get("id"),
        "name": r.get("name"),
        "unit": r.get("unit"),
        "source_id": src.get("id"),
        "source": src.get("value"),
        "source_note": r.get("sourceNote"),
        "source_organization": r.get("sourceOrganization"),
        "topics": ";".join([t for t in topic_vals if t]),
        "topic_ids": ";".join([t for t in topic_ids if t]),
    }

def get_indicator_metadata(codes: Optional[List[str]] = None, search: Optional[str] = None,
                           per_page: int = DEFAULT_PER_PAGE) -> pd.DataFrame:
    """Fetch indicator metadata using JSON (better for structured metadata)"""
    if codes:
        recs = []
        for code in codes:
            url = f"{BASE}/indicator/{code}"
            _, data = _request(url, params={"format": "json"}, format_type="json")
            for r in data:
                recs.append(_normalize_indicator_meta(r))
        return pd.DataFrame.from_records(recs)
    url = f"{BASE}/indicator"
    rows = list(_paged(url, {}, per_page=per_page, format_type="json"))
    df = pd.DataFrame.from_records([_normalize_indicator_meta(r) for r in rows])
    if search:
        s = search.lower()
        mask = df["id"].str.lower().str.contains(s) | df["name"].str.lower().str.contains(s)
        df = df.loc[mask].copy()
    return df

_BASIC_CONTEXT_CACHE: Optional[pd.DataFrame] = None
_GEO_CONTEXT_CACHE: Optional[pd.DataFrame] = None


def _get_geo_context() -> pd.DataFrame:
    """Return cached ISO3 → 3-field geographic context lookup table.

    Python equivalent of the Stata Phase-5 `geo` flag. Columns:
      countryiso3code, capital, latitude, longitude

    Cached separately from basic context so users can combine flags
    (basic + geo) or pick geo-only without re-fetching /country.
    """
    global _GEO_CONTEXT_CACHE
    if _GEO_CONTEXT_CACHE is None:
        cm = get_country_metadata()
        _GEO_CONTEXT_CACHE = cm[[
            "id", "capitalCity", "longitude", "latitude",
        ]].rename(columns={
            "id":          "countryiso3code",
            "capitalCity": "capital",
        })
    return _GEO_CONTEXT_CACHE


def _get_basic_context() -> pd.DataFrame:
    """Return cached ISO3 → 8-field basic country context lookup table.

    Python equivalent of the Stata Phase-5 auto-merge surface. Columns:
      countryiso3code, region, regionname, adminregion, adminregionname,
      incomelevel, incomelevelname, lendingtype, lendingtypename

    Cached at module level so multi-call workflows don't re-fetch
    country metadata from /country on every get_data() invocation.
    """
    global _BASIC_CONTEXT_CACHE
    if _BASIC_CONTEXT_CACHE is None:
        cm = get_country_metadata()
        _BASIC_CONTEXT_CACHE = cm[[
            "id",
            "region_id", "region",
            "adminregion_id", "adminregion",
            "incomeLevel_id", "incomeLevel",
            "lendingType_id", "lendingType",
        ]].rename(columns={
            "id":               "countryiso3code",
            "region_id":        "region",
            "region":           "regionname",
            "adminregion_id":   "adminregion",
            "adminregion":      "adminregionname",
            "incomeLevel_id":   "incomelevel",
            "incomeLevel":      "incomelevelname",
            "lendingType_id":   "lendingtype",
            "lendingType":      "lendingtypename",
        })
    return _BASIC_CONTEXT_CACHE


def enrich_country_context(
    df: pd.DataFrame,
    iso_col: str = "countryiso3code",
    *,
    basic: bool = True,
    geo: bool = False,
) -> pd.DataFrame:
    """Merge WB country-context fields into a user-supplied DataFrame
    (Python equivalent of Stata `wbopendata, match(varname) [basic geo]`).

    Args:
        df:      input DataFrame with an ISO3 country code column.
        iso_col: name of the ISO3 column in df (default 'countryiso3code').
        basic:   include the 8 basic-context fields (region/incomelevel/etc.).
                 Default True.
        geo:     include 3 geographic fields (capital/lat/long). Default False.

    Returns:
        New DataFrame (input not mutated) with context columns appended.
        Rows whose ISO3 isn't found in WB metadata get NaN in the new
        columns (left-join semantics).

    Raises:
        KeyError if `iso_col` isn't a column in `df`.
    """
    if iso_col not in df.columns:
        raise KeyError(f"iso_col {iso_col!r} not found in DataFrame columns: {list(df.columns)}")
    out = df.copy()
    if basic:
        bc = _get_basic_context()
        out = out.merge(bc, left_on=iso_col, right_on="countryiso3code", how="left")
        if iso_col != "countryiso3code":
            out = out.drop(columns="countryiso3code")
    if geo:
        gc = _get_geo_context()
        out = out.merge(gc, left_on=iso_col, right_on="countryiso3code", how="left")
        if iso_col != "countryiso3code":
            out = out.drop(columns="countryiso3code")
    return out


def get_data(indicators: List[str], countries: str = "all", date: Optional[str] = None,
             per_page: int = DEFAULT_PER_PAGE, long: bool = False,
             no_basic: bool = False, geo: bool = False) -> pd.DataFrame:
    """
    Fetch indicator data using CSV downloads (following Stata wbopendata approach)
    Much more reliable than JSON for bulk data.

    Phase 5 parity: by default also merges 8 basic country-context fields
    (region/regionname/adminregion/adminregionname/incomelevel/
     incomelevelname/lendingtype/lendingtypename) from /country.

    PR C parity: `geo=True` adds 3 geographic fields (capital, latitude,
    longitude) — supplementary to the basic merge, not exclusive of it.
    Flag matrix:
        no_basic=False, geo=False  →  8 basic fields           (default)
        no_basic=False, geo=True   →  8 basic + 3 geo = 11 fields
        no_basic=True,  geo=True   →  3 geo only
        no_basic=True,  geo=False  →  no merge (lean output)
    """
    if isinstance(indicators, str):
        indicators = [c.strip() for c in indicators.split(",") if c.strip()]
    indicators = list(dict.fromkeys(indicators))  # Remove duplicates

    global VERBOSE
    frames = []
    for ind in indicators:
        try:
            # Use CSV download approach like Stata wbopendata
            url = f"{BASE}/countries/{countries}/indicators/{ind}"

            # Build parameters for CSV download
            params = {
                "downloadformat": "CSV",
                "HREQ": "N",
                "filetype": "data"
            }
            if date:
                params["date"] = date

            # Make direct CSV request (no pagination needed for CSV)
            r = SESSION.get(url, params=params, timeout=60)
            if VERBOSE:
                print(f"Debug-fetch URL: {r.url}")
                text_raw = r.text
                print(f"Debug-fetch text length: {len(text_raw)}")
                print(f"Debug-fetch sample:\n{text_raw[:200]}")
            r.raise_for_status()

            # Parse CSV response from raw bytes to handle BOM and encoding correctly
            import io
            df = pd.read_csv(io.BytesIO(r.content), encoding='utf-8-sig', quoting=1)
            if VERBOSE:
                print(f"Debug: Columns for {ind}: {list(df.columns)}")
                print(f"Debug: Shape: {df.shape}")

            # The World Bank CSV comes in wide format with years as columns
            # Expected columns: Country Name, Country Code, Indicator Name, Indicator Code, 1960, 1961, etc.

            # Identify year columns (numeric column names)
            year_columns = []
            id_columns = []
            for col in df.columns:
                col_str = str(col).strip()
                if col_str.isdigit() and len(col_str) == 4:  # 4-digit years
                    year_columns.append(col)
                else:
                    id_columns.append(col)

            if not year_columns:
                print(f"Warning: No year columns found for {ind}. Columns: {list(df.columns)}")
                continue

            # Melt the dataframe to convert from wide to long format
            df_long = df.melt(
                id_vars=id_columns,
                value_vars=year_columns,
                var_name='date',
                value_name='value'
            )

            # Rename columns to standard format
            column_rename_map = {
                "Country Code": "countryiso3code",
                "Country Name": "country",
                "Indicator Code": "indicator_code",
                "Indicator Name": "indicator_name"
            }
            df_long = df_long.rename(columns=column_rename_map)

            # Ensure indicator column is set correctly
            df_long["indicator"] = ind

            # Convert date to numeric and value to numeric
            df_long["date"] = pd.to_numeric(df_long["date"], errors="coerce")
            df_long["value"] = pd.to_numeric(df_long["value"], errors="coerce")

            # Remove rows with NaN values
            df_long = df_long.dropna(subset=['value'])

            frames.append(df_long)

        except Exception as e:
            print(f"Error processing {ind}: {e}")
            import traceback
            traceback.print_exc()
            continue

    if not frames:
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=["countryiso3code", "country", "indicator", "date", "value"])

    # Combine all indicator data
    df_combined = pd.concat(frames, ignore_index=True)

    # Sort data
    df_combined = df_combined.sort_values(["countryiso3code", "indicator", "date"])

    # Phase-5 parity: auto-merge basic country context unless opted out.
    # PR C: also merge geo context when requested (supplementary to basic).
    # Done BEFORE the long/wide branch so context columns participate in
    # the wide-format pivot index correctly.
    if not no_basic:
        try:
            bc = _get_basic_context()
            df_combined = df_combined.merge(bc, on="countryiso3code", how="left")
        except Exception as e:
            print(f"Warning: basic country context merge skipped: {e}")
    if geo:
        try:
            gc = _get_geo_context()
            df_combined = df_combined.merge(gc, on="countryiso3code", how="left")
        except Exception as e:
            print(f"Warning: geo context merge skipped: {e}")

    if long:
        # Return long format (already is)
        return df_combined
    else:
        # Convert to wide format
        # The basic-context columns (if merged) need to be in the pivot
        # index so they don't get dropped by pivot_table's column reduction.
        ctx_cols = [
            "region", "regionname", "adminregion", "adminregionname",
            "incomelevel", "incomelevelname", "lendingtype", "lendingtypename",
            "capital", "latitude", "longitude",
        ]
        extra_idx = [c for c in ctx_cols if c in df_combined.columns]
        wide = df_combined.pivot_table(
            index=["countryiso3code", "country", "date"] + extra_idx,
            columns="indicator",
            values="value",
            aggfunc="first"
        ).reset_index()

        # Clean up column names (remove multi-index)
        wide.columns = [col if isinstance(col, str) else col[1] for col in wide.columns.values]

        return wide

def _save_df(df, out: Optional[str]) -> None:
    if not out:
        print(df.head(20).to_string(index=False))
        return
    out = out.strip(); lower = out.lower()
    if lower.endswith(".csv"):
        df.to_csv(out, index=False)
    elif lower.endswith(".parquet"):
        df.to_parquet(out, index=False)
    elif lower.endswith(".yaml") or lower.endswith(".yml"):
        try:
            import yaml
        except Exception as e:
            raise SystemExit("Install PyYAML for YAML output: pip install pyyaml") from e
        records = df.to_dict(orient="records")
        with open(out, "w", encoding="utf-8") as f:
            yaml.safe_dump(records, f, sort_keys=False, allow_unicode=True)
    else:
        df.to_csv(out, index=False)
    print(f"Wrote: {out}  (rows={len(df):,}, cols={len(df.columns)})")

def build_parser():
    p = argparse.ArgumentParser(description="World Bank API helper")
    p.add_argument("--verbose", action="store_true", help="Show debug output (verbose)")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_c = sub.add_parser("countries", help="Fetch country metadata")
    p_c.add_argument("--out")

    p_i = sub.add_parser("indicators", help="Fetch indicator metadata")
    p_i.add_argument("--codes")
    p_i.add_argument("--search")
    p_i.add_argument("--out")

    p_d = sub.add_parser("data", help="Fetch indicator data")
    p_d.add_argument("--indicators", required=True)
    p_d.add_argument("--countries", default="all")
    p_d.add_argument("--date")
    p_d.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    p_d.add_argument("--long", action="store_true")
    p_d.add_argument("--no-basic", action="store_true",
                     help="Skip the 8-field country-context auto-merge (Phase 5 parity)")
    p_d.add_argument("--geo", action="store_true",
                     help="Also merge capital/latitude/longitude (PR C; combinable with --no-basic for geo-only)")
    p_d.add_argument("--out")

    # --- Discovery subcommands (PR B) --------------------------------
    # All read from src/_/_wbopendata_*.yaml; run `wb-update-metadata`
    # first or use the `sync` subcommand below.
    p_src = sub.add_parser("sources", help="List WB data sources")
    p_src.add_argument("--limit", type=int, default=20,
                       help="Max sources to show (default 20; pass --all for no cap)")
    p_src.add_argument("--all", action="store_true", help="No limit (equivalent to allsources)")
    p_src.add_argument("--out")

    p_top = sub.add_parser("alltopics", help="List all WB topic categories")
    p_top.add_argument("--out")

    p_info = sub.add_parser("info", help="Show full metadata for one indicator (from YAML cache)")
    p_info.add_argument("id", help="Indicator code, e.g. SP.POP.TOTL")

    p_desc = sub.add_parser("describe", help="Fetch fresh metadata for one indicator (from WB API)")
    p_desc.add_argument("id", help="Indicator code, e.g. SP.POP.TOTL")

    p_srch = sub.add_parser("search", help="Paginated indicator search")
    p_srch.add_argument("term", nargs="?", default="", help="Substring to search (or empty for browse-mode)")
    p_srch.add_argument("--page", type=int, default=1)
    p_srch.add_argument("--limit", type=int, default=20)
    p_srch.add_argument("--source", help="Filter by source ID")
    p_srch.add_argument("--topic", help="Filter by topic ID")
    p_srch.add_argument("--field", default="name+description",
                        help='Search field(s): name | description | note | code | name+description | all')
    p_srch.add_argument("--exact", action="store_true", help="Exact code match (use with --field code)")
    p_srch.add_argument("--out")

    p_sync = sub.add_parser("sync", help="Refresh YAML metadata cache from WB API (Phase 1 pipeline)")
    p_sync.add_argument("--save-raw", action="store_true", dest="save_raw")
    p_sync.add_argument("--no-validate", action="store_true", dest="no_validate")
    p_sync.add_argument("--skip-diff", action="store_true", dest="skip_diff")
    p_sync.add_argument("--commit", action="store_true")
    p_sync.add_argument("--tag", action="store_true")

    return p

def main(argv=None):
    argv = argv or sys.argv[1:]
    args = build_parser().parse_args(argv)
    # Set global verbose
    global VERBOSE
    VERBOSE = args.verbose
    # Debug: print parsed arguments if verbose
    if VERBOSE:
        print(f"Debug-main args: cmd={args.cmd}, indicators={getattr(args, 'indicators', None)}, countries={getattr(args, 'countries', None)}, date={getattr(args, 'date', None)}, long={getattr(args, 'long', None)}, out={getattr(args, 'out', None)}")
    if args.cmd == "countries":
        df = get_country_metadata()
        _save_df(df, args.out)
    elif args.cmd == "indicators":
        codes = [c.strip() for c in (args.codes or "").split(",") if c.strip()] or None
        df = get_indicator_metadata(codes=codes, search=args.search)
        _save_df(df, args.out)
    elif args.cmd == "data":
        df = get_data(indicators=args.indicators, countries=args.countries,
                      date=args.date, per_page=args.per_page, long=args.long,
                      no_basic=args.no_basic, geo=args.geo)
        # Debug: show fetched data shape and sample if verbose
        if VERBOSE:
            print(f"Debug-final df shape: {df.shape}")
            if not df.empty:
                print(df.head(5).to_string(index=False))
        _save_df(df, args.out)
    elif args.cmd in ("sources", "alltopics", "info", "describe", "search", "sync"):
        # PR B discovery subcommands — delegate to wb_discovery
        from wb_discovery import sources, allsources, alltopics, info, describe, search, sync
        if args.cmd == "sources":
            recs = allsources() if args.all else sources(limit=args.limit)
            df = pd.DataFrame.from_records(recs)
            _save_df(df, args.out)
        elif args.cmd == "alltopics":
            df = pd.DataFrame.from_records(alltopics())
            _save_df(df, args.out)
        elif args.cmd == "info":
            rec = info(args.id)
            if rec is None:
                print(f"Indicator not found in YAML cache: {args.id}")
                return 1
            for k, v in rec.items():
                print(f"  {k}: {v}")
        elif args.cmd == "describe":
            rec = describe(args.id)
            if rec is None:
                print(f"Indicator not found via WB API: {args.id}")
                return 1
            for k, v in rec.items():
                print(f"  {k}: {v}")
        elif args.cmd == "search":
            res = search(args.term, page=args.page, limit=args.limit,
                         source=args.source, topic=args.topic,
                         field=args.field, exact=args.exact)
            print(f"  total={res['total']}  page={res['page']}/{res['pages']}  limit={res['limit']}")
            if args.out:
                _save_df(pd.DataFrame.from_records(res['results']), args.out)
            else:
                for r in res['results']:
                    print(f"  [{r.get('code'):<20}] {r.get('name')}")
        elif args.cmd == "sync":
            sub_argv = []
            if args.save_raw:    sub_argv.append("--save-raw")
            if args.no_validate: sub_argv.append("--no-validate")
            if args.skip_diff:   sub_argv.append("--skip-diff")
            if args.commit:      sub_argv.append("--commit")
            if args.tag:         sub_argv.append("--tag")
            return sync(sub_argv)
    return 0

if __name__ == "__main__":
    sys.exit(main() or 0)
