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

def get_data(indicators: List[str], countries: str = "all", date: Optional[str] = None,
             per_page: int = DEFAULT_PER_PAGE, long: bool = False) -> pd.DataFrame:
    """
    Fetch indicator data using CSV downloads (following Stata wbopendata approach)
    Much more reliable than JSON for bulk data
    """
    if isinstance(indicators, str):
        indicators = [c.strip() for c in indicators.split(",") if c.strip()]
    indicators = list(dict.fromkeys(indicators))  # Remove duplicates

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
            print(f"Debug-fetch URL: {r.url}")
            text_raw = r.text
            print(f"Debug-fetch text length: {len(text_raw)}")
            print(f"Debug-fetch sample:\n{text_raw[:200]}")
            r.raise_for_status()

            # Parse CSV response from raw bytes to handle BOM and encoding correctly
            import io
            df = pd.read_csv(io.BytesIO(r.content), encoding='utf-8-sig', quoting=1)
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

    if long:
        # Return long format (already is)
        return df_combined
    else:
        # Convert to wide format
        # First, create a pivot table
        wide = df_combined.pivot_table(
            index=["countryiso3code", "country", "date"],
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
    p_d.add_argument("--out")

    return p

def main(argv=None):
    argv = argv or sys.argv[1:]
    args = build_parser().parse_args(argv)
    # Debug: print parsed arguments
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
                      date=args.date, per_page=args.per_page, long=args.long)
        # Debug: show fetched data shape and sample
        print(f"Debug-final df shape: {df.shape}")
        if not df.empty:
            print(df.head(5).to_string(index=False))
        _save_df(df, args.out)

if __name__ == "__main__":
    main()
