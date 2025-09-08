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

def _request(url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    params = dict(params or {})
    params.setdefault("format", "json")
    last = None
    for i in range(RETRIES):
        try:
            r = SESSION.get(url, params=params, timeout=60)
            if r.status_code >= 500:
                raise requests.HTTPError(f"{r.status_code} {r.text[:200]}")
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, list) and len(payload) >= 2:
                return payload[0], payload[1]
            raise ValueError("Unexpected payload")
        except Exception as e:
            last = e
            time.sleep(BACKOFF * (2**i))
    raise RuntimeError(f"Request failed after {RETRIES} attempts: {last}")

def _paged(url: str, params: Optional[Dict[str, Any]] = None, per_page: int = DEFAULT_PER_PAGE) -> Iterable[Dict[str, Any]]:
    params = dict(params or {})
    params.update({"format":"json","per_page":per_page,"page":1})
    hdr, data = _request(url, params)
    if data: 
        for row in data: 
            yield row
    pages = int((hdr or {}).get("pages", 1) or 1)
    for p in range(2, pages+1):
        params["page"] = p
        _, data = _request(url, params)
        for row in data or []:
            yield row

def get_country_metadata(per_page: int = DEFAULT_PER_PAGE) -> pd.DataFrame:
    url = f"{BASE}/country"
    rows = list(_paged(url, {}, per_page=per_page))
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
    if codes:
        recs = []
        for code in codes:
            url = f"{BASE}/indicator/{code}"
            _, data = _request(url, params={"format":"json"})
            for r in data:
                recs.append(_normalize_indicator_meta(r))
        return pd.DataFrame.from_records(recs)
    url = f"{BASE}/indicator"
    rows = list(_paged(url, {}, per_page=per_page))
    df = pd.DataFrame.from_records([_normalize_indicator_meta(r) for r in rows])
    if search:
        s = search.lower()
        mask = df["id"].str.lower().str.contains(s) | df["name"].str.lower().str.contains(s)
        df = df.loc[mask].copy()
    return df

def get_data(indicators: List[str], countries: str = "all", date: Optional[str] = None,
             per_page: int = DEFAULT_PER_PAGE, long: bool = False) -> pd.DataFrame:
    if isinstance(indicators, str):
        indicators = [c.strip() for c in indicators.split(",") if c.strip()]
    indicators = list(dict.fromkeys(indicators))
    frames = []
    for ind in indicators:
        url = f"{BASE}/country/{countries}/indicator/{ind}"
        params = {}
        if date:
            params["date"] = date
        rows = list(_paged(url, params, per_page=per_page))
        if not rows: 
            continue
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
        import pandas as pd
        frames.append(pd.DataFrame.from_records(recs))
    if not frames:
        import pandas as pd
        return pd.DataFrame(columns=["countryiso3code","country","indicator","date","value"])
    import pandas as pd
    df_long = pd.concat(frames, ignore_index=True)
    df_long["date"] = pd.to_numeric(df_long["date"], errors="coerce")
    df_long.sort_values(["countryiso3code","indicator","date"], inplace=True)
    if long:
        return df_long
    wide = df_long.pivot_table(index=["countryiso3code","country","date"],
                               columns="indicator", values="value", aggfunc="first").reset_index()
    wide.columns = [c if isinstance(c, str) else c[1] for c in wide.columns.values]
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
        _save_df(df, args.out)

if __name__ == "__main__":
    main()
