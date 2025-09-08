
#!/usr/bin/env python3
"""
Create keyed YAML metadata:
- countries_keyed.yaml keyed by country id (ISO3)
- indicators_keyed.yaml keyed by indicator id
"""
import os, sys, json, time
import requests
import yaml

BASE = "https://api.worldbank.org/v2"
SESSION = requests.Session()

def _get_all(url, params=None):
    params = dict(params or {})
    params.update({"format":"json", "per_page": 1000, "page": 1})
    hdr, data = _request(url, params)
    out = list(data or [])
    pages = int((hdr or {}).get("pages", 1) or 1)
    for p in range(2, pages+1):
        params["page"] = p
        _, data = _request(url, params)
        out.extend(data or [])
    return out

def _request(url, params=None, retries=4, backoff=0.8):
    last = None
    for i in range(retries):
        try:
            r = SESSION.get(url, params=params or {}, timeout=60)
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, list) and len(payload) >= 2:
                return payload[0], payload[1]
            raise ValueError(f"Unexpected payload: {str(payload)[:200]}")
        except Exception as e:
            last = e
            time.sleep(backoff * (2**i))
    raise RuntimeError(f"Failed after {retries} attempts: {last}")

def normalize_country(r):
    def g(obj, *ks):
        cur = obj
        for k in ks:
            cur = (cur or {}).get(k) if isinstance(cur, dict) else None
        return cur
    return {
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
    }

def normalize_indicator(r):
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
        "topics": topic_values,
        "topic_ids": topic_ids,
    }

def main():
    root = os.path.dirname(os.path.dirname(__file__))
    outdir = os.path.join(root, "_data", "wb")
    os.makedirs(outdir, exist_ok=True)

    # Countries keyed
    countries = _get_all(f"{BASE}/country")
    keyed_c = {r.get("id"): normalize_country(r) for r in countries}
    with open(os.path.join(outdir, "countries_keyed.yaml"), "w", encoding="utf-8") as f:
        yaml.safe_dump(keyed_c, f, sort_keys=True, allow_unicode=True)

    # Indicators keyed
    indicators = _get_all(f"{BASE}/indicator")
    keyed_i = {r.get("id"): normalize_indicator(r) for r in indicators}
    with open(os.path.join(outdir, "indicators_keyed.yaml"), "w", encoding="utf-8") as f:
        yaml.safe_dump(keyed_i, f, sort_keys=True, allow_unicode=True)

if __name__ == "__main__":
    main()
