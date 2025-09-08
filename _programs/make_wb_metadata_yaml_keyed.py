
#!/usr/bin/env python3
import os, sys, time, requests, yaml
BASE="https://api.worldbank.org/v2"
def _req(url, params=None, retries=4, backoff=0.8):
    params=dict(params or {}); params.setdefault("format","json"); params.setdefault("per_page",1000); params.setdefault("page",1)
    last=None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=60); r.raise_for_status()
            j = r.json()
            if isinstance(j,list) and len(j)>=2: return j[0], j[1]
            raise ValueError("Unexpected payload")
        except Exception as e:
            last=e; time.sleep(backoff*(2**i))
    raise RuntimeError(f"Failed after {retries} attempts: {last}")
def _all(url):
    hdr, data = _req(url)
    out=list(data or [])
    pages=int((hdr or {}).get("pages",1) or 1)
    for p in range(2, pages+1):
        _, d = _req(url, {"page":p, "format":"json", "per_page":1000})
        out.extend(d or [])
    return out
def g(obj,*ks):
    cur=obj
    for k in ks: cur=(cur or {}).get(k) if isinstance(cur,dict) else None
    return cur
def norm_country(r):
    return {"id":r.get("id"),"iso2Code":r.get("iso2Code"),"name":r.get("name"),
            "region_id":g(r,"region","id"),"region":g(r,"region","value"),
            "adminregion_id":g(r,"adminregion","id"),"adminregion":g(r,"adminregion","value"),
            "incomeLevel_id":g(r,"incomeLevel","id"),"incomeLevel":g(r,"incomeLevel","value"),
            "lendingType_id":g(r,"lendingType","id"),"lendingType":g(r,"lendingType","value"),
            "capitalCity":r.get("capitalCity"),"longitude":r.get("longitude"),"latitude":r.get("latitude")}
def norm_indicator(r):
    topics=r.get("topics") or []
    return {"id":r.get("id"),"name":r.get("name"),"unit":r.get("unit"),
            "source_id":g(r,"source","id"),"source":g(r,"source","value"),
            "source_note":r.get("sourceNote"),"source_organization":r.get("sourceOrganization"),
            "topics":[t.get("value") for t in topics if isinstance(t,dict)],
            "topic_ids":[t.get("id") for t in topics if isinstance(t,dict)]}
def main():
    root = os.path.dirname(os.path.dirname(__file__))
    outdir = os.path.join(root, "_data", "wb"); os.makedirs(outdir, exist_ok=True)
    countries = _all(f"{BASE}/country")
    indicators = _all(f"{BASE}/indicator")
    keyed_c = {r.get("id"): norm_country(r) for r in countries}
    keyed_i = {r.get("id"): norm_indicator(r) for r in indicators}
    with open(os.path.join(outdir,"countries_keyed.yaml"),"w",encoding="utf-8") as f:
        yaml.safe_dump(keyed_c, f, sort_keys=True, allow_unicode=True)
    with open(os.path.join(outdir,"indicators_keyed.yaml"),"w",encoding="utf-8") as f:
        yaml.safe_dump(keyed_i, f, sort_keys=True, allow_unicode=True)
if __name__=="__main__": main()
