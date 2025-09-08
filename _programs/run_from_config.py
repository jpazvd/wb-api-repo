
#!/usr/bin/env python3
"""
Run batch World Bank data pulls from config.yaml.

Requires: requests, pandas, pyyaml
"""

import os, sys, yaml, subprocess, shutil, time, requests

PY = shutil.which("python") or sys.executable
ROOT = os.path.dirname(os.path.dirname(__file__))  # repo root
WB = os.path.join(ROOT, "_programs", "wb_api_tools.py")
CFG = os.path.join(ROOT, "config.yaml")

def run(cmd):
    print("+", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    if not os.path.exists(CFG):
        raise SystemExit(f"Config not found: {CFG}")
    with open(CFG, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    jobs = cfg.get("jobs") or []
    if not jobs:
        print("No jobs in config.yaml")
        return

def _req(url, retries=4, backoff=0.8):
    last=None
    for i in range(retries):
        try:
            r = requests.get(url, params={"format":"json"}, timeout=60)
            r.raise_for_status()
            j = r.json()
            if isinstance(j,list) and len(j)>=2:
                return j[1]
            raise ValueError("Unexpected payload")
        except Exception as e:
            last=e
            time.sleep(backoff*(2**i))
    raise RuntimeError(f"Indicator lookup failed after {retries} attempts: {last}")

def validate_indicators(codes):
    """Return (valid_codes, invalid_codes)."""
    valid=[]; invalid=[]
    for c in codes:
        try:
            data = _req(f"https://api.worldbank.org/v2/indicator/{c}")
            # WB returns a list with at least one indicator dict if valid
            if data and isinstance(data, list) and isinstance(data[0], dict) and data[0].get("id")==c:
                valid.append(c)
            else:
                invalid.append(c)
        except Exception:
            invalid.append(c)
    return valid, invalid

    for job in jobs:
        name = job.get("name") or "unnamed"
        inds = job.get("indicators")
        countries = job.get("countries", "all")
        date = job.get("date")
        out = job.get("out")
        long_flag = job.get("long", False)
        if not inds or not out:
            print(f"Skipping job {name}: indicators/out required")
            continue
        v, inv = validate_indicators(inds)
        if inv:
            print(f"[WARN] Job {name}: unknown indicator codes: {', '.join(inv)}")
        if not v:
            print(f"[SKIP] Job {name}: no valid indicators after validation")
            continue
        os.makedirs(os.path.dirname(os.path.join(ROOT, out)), exist_ok=True)
        args = [PY, WB, "data", "--indicators", ",".join(v), "--countries", str(countries)]
        if date:
            args += ["--date", str(date)]
        if long_flag:
            args += ["--long"]
        args += ["--out", os.path.join(ROOT, out)]
        run(args)

if __name__ == "__main__":
    main()
