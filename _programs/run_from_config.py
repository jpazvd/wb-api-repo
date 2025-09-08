
#!/usr/bin/env python3
"""
Run batch World Bank data pulls from config.yaml.

Requires: requests, pandas, pyyaml
"""

import os, sys, yaml, subprocess, shutil

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
        os.makedirs(os.path.dirname(os.path.join(ROOT, out)), exist_ok=True)
        args = [PY, WB, "data", "--indicators", ",".join(inds), "--countries", str(countries)]
        if date:
            args += ["--date", str(date)]
        if long_flag:
            args += ["--long"]
        args += ["--out", os.path.join(ROOT, out)]
        run(args)

if __name__ == "__main__":
    main()
