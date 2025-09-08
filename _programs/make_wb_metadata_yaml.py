
#!/usr/bin/env python3
import os, sys, subprocess, shutil

PY = shutil.which("python") or sys.executable
ROOT = os.path.dirname(os.path.dirname(__file__))  # repo root
WB = os.path.join(ROOT, "_programs", "wb_api_tools.py")

def run(cmd):
    print("+", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    outdir = os.path.join(ROOT, "_data", "wb")
    os.makedirs(outdir, exist_ok=True)
    run([PY, WB, "countries",  "--out", os.path.join(outdir, "countries.yaml")])
    run([PY, WB, "indicators", "--out", os.path.join(outdir, "indicators.yaml")])

if __name__ == "__main__":
    main()
