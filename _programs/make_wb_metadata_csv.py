
#!/usr/bin/env python3
import os, sys, subprocess, shutil
PY = shutil.which("python") or sys.executable
ROOT = os.path.dirname(os.path.dirname(__file__))
WB = os.path.join(ROOT, "_programs", "wb_api_tools.py")
def run(c): print("+", " ".join(c)); subprocess.check_call(c)
def main():
    outdir = os.path.join(ROOT, "_data", "wb"); os.makedirs(outdir, exist_ok=True)
    run([PY, WB, "countries",  "--out", os.path.join(outdir, "countries.csv")])
    run([PY, WB, "indicators", "--out", os.path.join(outdir, "indicators.csv")])
if __name__ == "__main__": main()
