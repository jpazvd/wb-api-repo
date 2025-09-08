
#!/usr/bin/env python3
import argparse, json
BANDS = ["0004","0509","1014","1519","2024","2529","3034","3539",
         "4044","4549","5054","5559","6064","6569","7074","7579","80UP"]
def make_codes(bands, shares=False):
    out=[]
    for b in bands:
        for sex in ["MA","FE"]:
            code = f"SP.POP.{b}.{sex}"
            if shares:
                code += ".5Y"
            out.append(code)
    return out
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bands", default=",".join(BANDS))
    ap.add_argument("--shares", action="store_true")
    args = ap.parse_args()
    bands = [s.strip() for s in args.bands.split(",") if s.strip()]
    print(json.dumps(make_codes(bands, shares=args.shares), indent=2))
