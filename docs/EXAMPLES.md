
# Examples: Historical & Projected Population by Age and Sex

This shows how to fetch **historical and projected population** by **age** and **sex** using:
- World Bank HTTP API (WDI + PEP datasets),
- Stata `wbopendata`,
- Python CLI in this repo.

## Indicator patterns
- Counts: `SP.POP.<AGECODE>.<MA|FE>`
- Shares: `SP.POP.<AGECODE>.<MA|FE>.5Y`
See **docs/AGE_BANDS.md** for the list of `<AGECODE>`.

## HTTP API
Brazil, males 0–4, all years:
```
https://api.worldbank.org/v2/country/BRA/indicator/SP.POP.0004.MA?format=json&per_page=20000
```

## Stata `wbopendata`
```stata
wbopendata, indicator(SP.POP.0004.MA SP.POP.0004.FE SP.POP.0509.MA SP.POP.0509.FE) ///
    clear long date(2000:2050)
```

## Python CLI (this repo)
```bash
python _programs/wb_api_tools.py data   --indicators SP.POP.0004.MA,SP.POP.0004.FE,SP.POP.0509.MA,SP.POP.0509.FE   --countries all   --date 2000:2050   --long   --out _data/wb/pop_age_sex_counts_long.csv
```
