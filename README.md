
# World Bank API Helper (`wb_api_tools.py`)

This script provides a **Python interface to the World Bank API**, modeled after functionality in the Stata `wbopendata` ado suite.  
It allows you to fetch **country metadata**, **indicator metadata**, and **indicator data** (long or wide format) with automatic pagination and retries.

---

## 📦 Installation

1. Save the script `wb_api_tools.py` to your project folder.
2. Install required Python libraries from `requirements.txt`:

```bash
pip install -r requirements.txt
```

3. Run from the command line:

```bash
python wb_api_tools.py <subcommand> [options]
```

---

## 🚀 Subcommands and Examples

### 1. Country Metadata
Fetch World Bank country metadata (ISO codes, region, income group, capital, coordinates, etc.).

```bash
python wb_api_tools.py countries --out countries.csv
```

Output columns include: `id`, `iso2Code`, `name`, `region`, `incomeLevel`, `lendingType`, `capitalCity`, `longitude`, `latitude`.

---

### 2. Indicator Metadata
Fetch indicator descriptions, units, sources, and topics.

```bash
# All indicators
python wb_api_tools.py indicators --out indicators.csv

# Specific indicators by code
python wb_api_tools.py indicators --codes SI.POV.DDAY,NY.GDP.PCAP.PP.KD --out ind_meta.csv

# Filter indicators by keyword (client-side search)
python wb_api_tools.py indicators --search "poverty" --out poverty_inds.csv
```

Columns include: `id`, `name`, `unit`, `source`, `topics`, `source_note`, `source_organization`.

---

### 3. Indicator Data
Fetch values for one or more indicators, for countries and years.

```bash
# Long format (tidy)
python wb_api_tools.py data   --indicators SI.POV.DDAY,NY.GDP.PCAP.PP.KD   --countries all   --date 2000:2023   --long   --out data_long.csv

# Wide format (default)
python wb_api_tools.py data   --indicators SI.POV.DDAY,NY.GDP.PCAP.PP.KD   --countries BRA,IND,ZAF   --date 2010:   --out data.csv

# Verbose debug output
python wb_api_tools.py --verbose data   --indicators NY.GDP.PCAP.PP.KD   --countries BRA   --date 2010:2020   --long   --out data_verbose.csv
```

- **Long format**: `countryiso3code, country, indicator, date, value`
- **Wide format**: One column per indicator.

---

## ⚙️ Options

- `--out` : Output file (`.csv` or `.parquet`). If omitted, prints preview to screen.
- `--codes` : Comma-separated list of indicator codes (metadata only).
- `--search` : Keyword filter on indicator names/codes (metadata only).
- `--indicators` : Comma-separated indicator codes (data only).
- `--countries` : `"all"` (default) or list of ISO3 codes (e.g., `BRA,IND,ZAF`).
- `--date` : `"YYYY"`, `"YYYY:YYYY"`, `"YYYY:"` (open-ended).
- `--long` : Return data in stacked/long format instead of wide.
- `--per-page` : Change pagination size (default 1000).

---

## 📝 Notes

- Built on the **World Bank API v2 JSON endpoints**:  
  <https://api.worldbank.org/>
- Handles retries with exponential backoff for reliability.
- Metadata is normalized: nested dictionaries (e.g., region/income level) are flattened.
- Long vs wide format allows flexibility for **analysis (long)** or **reporting (wide)**.

---

## 🔧 Example Workflow

1. Download **country metadata** to align codes and regions:

```bash
python wb_api_tools.py countries --out _data/wb/countries.csv
```

2. Download **indicator metadata** to document variables:

```bash
python wb_api_tools.py indicators --out _data/wb/indicators.csv
```

3. Pull **time series data** for key indicators:

```bash
python wb_api_tools.py data   --indicators SI.POV.DDAY,NY.GDP.PCAP.PP.KD,DT.ODA.DACD.HLTH.BAS.CD,DT.ODA.DACD.HLTH.CD,DT.ODA.DACD.HLTH.GEN.CD   --countries all   --date 2000:2023   --long   --out _data/wb/oda_health_long.csv
```

---

## ✅ Output Preview

Example (long format):

| countryiso3code | country     | indicator     | date | value   |
|-----------------|-------------|---------------|------|---------|
| BRA             | Brazil      | SI.POV.DDAY   | 2000 | 12.345  |
| BRA             | Brazil      | SI.POV.DDAY   | 2001 | 11.876  |
| IND             | India       | NY.GDP.PCAP.PP.KD | 2000 | 4532.1 |

---

## 🔒 Integration

This tool can be easily integrated into:
- **Makefiles** or pipelines (e.g., `make update-data`)
- **Stata workflows** (export CSV → `import delimited`)
- **R workflows** (`readr::read_csv` or `arrow::read_parquet`)
- **Jupyter notebooks** for analysis

---

## 👤 Author

Developed for bridging **Stata `wbopendata` workflows** with modern Python pipelines.  
Supports reproducible UNICEF/World Bank style analytics.


# World Bank API Helper (wb_api_tools)

This repo provides a lightweight CLI to fetch **country metadata**, **indicator metadata**, and **indicator data** from the World Bank API, plus automation for nightly metadata refresh and batch data pulls from `config.yaml`.

## Quick Start
```bash
pip install -r requirements.txt

# Metadata (YAML, CSV, keyed YAML)
make wb-metadata
make wb-metadata-csv
make wb-metadata-keyed

# Batch pulls from config.yaml
make wb-config
```

## Population by Age & Sex (Examples)
See **docs/EXAMPLES.md** and **docs/AGE_BANDS.md**.

Quick example:
```bash
python _programs/wb_api_tools.py data   --indicators SP.POP.0004.MA,SP.POP.0004.FE,SP.POP.0509.MA,SP.POP.0509.FE   --countries all   --date 2000:2050   --long   --out _data/wb/pop_age_sex_counts_long.csv
```

## Documentation & Examples
- **docs/EXAMPLES.md** — end-to-end instructions (API, Stata, Python)
- **docs/AGE_BANDS.md** — standard 5-year age band codes
- **_programs/examples/population_examples.sh** — runnable shell examples
- **_programs/examples/population_examples.do** — Stata examples
- **config_full_age_sex.yaml** — full age×sex batch pulls (counts + shares)

Generate full indicator lists programmatically:
```bash
python _programs/examples/generate_age_sex_codes.py            # counts
python _programs/examples/generate_age_sex_codes.py --shares   # shares
```
