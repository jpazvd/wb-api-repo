"""Build examples/readme_examples.ipynb from the same recipes as readme_examples.py.

The .py script generates the figures committed to docs/figures/ for the README
to embed; this notebook is the user-facing reproducible demo (open on GitHub or
nbviewer; click "Run all" in Jupyter / Colab to reproduce).

Run from repo root after `pip install -e ".[test]"`:

    PYTHONIOENCODING=utf-8 python examples/_build_readme_notebook.py

This writes the notebook + invokes `jupyter nbconvert --execute` to capture
outputs (DataFrame tables + inline figures) into the committed file.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import nbformat
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK = ROOT / "examples" / "readme_examples.ipynb"


def md(text: str) -> dict:
    return new_markdown_cell(text)


def code(src: str) -> dict:
    return new_code_cell(src)


CELLS = [
    md(
        "# `wb-api-tools` — quick-start examples\n"
        "\n"
        "Five illustrative examples covering the full library surface. Each shows\n"
        "the output inline (DataFrame tables and figures) so GitHub renders the\n"
        "notebook directly — no need to clone or install to see what the package does.\n"
        "\n"
        "Run all cells to reproduce locally; the same 5 examples are also available\n"
        "as a plain Python script at `examples/readme_examples.py` (which also\n"
        "writes the PNG/SVG figures committed to `docs/figures/`).\n"
        "\n"
        "**Numbering and theme mirror the Stata `wbopendata_examples.ado` patterns:**\n"
        "\n"
        "| # | Example | Stata analogue |\n"
        "| --- | --- | --- |\n"
        "| 1 | Population time-series, multiple countries | line chart |\n"
        "| 2 | G7 GDP per capita PPP cross-section | bar chart |\n"
        "| 3 | Poverty vs GDP per capita scatter | Stata example04 |\n"
        "| 4 | Discovery workflow: search -> info -> fetch | — |\n"
        "| 5 | Enrich a user DataFrame with country context | Stata example05 |\n"
    ),
    code(
        "import matplotlib.pyplot as plt\n"
        "import pandas as pd\n"
        "import wb_api_tools as wb\n"
        "\n"
        "plt.rcParams.update({\n"
        "    'figure.figsize': (8, 4.5), 'figure.dpi': 100,\n"
        "    'axes.spines.top': False, 'axes.spines.right': False,\n"
        "    'axes.grid': True, 'grid.alpha': 0.3, 'font.size': 10,\n"
        "})\n"
        "\n"
        "print('wb_api_tools version:', wb.__version__)\n"
    ),
    # ---- Example 1 ----------------------------------------------------------
    md(
        "## Example 1 — Population time-series\n"
        "\n"
        "Three countries, one indicator (`SP.POP.TOTL`), 2000-2023. Returns a tidy\n"
        "DataFrame; we re-plot as a line chart with one line per country.\n"
    ),
    code(
        "df = wb.get_data(['SP.POP.TOTL'], 'BRA;USA;IND', date='2000:2023', long=True, no_basic=True)\n"
        "df = df.dropna(subset=['value']).copy()\n"
        "df['date'] = pd.to_numeric(df['date'], errors='coerce').astype('Int64')\n"
        "df['pop_billions'] = df['value'] / 1e9\n"
        "df.head(6)[['country', 'date', 'pop_billions']]\n"
    ),
    code(
        "fig, ax = plt.subplots()\n"
        "for country, sub in df.sort_values('date').groupby('country'):\n"
        "    ax.plot(sub['date'], sub['pop_billions'], marker='o', markersize=3, label=country)\n"
        "ax.set_title('Population, 2000-2023')\n"
        "ax.set_xlabel('Year'); ax.set_ylabel('Population (billions)')\n"
        "ax.legend(); plt.show()\n"
    ),
    # ---- Example 2 ----------------------------------------------------------
    md(
        "## Example 2 — GDP per capita PPP for the G7 (latest year)\n"
        "\n"
        "Cross-country bar chart. Single year (2022), one indicator, 7 countries.\n"
    ),
    code(
        "g7 = 'CAN;DEU;FRA;GBR;ITA;JPN;USA'\n"
        "df2 = wb.get_data(['NY.GDP.PCAP.PP.KD'], g7, date='2022', long=True, no_basic=True)\n"
        "df2 = df2.dropna(subset=['value']).sort_values('value', ascending=True).copy()\n"
        "df2['gdp_pcap_k'] = df2['value'] / 1000\n"
        "df2[['country', 'date', 'gdp_pcap_k']]\n"
    ),
    code(
        "fig, ax = plt.subplots()\n"
        "ax.barh(df2['country'], df2['gdp_pcap_k'], color='steelblue')\n"
        "for i, v in enumerate(df2['gdp_pcap_k']):\n"
        "    ax.text(v + 0.5, i, f'${v:,.0f}k', va='center', fontsize=9)\n"
        "ax.set_title('GDP per capita, PPP (constant 2017 international $), 2022')\n"
        "ax.set_xlabel('Thousand $ PPP'); plt.show()\n"
    ),
    # ---- Example 3 ----------------------------------------------------------
    md(
        "## Example 3 — Poverty headcount vs GDP per capita (cross-section)\n"
        "\n"
        "Two indicators, all countries, single year. Mirrors Stata\n"
        "`wbopendata_examples.ado` example04. Region colour-coded; log-x.\n"
    ),
    code(
        "df3 = wb.get_data(['SI.POV.DDAY', 'NY.GDP.PCAP.PP.KD'], 'all', date='2019', no_basic=False)\n"
        "df3 = df3.dropna(subset=['SI.POV.DDAY', 'NY.GDP.PCAP.PP.KD'])\n"
        "df3 = df3[df3['region'].notna() & (df3['region'] != 'NA')]\n"
        "print(f'countries with both indicators in 2019: {len(df3)}')\n"
        "df3[['countryiso3code', 'country', 'SI.POV.DDAY', 'NY.GDP.PCAP.PP.KD']] \\\n"
        "    .sort_values('SI.POV.DDAY', ascending=False).head(8)\n"
    ),
    code(
        "fig, ax = plt.subplots()\n"
        "for region, sub in df3.groupby('regionname'):\n"
        "    ax.scatter(sub['NY.GDP.PCAP.PP.KD'], sub['SI.POV.DDAY'], alpha=0.7, s=30, label=region)\n"
        "ax.set_xscale('log')\n"
        "ax.set_xlabel('GDP per capita, PPP (log scale)')\n"
        "ax.set_ylabel('Poverty headcount at $2.15/day (% of population)')\n"
        "ax.set_title('Poverty vs GDP per capita, 2019 (cross-country)')\n"
        "ax.legend(fontsize=8, loc='upper right'); plt.show()\n"
    ),
    # ---- Example 4 ----------------------------------------------------------
    md(
        "## Example 4 — Discovery workflow\n"
        "\n"
        "`search` for an indicator by keyword, `info` to inspect its full metadata\n"
        "(from the local YAML cache after `wb-api-tools sync`).\n"
    ),
    code(
        "res = wb.search('education spending', limit=3)\n"
        "print(f\"total matches: {res['total']:,}; page {res['page']}/{res['pages']}\")\n"
        "for r in res['results']:\n"
        "    print(f\"  [{r['code']:<32}] {r['name']}\")\n"
    ),
    code(
        "meta = wb.info('SE.XPD.TOTL.GD.ZS')\n"
        "{k: v for k, v in meta.items() if k != 'description'} if meta else None\n"
    ),
    # ---- Example 5 ----------------------------------------------------------
    md(
        "## Example 5 — Enrich a user DataFrame with country context\n"
        "\n"
        "Mirrors Stata `wbopendata, match(varname) [basic geo]`. Left-join semantics;\n"
        "input frame not mutated.\n"
    ),
    code(
        "user_df = pd.DataFrame({\n"
        "    'iso3': ['BRA', 'USA', 'IND', 'DEU', 'JPN', 'NGA', 'EGY'],\n"
        "    'my_metric': [1.2, 3.4, 5.6, 7.8, 9.0, 2.1, 4.3],\n"
        "})\n"
        "user_df\n"
    ),
    code(
        "enriched = wb.enrich_country_context(user_df, iso_col='iso3', geo=True)\n"
        "enriched[['iso3', 'my_metric', 'region', 'incomelevelname',\n"
        "          'capital', 'latitude', 'longitude']]\n"
    ),
    # ---- Footer -------------------------------------------------------------
    md(
        "---\n"
        "\n"
        "### Next steps\n"
        "\n"
        "- **CLI counterpart**: every example above has a `wb-api-tools <subcmd>` form.\n"
        "  Run `wb-api-tools --help` for the full list.\n"
        "- **Full reference**: [`docs/PYTHON_USER_GUIDE.md`](../docs/PYTHON_USER_GUIDE.md).\n"
        "- **Stata parity**: [`docs/PYTHON_USER_GUIDE.md` §5](../docs/PYTHON_USER_GUIDE.md#5-parity-with-stata-wbopendata).\n"
    ),
]


def main() -> int:
    nb = new_notebook(
        cells=CELLS,
        metadata={
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
        },
    )
    nbformat.write(nb, NOTEBOOK)
    print(f"wrote skeleton: {NOTEBOOK}")

    # Execute in place to capture outputs (DataFrames + figures inline).
    # Needs the YAML cache for Example 4 (info / search) — set
    # WBOPENDATA_YAML_DIR explicitly via env if the user-cache is empty.
    print("executing notebook...")
    rc = subprocess.run(
        [
            sys.executable, "-m", "jupyter", "nbconvert",
            "--to", "notebook", "--execute", "--inplace",
            "--ExecutePreprocessor.timeout=120",
            str(NOTEBOOK),
        ],
        cwd=ROOT,
    ).returncode
    if rc != 0:
        print(f"nbconvert failed with exit code {rc}", file=sys.stderr)
        return rc
    print("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
