"""Generate the 5 illustrative examples used in the README.

Run from the repo root after `pip install -e .` (or `pip install wb-api-tools`):

    PYTHONIOENCODING=utf-8 python examples/readme_examples.py

Side effects:
    - Prints DataFrame output to stdout (mirrors what README shows).
    - Writes 3 figures (PNG + SVG twin per `feedback_emit_svg_alongside_png`)
      to docs/figures/. Existing files are overwritten.

The example numbering + theme mirror Stata's `wbopendata_examples.ado`:

    Example 1 — time-series for one indicator, multiple countries (line chart)
    Example 2 — cross-country comparison, latest values (bar chart)
    Example 3 — bivariate scatter (poverty vs GDP per capita)  -> Stata example04
    Example 4 — discovery workflow (search + info + fetch; no figure)
    Example 5 — country-context enrichment                     -> Stata example05
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import wb_api_tools as wb

# --- setup -----------------------------------------------------------------

# Reconfigure stdout for non-ASCII country names (Brasil, Côte d'Ivoire, etc.)
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

FIGS_DIR = Path(__file__).resolve().parent.parent / "docs" / "figures"
FIGS_DIR.mkdir(parents=True, exist_ok=True)

# Simple, README-friendly style — not paper-grade, just clean
plt.rcParams.update({
    "figure.figsize": (8, 4.5),
    "figure.dpi": 110,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "font.size": 10,
})


def save_fig(name: str, fig: plt.Figure) -> None:
    """Save PNG + SVG twin for the README."""
    for ext in ("png", "svg"):
        fig.savefig(FIGS_DIR / f"{name}.{ext}", bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: docs/figures/{name}.(png|svg)")


def section(title: str) -> None:
    print(); print("=" * 72); print(f"  {title}"); print("=" * 72)


# --- Example 1: time-series, multiple countries ----------------------------

def example_1_population_timeseries() -> None:
    section("Example 1 — Population time-series (BRA, USA, IND, 2000-2023)")

    df = wb.get_data(
        ["SP.POP.TOTL"],
        "BRA;USA;IND",
        date="2000:2023",
        long=True,
        no_basic=True,   # skip the country-context columns for a leaner table
    )
    df = df.dropna(subset=["value"])
    df["date"] = pd.to_numeric(df["date"], errors="coerce").astype("Int64")
    df["pop_billions"] = df["value"] / 1e9

    print(df.head(6)[["country", "date", "pop_billions"]].to_string(index=False))
    print(f"  ... {len(df)} rows total")

    fig, ax = plt.subplots()
    for country, sub in df.sort_values("date").groupby("country"):
        ax.plot(sub["date"], sub["pop_billions"], marker="o", markersize=3, label=country)
    ax.set_title("Population, 2000-2023")
    ax.set_xlabel("Year")
    ax.set_ylabel("Population (billions)")
    ax.legend()
    save_fig("example_1_population_timeseries", fig)


# --- Example 2: cross-country comparison -----------------------------------

def example_2_gdp_per_capita_bar() -> None:
    section("Example 2 — GDP per capita PPP for the G7 (latest year)")

    g7 = "CAN;DEU;FRA;GBR;ITA;JPN;USA"
    df = wb.get_data(
        ["NY.GDP.PCAP.PP.KD"],
        g7,
        date="2022",
        long=True,
        no_basic=True,
    )
    df = df.dropna(subset=["value"]).sort_values("value", ascending=True)
    df["gdp_pcap_k"] = df["value"] / 1000

    print(df[["country", "date", "gdp_pcap_k"]].to_string(index=False))

    fig, ax = plt.subplots()
    ax.barh(df["country"], df["gdp_pcap_k"], color="steelblue")
    for i, v in enumerate(df["gdp_pcap_k"]):
        ax.text(v + 0.5, i, f"${v:,.0f}k", va="center", fontsize=9)
    ax.set_title("GDP per capita, PPP (constant 2017 international $), 2022")
    ax.set_xlabel("Thousand $ PPP")
    save_fig("example_2_gdp_per_capita_bar", fig)


# --- Example 3: bivariate scatter (Stata wbopendata_examples.ado example04) -

def example_3_poverty_vs_gdp_scatter() -> None:
    section("Example 3 — Poverty headcount vs GDP per capita (cross-section)")

    # Two indicators, all countries, single year — mirrors Stata example04
    df = wb.get_data(
        ["SI.POV.DDAY", "NY.GDP.PCAP.PP.KD"],
        "all",
        date="2019",
        no_basic=False,   # we want region for color-coding
    )
    # Wide format: one row per country, columns per indicator
    df = df.dropna(subset=["SI.POV.DDAY", "NY.GDP.PCAP.PP.KD"])
    # Filter out aggregates / non-countries (region != "NA" + ISO3 present)
    df = df[df["region"].notna() & (df["region"] != "NA")]

    print(f"  countries with both indicators in 2019: {len(df)}")
    print(df[["countryiso3code", "country", "SI.POV.DDAY", "NY.GDP.PCAP.PP.KD"]]
          .sort_values("SI.POV.DDAY", ascending=False).head(8).to_string(index=False))

    fig, ax = plt.subplots()
    for region, sub in df.groupby("regionname"):
        ax.scatter(sub["NY.GDP.PCAP.PP.KD"], sub["SI.POV.DDAY"],
                   alpha=0.7, s=30, label=region)
    ax.set_xscale("log")
    ax.set_xlabel("GDP per capita, PPP (log scale)")
    ax.set_ylabel("Poverty headcount at $2.15/day (% of population)")
    ax.set_title("Poverty vs GDP per capita, 2019 (cross-country)")
    ax.legend(fontsize=8, loc="upper right")
    save_fig("example_3_poverty_vs_gdp_scatter", fig)


# --- Example 4: discovery workflow (no figure) -----------------------------

def example_4_discovery_workflow() -> None:
    section("Example 4 — Discovery workflow: search -> info -> fetch")

    print("\n--- wb.search('education spending', limit=3) ---")
    res = wb.search("education spending", limit=3)
    print(f"  total matches: {res['total']:,}; showing page {res['page']}/{res['pages']}")
    for r in res["results"]:
        print(f"  [{r['code']:<32}] {r['name']}")

    print("\n--- wb.info('SE.XPD.TOTL.GD.ZS') ---")
    meta = wb.info("SE.XPD.TOTL.GD.ZS")
    if meta:
        print(f"  name       : {meta['name']}")
        print(f"  source     : {meta['source_name']}")
        print(f"  unit       : {meta['unit'] or '(none)'}")
        print(f"  topics     : {meta['topic_names']}")
        print(f"  description: {meta['description'][:200]}...")


# --- Example 5: country-context enrichment (Stata example05) ---------------

def example_5_enrich_country_context() -> None:
    section("Example 5 — Enrich a user DataFrame with country context")

    # Pretend the user already has some custom metric per country
    user_df = pd.DataFrame({
        "iso3": ["BRA", "USA", "IND", "DEU", "JPN", "NGA", "EGY"],
        "my_metric": [1.2, 3.4, 5.6, 7.8, 9.0, 2.1, 4.3],
    })
    print("\nInput DataFrame (user-provided):")
    print(user_df.to_string(index=False))

    enriched = wb.enrich_country_context(user_df, iso_col="iso3", geo=True)
    print("\nAfter enrich_country_context(geo=True):")
    cols = ["iso3", "my_metric", "region", "incomelevelname", "capital", "latitude", "longitude"]
    print(enriched[cols].to_string(index=False))


# --- driver ---------------------------------------------------------------

def main() -> int:
    example_1_population_timeseries()
    example_2_gdp_per_capita_bar()
    example_3_poverty_vs_gdp_scatter()
    example_4_discovery_workflow()
    example_5_enrich_country_context()
    print(); print("Done. Figures in:", FIGS_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
