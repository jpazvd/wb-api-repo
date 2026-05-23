# Python library walkthrough — captured transcript

This is the verbatim stdout from running
`src/py/examples/demo_pr_b_c.py` against the live YAML cache that
PR #12 populated (and against the live World Bank API for the
`describe()` / `get_data()` calls).

To regenerate, run from the repo root:

```bash
PYTHONIOENCODING=utf-8 python src/py/examples/demo_pr_b_c.py
```

Discovery functions hit the cached YAML (microseconds after first
load), so the bulk of the wall-clock time is the live API calls
inside sections 3 & 4 (~10-15 s on a fast network).

What's exercised, by section:

| § | Surface                                       | Source                              |
| - | --------------------------------------------- | ----------------------------------- |
| 0 | YAML cache health-check                       | `src/_/_wbopendata_*.yaml` (PR #12) |
| 1 | `sources / allsources / alltopics / info`     | `wb_discovery.py` (PR B)            |
| 2 | `search` with filters + pagination            | `wb_discovery.py` (PR B)            |
| 3 | `describe()` live + multilingual              | `wb_discovery.py` (PR B + PR C)     |
| 4 | `get_data()` + `no_basic` / `geo` flag matrix | `wb_api_tools.py` (PR B + PR C)     |
| 5 | `enrich_country_context()` — Stata `match()`  | `wb_api_tools.py` (PR C)            |
| 6 | `wb_text.wrap / wrap_lines / truncate`        | `wb_text.py` (PR C)                 |

For unit-test coverage of the same surface see
`tests/test_wb_discovery.py`, `tests/test_wb_text.py`, and
`tests/test_wb_api_tools.py` — 62 cases in total, all green.

---

## Demo transcript (run 2026-05-23)

> **Note on `Unnamed: 5`** — early versions of the transcript captured a
> blank column the WB CSV download includes at the end of every row;
> pandas labels it `Unnamed: 5`. The demo now drops these via a tiny
> `_clean()` helper before display. It's a known artifact of the
> CSV-format endpoint that `get_data()` itself should clean — TODO
> for a future `wb_api_tools.py` cleanup.

```text
========================================================================
  wb-api-repo — Python library catch-up demo
  Exercises PR A (Phase 1 fixes) + PR B (discovery API)
                                 + PR C (geo / enrich / wb_text / language)
========================================================================

========================================================================
  0. YAML cache health-check (committed by PR #12)
========================================================================
  OK  _wbopendata_indicators.yaml           18,408,104 bytes
  OK  _wbopendata_sources.yaml                  11,322 bytes
  OK  _wbopendata_topics.yaml                   14,869 bytes

========================================================================
  1. Discovery — YAML cache reads (PR B)
========================================================================

--- wd.sources(limit=5) ---
  [  1] Doing Business
  [  2] World Development Indicators
  [  3] Worldwide Governance Indicators
  [  5] Subnational Malnutrition Database
  [  6] International Debt Statistics

--- wd.allsources() — total count ---
  total sources: 71

--- wd.alltopics() — first 5 of 21 ---
  [ 1] Agriculture & Rural Development
  [ 2] Aid Effectiveness
  [ 3] Economy & Growth
  [ 4] Education
  [ 5] Energy & Mining

--- wd.info('SP.POP.TOTL') ---
  code           SP.POP.TOTL
  name           Population, total
  source_name    World Development Indicators
  topic_names    ['Climate Change', 'Health']
  unit           

--- wd.info('sp.pop.totl') — case-insensitive fallback ---
  matched: True, code='SP.POP.TOTL'

--- wd.info('NOPE.NOT.HERE') — unknown returns None ---
  result: None

========================================================================
  2. Search — filters + pagination (PR B C3)
========================================================================

--- wd.search('poverty headcount', limit=3) — substring across name+desc ---
  total=111  page=1/37  limit=3
  [1.0.HCount.1.90usd      ] Poverty Headcount ($1.90 a day)
  [1.0.HCount.2.5usd       ] Poverty Headcount ($2.50 a day)
  [1.0.HCount.Mid10to50    ] Middle Class ($10-50 a day) Headcount

--- wd.search(topic='3', limit=3) — Economy & Growth (browse mode) ---
  total=306 indicators tagged with topic 3
  [5.0.AMeanIncGr.All      ] Annualized Mean Income Growth (2004-2014)
  [5.0.AMeanIncGr.B40      ] Annualized Mean Income Growth Bottom 40 Percent (2004-2014)
  [5.1.AMeanIncGr.All      ] Annualized Mean Income Growth (2004-2009)

--- wd.search('GDP', source='2', topic='3', limit=3) — combined filters ---
  total=55 (substring 'GDP' AND source=2/WDI AND topic=3/Economy)
  [BG.GSR.NFSV.GD.ZS       ] Trade in services (% of GDP)
  [BM.KLT.DINV.WD.GD.ZS    ] Foreign direct investment, net outflows (% of GDP)
  [BN.CAB.XOKA.GD.ZS       ] Current account balance (% of GDP)

--- wd.search('population', limit=2, page=2) — pagination ---
  total=5337  page=2/2669  (showing page 2)
  [1.0.HCount.Mid10to50    ] Middle Class ($10-50 a day) Headcount
  [1.0.HCount.Ofcl         ] Official Moderate Poverty Rate-National

========================================================================
  3. Live API — describe() + language= (PR B C4 + PR C C4)
========================================================================

--- wd.describe('SP.POP.TOTL') — English (default) ---
  name: Population, total
  desc: Total population is based on the de facto definition of population, which counts all residents regardless of legal status or citizenship. Th...

--- wd.describe('SP.POP.TOTL', language='es') — Spanish ---
  name: Población, total
  desc: La población total se basa en la definición de facto de población, que cuenta a todos los residentes independientemente de su estatus legal ...

--- Verification: describe() and info() return identical key sets ---
  info keys      : ['code', 'description', 'limited_data', 'name', 'note', 'source_id', 'source_name', 'source_org', 'topic_ids', 'topic_names', 'unit']
  describe keys  : ['code', 'description', 'limited_data', 'name', 'note', 'source_id', 'source_name', 'source_org', 'topic_ids', 'topic_names', 'unit']
  identical?     : True

========================================================================
  4. get_data() — country-context auto-merge (PR B C5 + PR C C1)
========================================================================

--- get_data(['SP.POP.TOTL'], 'BRA;USA;IND', date='2020') — DEFAULT (basic merge ON) ---
  shape: (3, 16)    columns: ['country', 'countryiso3code', 'indicator_name', 'indicator_code', 'Unnamed: 5', 'date', 'value', 'indicator', 'region', 'regionname', 'adminregion', 'adminregionname', 'incomelevel', 'incomelevelname', 'lendingtype', 'lendingtypename']
  BRA     208,660,842  region=LCN  income=UMC
  IND   1,402,617,695  region=SAS  income=LMC
  USA     331,577,720  region=NAC  income=HIC

--- get_data(..., no_basic=True) — LEAN (no merge) ---
  shape: (3, 8)    columns: ['country', 'countryiso3code', 'indicator_name', 'indicator_code', 'Unnamed: 5', 'date', 'value', 'indicator']
  no_basic suppressed 8 context cols

--- get_data(..., geo=True) — basic + 3 geo cols ---
  shape: (3, 19)    columns added vs lean: ['adminregion', 'adminregionname', 'capital', 'incomelevel', 'incomelevelname', 'latitude', 'lendingtype', 'lendingtypename', 'longitude', 'region', 'regionname']

--- get_data(..., no_basic=True, geo=True) — geo only ---
  shape: (3, 11)    columns: ['country', 'countryiso3code', 'indicator_name', 'indicator_code', 'Unnamed: 5', 'date', 'value', 'indicator', 'capital', 'longitude', 'latitude']

========================================================================
  5. enrich_country_context() — Stata `match()` for pandas (PR C C2)
========================================================================

--- User DataFrame with custom ISO column name ---
  Input:
iso3  my_metric
 BRA        1.2
 USA        3.4
 IND        5.6
 DEU        7.8
 JPN        9.0

--- enrich_country_context(user_df, iso_col='iso3') ---
iso3  my_metric region                 regionname     incomelevelname
 BRA        1.2    LCN Latin America & Caribbean  Upper middle income
 USA        3.4    NAC              North America         High income
 IND        5.6    SAS                 South Asia Lower middle income
 DEU        7.8    ECS      Europe & Central Asia         High income
 JPN        9.0    EAS        East Asia & Pacific         High income

--- Same with geo=True — adds 3 geographic fields ---
  Added by geo=True: ['capital', 'longitude', 'latitude']
iso3         capital latitude longitude
 BRA        Brasilia -15.7801  -47.9292
 USA Washington D.C.  38.8895   -77.032
 IND       New Delhi  28.6353    77.225
 DEU          Berlin  52.5235   13.4115
 JPN           Tokyo    35.67    139.77

========================================================================
  6. wb_text — text wrapping for publication graphs (PR C C3)
========================================================================

--- Input (251 chars) ---
  GDP per capita (current US$) — Gross domestic product divided by midyear population. GDP is the sum ...

--- wt.wrap(s, width=60, fmt="stack") — for Stata `graph ..., title(...)` ---
  "GDP per capita (current US$) — Gross domestic product" "divided by midyear population. GDP is the sum of gross value" "added by all resident producers in the economy plus any" "product taxes and minus any subsidies not included in the" "value of the products."

--- wt.wrap(s, width=60, fmt="newline") — for SMCL note/caption ---
  |GDP per capita (current US$) — Gross domestic product
  |divided by midyear population. GDP is the sum of gross value
  |added by all resident producers in the economy plus any
  |product taxes and minus any subsidies not included in the
  |value of the products.

--- wt.wrap(s, width=60, fmt="lines") — List[str] ---
  [1] GDP per capita (current US$) — Gross domestic product
  [2] divided by midyear population. GDP is the sum of gross value
  [3] added by all resident producers in the economy plus any
  [4] product taxes and minus any subsidies not included in the
  [5] value of the products.

--- wt.wrap(s, width=60, fmt="smcl") — Stata SMCL with {break} tag ---
  GDP per capita (current US$) — Gross domestic product{break}divided by midyear population. GDP is the sum of gross value{break}added by all resident producers in the economy plus any{break}product taxes and minus any subsidies not included in the{break}value of the products.

--- wt.truncate(s, width=80, suffix="...") — single-line cap ---
  GDP per capita (current US$) — Gross domestic product divided by midyear popu...

========================================================================
  Demo complete.
  See `tests/test_wb_discovery.py`, `tests/test_wb_text.py`, and
  `tests/test_wb_api_tools.py` for the 62-case pytest harness.
========================================================================
```
