# Development Log — Swiss Macro Monitor

A running record of every change made to this project.
Each entry explains what was built, why, and how it works.
Entries are appended in order — never edited or deleted.

---

## [001] 2026-04-04 — Project Initialisation

**What:** Created the project structure and GitHub repository.

**Why:** Portfolio project targeting Swiss finance companies (UBS, Julius Bär, Swiss Re). Goal is to build a self-updating macro intelligence platform for Switzerland using only free APIs. Demonstrates multi-source pipeline engineering — the core skill of an analytics engineer.

**What was created:**
- `README.md` — project overview, tools, indicators, roadmap
- `.gitignore` — excludes `.env`, service account JSON, dbt targets
- `.env.example` — template showing required environment variables without exposing secrets
- `.env` — local secrets file (never committed): FRED API key + GCP credentials path
- `requirements.txt` — Python dependencies: fredapi, google-cloud-bigquery, python-dotenv, yfinance, requests, pandas
- Folder structure: `ingestion/`, `dbt/models/staging|intermediate|marts`, `dbt/seeds/`, `.github/workflows/`, `docs/`

**GCP setup:**
- New GCP project created: `swiss-macro-monitor`
- BigQuery API enabled
- Service account created: `swiss-macro-pipeline` with BigQuery Admin role
- Service account JSON key saved locally (gitignored)

---

## [002] 2026-04-04 — FRED Ingestion Script

**What:** Built `ingestion/ingest_fred.py` — pulls 5 Swiss macro indicators from the FRED API into BigQuery.

**Why:** FRED (Federal Reserve Economic Data) is the most reliable free source for Swiss macroeconomic time series. It has clean, consistent data going back decades, a well-documented REST API, and a free Python library (`fredapi`). No web scraping needed — just authenticated API calls.

**How the API works:**
- Authentication: free API key registered at fred.stlouisfed.org
- Library: `fredapi` Python package — `Fred(api_key).get_series(series_id)` returns a pandas Series
- Returns: date-indexed float values, one row per observation period

**Series pulled:**

| Series ID | Indicator | Category | Unit |
|-----------|-----------|----------|------|
| LMUNRRTTCHM156N | Unemployment Rate | labour | persons |
| CPALTT01CHM657N | CPI All Items | prices | index |
| CLVMNACSAB1GQCH | Real GDP | growth | chf_millions |
| IRLTLT01CHM156N | 10Y Government Bond Yield | monetary | percent |
| FPCPITOTLZGCHE | CPI Annual Inflation | prices | percent |

**Key design decisions:**
- **Unified schema:** All indicators land in the same table with columns: `date, source, series_id, indicator_name, indicator_category, value, unit, frequency, ingested_at`. This makes it easy to join and compare indicators across sources in dbt later.
- **Incremental load:** Before loading, the script checks `MAX(date)` already in BigQuery for each series. Only new rows are appended. This avoids duplicates and keeps the pipeline cheap to run monthly.
- **Sandbox-compatible insert:** BigQuery free tier blocks streaming inserts. Used `load_table_from_json()` with `WRITE_APPEND` disposition instead — this is a batch load, not streaming, and works on the free tier.

**BigQuery table created:** `swiss-macro-monitor.swiss_macro_raw.raw_fred_indicators`
**Rows loaded:** 2,701 rows across 5 series

**Bug fixed during build:**
- Initial FRED series IDs were wrong (copied from search results). Corrected by searching FRED directly for Switzerland-tagged series.

---

## [003] 2026-04-04 — SNB Ingestion Script

**What:** Built `ingestion/ingest_snb.py` — pulls 5 Swiss financial indicators from the Swiss National Bank public API into BigQuery.

**Why:** The SNB is the authoritative source for Swiss monetary policy data. SARON (Swiss Average Rate Overnight) is the Swiss benchmark interest rate — the equivalent of SOFR in the US or ESTR in Europe. The SNB policy rate directly drives mortgage rates, credit costs and economic activity in Switzerland. No other free source has this data as cleanly.

**How the API works:**
- Authentication: none — fully public REST API
- Base URL: `https://data.snb.ch/api/cube/{cube_id}/data/csv/en`
- Parameters: `dimSel` (which series within the cube), `fromDate` (start date)
- Returns: semicolon-delimited CSV with 2 metadata header lines, then `Date;Dimension;Value`
- No API key, no rate limiting documented

**Series pulled:**

| Cube | Series Code | Indicator | Category | Unit | Frequency |
|------|-------------|-----------|----------|------|-----------|
| snbgwdzid | SARON | SARON Overnight Rate | monetary | percent | daily |
| snbgwdzid | ZIGBL | SNB Rate Upper Bound | monetary | percent | daily |
| snbgwdzid | ZIG | SNB Policy Rate | monetary | percent | daily |
| devkum | EUR1 | CHF per EUR | currency | chf | monthly |
| devkum | USD1 | CHF per USD | currency | chf | monthly |

**Key design decisions:**
- **Same unified schema as FRED:** `date, source, series_id, indicator_name, indicator_category, value, unit, frequency, ingested_at` — all sources feed the same pattern so dbt staging models are consistent.
- **fromDate=2000-01:** Without this parameter, the SNB API defaults to only the most recent few rows. Setting fromDate ensures full historical load on first run.
- **CSV parsing:** SNB response has 2 junk header lines before the actual column headers. Script skips them by finding the line that starts with `"Date"` and reading from there using pandas.
- **Incremental load:** Same pattern as FRED — checks MAX(date) per series before loading.

**BigQuery table created:** `swiss-macro-monitor.swiss_macro_raw.raw_snb_indicators`
**Rows loaded:** 9,655 rows across 5 series

**Bug fixed during build:**
- First run without `fromDate` only loaded 5 rows (API default = recent only). Fixed by adding `fromDate=2000-01` parameter.
- Table had to be deleted and recreated to force a clean full reload after the fix.

---

## [004] 2026-04-04 — OECD Ingestion Script

**What:** Built `ingestion/ingest_oecd.py` — pulls 6 Swiss economic indicators from the OECD KEI (Key short-term Economic Indicators) API into BigQuery.

**Why:** The OECD provides clean, internationally comparable economic statistics for Switzerland covering growth, trade, labour and monetary indicators. No authentication required. The KEI dataset is the most comprehensive free source for Swiss short-term economic activity data not available via FRED or SNB.

**How the API works:**
- Authentication: none — fully public SDMX REST API
- Base URL: `https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_KEI@DF_KEI/`
- Country filter: `CHE` for Switzerland
- Format: `csvfilewithlabels` — returns a wide CSV with labelled dimension columns
- One API call fetches all Swiss KEI series — filtered in Python per indicator
- No rate limiting documented

**Series pulled:**

| Series ID | Indicator | Category | Unit | Frequency |
|-----------|-----------|----------|------|-----------|
| KEI/CHE/B1GQ_Q/_T/GY | GDP Volume Growth YoY | growth | percent | quarterly |
| KEI/CHE/PRVM/C/GY | Manufacturing Production Growth YoY | growth | percent | monthly |
| KEI/CHE/TOVM/G47/GY | Retail Trade Volume Growth YoY | growth | percent | monthly |
| KEI/CHE/EX/_T/G1 | Merchandise Exports Growth QoQ | external | percent | quarterly |
| KEI/CHE/EMP/_T/_Z | Employment Total | labour | thousands | quarterly |
| KEI/CHE/IRSTCI/_Z/_Z | Call Money Interbank Rate | monetary | percent | monthly |

**Key design decisions:**
- **Single API call for all series:** All Swiss KEI data fetched in one call (16,201 rows) and filtered in Python. Faster, reduces API load, avoids rate limits.
- **Same unified schema:** Matches FRED and SNB — `date, source, series_id, indicator_name, indicator_category, value, unit, frequency, ingested_at`.
- **Incremental load:** Same MAX(date) checkpoint pattern as FRED and SNB.

**BigQuery table created:** `swiss-macro-monitor.swiss_macro_raw.raw_oecd_indicators`
**Rows loaded:** 2,290 rows across 6 series

**Bugs fixed during build:**
- Switzerland (CHE) is not in the OECD CLI dataset — only G20 countries. Switched to KEI dataset which includes Switzerland.
- Stray backtick in table reference string caused 400 error on first run. Fixed typo in `load_rows()`.

---

## [005] 2026-04-04 — dbt Beginner Guide

**What:** Created `DBT_GUIDE.md` — a plain-English explanation of dbt for someone who has never used it before.

**Why:** dbt transforms data through 4 stages (raw → staging → intermediate → marts). Without documentation, it is impossible to understand why the data looks different at each stage or how Tableau gets its final clean table. This guide explains every stage with before/after examples and real SQL snippets from this project.

**What the guide covers:**
- What dbt is and why we use it
- The golden rule: dbt never modifies raw data
- Full data flow diagram from APIs to dashboard
- Stage-by-stage explanation with before/after column examples
- How dbt resolves model run order automatically via DAG
- What dbt tests do and why they matter
- Every file in the dbt project folder explained

---

## [006] 2026-04-04 — GitHub Actions Monthly Schedule

**What:** Created `.github/workflows/monthly_ingest.yml` — automated pipeline that runs all 3 ingestion scripts on the 2nd of every month.

**Why:** Macro data (FRED, SNB, OECD) updates monthly, not daily. Running daily would waste GitHub Actions minutes and add no value. The 2nd of the month (not the 1st) gives data providers time to publish their monthly update before we try to fetch it.

**How it works:**
- Trigger: cron `0 7 2 * *` — 07:00 UTC on the 2nd of every month
- Manual trigger: `workflow_dispatch` — allows one-click run from GitHub Actions UI for testing
- Steps: checkout → Python 3.12 → install requirements → authenticate GCP → FRED → SNB → OECD → cleanup

**Secrets required:**
- `FRED_API_KEY` — registered at fred.stlouisfed.org (free)
- `GCP_SERVICE_ACCOUNT_KEY` — full JSON contents of GCP service account key

**Security:** GCP JSON key written to temp file during run, deleted in cleanup step that runs even if the job fails (`if: always()`).

**GitHub Actions cost:** ~2 min/run × 12 runs/year = ~24 min/year. SMI Risk Monitor uses ~110 min/month. Total across both projects well within 2,000 free minutes/month.

---

## [007] 2026-04-04 — dbt Staging Models (3/3 passing, 9/9 tests passing)

**What:** Built 3 dbt staging models — one per data source — plus sources declaration and test configuration.

**Why:** Staging is the first transformation layer. Its only job is to clean raw data: fix data types, standardise date formats across 3 different APIs, and remove null rows. No calculations happen here — just cleaning.

**Files created:**
- `dbt/dbt_project.yml` — project config: dataset names (swiss_macro_staging, _intermediate, _marts), materialisations (views for staging/intermediate, tables for marts)
- `dbt/profiles.yml` — local BigQuery connection (gitignored)
- `dbt/profiles.ci.yml` — CI BigQuery connection (committed, uses gcp-key.json from GitHub secret)
- `dbt/models/staging/_sources.yml` — declares the 3 raw BigQuery tables as dbt sources
- `dbt/models/staging/_staging.yml` — documents and tests all staging models
- `dbt/models/staging/stg_fred_indicators.sql` — cleans FRED data (DATE cast, null filter)
- `dbt/models/staging/stg_snb_indicators.sql` — cleans SNB data (handles YYYY-MM-DD and YYYY-MM formats)
- `dbt/models/staging/stg_oecd_indicators.sql` — cleans OECD data (handles YYYY-MM-DD, YYYY-MM, YYYY-QX and YYYY formats)

**Date format challenge — 3 sources, 4 different formats:**

| Source | Raw format | Example | Converted to |
|--------|-----------|---------|-------------|
| FRED | YYYY-MM-DD | 2024-01-01 | DATE directly |
| SNB daily | YYYY-MM-DD | 2024-03-15 | DATE directly |
| SNB monthly | YYYY-MM | 2024-03 | 2024-03-01 |
| OECD monthly | YYYY-MM | 2024-03 | 2024-03-01 |
| OECD quarterly | YYYY-QX | 2024-Q1 | 2024-01-01 |
| OECD annual | YYYY | 2024 | 2024-01-01 |

**Bug fixed during build:**
- Dataset name doubling (`swiss_macro_staging_swiss_macro_staging`) — caused by using `+dataset` instead of `+schema` in dbt_project.yml. Fixed to use `+schema` which appends to the profile dataset name.
- OECD staging had 156 null dates from annual series (4-digit year format `YYYY`). Added `length(date) = 4` case to the date CASE statement.

**BigQuery datasets created:**
- `swiss-macro-monitor.swiss_macro_staging` — 3 views

---

## [008] 2026-04-04 — dbt Intermediate Model (5/5 tests passing)

**What:** Built `dbt/models/intermediate/int_macro_indicators.sql` — combines all 3 staging sources into one unified table and adds calculated metrics.

**Why:** Staging only cleans data. The intermediate layer is where the analytical value gets added — MoM changes, YoY changes, rolling averages and economic signals. This is the layer that transforms cleaned numbers into insights. The dashboard doesn't need to calculate these — it just reads them.

**What this model does step by step:**

1. **UNION ALL** — merges stg_fred, stg_snb, stg_oecd into one table (~15,000 rows, all indicators together)
2. **Window functions** — for each series, looks back at previous rows to calculate:
   - `prev_period_value` — previous observation (using `LAG(value, 1)`)
   - `prev_year_value` — same period 12 months ago (using `LAG(value, 12)`)
   - `rolling_avg_3m` — average of current + 2 previous periods
   - `rolling_avg_12m` — average of current + 11 previous periods
3. **MoM and YoY % changes** — calculated as `(value - prev) / prev * 100`
4. **Trend** — 'up' if MoM > 0.5%, 'down' if < -0.5%, 'flat' otherwise
5. **Signal** — economic interpretation by category:

| Category | Rising = | Falling = | Logic |
|----------|----------|-----------|-------|
| growth | bullish | bearish | GDP, manufacturing, retail — up is good |
| labour | bearish | bullish | Unemployment — lower is better |
| prices | bearish if >3% | bearish if <0% | Ideal range 0–2% |
| external | bullish | bearish | Exports up = economy growing |
| currency | bullish | bearish | CHF weakening helps exports |
| monetary | neutral | neutral | Context-dependent, interpreted in marts |

**Key SQL patterns used:**
- `LAG()` window function — looks at the previous row per series
- `AVG() OVER (ROWS BETWEEN N PRECEDING AND CURRENT ROW)` — rolling average
- `SAFE_DIVIDE()` — handles division by zero without crashing
- `{{ ref('stg_fred_indicators') }}` — dbt ref() tells dbt to run staging first

**BigQuery dataset created:** `swiss-macro-monitor.swiss_macro_intermediate`
**Tests:** 5/5 passing (not_null on date, series_id, indicator_name, indicator_category, value)

---

## [009] 2026-04-03 — dbt Mart Models (6/6 tests passing)

**What:** Built two mart models — `mart_macro__time_series` and `mart_macro__overview` — the final dashboard-ready tables in BigQuery.

**Why:** The intermediate model has all the data in a "tall" format (one row per indicator per date). The dashboard needs two different views of this data: a tall format for line charts (filter by category) and a wide format for KPI tiles (one column per indicator). Separating these into two marts keeps the SQL clean and each mart optimised for its purpose.

---

### mart_macro__time_series (13,200 rows)

**Format:** Tall — one row per indicator per date.

**Powers:** Tabs 2, 3 and 4 of the dashboard (line charts for Growth & Labour, Monetary & Prices, Currency & External). Tableau filters this table by `indicator_category` to show the right indicators on each tab.

**Key addition:** `is_latest` flag — `ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY date DESC) = 1`. When `is_latest = true`, that row is the most recent reading for that series. Used for KPI tiles that show current values.

**All columns from intermediate are preserved** — date, source, series_id, indicator_name, indicator_category, value, unit, frequency, mom_change_pct, yoy_change_pct, rolling_avg_3m, rolling_avg_12m, trend, signal — plus `is_latest`.

---

### mart_macro__overview (5,000 rows)

**Format:** Wide — one row per date, every indicator as its own column.

**Powers:** Tab 1 of the dashboard (economy overview with KPI tiles for each indicator).

**Pivot technique:** `MAX(CASE WHEN series_id = '...' THEN value END) AS column_name` — standard BigQuery pivot. Each series becomes a column. One date = full picture of the Swiss economy that month.

**Columns per indicator:** value, MoM %, and signal — so Tableau can colour each KPI tile bullish/bearish without any calculations.

**Composite signal:** Counts how many indicators are bullish vs bearish on each date:
- `bullish_count` and `bearish_count` — sum of CASE WHEN signal = '...' THEN 1
- `overall_signal` — 'bullish' if bullish_count > bearish_count, 'bearish' if reversed, 'neutral' if equal

**Indicators in the composite signal:** gdp, manufacturing, retail trade, unemployment, employment, CPI inflation, exports, CHF/EUR

---

**BigQuery datasets created:**
- `swiss-macro-monitor.swiss_macro_marts` — 2 tables
  - `mart_macro__time_series` — 13,200 rows
  - `mart_macro__overview` — 5,000 rows

**Tests:** 6/6 passing (not_null on date, series_id, indicator_name, indicator_category, value in time_series; not_null on date in overview)

---

## [011] 2026-04-05 — dbt added to GitHub Actions (full pipeline now automated)

**What:** Added dbt run + dbt test to the monthly GitHub Actions workflow. Also added `dbt-bigquery` to `requirements.txt`.

**Why:** Ingestion scripts updated raw tables every month but dbt never ran in CI — staging, intermediate and mart models were never rebuilt automatically. Tableau was reading stale marts. The pipeline was only half-automated. Now the full flow runs end-to-end on the 2nd of every month: ingest → dbt run → dbt test.

**What changed:**
- `.github/workflows/monthly_ingest.yml` — added "Write dbt profiles for CI" and "Run dbt" steps; removed leftover BFS step
- `requirements.txt` — added `dbt-bigquery==1.8.0`

**Pipeline now:**
```
GitHub Actions (2nd of month, 07:00 UTC)
  → ingest_fred.py      → raw_fred_indicators
  → ingest_snb.py       → raw_snb_indicators
  → ingest_oecd.py      → raw_oecd_indicators (incl. UNEMP)
  → dbt run             → staging + intermediate + marts rebuilt
  → dbt test            → 20/20 tests must pass
```

---

## [010] 2026-04-05 — OECD UNEMP replaces FRED for unemployment data

**What:** Added `UNEMP` series to OECD KEI ingestion. Replaced FRED `LMUNRRTTCHM156N` with OECD `KEI/CHE/UNEMP/_T/_Z` across the pipeline.

**Why:** Tableau KPI tile for unemployment showed null for 2024–2026. Root cause: FRED sources Swiss unemployment from OECD and republishes it with an additional 3–6 month delay. The OECD KEI dataset we already pull has `UNEMP` directly with ~1 quarter lag and data confirmed up to Q4 2025. No new script, no new data source — just one extra filter in the existing OECD ingestion.

**What changed:**

| File | Change |
|------|--------|
| `ingestion/ingest_fred.py` | Removed `LMUNRRTTCHM156N` — OECD now owns unemployment |
| `ingestion/ingest_oecd.py` | Added `UNEMP/_T/Y/_Z` to `SERIES_FILTERS` |
| `dbt/models/marts/mart_macro__overview.sql` | Updated unemployment pivot to `KEI/CHE/UNEMP/_T/_Z` |

**Data note:** Historical FRED unemployment rows remain in BigQuery but are no longer updated or surfaced in the mart. OECD UNEMP is quarterly — unemployment tile will show the most recent quarter's value via `is_latest = true` in `mart_macro__time_series`.

**Series confirmed:** `UNEMP/_T/Y/_Z` — 4.84% for full year 2025, Q4 2025 = 5.08%

---
