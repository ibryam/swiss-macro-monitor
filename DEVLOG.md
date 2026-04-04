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
