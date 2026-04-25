# Swiss Macro Monitor

A self-updating data platform that tracks Swiss macroeconomic indicators and publishes them to a Tableau Public dashboard, rebuilt automatically every month.

---

## What it does

Every month, the project automatically:
- Downloads the latest Swiss economic data from SNB, FRED and OECD
- Saves it to Google BigQuery
- Rebuilds all dbt models (staging → intermediate → marts)
- Runs 27 data quality tests — pipeline fails loudly if any break
- Tableau reads from the marts directly — dashboard refreshes on next open

No manual work needed after setup.

---

## Tools used

| What | Tool |
|------|------|
| Macro data | FRED API (free), SNB API (free), OECD KEI API (free) |
| Database | Google BigQuery (free tier) |
| Transformations | dbt Core 1.9 |
| Scheduling | GitHub Actions |
| Dashboard | Tableau Public |

---

## Indicators tracked (15 series)

| Indicator | Source | Series ID | Category | Frequency | Latest |
|-----------|--------|-----------|----------|-----------|--------|
| SNB Policy Rate | SNB | snbgwdzid/ZIG | monetary | daily | Mar 2026 |
| SARON Overnight Rate | SNB | snbgwdzid/SARON | monetary | daily | Mar 2026 |
| SNB Rate Upper Bound | SNB | snbgwdzid/ZIGBL | monetary | daily | Mar 2026 |
| CHF per EUR | SNB | devkum/EUR1 | currency | monthly | Mar 2026 |
| CHF per USD | SNB | devkum/USD1 | currency | monthly | Mar 2026 |
| Real GDP | FRED | CLVMNACSAB1GQCH | growth | quarterly | Q3 2025 |
| 10Y Government Bond Yield | FRED | IRLTLT01CHM156N | monetary | monthly | Feb 2026 |
| GDP Volume Growth YoY | OECD KEI | KEI/CHE/B1GQ_Q/_T/GY | growth | quarterly | Q3 2025 |
| Manufacturing Production Growth YoY | OECD KEI | KEI/CHE/PRVM/C/GY | growth | monthly | Dec 2025 |
| Retail Trade Volume Growth YoY | OECD KEI | KEI/CHE/TOVM/G47/GY | growth | monthly | Jan 2026 |
| Merchandise Exports Growth QoQ | OECD KEI | KEI/CHE/EX/_T/G1 | external | monthly | Feb 2026 |
| Employment Total | OECD KEI | KEI/CHE/EMP/_T/_Z | labour | quarterly | Q3 2025 |
| Unemployment Rate | OECD KEI | KEI/CHE/UNEMP/_T/_Z | labour | quarterly | Q3 2025 |
| Call Money Interbank Rate | OECD KEI | KEI/CHE/IRSTCI/_Z/_Z | monetary | monthly | Mar 2026 |
| CPI Annual Growth YoY | OECD KEI | KEI/CHE/CP/_Z/GY | prices | monthly | Dec 2025 |

---

## Project structure

```
swiss-macro-monitor/
├── ingestion/
│   ├── ingest_fred.py      # FRED — Real GDP, 10Y bond yield
│   ├── ingest_snb.py       # SNB — policy rate, SARON, CHF/EUR, CHF/USD
│   └── ingest_oecd.py      # OECD KEI — growth, labour, prices, external, monetary
├── dbt/
│   ├── models/
│   │   ├── staging/        # Type casting, date normalisation, null removal
│   │   ├── intermediate/   # MoM/YoY changes, rolling averages, signals
│   │   └── marts/          # mart_macro__time_series, mart_macro__overview
│   ├── tests/              # 3 singular tests protecting dashboard integrity
│   └── ci/profiles.yml     # CI BigQuery connection (committed, no secrets)
├── .github/workflows/
│   └── monthly_ingest.yml  # Runs on 2nd of every month, 07:00 UTC
└── tableau/
    └── nav_tabs.csv        # Tab navigation seed for Tableau dashboard
```

---

## Data flow

```
APIs → raw_*_indicators (BigQuery) → staging (views) → intermediate (view) → marts (tables)
                                                                                    ↓
                                                                         Tableau Public dashboard
```

All 3 sources share the same raw schema: `date, source, series_id, indicator_name, indicator_category, value, unit, frequency, ingested_at`

---

## dbt tests (27 total)

| Layer | Test | Purpose |
|-------|------|---------|
| staging × 3 | not_null (date, series_id, value) | Raw data arrived clean |
| intermediate | not_null (date, series_id, name, category, value) | Union + enrichment clean |
| mart_macro__time_series | not_null (date, series_id, name, category, value, is_latest) | Mart complete |
| mart_macro__time_series | accepted_values: signal, trend, indicator_category | No unexpected enum values |
| mart_macro__overview | not_null (date) | Overview built |
| singular | assert_each_series_has_one_latest | Exactly 1 is_latest row per series |
| singular | assert_all_expected_series_present | All 15 series exist in mart |
| singular | assert_no_stale_series | No series older than 18 months |

---

## Roadmap

- [x] 1. GitHub repository setup
- [x] 2. FRED ingestion (Real GDP, 10Y bond yield)
- [x] 3. SNB ingestion (SARON, policy rate, upper bound, CHF/EUR, CHF/USD)
- [x] 4. OECD ingestion (GDP growth, manufacturing, retail, exports, employment, unemployment, interbank rate, CPI)
- [x] 5a. dbt staging models — 3 sources, unified schema, date normalisation (9 tests)
- [x] 5b. dbt intermediate model — MoM/YoY, rolling averages, signals (5 tests)
- [x] 5c. dbt mart models — time_series + overview, is_latest flag, composite signal (13 tests)
- [x] 6. GitHub Actions — full pipeline automated, dbt runs in CI, 27/27 tests passing
- [x] 7. Tableau Public dashboard — https://public.tableau.com/app/profile/ibryam/viz/SwissMacroMonitor/Overview

---

## How to run locally

```bash
git clone https://github.com/ibryam/swiss-macro-monitor.git
cd swiss-macro-monitor
pip install -r requirements.txt

# Copy and fill in .env (FRED_API_KEY + GOOGLE_APPLICATION_CREDENTIALS)
cp .env.example .env

# Run ingestion
python ingestion/ingest_fred.py
python ingestion/ingest_snb.py
python ingestion/ingest_oecd.py

# Run dbt
cd dbt
dbt run
dbt test
```

---

*Part of an analytics engineering portfolio targeting Swiss financial companies (UBS, Julius Bär, Swiss Re). Demonstrates end-to-end pipeline engineering: multi-source ingestion, BigQuery, dbt, automated CI, Tableau Public.*
