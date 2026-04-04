# Swiss Macro Monitor

A data platform that tracks Swiss macroeconomic indicators, updated automatically every month.

---

## What it does

Every month, the project automatically:
- Downloads the latest Swiss economic data from SNB, FRED, OECD and Yahoo Finance
- Saves the data to Google BigQuery
- Calculates month-over-month changes, rolling averages and trend signals
- Updates the Tableau dashboard

No manual work needed. Everything runs on a schedule inside GitHub.

---

## Tools used

| What | Tool |
|------|------|
| Macro data | FRED API (free), SNB API (free), OECD API (free) |
| Market data | Yahoo Finance (free) |
| Database | Google BigQuery |
| Data transformations | dbt Core |
| Scheduling | GitHub Actions |
| Dashboard | Tableau Public |
| Documentation | GitHub Pages |

---

## Indicators tracked

| Indicator | Source | Category | Frequency |
|-----------|--------|----------|-----------|
| Unemployment Rate | FRED | Labour | Monthly |
| CPI All Items | FRED | Prices | Monthly |
| CPI Annual Inflation | FRED | Prices | Annual |
| Real GDP | FRED | Growth | Quarterly |
| 10Y Government Bond Yield | FRED | Monetary | Monthly |
| SNB Policy Rate | SNB | Monetary | Monthly |
| SARON (overnight rate) | SNB | Monetary | Daily |
| Credit volumes by sector | SNB | Credit | Monthly |
| Mortgage lending | SNB | Credit | Monthly |
| CHF/EUR exchange rate | Yahoo Finance | Currency | Daily |
| CHF/USD exchange rate | Yahoo Finance | Currency | Daily |

---

## Project structure

```
swiss-macro-monitor/
├── ingestion/          # Downloads data from APIs into BigQuery
│   ├── ingest_fred.py      # FRED — GDP, CPI, unemployment, bond yields
│   ├── ingest_snb.py       # SNB — rates, credit, mortgages
│   ├── ingest_oecd.py      # OECD — PMI, business confidence
│   └── ingest_fx.py        # Yahoo Finance — CHF rates
├── dbt/                # Transforms and calculates metrics in BigQuery
│   ├── models/
│   │   ├── staging/        # Data cleaning, unified schema
│   │   ├── intermediate/   # MoM changes, rolling averages, signals
│   │   └── marts/          # Final tables for the dashboard
│   └── seeds/              # Indicator metadata
└── docs/               # How the project works
```

---

## Data architecture

All indicators from all sources are normalised into a single unified schema:

```
date | source | indicator_name | indicator_category | value | unit | frequency
```

This makes it possible to compare and combine indicators across sources in one place.

---

## Roadmap

- [x] 1. GitHub repository setup
- [x] 2. FRED ingestion (GDP, CPI, unemployment, bond yields)
- [x] 3. SNB ingestion (SARON, policy rate, CHF/EUR, CHF/USD)
- [x] 4. OECD ingestion (GDP growth, manufacturing, retail trade, exports, employment, interbank rate)
- [x] 5a. dbt staging models (stg_fred, stg_snb, stg_oecd — 9/9 tests passing)
- [x] 5b. dbt intermediate model (MoM/YoY changes, rolling averages, signals — 5/5 tests passing)
- [x] 5c. dbt mart models (mart_macro__time_series 13.2k rows, mart_macro__overview 5k rows — 6/6 tests passing)
- [x] 6. GitHub Actions automated schedule (monthly, 2nd of each month at 07:00 UTC)
- [ ] 7. Tableau Public dashboard

---

## How to run locally

```bash
# Clone the repo
git clone https://github.com/ibryam/swiss-macro-monitor.git
cd swiss-macro-monitor

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Add your FRED API key and GCP credentials path to .env

# Run ingestion
python ingestion/ingest_fred.py
```

---

*Part of an analytics engineering portfolio. Built to demonstrate data engineering skills for Swiss financial companies.*
