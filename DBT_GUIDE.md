# dbt Guide — Swiss Macro Monitor

A plain-English explanation of what dbt does, how it works,
and how data changes at every stage in this project.

---

## What is dbt?

dbt (data build tool) is a tool that transforms raw data inside your database
using SQL. It does not move data — it only reshapes it.

Think of it like a series of SQL views and tables, but with:
- Automatic dependency management (it knows which model to run first)
- Built-in testing (it checks your data is clean)
- Documentation (it generates a website explaining every table)

**Without dbt:**
```
You write SQL → run it manually → hope nothing breaks → repeat
```

**With dbt:**
```
You write SQL models → dbt figures out the order → runs them → tests them
```

---

## The golden rule

> dbt never touches your raw data.
> It only reads from it and writes to new tables/views.

Your raw data in `swiss_macro_raw` is always safe.
dbt creates separate datasets for staging, intermediate and marts.

---

## How data flows through this project

```
SOURCES (raw data, exactly as downloaded)
    │
    │  swiss_macro_raw.raw_fred_indicators
    │  swiss_macro_raw.raw_snb_indicators
    │  swiss_macro_raw.raw_oecd_indicators
    │
    ▼
STAGING (cleaned, standardised, one unified schema)
    │
    │  swiss_macro_staging.stg_fred_indicators
    │  swiss_macro_staging.stg_snb_indicators
    │  swiss_macro_staging.stg_oecd_indicators
    │
    ▼
INTERMEDIATE (calculations: MoM change, YoY change, rolling averages, signals)
    │
    │  swiss_macro_intermediate.int_macro_indicators
    │  swiss_macro_intermediate.int_macro_signals
    │
    ▼
MARTS (final tables, ready for the dashboard)

    swiss_macro_marts.mart_macro__overview
    swiss_macro_marts.mart_macro__growth
    swiss_macro_marts.mart_macro__monetary
    swiss_macro_marts.mart_macro__credit
    swiss_macro_marts.mart_macro__external
```

---

## Stage 1 — Raw (what we have now)

This is data exactly as it came from the API. We do not touch it.
It is our safety net — if anything goes wrong downstream, we can always
rebuild from raw.

**Example row in `raw_fred_indicators`:**

```
date        | source | series_id         | indicator_name   | value  | unit    | frequency
2024-01-01  | FRED   | LMUNRRTTCHM156N   | Unemployment Rate| 134200 | persons | monthly
```

**Problems with raw data:**
- Dates are stored as strings (`"2024-01"`, `"2024-Q1"`, `"2024-01-01"`)
- Values have no context — is 134200 good or bad? Up or down?
- Three separate tables, three different sources, hard to compare
- No calculated metrics — just raw numbers

---

## Stage 2 — Staging (cleaning)

Staging models fix the raw data problems:
- Convert all date formats to a single standard (`DATE` type)
- Rename columns consistently
- Filter out null or invalid values
- Cast data types correctly

**What changes:**

```
BEFORE (raw):                          AFTER (staging):
─────────────────────────────          ──────────────────────────────────
date: "2024-01"          ──────►       date: 2024-01-01  (DATE type)
value: "134200"          ──────►       value: 134200.0   (FLOAT64)
series_id: "LMUN..."     ──────►       series_id: "LMUNRRTTCHM156N"
source: "FRED"           ──────►       source: "FRED"
[no validation]          ──────►       WHERE value IS NOT NULL AND value > 0
```

**dbt model file: `stg_fred_indicators.sql`**
```sql
select
    -- Standardise date: FRED uses YYYY-MM-DD, SNB uses YYYY-MM, OECD uses YYYY-QX
    -- All converted to first day of the period
    cast(date as date)          as date,
    source,
    series_id,
    indicator_name,
    indicator_category,
    cast(value as float64)      as value,
    unit,
    frequency,
    ingested_at
from {{ source('swiss_macro_raw', 'raw_fred_indicators') }}
where value is not null
```

The `{{ source(...) }}` syntax tells dbt where to read from.
dbt resolves this automatically — you never hardcode project names.

---

## Stage 3 — Intermediate (calculations)

Intermediate models add the metrics that make the data useful:
- Month-over-month change
- Year-over-year change
- 3-month and 12-month rolling averages
- Trend direction (up / flat / down)
- Economic signal (bullish / neutral / bearish)

**What changes:**

```
BEFORE (staging):                      AFTER (intermediate):
─────────────────────────────          ──────────────────────────────────────────
date: 2024-03-01                       date: 2024-03-01
indicator: Unemployment Rate           indicator: Unemployment Rate
value: 139800                          value: 139800
[nothing else]                         prev_month_value: 137200
                                       mom_change: +2600
                                       mom_change_pct: +1.89%
                                       prev_year_value: 125400
                                       yoy_change_pct: +11.48%
                                       rolling_avg_3m: 138100
                                       trend: "up"
                                       signal: "bearish"   ← unemployment rising = bad
```

**dbt model file: `int_macro_indicators.sql`**
```sql
with base as (
    select * from {{ ref('stg_fred_indicators') }}
    union all
    select * from {{ ref('stg_snb_indicators') }}
    union all
    select * from {{ ref('stg_oecd_indicators') }}
),

with_changes as (
    select
        *,
        lag(value) over (
            partition by series_id
            order by date
        )                                               as prev_month_value,

        round(
            safe_divide(
                value - lag(value) over (partition by series_id order by date),
                lag(value) over (partition by series_id order by date)
            ) * 100, 2
        )                                               as mom_change_pct,

        avg(value) over (
            partition by series_id
            order by date
            rows between 2 preceding and current row
        )                                               as rolling_avg_3m

    from base
)

select * from with_changes
```

The `{{ ref(...) }}` syntax tells dbt to read from another dbt model.
dbt automatically runs the staging model first, then this one.
You never have to manage the order manually.

---

## Stage 3 — Intermediate (BUILT ✓)

The intermediate model `int_macro_indicators` is now live in BigQuery.

**What it actually does to the data:**

Step 1 — UNION ALL merges all 3 sources:
```
stg_fred  (2,701 rows)  ─┐
stg_snb   (9,655 rows)  ─┼─► int_macro_indicators (~15,000 rows combined)
stg_oecd  (2,290 rows)  ─┘
```

Step 2 — Window functions add calculated columns:
```sql
-- Look back at the previous observation for this series
lag(value) over (partition by series_id order by date) as prev_period_value

-- Look back 12 observations (approx 1 year for monthly data)
lag(value, 12) over (partition by series_id order by date) as prev_year_value

-- Smooth out noise with a 3-period rolling average
avg(value) over (
    partition by series_id
    order by date
    rows between 2 preceding and current row
) as rolling_avg_3m
```

Step 3 — MoM and YoY % changes:
```
value: 139,800   prev: 137,200   →   mom_change_pct: +1.89%
value: 139,800   year_ago: 125,400  →  yoy_change_pct: +11.48%
```

Step 4 — Trend and signal assigned:
```
mom_change_pct: +1.89%   indicator_category: labour
→ trend: 'up'
→ signal: 'bearish'   (unemployment rising = bad for economy)
```

**Real example — SNB Policy Rate on 2024-09-26:**
```
date:              2024-09-26
indicator_name:    SNB Policy Rate
value:             1.00          ← SNB cut from 1.25 to 1.00
prev_period_value: 1.25
mom_change_pct:    -20.0%
rolling_avg_3m:    1.17
trend:             down
signal:            neutral       ← monetary always neutral, context in marts
```

---

## Stage 4 — Marts (dashboard-ready)

Mart models are the final output — clean, wide tables ready for Tableau.
Each mart answers one specific business question.

**What changes:**

```
BEFORE (intermediate):                 AFTER (mart):
─────────────────────────────          ──────────────────────────────────────────
One row per indicator per date         One row per date with ALL indicators as columns
All indicators mixed together          Organised by theme (growth, monetary, etc.)
Technical column names                 Human-readable labels
```

**Example: `mart_macro__overview`**

```
date       | unemployment | unemployment_signal | gdp_growth | gdp_signal | snb_rate | snb_rate_signal | overall_signal
2024-03-01 | 139800       | bearish             | 1.2        | neutral    | 1.75     | neutral         | neutral
2024-02-01 | 137200       | neutral             | 1.1        | neutral    | 1.75     | neutral         | bullish
2024-01-01 | 134100       | bullish             | 1.4        | bullish    | 1.75     | neutral         | bullish
```

This is exactly what Tableau reads. One row per date. All signals visible.
A filter on `date = MAX(date)` gives you the current state of the Swiss economy.

---

## How dbt knows what order to run things

dbt builds a **DAG** (Directed Acyclic Graph) — a map of dependencies.

```
raw_fred  ──►  stg_fred  ──►
                              int_macro_indicators  ──►  mart_macro__overview
raw_snb   ──►  stg_snb   ──►
raw_oecd  ──►  stg_oecd  ──►
```

When you run `dbt run`, it reads all the `{{ ref() }}` and `{{ source() }}`
calls in your SQL files, figures out the dependency order automatically,
and runs them in the right sequence. You never have to specify the order.

---

## dbt tests

After building each model, dbt can test the data automatically.

**Example tests we will configure:**
```yaml
- name: stg_fred_indicators
  columns:
    - name: date
      tests: [not_null]
    - name: value
      tests: [not_null]
    - name: series_id
      tests: [not_null]
```

If any test fails, dbt reports it and the pipeline stops.
This catches data quality issues before they reach the dashboard.

---

## dbt project files explained

```
dbt/
├── dbt_project.yml       # Project config: name, BigQuery dataset names
├── profiles.yml          # Connection details: which BigQuery project to use
├── packages.yml          # External dbt packages (like dbt_utils)
├── models/
│   ├── staging/
│   │   ├── _sources.yml  # Declares where raw data lives (BigQuery tables)
│   │   ├── _staging.yml  # Documents and tests staging models
│   │   ├── stg_fred_indicators.sql
│   │   ├── stg_snb_indicators.sql
│   │   └── stg_oecd_indicators.sql
│   ├── intermediate/
│   │   ├── _intermediate.yml
│   │   └── int_macro_indicators.sql
│   └── marts/
│       ├── _marts.yml
│       ├── mart_macro__overview.sql
│       ├── mart_macro__growth.sql
│       ├── mart_macro__monetary.sql
│       └── mart_macro__external.sql
└── seeds/
    └── indicator_metadata.csv   # Static reference data (signal thresholds etc.)
```

---

## Summary: what happens at each stage

| Stage | Input | Output | What changes |
|-------|-------|--------|-------------|
| Raw | API response | BigQuery table | Nothing — saved as-is |
| Staging | Raw table | Clean table | Types fixed, nulls removed, dates standardised |
| Intermediate | Staging tables | Combined + enriched | MoM/YoY changes, rolling averages, signals added |
| Marts | Intermediate | Wide dashboard table | Pivoted, themed, human-readable, Tableau-ready |

---

*This file is updated every time a new dbt model is added or changed.*
