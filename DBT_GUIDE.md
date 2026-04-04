# dbt Guide — Swiss Macro Monitor

A plain-English explanation of what dbt does, how it works,
and how data changes at every stage in this project.

---

## What is dbt?

dbt (data build tool) is a tool that transforms raw data inside your database
using SQL. It does not move data — it only reshapes it.

Think of it like a series of SQL views and tables, but with:
- Automatic dependency management (it knows which model to run first)
- Built-in testing (it checks your data is clean after every run)
- Documentation (it generates a website explaining every table)

**Without dbt:**
```
You write SQL → run it manually → hope nothing breaks → repeat
```

**With dbt:**
```
You write SQL models → dbt figures out the order → runs them → tests them
```

**Concrete example of dependency management:**

You have three models: `stg_fred` → `int_macro_indicators` → `mart_macro__overview`.

If you accidentally run `mart_macro__overview` before `stg_fred` is built, dbt stops with an error:
```
Relation "swiss_macro_staging.stg_fred_indicators" does not exist.
```

You never have to think about run order. dbt reads your `{{ ref() }}` calls,
builds the dependency map automatically, and always runs in the right sequence.

---

## The golden rule

> dbt never touches your raw data.
> It only reads from it and writes to new tables/views.

Your raw data in `swiss_macro_raw` is always safe.
dbt creates separate BigQuery datasets for each layer:

```
swiss_macro_raw          ← ingestion writes here (Python scripts)
swiss_macro_staging      ← dbt staging models write here (views)
swiss_macro_intermediate ← dbt intermediate model writes here (view)
swiss_macro_marts        ← dbt mart models write here (tables)
```

If anything goes wrong in a mart, you can always rebuild from raw — it is untouched.

---

## How data flows through this project

```
SOURCES (raw data, exactly as downloaded from APIs)
    │
    │  swiss_macro_raw.raw_fred_indicators   ← 5 Swiss indicators from FRED
    │  swiss_macro_raw.raw_snb_indicators    ← 5 series from Swiss National Bank
    │  swiss_macro_raw.raw_oecd_indicators   ← 6 series from OECD KEI
    │
    ▼
STAGING (cleaned, standardised, one unified schema)
    │
    │  swiss_macro_staging.stg_fred_indicators   ← dates cast, nulls removed
    │  swiss_macro_staging.stg_snb_indicators    ← mixed date formats handled
    │  swiss_macro_staging.stg_oecd_indicators   ← quarterly dates parsed
    │
    ▼
INTERMEDIATE (one unified table + calculations added)
    │
    │  swiss_macro_intermediate.int_macro_indicators
    │     ← UNION ALL of all 3 sources (~15,000 rows)
    │     ← MoM/YoY changes, rolling averages, trend, signal
    │
    ▼
MARTS (final tables, ready for the Tableau dashboard)

    swiss_macro_marts.mart_macro__time_series   ← tall format, line charts
    swiss_macro_marts.mart_macro__overview      ← wide format, KPI tiles
```

---

## Stage 1 — Raw (what we have now) — BUILT ✓

This is data exactly as it came from the API. We do not touch it.
It is our safety net — if anything goes wrong downstream, we can always rebuild.

**What raw data looks like from each source:**

**FRED** (`raw_fred_indicators`) — date is already `YYYY-MM-DD`:
```
date         | source | series_id          | indicator_name    | value    | unit    | frequency
2024-01-01   | FRED   | LMUNRRTTCHM156N    | Unemployment Rate | 134200   | persons | monthly
2024-01-01   | FRED   | FPCPITOTLZGCHE     | CPI Inflation     | 1.31     | percent | annual
2024-01-01   | FRED   | CLVMNACSAB1GQCH    | Real GDP          | 179421   | chf_mln | quarterly
```

**SNB** (`raw_snb_indicators`) — date format depends on the series:
```
date         | source | series_id          | indicator_name    | value    | unit    | frequency
2024-03-15   | SNB    | devkum/EUR1        | CHF/EUR Rate      | 0.9431   | chf     | daily
2024-03-01   | SNB    | snbgwdzid/ZIG      | SNB Policy Rate   | 1.75     | percent | monthly
2024-03     ← monthly series stores date as "2024-03", not "2024-03-01"
```

**OECD** (`raw_oecd_indicators`) — multiple date formats mixed together:
```
date         | source | series_id               | indicator_name      | value   | frequency
2024-03      | OECD   | KEI/CHE/PRVM/C/GY       | Manufacturing       | 3.2     | monthly
2024-Q1      | OECD   | KEI/CHE/B1GQ_Q/_T/GY    | GDP Growth          | 0.5     | quarterly
2024         | OECD   | KEI/CHE/EMP/_T/_Z        | Employment          | 5220    | annual
```

**Problems with raw data:**
- FRED dates: string `"2024-01-01"` — not yet a DATE type
- SNB monthly dates: string `"2024-03"` — can't use this as a date in SQL
- OECD quarterly dates: string `"2024-Q1"` — not a valid date format anywhere
- OECD annual dates: string `"2024"` — just a year, no month or day
- Values are stored as strings — must cast to FLOAT64 before doing maths
- Three separate tables — you can't compare CHF/EUR against GDP without a JOIN
- No calculated metrics — just the raw numbers, no context

---

## Stage 2 — Staging (cleaning) — BUILT ✓

Staging models fix the raw data problems. One model per source.
After staging, all three sources have the same schema — they can be combined.

---

### stg_fred_indicators

**Problem:** FRED dates come as strings in `YYYY-MM-DD` format.
**Fix:** Simple `cast(date as date)` — no manipulation needed.

```sql
select
    cast(date as date)              as date,    -- "2024-01-01" → DATE 2024-01-01
    source,
    series_id,
    indicator_name,
    indicator_category,
    cast(value as float64)          as value,   -- "134200" → 134200.0
    unit,
    frequency,
    cast(ingested_at as timestamp)  as ingested_at
from {{ source('swiss_macro_raw', 'raw_fred_indicators') }}
where value is not null
```

**Before → After:**
```
BEFORE (raw):                 AFTER (stg_fred_indicators):
──────────────────────        ─────────────────────────────────────
date:  "2024-01-01"   ──►     date:  2024-01-01    (DATE)
value: "134200"       ──►     value: 134200.0      (FLOAT64)
```

---

### stg_snb_indicators

**Problem:** SNB has two date formats depending on the series:
- Daily series (SARON, CHF/EUR): `"2024-03-15"` — 10 characters
- Monthly series (SNB rate, policy rate): `"2024-03"` — 7 characters

**Fix:** Check the length of the string and handle each format:

```sql
select
    case
        when length(date) = 10 then cast(date as date)             -- "2024-03-15" → 2024-03-15
        when length(date) = 7  then cast(concat(date, '-01') as date)  -- "2024-03" → 2024-03-01
        else null
    end                             as date,
    source,
    series_id,
    ...
from {{ source('swiss_macro_raw', 'raw_snb_indicators') }}
where value is not null
```

**Before → After:**
```
BEFORE (raw):                 AFTER (stg_snb_indicators):
──────────────────────        ─────────────────────────────────────
date:  "2024-03-15"   ──►     date:  2024-03-15    (daily — kept as-is)
date:  "2024-03"      ──►     date:  2024-03-01    (monthly → first of month)
```

---

### stg_oecd_indicators

**Problem:** OECD has four date formats depending on the series frequency:
- Monthly: `"2024-03"` — 7 characters, no Q
- Quarterly: `"2024-Q1"` through `"2024-Q4"` — needs to map Q → month
- Annual: `"2024"` — just 4 characters, no month or day

**Fix:** A CASE statement that handles every format:

```sql
select
    case
        when length(date) = 4
            then cast(concat(date, '-01-01') as date)           -- "2024"    → 2024-01-01
        when length(date) = 7 and date not like '%-Q%'
            then cast(concat(date, '-01') as date)              -- "2024-03" → 2024-03-01
        when date like '%-Q1'
            then cast(concat(left(date, 4), '-01-01') as date)  -- "2024-Q1" → 2024-01-01
        when date like '%-Q2'
            then cast(concat(left(date, 4), '-04-01') as date)  -- "2024-Q2" → 2024-04-01
        when date like '%-Q3'
            then cast(concat(left(date, 4), '-07-01') as date)  -- "2024-Q3" → 2024-07-01
        when date like '%-Q4'
            then cast(concat(left(date, 4), '-10-01') as date)  -- "2024-Q4" → 2024-10-01
        else null
    end                             as date,
    ...
from {{ source('swiss_macro_raw', 'raw_oecd_indicators') }}
where value is not null
```

**Before → After:**
```
BEFORE (raw):                 AFTER (stg_oecd_indicators):
──────────────────────        ─────────────────────────────────────
date:  "2024-03"      ──►     date:  2024-03-01    (monthly → first of month)
date:  "2024-Q1"      ──►     date:  2024-01-01    (Q1 → January)
date:  "2024-Q2"      ──►     date:  2024-04-01    (Q2 → April)
date:  "2024-Q3"      ──►     date:  2024-07-01    (Q3 → July)
date:  "2024-Q4"      ──►     date:  2024-10-01    (Q4 → October)
date:  "2024"         ──►     date:  2024-01-01    (annual → Jan 1)
```

---

### What staging achieves

After all three staging models run, all sources share one unified schema:
```
date (DATE) | source | series_id | indicator_name | indicator_category | value (FLOAT64) | unit | frequency
```

This makes the UNION ALL in the intermediate layer possible — all three tables
have matching columns and compatible types.

**Tests — 9/9 passing:**
Each staging model has `not_null` tests on `date`, `series_id`, and `value`.
If any row has a null in these columns, dbt reports a failure and the pipeline stops.

---

## Stage 3 — Intermediate (calculations) — BUILT ✓

The intermediate model `int_macro_indicators` does two things:
1. Merges all three staging sources into one table
2. Adds calculated metrics to every row

---

### Step 1 — UNION ALL merges the three sources

```sql
with combined as (
    select * from {{ ref('stg_fred_indicators') }}   -- 2,701 rows
    union all
    select * from {{ ref('stg_snb_indicators') }}    -- 9,655 rows
    union all
    select * from {{ ref('stg_oecd_indicators') }}   -- 2,290 rows
)
-- result: ~14,646 rows, all indicators together
```

Because all three staging models share the same schema (same column names and types),
`UNION ALL` works cleanly. No JOINs, no renaming. This is why the staging step matters.

---

### Step 2 — Window functions look backwards in time

Window functions let us look at previous rows per series without a self-join.
`PARTITION BY series_id` means "only look at other rows for the same indicator".
`ORDER BY date` means "look at them in chronological order".

```sql
-- Previous period value (one row back for the same series)
lag(value, 1) over (
    partition by series_id
    order by date
) as prev_period_value

-- Same period last year (12 rows back — works for monthly data)
lag(value, 12) over (
    partition by series_id
    order by date
) as prev_year_value

-- 3-period rolling average (current + 2 previous = smooths short-term noise)
avg(value) over (
    partition by series_id
    order by date
    rows between 2 preceding and current row
) as rolling_avg_3m

-- 12-period rolling average (current + 11 previous = shows long-term trend)
avg(value) over (
    partition by series_id
    order by date
    rows between 11 preceding and current row
) as rolling_avg_12m
```

**Concrete example — Unemployment Rate (LMUNRRTTCHM156N):**
```
date         | value   | prev_period | prev_year | rolling_3m | rolling_12m
2024-01-01   | 134200  | 133800      | 125400    | 134100     | 131200
2024-02-01   | 135100  | 134200      | 126800    | 134367     | 131500
2024-03-01   | 139800  | 135100      | 127200    | 136367     | 132000
               ↑ value    ↑ Jan value  ↑ Mar 2023  ↑ avg Jan-Mar ↑ avg 12m
```

The first row of every series has `null` for `prev_period_value` — there is no previous row.
dbt handles this cleanly with `SAFE_DIVIDE()` — null divided by anything returns null, not an error.

---

### Step 3 — MoM and YoY % changes

```sql
-- Month-over-month: how much did this change vs last month?
round(
    safe_divide(value - prev_period_value, prev_period_value) * 100,
    2
) as mom_change_pct

-- Year-over-year: how much did this change vs same month last year?
round(
    safe_divide(value - prev_year_value, prev_year_value) * 100,
    2
) as yoy_change_pct
```

**Example calculations:**
```
Unemployment Rate, March 2024:
  value: 139,800   prev_month: 135,100
  mom_change_pct = (139800 - 135100) / 135100 * 100 = +3.48%

  value: 139,800   prev_year: 127,200
  yoy_change_pct = (139800 - 127200) / 127200 * 100 = +9.91%

SNB Policy Rate, September 2024:
  value: 1.00   prev_month: 1.25
  mom_change_pct = (1.00 - 1.25) / 1.25 * 100 = -20.0%  ← SNB cut rates
```

---

### Step 4 — Trend and Signal

**Trend** is simple — which direction is the MoM number?
```sql
case
    when mom_change_pct >  0.5 then 'up'
    when mom_change_pct < -0.5 then 'down'
    else 'flat'
end as trend
```

**Signal** is the economic interpretation — what does this movement mean for Switzerland?
The threshold ±0.5% is used to ignore rounding noise:

```sql
case
    -- Growth (GDP, manufacturing, retail trade, exports): up is good
    when indicator_category = 'growth' and mom_change_pct > 0.5  then 'bullish'
    when indicator_category = 'growth' and mom_change_pct < -0.5 then 'bearish'

    -- Labour (unemployment): DOWN is good — fewer unemployed people
    when indicator_category = 'labour' and mom_change_pct < -0.5 then 'bullish'
    when indicator_category = 'labour' and mom_change_pct > 0.5  then 'bearish'

    -- Prices (CPI): SNB target is 0-2%. Above 3% or below 0% is bearish
    when indicator_category = 'prices'
        and value between 0 and 2
        and mom_change_pct between -0.5 and 0.5 then 'bullish'
    when indicator_category = 'prices'
        and (value > 3 or value < 0)            then 'bearish'

    -- External (exports): up is good
    when indicator_category = 'external' and mom_change_pct > 0.5  then 'bullish'
    when indicator_category = 'external' and mom_change_pct < -0.5 then 'bearish'

    -- Currency: CHF strengthening (value falling) = exports become expensive = bearish
    when indicator_category = 'currency' and mom_change_pct < -0.5 then 'bearish'
    when indicator_category = 'currency' and mom_change_pct > 0.5  then 'bullish'

    -- Monetary (SNB rate, SARON, bond yield): context-dependent, stored as neutral
    else 'neutral'
end as signal
```

**Signal logic summary:**

| Category | Rising | Falling | Why |
|----------|--------|---------|-----|
| growth | bullish | bearish | GDP/manufacturing up = economy expanding |
| labour | bearish | bullish | Unemployment rising = job market weakening |
| prices | bearish if >3% or <0% | — | SNB target is 0–2% stable inflation |
| external | bullish | bearish | Exports up = demand for Swiss goods growing |
| currency | bullish | bearish | CHF weakening helps Swiss exporters |
| monetary | neutral | neutral | Rate changes are context-dependent |

**Real examples from the data:**

```
Unemployment Rate, March 2024:
  mom_change_pct: +3.48%   category: labour
  → trend:  'up'
  → signal: 'bearish'   ← unemployment rising is bad news

SNB Policy Rate, September 2024:
  value: 1.00   mom_change_pct: -20.0%   category: monetary
  → trend:  'down'
  → signal: 'neutral'   ← rate cuts are context-dependent

CPI Inflation, January 2024:
  value: 1.31%   mom_change_pct: -0.2%   category: prices
  → trend:  'flat'
  → signal: 'bullish'   ← stable inflation inside the 0–2% target

GDP Growth, Q1 2024:
  mom_change_pct: +2.1%   category: growth
  → trend:  'up'
  → signal: 'bullish'   ← economy growing
```

**Tests — 5/5 passing:**
`not_null` on `date`, `series_id`, `indicator_name`, `indicator_category`, `value`.

---

## Stage 4 — Marts (dashboard-ready) — BUILT ✓

Mart models are the final output — clean tables ready for Tableau.
This project has two marts, each answering a different dashboard question.

**The problem with intermediate data:**
The intermediate model has one row per indicator per date — great for calculations,
but hard for a dashboard to use directly. Tableau needs different shapes for different
chart types.

---

### mart_macro__time_series (tall format, 13,200 rows)

**Shape:** One row per indicator per date — same structure as intermediate, with one extra column.

**What it adds:** `is_latest` flag.

```sql
row_number() over (
    partition by series_id
    order by date desc
) = 1   as is_latest
```

For each series, the most recent row gets `is_latest = true`. All older rows get `false`.

**Example — Unemployment Rate (most recent 3 rows):**
```
date         | indicator_name    | value  | mom_change_pct | trend | signal  | is_latest
2024-03-01   | Unemployment Rate | 139800 | +3.48%         | up    | bearish | true   ← most recent
2024-02-01   | Unemployment Rate | 135100 | +0.67%         | up    | bearish | false
2024-01-01   | Unemployment Rate | 134200 | +0.30%         | flat  | neutral | false
```

Tableau uses `is_latest = true` to show the current value in a KPI tile without
needing a `MAX(date)` filter in the dashboard — just drag and filter.

**Why tall format here?** Line charts in Tableau need one row per data point.
Tableau then filters by `indicator_category` to show the right indicators per tab:

```
Tab 2 — Growth & Labour:   indicator_category IN ('growth', 'labour')
Tab 3 — Monetary & Prices: indicator_category IN ('monetary', 'prices')
Tab 4 — Currency & External: indicator_category IN ('currency', 'external')
```

One table feeds all three tabs. No duplication.

---

### mart_macro__overview (wide format, 5,000 rows)

**Shape:** One row per date — every indicator becomes its own column.

**Why wide format here?** KPI tiles on the overview tab need all indicator values
on the same row. If the data is tall (one indicator per row), Tableau would need
to pivot it on-the-fly using LOD expressions — slow and complex. If the data is
wide (one row per date), Tableau just reads columns directly — simple and fast.

**How the pivot works:**

The SQL uses a pattern called `MAX(CASE WHEN)`. For each date, GROUP BY date and
extract each indicator into its own column:

```sql
select
    date,

    -- One block per indicator: value, MoM, and signal
    max(case when series_id = 'LMUNRRTTCHM156N'      then value end)          as unemployment,
    max(case when series_id = 'LMUNRRTTCHM156N'      then mom_change_pct end) as unemployment_mom,
    max(case when series_id = 'LMUNRRTTCHM156N'      then signal end)         as unemployment_signal,

    max(case when series_id = 'KEI/CHE/B1GQ_Q/_T/GY' then value end)          as gdp_growth,
    max(case when series_id = 'KEI/CHE/B1GQ_Q/_T/GY' then mom_change_pct end) as gdp_growth_mom,
    max(case when series_id = 'KEI/CHE/B1GQ_Q/_T/GY' then signal end)         as gdp_signal,

    max(case when series_id = 'snbgwdzid/ZIG'         then value end)          as snb_policy_rate,
    max(case when series_id = 'snbgwdzid/ZIG'         then mom_change_pct end) as snb_rate_mom,
    max(case when series_id = 'snbgwdzid/ZIG'         then signal end)         as snb_rate_signal,

    -- ... one block per indicator (12 indicators total)

from {{ ref('mart_macro__time_series') }}
group by date
```

Why `MAX()`? For each date, there is exactly one row per series in the tall table.
`MAX()` just picks that single value — it is not aggregating multiple values, it is
extracting one value per column. This is the standard BigQuery pivot pattern.

**Composite economic signal:**

After pivoting, count how many indicators are bullish vs bearish on each date:

```sql
-- Count bullish signals
(case when gdp_signal          = 'bullish' then 1 else 0 end +
 case when manufacturing_signal = 'bullish' then 1 else 0 end +
 case when unemployment_signal  = 'bullish' then 1 else 0 end +
 case when cpi_signal           = 'bullish' then 1 else 0 end +
 -- ... 8 indicators total
) as bullish_count,

-- Overall signal = majority wins
case
    when bullish_count > bearish_count then 'bullish'
    when bearish_count > bullish_count then 'bearish'
    else 'neutral'
end as overall_signal
```

**Example output (one row = one full month of Swiss macro data):**
```
date       | unemployment | unemp_signal | gdp_growth | gdp_signal | snb_rate | overall_signal | bullish | bearish
2024-03-01 | 139800       | bearish      | 0.5        | bullish    | 1.75     | neutral        | 3       | 3
2024-02-01 | 135100       | bearish      | 0.5        | bullish    | 1.75     | bullish        | 4       | 2
2024-01-01 | 134200       | neutral      | 0.3        | neutral    | 1.75     | bullish        | 5       | 1
```

March 2024: 3 bullish, 3 bearish → overall_signal = 'neutral'
February 2024: 4 bullish, 2 bearish → overall_signal = 'bullish'

**Tests — 6/6 passing:**
`not_null` on `date`, `series_id`, `indicator_name`, `indicator_category`, `value` in
`mart_macro__time_series`; `not_null` on `date` in `mart_macro__overview`.

**BigQuery result:**
- `swiss_macro_marts.mart_macro__time_series` — 13,200 rows, powers Tabs 2/3/4
- `swiss_macro_marts.mart_macro__overview` — 5,000 rows, powers Tab 1

---

## How dbt knows what order to run things

dbt builds a **DAG** (Directed Acyclic Graph) — a map of dependencies derived
automatically from the `{{ ref() }}` and `{{ source() }}` calls in your SQL.

```
raw_fred_indicators ──►  stg_fred_indicators  ─┐
                                                │
raw_snb_indicators  ──►  stg_snb_indicators   ─┼──►  int_macro_indicators  ──►  mart_macro__time_series  ──►  mart_macro__overview
                                                │
raw_oecd_indicators ──►  stg_oecd_indicators  ─┘
```

When you run `dbt run`:
1. dbt reads every `.sql` file and finds all `{{ ref() }}` and `{{ source() }}` calls
2. It builds the dependency graph
3. It runs models in order: staging first, then intermediate, then marts
4. It never runs a model before its dependencies are ready

You do not specify the order — dbt figures it out from your SQL.

**Run command and output:**
```bash
dbt run

# dbt output:
Running with dbt=1.7.0
Found 5 models, 20 tests, 3 sources

Concurrency: 1 thread

1 of 5 START sql view model swiss_macro_staging.stg_fred_indicators  .... [RUN]
1 of 5 OK created sql view stg_fred_indicators                        .... [OK in 1.2s]
2 of 5 START sql view model swiss_macro_staging.stg_snb_indicators    .... [RUN]
2 of 5 OK created sql view stg_snb_indicators                         .... [OK in 0.9s]
3 of 5 START sql view model swiss_macro_staging.stg_oecd_indicators   .... [RUN]
3 of 5 OK created sql view stg_oecd_indicators                        .... [OK in 1.1s]
4 of 5 START sql view model swiss_macro_intermediate.int_macro_indicators [RUN]
4 of 5 OK created sql view int_macro_indicators                       .... [OK in 1.4s]
5 of 5 START sql table model swiss_macro_marts.mart_macro__time_series    [RUN]
5 of 5 OK created sql table mart_macro__time_series                   .... [OK in 8.3s]
...

Finished running 5 models in 0:00:14.
```

---

## dbt tests — 20/20 passing

After building each model, `dbt test` checks that the data is clean.
Tests are defined in `.yml` files next to the `.sql` model files.

**What a test definition looks like (`_staging.yml`):**
```yaml
models:
  - name: stg_fred_indicators
    columns:
      - name: date
        tests: [not_null]       # Every row must have a date
      - name: series_id
        tests: [not_null]       # Every row must have a series ID
      - name: value
        tests: [not_null]       # Every row must have a value (nulls were filtered out)

  - name: stg_snb_indicators
    columns:
      - name: date
        tests: [not_null]       # Verifies our CASE statement handled all date formats
      - name: series_id
        tests: [not_null]
      - name: value
        tests: [not_null]
```

**What happens when a test fails:**
```
dbt test

FAIL not_null_stg_oecd_indicators_date
  Got 156 results, configured to fail if != 0.
  Failing rows:
    series_id: 'KEI/CHE/EMP/_T/_Z', date: null (annual format '2024' not handled)
```

This is exactly what happened when the OECD annual date format (`YYYY`) was not
handled in the CASE statement — 156 rows had null dates. The test caught it before
the dashboard showed any missing data. The fix was adding the `length(date) = 4` case.

**Test counts by layer:**
```
Staging:      9/9  passing   (3 models × 3 columns each)
Intermediate: 5/5  passing   (date, series_id, indicator_name, indicator_category, value)
Marts:        6/6  passing   (5 columns in time_series + date in overview)
Total:       20/20 passing
```

---

## dbt project files explained

```
dbt/
├── dbt_project.yml       # Project config: name, BigQuery dataset names, materializations
├── profiles.yml          # Connection details: which BigQuery project/credentials to use
├── packages.yml          # External dbt packages (e.g. dbt_utils for extra functions)
├── models/
│   ├── staging/
│   │   ├── _sources.yml  # Declares raw BigQuery tables as dbt sources
│   │   ├── _staging.yml  # Documents and tests all staging models
│   │   ├── stg_fred_indicators.sql   ← simple DATE cast
│   │   ├── stg_snb_indicators.sql    ← 2 date formats handled
│   │   └── stg_oecd_indicators.sql   ← 4 date formats handled (monthly/quarterly/annual)
│   ├── intermediate/
│   │   ├── _intermediate.yml
│   │   └── int_macro_indicators.sql  ← UNION ALL + window functions + signals
│   └── marts/
│       ├── _marts.yml
│       ├── mart_macro__time_series.sql   ← tall format, is_latest flag, line charts
│       └── mart_macro__overview.sql      ← wide format, pivot, composite signal, KPI tiles
└── seeds/
    └── indicator_metadata.csv   ← static reference data
```

**`dbt_project.yml` — controls where each layer lands in BigQuery:**
```yaml
models:
  swiss_macro_monitor:
    staging:
      +materialized: view     # Views are cheap — recomputed on query, no storage cost
      +schema: staging        # → swiss_macro_staging dataset
    intermediate:
      +materialized: view
      +schema: intermediate   # → swiss_macro_intermediate dataset
    marts:
      +materialized: table    # Tables are pre-computed — fast for Tableau to query
      +schema: marts          # → swiss_macro_marts dataset
```

Why views for staging/intermediate and tables for marts? Views cost nothing to build —
BigQuery runs the SQL on-the-fly when something queries them. Tables are pre-computed
and stored. Tableau queries marts constantly, so pre-computing them as tables is faster.

---

## Summary: what happens at each stage

| Stage | Status | Input | Output | What changes |
|-------|--------|-------|--------|-------------|
| Raw | BUILT ✓ | API response | BigQuery table | Nothing — saved as-is |
| Staging | BUILT ✓ | Raw table | Clean view | Types fixed, nulls removed, dates standardised across 4 formats |
| Intermediate | BUILT ✓ | 3 staging views | One combined view | UNION ALL, MoM/YoY changes, rolling averages, trend, signal |
| Marts | BUILT ✓ | Intermediate view | 2 tables | Tall for line charts (+ is_latest), wide for KPI tiles (pivot + composite signal) |

---

*This file is updated every time a new dbt model is added or changed.*
