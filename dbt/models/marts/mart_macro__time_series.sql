{{
    config(materialized='table')
}}

/*
    mart_macro__time_series

    One row per indicator per date. Powers Tabs 2, 3 and 4 of the dashboard.

    Tab 2 — Growth & Labour:
        Filter: indicator_category IN ('growth', 'labour')
        Charts: GDP growth, manufacturing, retail trade, unemployment over time

    Tab 3 — Monetary & Prices:
        Filter: indicator_category IN ('monetary', 'prices')
        Charts: SNB rate, SARON, CPI, bond yield over time

    Tab 4 — Currency & External:
        Filter: indicator_category IN ('currency', 'external')
        Charts: CHF/EUR, CHF/USD, exports over time

    All line charts use this single table — Tableau filters by category.

    Columns:
        date                  - observation date
        source                - FRED, SNB or OECD
        series_id             - unique series identifier
        indicator_name        - human-readable name
        indicator_category    - growth / labour / prices / monetary / external / currency
        value                 - raw indicator value
        unit                  - percent, index, chf, thousands, etc.
        frequency             - monthly, quarterly, daily
        mom_change_pct        - month-over-month % change
        yoy_change_pct        - year-over-year % change
        rolling_avg_3m        - 3-period rolling average (smooths noise)
        rolling_avg_12m       - 12-period rolling average (long-term trend)
        trend                 - 'up', 'down', 'flat'
        signal                - 'bullish', 'neutral', 'bearish'
        is_latest             - true for the most recent row per series (for KPI tiles)
*/

with base as (

    select * from {{ ref('int_macro_indicators') }}

),

with_latest_flag as (

    select
        *,
        row_number() over (
            partition by series_id
            order by date desc
        ) = 1                           as is_latest

    from base

)

select
    date,
    source,
    series_id,
    indicator_name,
    indicator_category,
    value,
    unit,
    frequency,
    mom_change_pct,
    yoy_change_pct,
    rolling_avg_3m,
    rolling_avg_12m,
    trend,
    signal,
    is_latest

from with_latest_flag
order by indicator_name, date
