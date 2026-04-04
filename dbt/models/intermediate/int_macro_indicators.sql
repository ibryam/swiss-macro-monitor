{{
    config(materialized='view')
}}

/*
    int_macro_indicators

    Combines all 3 staging sources into one unified table,
    then adds calculated metrics for every indicator:

    - mom_change_pct   : month-over-month % change vs previous observation
    - yoy_change_pct   : year-over-year % change vs same period last year
    - rolling_avg_3m   : 3-period rolling average (smooths short-term noise)
    - rolling_avg_12m  : 12-period rolling average (shows long-term trend)
    - trend            : 'up', 'down', or 'flat' based on mom_change_pct
    - signal           : 'bullish', 'bearish', or 'neutral' — direction relative
                         to what is good for the Swiss economy

    Signal logic per category:
      growth    : up = bullish,  down = bearish
      labour    : up = bearish,  down = bullish  (unemployment — lower is better)
      prices    : high/rising above 2% = bearish, low/stable = neutral, deflation = bearish
      monetary  : context-dependent — stored as neutral, interpreted in marts
      external  : up = bullish,  down = bearish
      currency  : CHF strengthening (value falling) = bearish for exports
*/

with combined as (

    select * from {{ ref('stg_fred_indicators') }}
    union all
    select * from {{ ref('stg_snb_indicators') }}
    union all
    select * from {{ ref('stg_oecd_indicators') }}

),

with_window_metrics as (

    select
        date,
        source,
        series_id,
        indicator_name,
        indicator_category,
        value,
        unit,
        frequency,
        ingested_at,

        -- Previous period value (for MoM)
        lag(value) over (
            partition by series_id
            order by date
        )                                                           as prev_period_value,

        -- Same period last year value (for YoY)
        lag(value, 12) over (
            partition by series_id
            order by date
        )                                                           as prev_year_value,

        -- 3-period rolling average
        round(
            avg(value) over (
                partition by series_id
                order by date
                rows between 2 preceding and current row
            ), 4
        )                                                           as rolling_avg_3m,

        -- 12-period rolling average
        round(
            avg(value) over (
                partition by series_id
                order by date
                rows between 11 preceding and current row
            ), 4
        )                                                           as rolling_avg_12m

    from combined

),

with_changes as (

    select
        *,

        -- Month-over-month % change
        round(
            safe_divide(value - prev_period_value, prev_period_value) * 100,
            2
        )                                                           as mom_change_pct,

        -- Year-over-year % change
        round(
            safe_divide(value - prev_year_value, prev_year_value) * 100,
            2
        )                                                           as yoy_change_pct

    from with_window_metrics

),

with_signals as (

    select
        *,

        -- Trend: direction of the most recent change
        case
            when mom_change_pct >  0.5  then 'up'
            when mom_change_pct < -0.5  then 'down'
            else 'flat'
        end                                                         as trend,

        -- Economic signal: what this value means for Switzerland
        case
            -- Growth indicators: up is good
            when indicator_category = 'growth' and mom_change_pct > 0.5
                then 'bullish'
            when indicator_category = 'growth' and mom_change_pct < -0.5
                then 'bearish'

            -- Labour: unemployment — down is good
            when indicator_category = 'labour' and mom_change_pct < -0.5
                then 'bullish'
            when indicator_category = 'labour' and mom_change_pct > 0.5
                then 'bearish'

            -- Prices: stable 0-2% is ideal, above 2% or deflation is bearish
            when indicator_category = 'prices'
                and value between 0 and 2
                and mom_change_pct between -0.5 and 0.5
                then 'bullish'
            when indicator_category = 'prices'
                and (value > 3 or value < 0)
                then 'bearish'

            -- External: exports/trade up is good
            when indicator_category = 'external' and mom_change_pct > 0.5
                then 'bullish'
            when indicator_category = 'external' and mom_change_pct < -0.5
                then 'bearish'

            -- Currency: CHF strengthening (value going down) = bearish for exports
            when indicator_category = 'currency' and mom_change_pct < -0.5
                then 'bearish'
            when indicator_category = 'currency' and mom_change_pct > 0.5
                then 'bullish'

            -- Monetary: neutral by default — interpreted in context in marts
            else 'neutral'
        end                                                         as signal

    from with_changes

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
    prev_period_value,
    prev_year_value,
    mom_change_pct,
    yoy_change_pct,
    rolling_avg_3m,
    rolling_avg_12m,
    trend,
    signal,
    ingested_at

from with_signals
where date is not null
