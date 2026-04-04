{{
    config(materialized='table')
}}

/*
    mart_macro__overview

    One row per date — wide format. Powers Tab 1 of the dashboard.

    Each indicator becomes its own column so Tableau can display KPI tiles
    without needing to filter or pivot. One row = one month = full picture
    of the Swiss economy on that date.

    Also includes:
        overall_signal  — composite signal across all indicators on that date
                          'bullish'  if majority of signals are bullish
                          'bearish'  if majority of signals are bearish
                          'neutral'  if mixed or equal

    Indicators included (latest available value per month):
        Growth:    gdp_growth, manufacturing_growth, retail_trade_growth
        Labour:    unemployment
        Prices:    cpi_inflation, cpi_all_items
        Monetary:  snb_policy_rate, saron, bond_yield_10y, interbank_rate
        External:  exports_growth
        Currency:  chf_per_eur, chf_per_usd
*/

with time_series as (

    select * from {{ ref('mart_macro__time_series') }}

),

-- Pivot each indicator to its own column
-- Using MAX(CASE WHEN) pattern — standard BigQuery pivot approach
pivoted as (

    select
        date,

        -- Growth
        max(case when series_id = 'KEI/CHE/B1GQ_Q/_T/GY'   then value end)          as gdp_growth,
        max(case when series_id = 'KEI/CHE/B1GQ_Q/_T/GY'   then mom_change_pct end) as gdp_growth_mom,
        max(case when series_id = 'KEI/CHE/B1GQ_Q/_T/GY'   then yoy_change_pct end) as gdp_growth_yoy,
        max(case when series_id = 'KEI/CHE/B1GQ_Q/_T/GY'   then signal end)         as gdp_signal,

        max(case when series_id = 'KEI/CHE/PRVM/C/GY'      then value end)          as manufacturing_growth,
        max(case when series_id = 'KEI/CHE/PRVM/C/GY'      then mom_change_pct end) as manufacturing_mom,
        max(case when series_id = 'KEI/CHE/PRVM/C/GY'      then signal end)         as manufacturing_signal,

        max(case when series_id = 'KEI/CHE/TOVM/G47/GY'    then value end)          as retail_trade_growth,
        max(case when series_id = 'KEI/CHE/TOVM/G47/GY'    then mom_change_pct end) as retail_trade_mom,
        max(case when series_id = 'KEI/CHE/TOVM/G47/GY'    then signal end)         as retail_trade_signal,

        -- Labour
        max(case when series_id = 'LMUNRRTTCHM156N'         then value end)          as unemployment,
        max(case when series_id = 'LMUNRRTTCHM156N'         then mom_change_pct end) as unemployment_mom,
        max(case when series_id = 'LMUNRRTTCHM156N'         then yoy_change_pct end) as unemployment_yoy,
        max(case when series_id = 'LMUNRRTTCHM156N'         then signal end)         as unemployment_signal,

        max(case when series_id = 'KEI/CHE/EMP/_T/_Z'       then value end)          as employment_thousands,
        max(case when series_id = 'KEI/CHE/EMP/_T/_Z'       then mom_change_pct end) as employment_mom,
        max(case when series_id = 'KEI/CHE/EMP/_T/_Z'       then signal end)         as employment_signal,

        -- Prices
        max(case when series_id = 'FPCPITOTLZGCHE'          then value end)          as cpi_inflation_pct,
        max(case when series_id = 'FPCPITOTLZGCHE'          then mom_change_pct end) as cpi_inflation_mom,
        max(case when series_id = 'FPCPITOTLZGCHE'          then signal end)         as cpi_signal,

        max(case when series_id = 'CPALTT01CHM657N'         then value end)          as cpi_all_items,
        max(case when series_id = 'CPALTT01CHM657N'         then mom_change_pct end) as cpi_all_items_mom,

        -- Monetary
        max(case when series_id = 'snbgwdzid/ZIG'           then value end)          as snb_policy_rate,
        max(case when series_id = 'snbgwdzid/ZIG'           then mom_change_pct end) as snb_rate_mom,
        max(case when series_id = 'snbgwdzid/ZIG'           then signal end)         as snb_rate_signal,

        max(case when series_id = 'snbgwdzid/SARON'         then value end)          as saron,
        max(case when series_id = 'snbgwdzid/SARON'         then mom_change_pct end) as saron_mom,

        max(case when series_id = 'snbgwdzid/ZIGBL'         then value end)          as snb_rate_upper_bound,

        max(case when series_id = 'IRLTLT01CHM156N'         then value end)          as bond_yield_10y,
        max(case when series_id = 'IRLTLT01CHM156N'         then mom_change_pct end) as bond_yield_mom,

        max(case when series_id = 'KEI/CHE/IRSTCI/_Z/_Z'    then value end)          as interbank_rate,

        -- External
        max(case when series_id = 'KEI/CHE/EX/_T/G1'        then value end)          as exports_growth,
        max(case when series_id = 'KEI/CHE/EX/_T/G1'        then mom_change_pct end) as exports_mom,
        max(case when series_id = 'KEI/CHE/EX/_T/G1'        then signal end)         as exports_signal,

        -- Currency
        max(case when series_id = 'devkum/EUR1'              then value end)          as chf_per_eur,
        max(case when series_id = 'devkum/EUR1'              then mom_change_pct end) as chf_eur_mom,
        max(case when series_id = 'devkum/EUR1'              then signal end)         as chf_eur_signal,

        max(case when series_id = 'devkum/USD1'              then value end)          as chf_per_usd,
        max(case when series_id = 'devkum/USD1'              then mom_change_pct end) as chf_usd_mom,
        max(case when series_id = 'devkum/USD1'              then signal end)         as chf_usd_signal

    from time_series
    group by date

),

-- Count bullish/bearish signals per date for the composite score
with_signal_counts as (

    select
        p.*,

        -- Count how many indicators are bullish vs bearish on this date
        (
            case when gdp_signal         = 'bullish' then 1 else 0 end +
            case when manufacturing_signal = 'bullish' then 1 else 0 end +
            case when retail_trade_signal  = 'bullish' then 1 else 0 end +
            case when unemployment_signal  = 'bullish' then 1 else 0 end +
            case when employment_signal    = 'bullish' then 1 else 0 end +
            case when cpi_signal           = 'bullish' then 1 else 0 end +
            case when exports_signal       = 'bullish' then 1 else 0 end +
            case when chf_eur_signal       = 'bullish' then 1 else 0 end
        )                                                               as bullish_count,

        (
            case when gdp_signal           = 'bearish' then 1 else 0 end +
            case when manufacturing_signal = 'bearish' then 1 else 0 end +
            case when retail_trade_signal  = 'bearish' then 1 else 0 end +
            case when unemployment_signal  = 'bearish' then 1 else 0 end +
            case when employment_signal    = 'bearish' then 1 else 0 end +
            case when cpi_signal           = 'bearish' then 1 else 0 end +
            case when exports_signal       = 'bearish' then 1 else 0 end +
            case when chf_eur_signal       = 'bearish' then 1 else 0 end
        )                                                               as bearish_count

    from pivoted p

)

select
    date,

    -- Growth
    gdp_growth,
    gdp_growth_mom,
    gdp_growth_yoy,
    gdp_signal,
    manufacturing_growth,
    manufacturing_mom,
    manufacturing_signal,
    retail_trade_growth,
    retail_trade_mom,
    retail_trade_signal,

    -- Labour
    unemployment,
    unemployment_mom,
    unemployment_yoy,
    unemployment_signal,
    employment_thousands,
    employment_mom,
    employment_signal,

    -- Prices
    cpi_inflation_pct,
    cpi_inflation_mom,
    cpi_signal,
    cpi_all_items,
    cpi_all_items_mom,

    -- Monetary
    snb_policy_rate,
    snb_rate_mom,
    snb_rate_signal,
    saron,
    saron_mom,
    snb_rate_upper_bound,
    bond_yield_10y,
    bond_yield_mom,
    interbank_rate,

    -- External
    exports_growth,
    exports_mom,
    exports_signal,

    -- Currency
    chf_per_eur,
    chf_eur_mom,
    chf_eur_signal,
    chf_per_usd,
    chf_usd_mom,
    chf_usd_signal,

    -- Composite signal
    bullish_count,
    bearish_count,
    case
        when bullish_count > bearish_count then 'bullish'
        when bearish_count > bullish_count then 'bearish'
        else 'neutral'
    end                                                                 as overall_signal

from with_signal_counts
order by date
