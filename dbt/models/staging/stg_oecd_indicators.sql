{{
    config(materialized='view')
}}

/*
    stg_oecd_indicators

    Cleans raw OECD data:
    - OECD dates come in two formats:
        monthly:   YYYY-MM   → first day of month
        quarterly: YYYY-QX   → first day of the quarter
            Q1 → 01-01
            Q2 → 04-01
            Q3 → 07-01
            Q4 → 10-01
    - Removes null values
    - Standardises column names
*/

select
    case
        when length(date) = 4
            then cast(concat(date, '-01-01') as date)
        when length(date) = 7 and date not like '%-Q%'
            then cast(concat(date, '-01') as date)
        when date like '%-Q1'
            then cast(concat(left(date, 4), '-01-01') as date)
        when date like '%-Q2'
            then cast(concat(left(date, 4), '-04-01') as date)
        when date like '%-Q3'
            then cast(concat(left(date, 4), '-07-01') as date)
        when date like '%-Q4'
            then cast(concat(left(date, 4), '-10-01') as date)
        else null
    end                             as date,
    source,
    series_id,
    indicator_name,
    indicator_category,
    cast(value as float64)          as value,
    unit,
    frequency,
    cast(ingested_at as timestamp)  as ingested_at

from {{ source('swiss_macro_raw', 'raw_oecd_indicators') }}

where value is not null
