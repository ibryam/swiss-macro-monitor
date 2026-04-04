{{
    config(materialized='view')
}}

/*
    stg_snb_indicators

    Cleans raw SNB data:
    - SNB dates come in mixed formats:
        daily series:   YYYY-MM-DD  → cast to DATE directly
        monthly series: YYYY-MM     → parse as first day of month
    - Removes null values
    - Standardises column names
*/

select
    case
        when length(date) = 10 then cast(date as date)
        when length(date) = 7  then cast(concat(date, '-01') as date)
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

from {{ source('swiss_macro_raw', 'raw_snb_indicators') }}

where value is not null
