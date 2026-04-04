{{
    config(materialized='view')
}}

/*
    stg_fred_indicators

    Cleans raw FRED data:
    - Casts date string to DATE type
    - Removes null values
    - Standardises column names

    FRED dates are in YYYY-MM-DD format — cast directly to DATE.
*/

select
    cast(date as date)              as date,
    source,
    series_id,
    indicator_name,
    indicator_category,
    cast(value as float64)          as value,
    unit,
    frequency,
    cast(ingested_at as timestamp)  as ingested_at

from {{ source('swiss_macro_raw', 'raw_fred_indicators') }}

where value is not null
