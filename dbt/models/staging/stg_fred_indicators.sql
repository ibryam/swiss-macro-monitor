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
  -- Exclude deprecated series replaced by fresher OECD sources:
  -- LMUNRRTTCHM156N: unemployment replaced by OECD UNEMP (quarterly, unit stored as "persons" — incorrect)
  -- CPALTT01CHM657N: CPI All Items — FRED stopped updating in Mar 2024
  -- FPCPITOTLZGCHE:  CPI Annual Inflation — FRED stopped updating in Jan 2024
  and series_id not in ('LMUNRRTTCHM156N', 'CPALTT01CHM657N', 'FPCPITOTLZGCHE')
