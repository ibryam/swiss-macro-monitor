-- Every series that powers a dashboard KPI tile must have an is_latest = TRUE row.
-- If a source changes its API, series code, or stops publishing, this test fails
-- before the dashboard silently shows a blank tile.

with expected as (

    select series_id from unnest([
        -- SNB
        'snbgwdzid/ZIG',
        'snbgwdzid/SARON',
        'snbgwdzid/ZIGBL',
        -- SNB FX
        'devkum/EUR1',
        'devkum/USD1',
        -- FRED
        'CLVMNACSAB1GQCH',
        'IRLTLT01CHM156N',
        -- OECD
        'KEI/CHE/B1GQ_Q/_T/GY',
        'KEI/CHE/PRVM/C/GY',
        'KEI/CHE/TOVM/G47/GY',
        'KEI/CHE/EX/_T/G1',
        'KEI/CHE/EMP/_T/_Z',
        'KEI/CHE/UNEMP/_T/_Z',
        'KEI/CHE/IRSTCI/_Z/_Z',
        'KEI/CHE/CP/_Z/GY'
    ]) as series_id

),

present as (

    select distinct series_id
    from {{ ref('mart_macro__time_series') }}
    where is_latest = true

)

-- Returns any expected series that is missing — test fails if any rows returned
select e.series_id
from expected e
left join present p using (series_id)
where p.series_id is null
