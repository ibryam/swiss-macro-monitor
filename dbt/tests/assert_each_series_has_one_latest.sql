-- Each series must have exactly one is_latest = TRUE row.
-- More than one means duplicate dates slipped through and Tableau KPI tiles
-- will show the wrong value (or both values stacked).
-- Zero means the series vanished from the mart entirely.

select
    series_id,
    countif(is_latest = true) as latest_count
from {{ ref('mart_macro__time_series') }}
group by series_id
having latest_count != 1
