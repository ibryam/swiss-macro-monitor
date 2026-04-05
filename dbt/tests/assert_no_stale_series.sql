-- No series should have gone more than 18 months without new data.
-- 18 months is generous enough to accommodate quarterly series with normal
-- publication lag (typically 3-6 months), but will catch a source that has
-- silently stopped publishing (broken API, retired series, auth failure).

select
    series_id,
    indicator_name,
    max(date) as latest_date
from {{ ref('mart_macro__time_series') }}
where is_latest = true
  and date < date_sub(current_date(), interval 18 month)
group by series_id, indicator_name
