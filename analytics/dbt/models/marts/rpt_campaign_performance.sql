select
    conversion_date,
    coalesce(campaign_id, 'unknown') as campaign_id,
    coalesce(app_id, 'unknown') as app_id,
    coalesce(country, 'unknown') as country,
    count(*) as conversion_count,
    count(distinct click_id) as unique_clicks,
    sum(event_revenue) as total_revenue,
    avg(event_revenue) as average_revenue,
    count(*) filter (where event_type = 'purchase') as purchase_count,
    max(created_at) as latest_conversion_at
from {{ ref('fct_conversions') }}
group by
    conversion_date,
    coalesce(campaign_id, 'unknown'),
    coalesce(app_id, 'unknown'),
    coalesce(country, 'unknown')
