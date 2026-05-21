select
    id as conversion_id,
    nullif(trim(click_id), '') as click_id,
    lower(nullif(trim(event_type), '')) as event_type,
    nullif(trim(app_id), '') as app_id,
    nullif(trim(campaign_id), '') as campaign_id,
    nullif(trim(partner_id), '') as partner_id,
    nullif(trim(publisher_id), '') as publisher_id,
    event_revenue,
    upper(nullif(trim(event_currency), '')) as event_currency,
    upper(nullif(trim(country), '')) as country,
    event_time,
    raw_postback_id,
    created_at
from {{ source('mobiflow', 'conversions') }}
