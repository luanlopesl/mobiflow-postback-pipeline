select
    conversions.conversion_id,
    conversions.raw_postback_id,
    conversions.click_id,
    conversions.event_type,
    conversions.app_id,
    conversions.campaign_id,
    conversions.partner_id,
    conversions.publisher_id,
    conversions.country,
    conversions.event_time,
    coalesce(conversions.event_time, conversions.created_at)::date as conversion_date,
    conversions.event_revenue,
    conversions.event_currency,
    raw_postbacks.received_at as raw_received_at,
    conversions.created_at,
    case
        when raw_postbacks.received_at is not null
            then extract(epoch from (conversions.created_at - raw_postbacks.received_at)) / 3600.0
    end as hours_from_receive_to_conversion
from {{ ref('stg_conversions') }} as conversions
left join {{ ref('stg_raw_postbacks') }} as raw_postbacks
    on conversions.raw_postback_id = raw_postbacks.raw_postback_id
