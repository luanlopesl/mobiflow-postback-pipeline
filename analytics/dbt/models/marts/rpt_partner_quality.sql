with raw_postbacks as (
    select
        raw_postback_id,
        coalesce(partner_id, 'unknown') as partner_id,
        received_at
    from {{ ref('stg_raw_postbacks') }}
),

converted_raw_postbacks as (
    select distinct raw_postback_id
    from {{ ref('stg_conversions') }}
    where raw_postback_id is not null
),

dlq_raw_postbacks as (
    select distinct raw_postback_id
    from {{ ref('stg_dlq_events') }}
    where raw_postback_id is not null
)

select
    raw_postbacks.partner_id,
    count(*) as raw_postback_count,
    count(converted_raw_postbacks.raw_postback_id) as converted_raw_postback_count,
    count(dlq_raw_postbacks.raw_postback_id) as dlq_raw_postback_count,
    round(count(converted_raw_postbacks.raw_postback_id)::numeric / nullif(count(*), 0), 4) as conversion_rate,
    round(count(dlq_raw_postbacks.raw_postback_id)::numeric / nullif(count(*), 0), 4) as dlq_rate,
    max(raw_postbacks.received_at) as latest_postback_at
from raw_postbacks
left join converted_raw_postbacks
    on raw_postbacks.raw_postback_id = converted_raw_postbacks.raw_postback_id
left join dlq_raw_postbacks
    on raw_postbacks.raw_postback_id = dlq_raw_postbacks.raw_postback_id
group by raw_postbacks.partner_id
