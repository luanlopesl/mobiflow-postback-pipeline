select
    id as queue_item_id,
    raw_postback_id,
    lower(nullif(trim(status), '')) as status,
    attempts,
    max_attempts,
    error_message,
    created_at,
    claimed_at,
    processed_at,
    case
        when claimed_at is not null and processed_at is not null
            then extract(epoch from (processed_at - claimed_at))
    end as processing_seconds
from {{ source('mobiflow', 'event_queue') }}
