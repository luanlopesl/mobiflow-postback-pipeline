select
    id as dlq_event_id,
    raw_postback_id,
    queue_item_id,
    nullif(trim(error_type), '') as error_type,
    nullif(trim(error_message), '') as error_message,
    payload_snapshot,
    failed_at,
    replayed_at,
    nullif(trim(replay_status), '') as replay_status,
    replayed_at is not null or replay_status = 'replayed' as was_replayed
from {{ source('mobiflow', 'dlq_events') }}
