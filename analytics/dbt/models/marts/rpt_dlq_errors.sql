select
    coalesce(error_type, 'unknown') as error_type,
    coalesce(replay_status, 'not_replayed') as replay_status,
    count(*) as error_count,
    count(distinct raw_postback_id) as affected_raw_postbacks,
    count(*) filter (where was_replayed) as replayed_count,
    min(failed_at) as first_failed_at,
    max(failed_at) as latest_failed_at
from {{ ref('stg_dlq_events') }}
group by
    coalesce(error_type, 'unknown'),
    coalesce(replay_status, 'not_replayed')
