select
    status,
    count(*) as queue_item_count,
    sum(attempts) as total_attempts,
    avg(attempts) as average_attempts,
    count(*) filter (
        where attempts >= max_attempts
          and status <> 'done'
    ) as exhausted_item_count,
    min(created_at) as oldest_created_at,
    max(processed_at) as latest_processed_at,
    round(avg(processing_seconds)::numeric, 2) as average_processing_seconds
from {{ ref('stg_event_queue') }}
group by status
