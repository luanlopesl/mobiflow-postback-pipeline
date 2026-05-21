# Design Decisions

Notes on the choices behind Mobiflow.

## Flask only receives

The Flask app does the smallest useful job:

- parse JSON;
- save the raw payload;
- create a queue item;
- return `202`.

It does not reject events because `click_id` is missing, revenue is negative, or `event_type` is weird. Those are data quality decisions, not HTTP decisions.

That split matters. If the endpoint rejects too much, the event disappears before anyone can inspect it. Here, valid JSON is captured first and judged later.

## Raw data stays raw

`raw_postbacks` is the receipt log.

If a partner sends `{}`, I still want to know that `{}` arrived. Same for duplicated payloads, malformed business fields, future timestamps, or strange revenue values.

The processor reads from `raw_postbacks`; it does not rewrite it. That keeps replay and debugging honest.

## Postgres queue

The queue is a table: `event_queue`.

That keeps the whole local project easy to inspect:

```sql
select status, count(*)
from event_queue
group by status;
```

The processor claims work with:

```sql
FOR UPDATE SKIP LOCKED
```

So two processors can run at the same time without claiming the same row.

Mapping to SQS:

| SQS | Mobiflow |
|---|---|
| message | `event_queue` row |
| message body | `raw_postbacks.raw_payload` |
| receive | claim pending rows |
| visibility timeout | `processing` + `claimed_at` |
| delete/ack | mark `done` |
| DLQ | `dlq_events` |

For a real high-volume system, I would usually move the queue to SQS. For this project, Postgres is better because the queue behavior is visible and testable with SQL.

## Explicit SQL

The project uses `psycopg 3` and SQL directly.

The important parts here are SQL-shaped:

- transactions;
- row locks;
- `SKIP LOCKED`;
- `RETURNING`;
- `ON CONFLICT`;
- queue status updates;
- JSONB payload storage.

An ORM would add ceremony without helping much in this codebase.

## Processor owns data quality

The processor is where a raw postback becomes either:

- a trusted conversion;
- a duplicate delivery;
- a DLQ event;
- a technical retry.

Keeping that logic in one worker makes the lifecycle easier to reason about. Flask does not know business rules. dbt does not decide whether a raw event was processable. The processor owns that middle step.

## Idempotency

Current conversion key:

```sql
UNIQUE (click_id, event_type)
```

Processor insert:

```sql
ON CONFLICT (click_id, event_type) DO NOTHING
```

A duplicate delivery becomes:

```text
event_queue.status = done
no new row in conversions
```

That is intentional. Duplicate delivery is not the same thing as failed processing.

Important detail: `event_id` is not the idempotency key here. `event_id` is stored for audit/reconciliation when the partner sends it, but the simulator models duplicates as repeated delivery of the same `click_id + event_type`.

For production, I would not blindly keep this key for every event type. Purchases can repeat. Subscriptions can renew. Some partners have reliable `event_id`; others do not. The right key depends on the contract:

- installs/registers may work with `click_id + event_type`;
- purchases often need `transaction_id` or order ID;
- partner-level event dedup may use `partner_id + event_id`;
- some flows need event-specific rules.

The point is not that `click_id + event_type` is universal. The point is that the pipeline has an explicit dedup boundary.

## DLQ

Invalid events go to `dlq_events`.

Stored there:

- raw postback id;
- queue item id;
- error type;
- error message;
- payload snapshot;
- failure timestamp;
- replay state.

A failed event is not just a log line. It becomes queryable data.

## Replay

Replay creates a new `pending` queue item for an existing raw postback.

It does not:

- change `raw_postbacks`;
- directly insert a conversion;
- bypass validation.

That keeps replay boring in a good way: the same processor logic runs again.

Useful cases:

- validation rule fixed;
- partner contract clarified;
- processor bug fixed;
- temporary schema/DB issue resolved.

## Stale recovery

If a worker dies after claiming a row, the row can stay in `processing`.

The processor checks for old `processing` rows before claiming new work:

- attempts left: reset to `pending`;
- attempts exhausted: mark `failed` and write `processing_timeout` to DLQ.

This is the local version of a visibility timeout.

## dbt

The dbt layer starts from operational tables and builds reporting views.

Staging models clean names and formats:

- `stg_raw_postbacks`
- `stg_event_queue`
- `stg_conversions`
- `stg_dlq_events`

Marts answer the questions people actually ask:

- campaign performance;
- partner quality;
- queue health;
- DLQ errors;
- validated conversions.

The tests are basic on purpose: uniqueness, not-null, accepted values. Enough to show the contract without turning the project into a testing framework exercise.

## What I would change next

If this moved beyond local demo:

- SQS for the queue;
- S3 for raw event archive;
- Redshift or another warehouse for analytics;
- Lambda or container workers;
- worker id and heartbeat;
- structured logs and metrics;
- alerts on DLQ rate, pending age, stuck processing, retry exhaustion;
- partner-specific validation contracts;
- event-specific idempotency keys;
- CI for Python and dbt;
- governed replay flow.
