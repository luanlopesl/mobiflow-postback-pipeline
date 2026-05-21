# Runbook

Commands I use to run, test, and debug Mobiflow locally.

## Start

```sh
cp .env.example .env
docker compose up --build -d
docker compose ps
curl http://localhost:5000/health
```

Expected health shape:

```json
{"database":"ok","service":"mobiflow-ingestion-api","status":"ok"}
```

## Reset

Clean database:

```sh
docker compose down -v
docker compose up --build -d
```

`-v` removes the Postgres volume. Use it when you want a clean demo run.

## Python setup

The API runs in Docker. The simulator and worker usually run from the host.

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Send postbacks

Small run:

```sh
python simulator/generate_postbacks.py --total 1000 --seed 42 --delay 0
```

Larger run:

```sh
python simulator/generate_postbacks.py --total 5000 --seed 2026 --delay 0
```

No seed:

```sh
python simulator/generate_postbacks.py --total 1000 --delay 0
```

The simulator sends valid events, duplicate deliveries, and invalid payloads.

## Process queue

One batch:

```sh
python -m processing.processor --once --batch-size 500
```

Keep polling:

```sh
python -m processing.processor --watch --batch-size 100 --sleep-seconds 2
```

Stop with `Ctrl+C`.

If watch mode says `queue empty`, it is still running. That is expected.

## Replay DLQ

Preview first:

```sh
python -m processing.replay --all --limit 10 --dry-run
```

By error type:

```sh
python -m processing.replay --error-type invalid_event_type --limit 5 --dry-run
```

One DLQ row:

```sh
python -m processing.replay --dlq-id 1
```

Then process whatever replay created:

```sh
python -m processing.processor --once --batch-size 100
```

If the payload is still invalid, it should go back to DLQ. That is fine.

## dbt

```sh
docker compose run --rm dbt debug
docker compose run --rm dbt run
docker compose run --rm dbt test
```

Generated dbt files are ignored:

```text
analytics/dbt/target/
analytics/dbt/logs/
analytics/dbt/dbt_packages/
```

## Postgres

Open psql:

```sh
docker exec -it mobiflow_postgres psql -U mobiflow -d mobiflow
```

Queue status:

```sql
select status, count(*)
from event_queue
group by status
order by status;
```

Conversions:

```sql
select count(*) as conversions,
       round(sum(event_revenue)::numeric, 2) as revenue
from conversions;
```

DLQ:

```sql
select error_type, count(*)
from dlq_events
group by error_type
order by count(*) desc;
```

Stuck processing:

```sql
select count(*)
from event_queue
where status = 'processing';
```

Duplicate safety:

```sql
select count(*) as duplicate_conversion_keys
from (
    select click_id, event_type, count(*)
    from conversions
    group by click_id, event_type
    having count(*) > 1
) d;
```

Clean drain check:

```sql
select count(*) as pending_or_processing
from event_queue
where status in ('pending', 'processing');
```

## Manual API checks

Valid payload:

```sh
curl -i -X POST http://localhost:5000/postback \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt_runbook_001",
    "click_id": "clk_runbook_001",
    "partner_id": "appsflyer",
    "app_id": "com.demo.wordpuzzle",
    "campaign_id": "camp_ios_us_cpi_burst_001",
    "publisher_id": "pub_ironSource_001",
    "event_type": "install",
    "event_revenue": 0,
    "event_currency": "USD",
    "country": "US",
    "os": "ios",
    "event_time": "2026-05-18T12:00:00+00:00"
  }'
```

Malformed JSON:

```sh
curl -i -X POST http://localhost:5000/postback \
  -H "Content-Type: application/json" \
  -d 'not-json'
```

Expected: `400`.

Business-invalid JSON:

```sh
curl -i -X POST http://localhost:5000/postback \
  -H "Content-Type: application/json" \
  -d '{"click_id": "", "event_revenue": -10}'
```

Expected: Flask returns `202`; processor sends it to DLQ.

## Happy path

```sh
curl http://localhost:5000/health
python simulator/generate_postbacks.py --total 1000 --seed 42 --delay 0
python -m processing.processor --once --batch-size 500
python -m processing.processor --once --batch-size 500
docker compose run --rm dbt run
docker compose run --rm dbt test
```

Then:

```sql
select status, count(*) from event_queue group by status order by status;
select count(*) from conversions;
select error_type, count(*) from dlq_events group by error_type order by count(*) desc;
```

## Duplicate check

Run simulator and processor:

```sh
python simulator/generate_postbacks.py --total 1000 --seed 42 --delay 0
python -m processing.processor --once --batch-size 1000
```

Check:

```sql
select count(*) as duplicate_conversion_keys
from (
    select click_id, event_type, count(*)
    from conversions
    group by click_id, event_type
    having count(*) > 1
) d;
```

Expected: `0`.

The processor may still log `dupes`. That means duplicate deliveries were seen and ignored correctly.

## Stale recovery

Normal recovery command:

```sh
python -m processing.processor --once --batch-size 500 --timeout 60
```

What it does:

- old `processing` rows with attempts left go back to `pending`;
- exhausted rows become `failed` and get a `processing_timeout` DLQ row.

I would explain this in a demo instead of manually editing queue state.

## Symptoms

| Symptom | Check |
|---|---|
| API returns `500` | `/health`, Flask logs, Postgres container |
| Queue stays `pending` | processor is not running |
| Queue stuck in `processing` | worker was interrupted; run processor with stale timeout |
| Many DLQ rows | `select error_type, count(*) from dlq_events group by error_type` |
| Duplicate conversion keys > 0 | unique constraint or insert conflict handling is wrong |
| dbt cannot connect | `docker compose ps`, `docker compose run --rm dbt debug` |

## Demo checklist

- [ ] `.env` exists
- [ ] `docker compose ps` looks healthy
- [ ] `/health` returns `database: ok`
- [ ] simulator returns `202`
- [ ] processor logs converted / dupes / invalid
- [ ] no `pending` or `processing` rows after draining
- [ ] DLQ has classified errors
- [ ] replay dry-run works
- [ ] dbt run/test works
- [ ] I can explain why `event_id` is not the idempotency key
