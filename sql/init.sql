-- sql/init.sql
-- Automatically run by Postgres on first boot.
-- To rerun from scratch: docker compose down -v && docker compose up

-- raw_postbacks
CREATE TABLE IF NOT EXISTS raw_postbacks (
    id             SERIAL PRIMARY KEY,
    event_id       TEXT,
    click_id       TEXT,
    app_id         TEXT,
    campaign_id    TEXT,
    partner_id     TEXT,
    publisher_id   TEXT,
    event_type     TEXT,
    event_revenue  NUMERIC(10,2),
    event_currency TEXT,
    device_id      TEXT,
    os             TEXT,
    country        TEXT,
    event_time     TIMESTAMPTZ,
    raw_payload    JSONB        NOT NULL,
    source_ip      TEXT,
    user_agent     TEXT,
    content_type   TEXT,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Keeps older local databases compatible if this file is run manually after
-- the table already exists. Docker only auto-runs init.sql on a fresh volume.
ALTER TABLE raw_postbacks ADD COLUMN IF NOT EXISTS event_id TEXT;
ALTER TABLE raw_postbacks ADD COLUMN IF NOT EXISTS source_ip TEXT;
ALTER TABLE raw_postbacks ADD COLUMN IF NOT EXISTS user_agent TEXT;
ALTER TABLE raw_postbacks ADD COLUMN IF NOT EXISTS content_type TEXT;

CREATE INDEX IF NOT EXISTS idx_raw_event_id   ON raw_postbacks (event_id);
CREATE INDEX IF NOT EXISTS idx_raw_click_id   ON raw_postbacks (click_id);
CREATE INDEX IF NOT EXISTS idx_raw_received_at ON raw_postbacks (received_at);


-- event_queue
CREATE TABLE IF NOT EXISTS event_queue (
    id               SERIAL PRIMARY KEY,
    raw_postback_id  INT         NOT NULL REFERENCES raw_postbacks(id),
    status           TEXT        NOT NULL DEFAULT 'pending',
    attempts         INT         NOT NULL DEFAULT 0,
    max_attempts     INT         NOT NULL DEFAULT 3,
    error_message    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at       TIMESTAMPTZ,
    processed_at     TIMESTAMPTZ
);

-- Partial index: only rows the processor will filter
CREATE INDEX IF NOT EXISTS idx_queue_pending
    ON event_queue (created_at)
    WHERE status = 'pending';


-- conversions
CREATE TABLE IF NOT EXISTS conversions (
    id              SERIAL PRIMARY KEY,
    click_id        TEXT         NOT NULL,
    event_type      TEXT         NOT NULL,
    app_id          TEXT,
    campaign_id     TEXT,
    partner_id      TEXT,
    publisher_id    TEXT,
    event_revenue   NUMERIC(10,2) NOT NULL DEFAULT 0,
    event_currency  TEXT          NOT NULL DEFAULT 'USD',
    country         TEXT,
    event_time      TIMESTAMPTZ,
    raw_postback_id INT,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_conversion UNIQUE (click_id, event_type)
);

CREATE INDEX IF NOT EXISTS idx_conv_campaign ON conversions (campaign_id);
CREATE INDEX IF NOT EXISTS idx_conv_time     ON conversions (event_time);
CREATE INDEX IF NOT EXISTS idx_conv_partner  ON conversions (partner_id);


-- dlq_events
CREATE TABLE IF NOT EXISTS dlq_events (
    id               SERIAL PRIMARY KEY,
    raw_postback_id  INT,
    queue_item_id    INT,
    error_type       TEXT,
    error_message    TEXT,
    payload_snapshot JSONB,
    failed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at      TIMESTAMPTZ,
    replay_status    TEXT
);

CREATE INDEX IF NOT EXISTS idx_dlq_error_type    ON dlq_events (error_type);
CREATE INDEX IF NOT EXISTS idx_dlq_replay_status ON dlq_events (replay_status)
    WHERE replay_status IS NULL OR replay_status = 'pending_replay';
