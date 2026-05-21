#!/usr/bin/env python3
"""Process pending postbacks from the Postgres queue."""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg
import psycopg.rows
from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

VALID_EVENT_TYPES = frozenset(
    {"install", "register", "purchase", "subscription", "level_complete"}
)
VALID_OS = frozenset({"ios", "android"})


def env_int(primary: str, legacy: str, default: str) -> int:
    return int(os.getenv(primary, os.getenv(legacy, default)))


def env_float(primary: str, legacy: str, default: str) -> float:
    return float(os.getenv(primary, os.getenv(legacy, default)))


BATCH_SIZE = env_int("PROCESSOR_BATCH_SIZE", "BATCH_SIZE", "50")
SLEEP_SECONDS = env_float("PROCESSOR_SLEEP_SECONDS", "SLEEP_SECONDS", "2")
STALE_TIMEOUT_SECONDS = env_int("PROCESSOR_STALE_TIMEOUT_SECONDS", "STALE_TIMEOUT_SECONDS", "600")


@dataclass(frozen=True)
class ValidatedPostback:
    click_id: str
    event_type: str
    app_id: str | None
    campaign_id: str | None
    partner_id: str | None
    publisher_id: str | None
    event_revenue: float
    event_currency: str
    country: str | None
    event_time: str | None
    raw_postback_id: int


@dataclass
class RunStats:
    claimed: int = 0
    conversions_created: int = 0
    duplicates_ignored: int = 0
    validation_failed: int = 0
    processing_retried: int = 0
    processing_failed: int = 0
    stale_released: int = 0

    def log_summary(self) -> None:
        log.info(
            "run complete - "
            "claimed=%d  converted=%d  dupes=%d  "
            "invalid=%d  retried=%d  failed=%d  stale=%d",
            self.claimed,
            self.conversions_created,
            self.duplicates_ignored,
            self.validation_failed,
            self.processing_retried,
            self.processing_failed,
            self.stale_released,
        )


def build_conninfo() -> str:
    return make_conninfo(
        "",
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "mobiflow"),
        user=os.getenv("POSTGRES_USER", "mobiflow"),
        password=os.getenv("POSTGRES_PASSWORD", "mobiflow"),
    )


def connect() -> psycopg.Connection:
    return psycopg.connect(build_conninfo(), row_factory=psycopg.rows.dict_row)


def release_stale_processing(conn: psycopg.Connection, timeout_seconds: int) -> int:
    """Move timed-out processing rows back to pending, or fail them after max attempts."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, raw_postback_id, attempts, max_attempts
            FROM   event_queue
            WHERE  status     = 'processing'
              AND  claimed_at < NOW() - %s
            FOR UPDATE SKIP LOCKED
            """,
            (timedelta(seconds=timeout_seconds),),
        )
        stale = cur.fetchall()

        if not stale:
            conn.commit()
            return 0

        for item in stale:
            if item["attempts"] < item["max_attempts"]:
                cur.execute(
                    """
                    UPDATE event_queue
                    SET    status        = 'pending',
                           claimed_at    = NULL,
                           error_message = NULL
                    WHERE  id = %s
                    """,
                    (item["id"],),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO dlq_events
                        (raw_postback_id, queue_item_id, error_type, error_message)
                    VALUES (%s, %s, 'processing_timeout', 'Worker timed out; max attempts reached')
                    """,
                    (item["raw_postback_id"], item["id"]),
                )
                cur.execute(
                    """
                    UPDATE event_queue
                    SET    status        = 'failed',
                           processed_at  = NOW(),
                           error_message = 'processing_timeout: max attempts reached'
                    WHERE  id = %s
                    """,
                    (item["id"],),
                )

        conn.commit()
    return len(stale)


def claim_batch(conn: psycopg.Connection, batch_size: int) -> list[dict]:
    """Claim pending queue items without blocking other workers."""
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH claimed AS (
                SELECT id
                FROM   event_queue
                WHERE  status   = 'pending'
                  AND  attempts < max_attempts
                ORDER BY created_at
                LIMIT  %s
                FOR UPDATE SKIP LOCKED
            ),
            updated AS (
                UPDATE event_queue q
                SET    status     = 'processing',
                       claimed_at = NOW(),
                       attempts   = q.attempts + 1
                FROM   claimed
                WHERE  q.id = claimed.id
                RETURNING
                    q.id              AS queue_id,
                    q.raw_postback_id,
                    q.attempts,
                    q.max_attempts
            )
            SELECT
                u.queue_id,
                u.raw_postback_id,
                u.attempts,
                u.max_attempts,
                r.raw_payload
            FROM   updated u
            JOIN   raw_postbacks r ON r.id = u.raw_postback_id
            """,
            (batch_size,),
        )
        items = cur.fetchall()
        conn.commit()
    return items


def validate_payload(payload: object) -> tuple[bool, str, str]:
    """Return (is_valid, error_type, error_message)."""
    if not isinstance(payload, dict):
        return False, "invalid_payload_type", f"expected dict, got {type(payload).__name__}"

    if not payload:
        return False, "completely_empty", "payload has no fields"

    click_id = payload.get("click_id")
    if click_id is None:
        return False, "missing_click_id", "click_id is required"
    if click_id == "":
        return False, "empty_click_id", "click_id cannot be an empty string"
    if len(str(click_id)) < 4:
        return False, "click_id_too_short", f"click_id '{click_id}' is too short (min 4 chars)"

    event_type = payload.get("event_type")
    if event_type is None:
        return False, "missing_event_type", "event_type is required"
    if event_type not in VALID_EVENT_TYPES:
        return False, "invalid_event_type", (
            f"'{event_type}' is not a valid event type - "
            f"allowed: {sorted(VALID_EVENT_TYPES)}"
        )

    if not payload.get("app_id"):
        return False, "missing_app_id", "app_id is required"

    if not payload.get("campaign_id"):
        return False, "missing_campaign_id", "campaign_id is required"

    partner_id = payload.get("partner_id")
    if partner_id is None or str(partner_id).strip() == "":
        return False, "null_partner_id", "partner_id is required and cannot be null"

    revenue = payload.get("event_revenue", 0)
    if isinstance(revenue, bool) or not isinstance(revenue, (int, float)):
        return False, "invalid_revenue", (
            f"event_revenue must be a number, got {type(revenue).__name__}: {revenue!r}"
        )
    if revenue < 0:
        return False, "negative_revenue", f"event_revenue cannot be negative: {revenue}"

    event_time = payload.get("event_time")
    if event_time is not None:
        try:
            parsed = datetime.fromisoformat(str(event_time))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            if parsed > datetime.now(timezone.utc):
                return False, "event_time_in_future", (
                    f"event_time '{event_time}' is in the future"
                )
        except ValueError:
            return False, "invalid_event_time", (
                f"event_time '{event_time}' is not a valid ISO 8601 datetime"
            )

    currency = payload.get("event_currency")
    if currency and currency != "USD":
        return False, "invalid_currency", f"expected USD, got '{currency}'"

    os_val = payload.get("os")
    if os_val and os_val not in VALID_OS:
        return False, "invalid_os", f"'{os_val}' is not a valid OS - allowed: {sorted(VALID_OS)}"

    country = payload.get("country")
    if country and len(str(country)) != 2:
        return False, "invalid_country", (
            f"country '{country}' must be a 2-char ISO 3166-1 alpha-2 code"
        )

    return True, "", ""


def send_to_dlq(
    conn: psycopg.Connection,
    item: dict,
    error_type: str,
    error_message: str,
    payload: object,
) -> None:
    snapshot = Jsonb(payload)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dlq_events
                (raw_postback_id, queue_item_id, error_type, error_message, payload_snapshot)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (item["raw_postback_id"], item["queue_id"], error_type, error_message, snapshot),
        )


def mark_done(conn: psycopg.Connection, queue_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE event_queue
            SET    status        = 'done',
                   processed_at  = NOW(),
                   error_message = NULL
            WHERE  id = %s
            """,
            (queue_id,),
        )


def mark_failed(conn: psycopg.Connection, queue_id: int, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE event_queue
            SET    status        = 'failed',
                   processed_at  = NOW(),
                   error_message = %s
            WHERE  id = %s
            """,
            (error_message, queue_id),
        )


def insert_conversion(
    conn: psycopg.Connection,
    item: dict,
    postback: ValidatedPostback,
) -> bool:
    """Insert a conversion. Return False when the idempotency key already exists."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO conversions (
                click_id, event_type, app_id, campaign_id, partner_id,
                publisher_id, event_revenue, event_currency, country,
                event_time, raw_postback_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (click_id, event_type) DO NOTHING
            RETURNING id
            """,
            (
                postback.click_id,
                postback.event_type,
                postback.app_id,
                postback.campaign_id,
                postback.partner_id,
                postback.publisher_id,
                postback.event_revenue,
                postback.event_currency,
                postback.country,
                postback.event_time,
                postback.raw_postback_id,
            ),
        )
        return cur.fetchone() is not None


def retry_or_fail_processing_error(
    conn: psycopg.Connection,
    item: dict,
    exc: Exception,
    payload: object,
) -> bool:
    """Retry technical errors until max_attempts is reached."""
    error_message = f"{type(exc).__name__}: {exc}"
    with conn.cursor() as cur:
        if item["attempts"] < item["max_attempts"]:
            cur.execute(
                """
                UPDATE event_queue
                SET    status        = 'pending',
                       claimed_at    = NULL,
                       error_message = %s
                WHERE  id = %s
                """,
                (error_message, item["queue_id"]),
            )
            return True
        else:
            send_to_dlq(conn, item, "processing_error", error_message, payload)
            mark_failed(conn, item["queue_id"], error_message)
            return False


def process_item(conn: psycopg.Connection, item: dict, stats: RunStats) -> None:
    """Process one claimed queue item in its own transaction."""
    payload = item.get("raw_payload")
    if payload is None:
        payload = {}
    queue_id = item["queue_id"]
    try:
        is_valid, error_type, error_message = validate_payload(payload)

        if not is_valid:
            send_to_dlq(conn, item, error_type, error_message, payload)
            mark_failed(conn, queue_id, f"{error_type}: {error_message}")
            conn.commit()
            stats.validation_failed += 1
            log.debug("dlq    queue_id=%-4d  %s", queue_id, error_type)
            return

        postback = ValidatedPostback(
            click_id=str(payload["click_id"]),
            event_type=str(payload["event_type"]),
            app_id=payload.get("app_id"),
            campaign_id=payload.get("campaign_id"),
            partner_id=payload.get("partner_id"),
            publisher_id=payload.get("publisher_id"),
            event_revenue=float(payload.get("event_revenue") or 0),
            event_currency=str(payload.get("event_currency") or "USD"),
            country=payload.get("country"),
            event_time=payload.get("event_time"),
            raw_postback_id=item["raw_postback_id"],
        )

        created = insert_conversion(conn, item, postback)
        mark_done(conn, queue_id)
        conn.commit()

        if created:
            stats.conversions_created += 1
            log.debug(
                "conv   queue_id=%-4d  click=%s  type=%s",
                queue_id, postback.click_id, postback.event_type,
            )
        else:
            stats.duplicates_ignored += 1
            log.debug(
                "dupe   queue_id=%-4d  click=%s  type=%s",
                queue_id, postback.click_id, postback.event_type,
            )

    except Exception as exc:  # noqa: BLE001
        log.warning("error  queue_id=%-4d  %s: %s", queue_id, type(exc).__name__, exc)
        try:
            conn.rollback()
            will_retry = retry_or_fail_processing_error(conn, item, exc, payload)
            conn.commit()
            if will_retry:
                stats.processing_retried += 1
            else:
                stats.processing_failed += 1
        except Exception:
            log.exception("failed to handle error for queue_id=%d", queue_id)
            conn.rollback()


def run_once(
    batch_size: int = BATCH_SIZE,
    timeout_seconds: int = STALE_TIMEOUT_SECONDS,
) -> RunStats:
    stats = RunStats()
    conn = connect()

    try:
        stale = release_stale_processing(conn, timeout_seconds)
        if stale:
            log.info("stale  released %d items back to pending (or DLQ)", stale)
        stats.stale_released = stale

        items = claim_batch(conn, batch_size)
        stats.claimed = len(items)

        if not items:
            log.info("queue empty - nothing to process")
            return stats

        log.info("claimed %d items", len(items))

        for item in items:
            process_item(conn, item, stats)

    finally:
        conn.close()

    stats.log_summary()
    return stats


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be 0 or greater")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m processing.processor",
        description="Process pending postbacks from the Postgres queue.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m processing.processor --once
  python -m processing.processor --once --batch-size 100
  python -m processing.processor --watch --sleep-seconds 2
        """,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="Process one batch and exit")
    mode.add_argument("--watch", action="store_true", help="Poll continuously until Ctrl-C")

    parser.add_argument(
        "--batch-size", type=positive_int, default=BATCH_SIZE, metavar="N",
        help=f"Items per batch (default: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--sleep-seconds", type=non_negative_float, default=SLEEP_SECONDS, metavar="S",
        help=f"Seconds between polls in --watch mode (default: {SLEEP_SECONDS})",
    )
    parser.add_argument(
        "--timeout", type=positive_int, default=STALE_TIMEOUT_SECONDS, metavar="S",
        help=f"Stale processing timeout in seconds (default: {STALE_TIMEOUT_SECONDS})",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    if args.once:
        run_once(batch_size=args.batch_size, timeout_seconds=args.timeout)
        return

    log.info("watching queue - batch=%d  sleep=%.1fs", args.batch_size, args.sleep_seconds)
    try:
        while True:
            run_once(batch_size=args.batch_size, timeout_seconds=args.timeout)
            time.sleep(args.sleep_seconds)
    except KeyboardInterrupt:
        log.info("stopped by user")


if __name__ == "__main__":
    main()
