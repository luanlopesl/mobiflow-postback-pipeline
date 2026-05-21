#!/usr/bin/env python3
"""Requeue DLQ rows into event_queue for another processor pass."""

from __future__ import annotations

import argparse
import logging
import os
import sys

import psycopg
import psycopg.rows
from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_LIMIT = 500


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


def select_replayable(
    conn: psycopg.Connection,
    error_type: str | None,
    dlq_id: int | None,
    limit: int,
) -> list[dict]:
    filters = ["replay_status IS NULL"]
    params: dict[str, object] = {"limit": limit}

    if error_type is not None:
        filters.append("error_type = %(error_type)s")
        params["error_type"] = error_type

    if dlq_id is not None:
        filters.append("id = %(dlq_id)s")
        params["dlq_id"] = dlq_id

    where_clause = " AND ".join(filters)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                id,
                raw_postback_id,
                error_type,
                error_message,
                payload_snapshot,
                failed_at
            FROM  dlq_events
            WHERE {where_clause}
            ORDER BY failed_at
            LIMIT %(limit)s
            """,
            params,
        )
        return cur.fetchall()


def enqueue_item(conn: psycopg.Connection, dlq_item: dict) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO event_queue (raw_postback_id, status, attempts)
            VALUES (%s, 'pending', 0)
            RETURNING id
            """,
            (dlq_item["raw_postback_id"],),
        )
        return cur.fetchone()["id"]


def mark_replayed(conn: psycopg.Connection, dlq_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dlq_events
            SET    replay_status = 'replayed',
                   replayed_at   = NOW()
            WHERE  id = %s
            """,
            (dlq_id,),
        )


def run_replay(
    error_type: str | None,
    dlq_id: int | None,
    limit: int,
    dry_run: bool,
) -> int:
    conn = connect()
    enqueued = 0

    try:
        items = select_replayable(conn, error_type, dlq_id, limit)

        if not items:
            log.info("no replayable items found (replay_status IS NULL)")
            return 0

        log.info(
            "%s %d item(s)%s",
            "would replay" if dry_run else "replaying",
            len(items),
            f" with error_type='{error_type}'" if error_type else "",
        )

        for item in items:
            snapshot = item["payload_snapshot"]
            if isinstance(snapshot, dict):
                click_id = snapshot.get("click_id", "-")
                etype = snapshot.get("event_type", "-")
            else:
                click_id = "-"
                etype = "-"

            if dry_run:
                log.info(
                    "  [dry-run] dlq_id=%-4d  raw_id=%-4d  error=%-30s  "
                    "click=%s  event_type=%s",
                    item["id"],
                    item["raw_postback_id"],
                    item["error_type"] or "-",
                    click_id,
                    etype,
                )
                enqueued += 1
                continue

            try:
                queue_id = enqueue_item(conn, item)
                mark_replayed(conn, item["id"])
                conn.commit()

                log.info(
                    "  replayed  dlq_id=%-4d  ->  queue_id=%-4d  "
                    "error=%-30s  click=%s  event_type=%s",
                    item["id"],
                    queue_id,
                    item["error_type"] or "-",
                    click_id,
                    etype,
                )
                enqueued += 1

            except Exception as exc:  # noqa: BLE001
                log.error(
                    "  FAILED    dlq_id=%-4d  %s: %s",
                    item["id"], type(exc).__name__, exc,
                )
                conn.rollback()

    finally:
        conn.close()

    if dry_run:
        log.info("dry-run complete - %d item(s) would be re-enqueued", enqueued)
    else:
        log.info(
            "replay complete - %d item(s) re-enqueued; "
            "run processor to process them",
            enqueued,
        )

    return enqueued


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m processing.replay",
        description="Re-enqueue dlq_events into event_queue for reprocessing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m processing.replay --all --dry-run
  python -m processing.replay --all
  python -m processing.replay --error-type revenue_as_string
  python -m processing.replay --error-type invalid_event_type --limit 100
  python -m processing.replay --dlq-id 42
  python -m processing.processor --once
        """,
    )

    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--all", action="store_true",
        help="Replay all items where replay_status IS NULL",
    )
    target.add_argument(
        "--error-type", metavar="TYPE",
        help="Replay only items with this error_type (e.g. revenue_as_string)",
    )
    target.add_argument(
        "--dlq-id", type=positive_int, metavar="ID",
        help="Replay a single dlq_events row by id",
    )

    parser.add_argument(
        "--limit", type=positive_int, default=DEFAULT_LIMIT, metavar="N",
        help=f"Max items to replay in one run (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be replayed without writing anything",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    error_type = args.error_type if hasattr(args, "error_type") else None
    dlq_id = args.dlq_id if hasattr(args, "dlq_id") else None

    if args.all:
        error_type = None
        dlq_id = None

    count = run_replay(
        error_type=error_type,
        dlq_id=dlq_id,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    sys.exit(0 if count >= 0 else 1)


if __name__ == "__main__":
    main()
