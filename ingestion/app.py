from __future__ import annotations

import logging
import os

from dotenv import load_dotenv
from flask import Flask, request
from psycopg.types.json import Jsonb

try:
    from ingestion.db import get_connection
except ImportError:  # pragma: no cover - supports `python ingestion/app.py`
    from db import get_connection


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.get("/health")
def get_health():
    database_status = "OK"
    status_code = 200

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except Exception:  # noqa: BLE001
        logger.exception("Database health check failed")
        database_status = "error"
        status_code = 503

    return {
        "status": "OK" if database_status == "OK" else "degraded",
        "service": "mobiflow-ingestion-api",
        "database": database_status,
    }, status_code


@app.post("/postback")
def receive_postback():
    payload = request.get_json(silent=True)

    if payload is None:
        return {"error": "invalid_json"}, 400

    payload_fields = payload if isinstance(payload, dict) else {}

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw_postbacks (
                        event_id,
                        click_id,
                        app_id,
                        campaign_id,
                        partner_id,
                        publisher_id,
                        event_type,
                        raw_payload,
                        source_ip,
                        user_agent,
                        content_type,
                        received_at
                    )
                    VALUES (
                        %(event_id)s,
                        %(click_id)s,
                        %(app_id)s,
                        %(campaign_id)s,
                        %(partner_id)s,
                        %(publisher_id)s,
                        %(event_type)s,
                        %(raw_payload)s,
                        %(source_ip)s,
                        %(user_agent)s,
                        %(content_type)s,
                        NOW()
                    )
                    RETURNING id
                    """,
                    {
                        "event_id": payload_fields.get("event_id"),
                        "click_id": payload_fields.get("click_id"),
                        "app_id": payload_fields.get("app_id"),
                        "campaign_id": payload_fields.get("campaign_id"),
                        "partner_id": payload_fields.get("partner_id"),
                        "publisher_id": payload_fields.get("publisher_id"),
                        "event_type": payload_fields.get("event_type"),
                        "raw_payload": Jsonb(payload),
                        "source_ip": request.remote_addr,
                        "user_agent": request.headers.get("User-Agent"),
                        "content_type": request.content_type,
                    },
                )
                raw_postback_id = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO event_queue (
                        raw_postback_id,
                        status,
                        attempts
                    )
                    VALUES (
                        %(raw_postback_id)s,
                        'pending',
                        0
                    )
                    RETURNING id
                    """,
                    {"raw_postback_id": raw_postback_id},
                )
                queue_id = cur.fetchone()[0]
    except Exception:  # noqa: BLE001
        logger.exception("Failed to ingest postback")
        return {"error": "ingestion_failed"}, 500

    return {
        "status": "accepted",
        "raw_postback_id": raw_postback_id,
        "queue_id": queue_id,
    }, 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("FLASK_PORT", "5000")))
