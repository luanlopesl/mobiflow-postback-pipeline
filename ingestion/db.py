from __future__ import annotations

import atexit
import os
from contextlib import contextmanager
from typing import Iterator

import psycopg
from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool


load_dotenv()

_pool: ConnectionPool | None = None


def build_conninfo() -> str:
    return make_conninfo(
        "",
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "mobiflow"),
        user=os.getenv("POSTGRES_USER", "mobiflow"),
        password=os.getenv("POSTGRES_PASSWORD", "mobiflow"),
    )


def get_pool() -> ConnectionPool:
    global _pool

    if _pool is None or _pool.closed:
        _pool = ConnectionPool(
            conninfo=build_conninfo(),
            min_size=int(os.getenv("POSTGRES_POOL_MIN_SIZE", "1")),
            max_size=int(os.getenv("POSTGRES_POOL_MAX_SIZE", "10")),
            timeout=float(os.getenv("POSTGRES_POOL_TIMEOUT", "5")),
            open=False,
        )
        _pool.open(wait=False)

    return _pool


@contextmanager
def get_connection() -> Iterator[psycopg.Connection]:
    with get_pool().connection() as conn:
        yield conn


def close_pool() -> None:
    global _pool

    if _pool is not None and not _pool.closed:
        _pool.close()
    _pool = None


atexit.register(close_pool)
