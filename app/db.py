"""SQLite access helpers.

WAL mode allows concurrent readers with a single writer across processes;
busy_timeout makes the loser of a write race wait instead of failing.
Connections run in autocommit mode (isolation_level=None); every state
mutation is a single statement, hence atomic.
"""
from __future__ import annotations

import sqlite3
import time

from .ratelimit import RATE_LIMIT_SCHEMA, set_wal_with_retry

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id            TEXT PRIMARY KEY,
    filename      TEXT NOT NULL,
    size          INTEGER NOT NULL,
    sha256        TEXT NOT NULL,
    chunk_size    INTEGER NOT NULL,
    total_chunks  INTEGER NOT NULL,
    state         TEXT NOT NULL DEFAULT 'uploading'
                  CHECK (state IN ('uploading', 'published', 'failed')),
    created_at    TEXT NOT NULL,
    published_at  TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    task_id     TEXT NOT NULL REFERENCES tasks(id),
    chunk_index INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL,
    PRIMARY KEY (task_id, chunk_index)
);
"""

SCHEMA = SCHEMA + RATE_LIMIT_SCHEMA


def connect(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path), timeout=30.0, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    # Switching journal_mode needs a brief exclusive lock.  Several workers
    # may start at the same instant (e.g. "docker compose up" with
    # --workers 2), so retry instead of failing startup.  busy_timeout is set
    # first so these PRAGMAs themselves wait for a concurrent starter.
    conn.execute("PRAGMA busy_timeout=30000")
    set_wal_with_retry(conn)
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path) -> None:
    conn = connect(db_path)
    try:
        for attempt in range(50):
            try:
                conn.executescript(SCHEMA)
                return
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "locked" not in message and "busy" not in message:
                    raise
                if attempt == 49:
                    raise
                time.sleep(0.1)
    finally:
        conn.close()
