"""Cross-process download rate limiting.

All uvicorn workers of one service share a single token bucket that lives in
the service's SQLite database, so the configured byte-per-second budget and
burst allowance apply to the *service*, not to each worker.

The bucket is refilled lazily from wall-clock time every time a sender asks
for a lease.  The settings row is read in the same short transaction, which
means a settings change made from the web UI is picked up by the next lease
of every in-flight download (leases are at most a few tens of milliseconds
worth of bytes), without restarting any process.

Nothing here is on the critical consistency path of uploads: losing a few
token updates after a crash only resets the budget to "at most a burst", it
can never publish a partial package.  Settings writes, on the other hand, use
a synchronous=FULL connection so an acknowledged settings change survives a
service restart.
"""
from __future__ import annotations

import asyncio
import math
import sqlite3
import threading
import time
DEFAULT_BURST_BYTES = 1024 * 1024  # 1 MiB
MAX_RATE_BPS = 10**12
MAX_BURST_BYTES = 10**12

# Ask for at least this much worth of time with each lease.  Keeps the number
# of SQLite transactions bounded at very small rates while adding only a few
# tens of milliseconds of latency.
MIN_BATCH_SECONDS = 0.05
# Never sleep longer than this between polls, so a settings change (or a
# disconnect) is observed quickly.
MAX_POLL_SLEEP = 0.25
# Retry delay when SQLite reports transient lock contention.
LOCK_RETRY_SLEEP = 0.05

RATE_LIMIT_SCHEMA = """
CREATE TABLE IF NOT EXISTS download_rate (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    rate_bps     INTEGER NOT NULL,
    burst_bytes  INTEGER NOT NULL,
    tokens       REAL    NOT NULL,
    last_refill  REAL    NOT NULL
);
"""


def validate_settings(rate_bps: int, burst_bytes: int) -> tuple[int, int]:
    """Validate and normalise a settings pair.

    `rate_bps == 0` means "unlimited"; a burst is still kept because it is
    used as the bucket capacity once a rate is enabled.
    """
    if isinstance(rate_bps, bool) or not isinstance(rate_bps, int):
        raise ValueError("rate_bps must be an integer")
    if isinstance(burst_bytes, bool) or not isinstance(burst_bytes, int):
        raise ValueError("burst_bytes must be an integer")
    if not 0 <= rate_bps <= MAX_RATE_BPS:
        raise ValueError(f"rate_bps must be between 0 and {MAX_RATE_BPS}")
    if not 1 <= burst_bytes <= MAX_BURST_BYTES:
        raise ValueError(f"burst_bytes must be between 1 and {MAX_BURST_BYTES}")
    return rate_bps, burst_bytes


class RateSettings:
    __slots__ = ("rate_bps", "burst_bytes")

    def __init__(self, rate_bps: int, burst_bytes: int) -> None:
        self.rate_bps = int(rate_bps)
        self.burst_bytes = int(burst_bytes)

    @property
    def unlimited(self) -> bool:
        return self.rate_bps <= 0

    def as_dict(self) -> dict:
        return {
            "rate_bps": self.rate_bps,
            "burst_bytes": self.burst_bytes,
            "unlimited": self.unlimited,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"RateSettings(rate_bps={self.rate_bps}, burst_bytes={self.burst_bytes})"


def set_wal_with_retry(conn: sqlite3.Connection, attempts: int = 50) -> None:
    """Switch *conn* to WAL, tolerating a concurrent starter.

    ``PRAGMA journal_mode=WAL`` briefly needs an exclusive lock.  When several
    service processes start at the same moment (docker compose with
    ``--workers 2``), SQLite may report "database is locked" even though
    busy_timeout is set; retry instead of failing startup.
    """
    for attempt in range(attempts):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise
            if attempt == attempts - 1:
                raise
            time.sleep(0.1)


def _connect(db_path, synchronous: str, timeout: float) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path), timeout=timeout, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    set_wal_with_retry(conn)
    conn.execute(f"PRAGMA synchronous={synchronous}")
    return conn


class DownloadRateLimiter:
    """Token bucket shared by every process that opens the same database."""

    def __init__(self, db_path, clock=time.time) -> None:
        self.db_path = str(db_path)
        self._clock = clock
        self._lock = threading.Lock()
        self._conn = _connect(self.db_path, "NORMAL", 10.0)
        self._closed = False
        self._ensure_row()

    # ----------------------------------------------------------- lifecycle
    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                self._conn.close()

    def _ensure_row(self) -> None:
        now = self._clock()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT OR IGNORE INTO download_rate"
                    " (id, rate_bps, burst_bytes, tokens, last_refill)"
                    " VALUES (1, 0, ?, ?, ?)",
                    (DEFAULT_BURST_BYTES, float(DEFAULT_BURST_BYTES), now),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------ settings
    def get_settings(self) -> RateSettings:
        with self._lock:
            row = self._conn.execute(
                "SELECT rate_bps, burst_bytes FROM download_rate WHERE id = 1"
            ).fetchone()
        if row is None:
            return RateSettings(0, DEFAULT_BURST_BYTES)
        return RateSettings(row["rate_bps"], row["burst_bytes"])

    def set_settings(self, rate_bps: int, burst_bytes: int) -> RateSettings:
        """Persist new settings and make them effective immediately.

        Uses its own synchronous=FULL connection so the change is durable
        across a process crash/restart.  Refills the bucket at the *old* rate
        up to now, then clamps it to the new burst; enabling a limit from
        "unlimited" starts with a full burst.
        """
        rate_bps, burst_bytes = validate_settings(rate_bps, burst_bytes)
        now = self._clock()
        conn = _connect(self.db_path, "FULL", 30.0)
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT rate_bps, burst_bytes, tokens, last_refill"
                    " FROM download_rate WHERE id = 1"
                ).fetchone()
                if row is None:
                    old_rate, tokens = 0, float(burst_bytes)
                    conn.execute(
                        "INSERT INTO download_rate"
                        " (id, rate_bps, burst_bytes, tokens, last_refill)"
                        " VALUES (1, ?, ?, ?, ?)",
                        (rate_bps, burst_bytes, tokens, now),
                    )
                else:
                    old_rate = int(row["rate_bps"])
                    old_burst = max(1, int(row["burst_bytes"]))
                    tokens = float(row["tokens"])
                    elapsed = max(0.0, now - float(row["last_refill"]))
                    if old_rate > 0:
                        tokens = min(float(old_burst), tokens + elapsed * old_rate)
                    else:
                        tokens = float(old_burst)
                    if rate_bps > 0 and old_rate <= 0:
                        tokens = float(burst_bytes)  # fresh bucket when enabling
                    else:
                        tokens = min(tokens, float(burst_bytes))
                    conn.execute(
                        "UPDATE download_rate SET rate_bps = ?, burst_bytes = ?,"
                        " tokens = ?, last_refill = ? WHERE id = 1",
                        (rate_bps, burst_bytes, tokens, now),
                    )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return RateSettings(rate_bps, burst_bytes)

    # -------------------------------------------------------------- tokens
    def refund(self, nbytes: int) -> None:
        """Give back bytes that were leased but could not be delivered.

        Used when a connection disappears (or a write fails) after a lease
        was granted: without this, one abandoned 64 KiB lease could hold back
        other downloads for a long time at a low configured rate.  Refunds
        never exceed the burst, so the budget invariant still holds.
        """
        nbytes = int(nbytes)
        if nbytes <= 0:
            return
        now = self._clock()
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT rate_bps, burst_bytes, tokens, last_refill"
                    " FROM download_rate WHERE id = 1"
                ).fetchone()
                if row is None:
                    conn.execute("COMMIT")
                    return
                burst = max(1, int(row["burst_bytes"]))
                tokens = min(float(burst), float(row["tokens"]) + nbytes)
                conn.execute(
                    "UPDATE download_rate SET tokens = ?, last_refill = ?"
                    " WHERE id = 1",
                    (tokens, now),
                )
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    def try_acquire(self, max_bytes: int) -> tuple[int, float]:
        """Try to take up to *max_bytes* from the bucket.

        Returns ``(granted, wait_seconds)``.  Exactly one of them is
        non-zero: either bytes were deducted and may be sent immediately, or
        the caller should sleep *wait_seconds* and ask again.  The bucket
        state and the current settings are read in one serialized
        transaction so concurrent workers cannot both spend the same tokens.
        """
        max_bytes = int(max_bytes)
        if max_bytes <= 0:
            return 0, 0.0
        now = self._clock()
        with self._lock:
            conn = self._conn
            # Fast path: unlimited means no accounting at all.  A bare SELECT
            # is atomic in SQLite and takes no write lock, so an unthrottled
            # transfer costs one read of one row per chunk and nothing else.
            row = conn.execute(
                "SELECT rate_bps, burst_bytes, tokens, last_refill"
                " FROM download_rate WHERE id = 1"
            ).fetchone()
            if row is not None and int(row["rate_bps"]) <= 0:
                return max_bytes, 0.0

            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT rate_bps, burst_bytes, tokens, last_refill"
                    " FROM download_rate WHERE id = 1"
                ).fetchone()
                if row is None:
                    rate, burst = 0, DEFAULT_BURST_BYTES
                    tokens = float(burst)
                    conn.execute(
                        "INSERT INTO download_rate"
                        " (id, rate_bps, burst_bytes, tokens, last_refill)"
                        " VALUES (1, ?, ?, ?, ?)",
                        (rate, burst, tokens, now),
                    )
                else:
                    rate = max(0, int(row["rate_bps"]))
                    burst = max(1, int(row["burst_bytes"]))
                    tokens = float(row["tokens"])
                    elapsed = now - float(row["last_refill"])
                    if elapsed > 0:
                        tokens = min(float(burst), tokens + elapsed * rate)
                    else:
                        tokens = min(tokens, float(burst))

                if rate <= 0:  # became unlimited since the fast-path read
                    conn.execute("COMMIT")
                    return max_bytes, 0.0

                # Wait until a short slice of budget has accrued before
                # handing out bytes: at very low rates this keeps the number
                # of SQLite transactions (~50 ms worth per poll) sane
                # instead of granting one byte at a time.  target is bounded
                # by the caller's request, so the final small tail of a
                # download is released as soon as it is available.
                target = max(
                    1,
                    min(max_bytes, burst, int(math.ceil(rate * MIN_BATCH_SECONDS))),
                )
                available = int(tokens)  # whole bytes only
                if available >= target:
                    granted = min(available, max_bytes)
                    tokens -= granted
                    conn.execute(
                        "UPDATE download_rate SET tokens = ?, last_refill = ?"
                        " WHERE id = 1",
                        (tokens, now),
                    )
                    conn.execute("COMMIT")
                    return granted, 0.0

                wait = max(0.001, (target - tokens) / rate)
                conn.execute(
                    "UPDATE download_rate SET tokens = ?, last_refill = ?"
                    " WHERE id = 1",
                    (tokens, now),
                )
                conn.execute("COMMIT")
                return 0, wait
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    async def acquire(self, max_bytes: int) -> int:
        """Wait until at least one byte of budget is available.

        Waiting is ordinary backpressure, never an error: the returned grant
        is always in ``1..max_bytes``.  Sleeping in short slices means an
        in-flight download reacts to settings changes (and to client
        disconnects, which cancel this coroutine) within MAX_POLL_SLEEP.
        """
        while True:
            try:
                granted, wait = await asyncio.to_thread(self.try_acquire, max_bytes)
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                await asyncio.sleep(LOCK_RETRY_SLEEP)
                continue
            if granted > 0:
                return granted
            await asyncio.sleep(min(max(wait, 0.001), MAX_POLL_SLEEP))
