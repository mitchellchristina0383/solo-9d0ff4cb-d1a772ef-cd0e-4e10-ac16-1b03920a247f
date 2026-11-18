"""Unit tests for the shared token bucket (fake clock, no sleeps)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import init_db  # noqa: E402
from app.ratelimit import (  # noqa: E402
    DEFAULT_BURST_BYTES,
    DownloadRateLimiter,
    validate_settings,
)


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture()
def limiter_and_clock(tmp_path):
    db = tmp_path / "db.sqlite3"
    init_db(db)
    clock = FakeClock()
    limiter = DownloadRateLimiter(db, clock=clock)
    try:
        yield limiter, clock, db
    finally:
        limiter.close()


def test_default_is_unlimited(limiter_and_clock):
    limiter, _, _ = limiter_and_clock
    s = limiter.get_settings()
    assert s.rate_bps == 0 and s.burst_bytes == DEFAULT_BURST_BYTES and s.unlimited
    assert limiter.try_acquire(64 * 1024) == (64 * 1024, 0.0)


def test_burst_then_throttle_then_refill(limiter_and_clock):
    limiter, clock, _ = limiter_and_clock
    limiter.set_settings(rate_bps=100_000, burst_bytes=10_000)

    # Enabling the limit starts with a full bucket -> exactly the burst.
    assert limiter.try_acquire(10_000) == (10_000, 0.0)
    # Empty now: not enough tokens for the next chunk, must wait.
    granted, wait = limiter.try_acquire(64 * 1024)
    assert granted == 0 and wait > 0
    # 0.1 s at 100 kB/s = 10 kB, enough to send immediately again.
    clock.advance(0.1)
    assert limiter.try_acquire(64 * 1024) == (10_000, 0.0)
    # Fractional tokens are only ever truncated, never overdrawn: 10 ms at
    # 100 kB/s is 1 kB, less than one 50 ms batch, so the sender waits.
    clock.advance(0.01)
    assert limiter.try_acquire(64 * 1024)[0] == 0
    clock.advance(0.05)  # now 6 kB has accrued, above the 5 kB batch floor
    granted, _ = limiter.try_acquire(64 * 1024)
    assert 5_999 <= granted <= 6_000
    # The request size is respected as well.
    clock.advance(1.0)
    assert limiter.try_acquire(1_234) == (1_234, 0.0)


def test_grant_never_exceeds_burst_or_request(limiter_and_clock):
    limiter, clock, _ = limiter_and_clock
    limiter.set_settings(rate_bps=1_000_000, burst_bytes=5_000)
    granted, _ = limiter.try_acquire(64 * 1024)
    assert granted == 5_000  # capped by the burst, even though 5+ kB accrued
    clock.advance(10)
    granted, _ = limiter.try_acquire(1_234)
    assert granted == 1_234  # capped by the request


def test_two_limiter_instances_share_the_bucket(limiter_and_clock):
    limiter, clock, db = limiter_and_clock
    other = DownloadRateLimiter(db, clock=clock)
    try:
        limiter.set_settings(rate_bps=1000, burst_bytes=3000)
        assert limiter.try_acquire(10_000) == (3_000, 0.0)
        assert other.try_acquire(10_000)[0] == 0  # second process sees empty bucket
        clock.advance(1.0)  # 1000 more tokens
        assert other.try_acquire(10_000) == (1_000, 0.0)
        assert limiter.try_acquire(10_000)[0] == 0
    finally:
        other.close()


def test_settings_change_applies_without_restart(limiter_and_clock):
    limiter, clock, db = limiter_and_clock
    limiter.set_settings(rate_bps=1000, burst_bytes=1000)
    assert limiter.try_acquire(10_000) == (1_000, 0.0)
    assert limiter.try_acquire(10_000)[0] == 0

    # A different process raises the budget -> this one sees it immediately.
    other = DownloadRateLimiter(db, clock=clock)
    try:
        other.set_settings(rate_bps=50_000, burst_bytes=2000)
    finally:
        other.close()
    # No time passed, so no free bytes appear; but the refill rate is now the
    # new one: 0.05 s yields 2'500 B (capped by the 2 kB burst), where the old
    # rate would only have yielded 50 B.
    assert limiter.try_acquire(10_000)[0] == 0
    clock.advance(0.05)
    granted, _ = limiter.try_acquire(10_000)
    assert 1_999 <= granted <= 2_000

    # Lowering it also takes effect immediately.
    limiter.set_settings(rate_bps=1000, burst_bytes=500)
    assert limiter.try_acquire(10_000)[0] == 0
    clock.advance(0.1)  # 100 ms at 1 kB/s = 100 B, above the 50 B batch
    granted, _ = limiter.try_acquire(10_000)
    assert 99 <= granted <= 100


def test_settings_persist_across_instances(limiter_and_clock):
    limiter, clock, db = limiter_and_clock
    limiter.set_settings(rate_bps=12_345, burst_bytes=67_890)
    limiter.close()
    reopened = DownloadRateLimiter(db, clock=clock)
    try:
        s = reopened.get_settings()
        assert s.rate_bps == 12_345 and s.burst_bytes == 67_890
    finally:
        reopened.close()


def test_unlimited_again_ignores_accounting(limiter_and_clock):
    limiter, clock, _ = limiter_and_clock
    limiter.set_settings(rate_bps=10, burst_bytes=10)
    limiter.try_acquire(10)
    limiter.set_settings(rate_bps=0, burst_bytes=10)
    assert limiter.try_acquire(1 << 20) == (1 << 20, 0.0)


def test_refund_returns_unsent_bytes_and_is_capped(limiter_and_clock):
    limiter, clock, _ = limiter_and_clock
    limiter.set_settings(rate_bps=100_000, burst_bytes=10_000)
    assert limiter.try_acquire(10_000) == (10_000, 0.0)
    assert limiter.try_acquire(10_000)[0] == 0
    # Abandoned lease: the bytes come back, so another sender need not wait.
    limiter.refund(10_000)
    assert limiter.try_acquire(10_000) == (10_000, 0.0)
    # A refund can never inflate the bucket past the burst.
    limiter.refund(10_000_000)
    assert limiter.try_acquire(10 ** 9) == (10_000, 0.0)
    # Zero/negative refunds are ignored.
    limiter.refund(0)
    limiter.refund(-5)
    assert limiter.try_acquire(1)[0] == 0


def test_validate_settings():
    assert validate_settings(0, 1) == (0, 1)
    for bad in [(-1, 1), (1, 0), (1, -5)]:
        with pytest.raises(ValueError):
            validate_settings(*bad)
    with pytest.raises(ValueError):
        validate_settings(1.5, 10)
    with pytest.raises(ValueError):
        validate_settings(True, 10)
