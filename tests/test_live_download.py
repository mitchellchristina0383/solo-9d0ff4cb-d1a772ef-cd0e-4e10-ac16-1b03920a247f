"""Live multi-process tests for the service-wide download budget.

Two real uvicorn processes share one data directory (as in the container,
which runs `--workers 2`).  They must honour one shared byte budget: downloads
started on both processes together stay within the configured rate, each
connection still receives the exact published bytes, a settings change made
on one process applies to transfers running on the other, and the setting
survives a restart of every process.

Run:  .venv/bin/python -m pytest tests/test_live_download.py -v
"""
import random
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

import live_helpers as lh


def _download(base: str, tid: str) -> tuple[bytes, float, int]:
    with httpx.Client(timeout=120) as client:
        start = time.monotonic()
        r = client.get(f"{base}/api/tasks/{tid}/download")
        elapsed = time.monotonic() - start
        return r.content, elapsed, r.status_code


def test_two_processes_share_one_budget(tmp_path):
    data_dir = tmp_path / "data"
    s1, s2 = lh.Server(data_dir, tmp_path), lh.Server(data_dir, tmp_path)
    try:
        s1.wait_ready()
        s2.wait_ready()
        data = random.Random(21).randbytes(400 * 1024)
        with httpx.Client(timeout=120) as client:
            tid = lh.publish(client, s1.base, data)
            lh.set_rate(client, s1.base, 200_000, 64_000)
            # The other process sees the setting immediately.
            assert client.get(f"{s2.base}/api/settings").json()["rate_bps"] == 200_000

            # One download on each process, at the same time.
            with ThreadPoolExecutor(max_workers=2) as ex:
                fut_a = ex.submit(_download, s1.base, tid)
                fut_b = ex.submit(_download, s2.base, tid)
                a, b = fut_a.result(), fut_b.result()

            for body, _, status in (a, b):
                assert status == 200
                assert body == data  # byte-exact on both processes
            # 800 kB at 200 kB/s ≈ 4 s if the budget is truly shared.  With
            # one bucket per process it would be ~2 s.
            slowest = max(a[1], b[1])
            assert slowest >= 3.0, (a[1], b[1])
            assert slowest < 20.0
    finally:
        s1.stop()
        s2.stop()


def test_live_settings_change_on_other_process(tmp_path):
    data_dir = tmp_path / "data"
    s1, s2 = lh.Server(data_dir, tmp_path), lh.Server(data_dir, tmp_path)
    try:
        s1.wait_ready()
        s2.wait_ready()
        data = random.Random(22).randbytes(600 * 1024)
        with httpx.Client(timeout=120) as client:
            tid = lh.publish(client, s1.base, data)
            lh.set_rate(client, s1.base, 100_000, 64_000)  # ~6 s for 600 kB

            with ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(_download, s2.base, tid)  # runs on s2
                time.sleep(0.5)
                # Change the budget through the *other* process' web API.
                lh.set_rate(client, s1.base, 5_000_000, 400_000)
                body, elapsed, status = fut.result()

            assert status == 200 and body == data
            assert elapsed < 4.0, elapsed  # not the ~6 s of the old rate
    finally:
        s1.stop()
        s2.stop()


def test_budget_survives_full_restart(tmp_path):
    data_dir = tmp_path / "data"
    data = random.Random(23).randbytes(300 * 1024)
    s1 = lh.Server(data_dir, tmp_path)
    s1.wait_ready()
    with httpx.Client(timeout=120) as client:
        tid = lh.publish(client, s1.base, data)
        lh.set_rate(client, s1.base, 200_000, 64_000)
    s1.stop()

    s2 = lh.Server(data_dir, tmp_path)
    try:
        s2.wait_ready()
        with httpx.Client(timeout=120) as client:
            s = client.get(f"{s2.base}/api/settings").json()
            assert s["rate_bps"] == 200_000 and s["burst_bytes"] == 64_000
            start = time.monotonic()
            r = client.get(f"{s2.base}/api/tasks/{tid}/download")
            elapsed = time.monotonic() - start
            assert r.status_code == 200 and r.content == data
            # (300 - 64) kB at 200 kB/s ≈ 1.2 s: the persisted rate is live.
            assert elapsed >= 0.8, elapsed
    finally:
        s2.stop()


def test_client_disconnect_releases_budget_on_live_server(tmp_path):
    """A dead client must not keep draining the shared budget."""
    data_dir = tmp_path / "data"
    s1 = lh.Server(data_dir, tmp_path)
    s1.wait_ready()
    try:
        data = random.Random(24).randbytes(2 * 1024 * 1024)
        rate = 400_000
        with httpx.Client(timeout=120) as client:
            tid = lh.publish(client, s1.base, data)
            lh.set_rate(client, s1.base, rate, 64_000)

            # Read a little, then hang up mid-body.
            with httpx.Client(timeout=120) as slow:
                with slow.stream("GET", f"{s1.base}/api/tasks/{tid}/download") as r:
                    assert r.status_code == 200
                    for _ in r.iter_bytes(8192):
                        break
            time.sleep(0.5)  # let the server observe the disconnect

            # Same low rate: a single live stream should get the whole
            # budget.  (~2 MiB at 400 kB/s ≈ 5 s; if the dead connection were
            # still consuming, it would take ~10 s.)
            start = time.monotonic()
            r = client.get(f"{s1.base}/api/tasks/{tid}/download")
            elapsed = time.monotonic() - start
            assert r.status_code == 200 and r.content == data
            assert elapsed < 8.5, elapsed
            assert elapsed >= 4.0, elapsed  # the rate still applies

            # Several sequential dead connections must not starve anyone.
            for _ in range(3):
                with httpx.Client(timeout=120) as slow:
                    with slow.stream(
                        "GET", f"{s1.base}/api/tasks/{tid}/download"
                    ) as r:
                        assert r.status_code == 200
                        for _ in r.iter_bytes(4096):
                            break
            time.sleep(0.5)
            lh.set_rate(client, s1.base, 20_000_000, 2 * 1024 * 1024)
            start = time.monotonic()
            r = client.get(f"{s1.base}/api/tasks/{tid}/download")
            elapsed = time.monotonic() - start
            assert r.status_code == 200 and r.content == data
            assert elapsed < 5.0, elapsed
    finally:
        s1.stop()
