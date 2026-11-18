"""Functional tests for the service-wide download rate limit.

Real timing is used (a few hundred milliseconds per test), because the
behaviour under test *is* the rate: waiting instead of failing, sharing one
budget between connections, applying changes to live transfers, and releasing
the budget when a client goes away.
"""
import hashlib
import random
import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import CHUNK_SIZE, create_app  # noqa: E402


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def make_client(tmp_path: Path) -> TestClient:
    return TestClient(create_app(tmp_path / "data"))


def publish(client: TestClient, data: bytes, filename="pkg.bin") -> str:
    tid = client.post(
        "/api/tasks",
        json={"filename": filename, "size": len(data), "sha256": sha256(data)},
    ).json()["task_id"]
    for i in range(0, len(data), CHUNK_SIZE):
        r = client.put(
            f"/api/tasks/{tid}/chunks/{i // CHUNK_SIZE}",
            content=data[i : i + CHUNK_SIZE],
        )
        assert r.status_code == 200, r.text
    assert client.post(f"/api/tasks/{tid}/finalize").json()["state"] == "published"
    return tid


def set_rate(client: TestClient, rate_bps: int, burst_bytes: int):
    r = client.put(
        "/api/settings", json={"rate_bps": rate_bps, "burst_bytes": burst_bytes}
    )
    assert r.status_code == 200, r.text
    return r.json()


def timed_download(client: TestClient, tid: str) -> tuple[bytes, float]:
    start = time.monotonic()
    r = client.get(f"/api/tasks/{tid}/download")
    elapsed = time.monotonic() - start
    assert r.status_code == 200, r.text
    return r.content, elapsed


@pytest.fixture()
def client(tmp_path):
    c = make_client(tmp_path)
    yield c
    c.close()


# ------------------------------------------------------------------ settings
def test_settings_defaults_and_roundtrip(client):
    s = client.get("/api/settings").json()
    assert s == {"rate_bps": 0, "burst_bytes": 1048576, "unlimited": True}

    s = set_rate(client, 250_000, 500_000)
    assert s == {"rate_bps": 250_000, "burst_bytes": 500_000, "unlimited": False}
    assert client.get("/api/settings").json()["rate_bps"] == 250_000


def test_settings_validation(client):
    bad = [
        {"rate_bps": -1, "burst_bytes": 1024},
        {"rate_bps": 10, "burst_bytes": 0},
        {"rate_bps": 10, "burst_bytes": -5},
        {"rate_bps": "fast", "burst_bytes": 1024},
    ]
    for body in bad:
        r = client.put("/api/settings", json=body)
        assert r.status_code in (400, 422), (body, r.text)
    assert client.get("/api/settings").json()["rate_bps"] == 0  # unchanged


def test_settings_survive_restart(tmp_path):
    c1 = make_client(tmp_path)
    set_rate(c1, 123_456, 654_321)
    c1.close()

    c2 = make_client(tmp_path)
    try:
        s = c2.get("/api/settings").json()
        assert s["rate_bps"] == 123_456 and s["burst_bytes"] == 654_321
    finally:
        c2.close()


# -------------------------------------------------------------- throttling
def test_unlimited_download_is_fast_and_exact(client):
    data = random.Random(1).randbytes(4 * CHUNK_SIZE)
    tid = publish(client, data)
    body, elapsed = timed_download(client, tid)
    assert body == data
    assert elapsed < 2.0


def test_throttled_download_is_slow_but_complete(client):
    data = random.Random(2).randbytes(400 * 1024)
    tid = publish(client, data)
    # 200 kB/s with a small burst: ~400 kB needs ~2 s.
    set_rate(client, 200_000, 64_000)
    body, elapsed = timed_download(client, tid)
    assert body == data  # throttling is never a corrupt file
    assert elapsed >= 1.5
    assert elapsed < 8.0


def test_rate_is_enforced_across_sequential_downloads(client):
    """The budget is service-wide, not per connection."""
    data = random.Random(3).randbytes(300 * 1024)
    tid = publish(client, data)
    set_rate(client, 300_000, 32_000)
    t0 = time.monotonic()
    for _ in range(2):
        body, _ = timed_download(client, tid)
        assert body == data
    total = time.monotonic() - t0
    # 600 kB at 300 kB/s ≈ 2 s (the 32 kB burst is negligible).
    assert total >= 1.6
    assert total < 8.0


def test_concurrent_downloads_share_budget_and_get_exact_bytes(client):
    data = random.Random(4).randbytes(320 * 1024)
    tid = publish(client, data)
    set_rate(client, 200_000, 64_000)

    results: dict[int, tuple[bytes, float]] = {}

    def worker(n: int):
        start = time.monotonic()
        r = client.get(f"/api/tasks/{tid}/download")
        results[n] = (r.content, time.monotonic() - start)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.monotonic() - t0

    # Each connection receives exactly the original bytes, in order.
    for n, (body, _) in results.items():
        assert body == data, f"client {n} got wrong bytes"
    # 3 x 320 kB at 200 kB/s ≈ 4.8 s if the budget is shared; if each
    # connection had its own budget it would be ~1.6 s.
    assert wall >= 3.5
    assert wall < 15.0


def test_settings_change_applies_to_a_live_download(client):
    """A download in progress picks up the new rate without reconnecting."""
    data = random.Random(5).randbytes(600 * 1024)
    tid = publish(client, data)
    # Start slow: ~600 kB at 100 kB/s would take ~6 s.
    set_rate(client, 100_000, 64_000)

    outcome: dict[str, object] = {}

    def download():
        start = time.monotonic()
        r = client.get(f"/api/tasks/{tid}/download")
        outcome["body"] = r.content
        outcome["elapsed"] = time.monotonic() - start
        outcome["status"] = r.status_code

    t = threading.Thread(target=download)
    t.start()
    time.sleep(0.4)  # it is now mid-transfer, throttled
    # Raise the budget from the "web UI" while the transfer is running.
    set_rate(client, 5_000_000, 400_000)
    t.join(timeout=30)

    assert not t.is_alive()
    assert outcome["status"] == 200
    assert outcome["body"] == data
    assert outcome["elapsed"] < 3.0  # not the ~6 s the old rate needed


def test_disconnected_client_stops_consuming_budget(client):
    data = random.Random(6).randbytes(2 * CHUNK_SIZE)
    tid = publish(client, data)
    set_rate(client, 100_000, 32_000)

    # Start a download and abort it after the headers/first chunk.
    with client.stream("GET", f"/api/tasks/{tid}/download") as r:
        assert r.status_code == 200
        it = r.iter_bytes()
        next(it)  # some bytes arrived
        # leaving the context closes the connection mid-body

    # Wait a moment for the server task to observe the disconnect, then
    # download the whole file with a generous budget: it must not be stuck
    # waiting for tokens the dead client had "reserved".
    time.sleep(0.5)
    set_rate(client, 10_000_000, 2 * CHUNK_SIZE)
    body, elapsed = timed_download(client, tid)
    assert body == data
    assert elapsed < 3.0

    # And the bucket is alive and still accepting changes afterwards.
    assert client.get("/api/settings").json()["rate_bps"] == 10_000_000


def test_range_request_streams_exact_slice(client):
    data = random.Random(7).randbytes(300 * 1024)
    tid = publish(client, data)
    set_rate(client, 0, 64_000)

    r = client.get(
        f"/api/tasks/{tid}/download", headers={"Range": "bytes=1000-2047"}
    )
    assert r.status_code == 206
    assert r.content == data[1000:2048]
    assert r.headers["content-range"] == f"bytes 1000-2047/{len(data)}"
    assert r.headers["accept-ranges"] == "bytes"

    # Suffix and open-ended ranges resume the same way.
    r = client.get(f"/api/tasks/{tid}/download", headers={"Range": "bytes=-100"})
    assert r.status_code == 206 and r.content == data[-100:]
    r = client.get(
        f"/api/tasks/{tid}/download", headers={"Range": "bytes=1000-"}
    )
    assert r.status_code == 206 and r.content == data[1000:]

    # Past the end is 416, not a truncated 200.
    r = client.get(
        f"/api/tasks/{tid}/download",
        headers={"Range": f"bytes={len(data)}-"},
    )
    assert r.status_code == 416
    assert r.headers["content-range"] == f"bytes */{len(data)}"

    # A multi-range request is simply served whole.
    r = client.get(
        f"/api/tasks/{tid}/download", headers={"Range": "bytes=0-99,200-299"}
    )
    assert r.status_code == 200 and r.content == data


def test_head_download_has_headers_without_body_or_budget(client):
    data = random.Random(8).randbytes(100 * 1024)
    tid = publish(client, data)
    set_rate(client, 1000, 1000)  # tiny budget: a HEAD must not need it
    r = client.head(f"/api/tasks/{tid}/download")
    assert r.status_code == 200
    assert r.content == b""
    assert r.headers["content-length"] == str(len(data))


def test_download_of_unpublished_task_is_still_rejected(client):
    tid = client.post(
        "/api/tasks",
        json={"filename": "x", "size": 3, "sha256": sha256(b"abc")},
    ).json()["task_id"]
    assert client.get(f"/api/tasks/{tid}/download").status_code == 409
    assert client.head(f"/api/tasks/{tid}/download").status_code == 409
    assert client.get(f"/api/tasks/{'0' * 32}/download").status_code == 404


def test_upload_and_publish_still_work_while_download_is_throttled(client):
    data = random.Random(9).randbytes(400 * 1024)
    tid = publish(client, data)
    set_rate(client, 100_000, 32_000)

    result: dict[str, object] = {}

    def download():
        result["body"] = client.get(f"/api/tasks/{tid}/download").content

    t = threading.Thread(target=download)
    t.start()
    time.sleep(0.2)
    # A new upload/publish must not be delayed by the download budget.
    other = random.Random(10).randbytes(2 * CHUNK_SIZE + 1)
    started = time.monotonic()
    tid2 = publish(client, other)
    assert time.monotonic() - started < 3.0
    assert client.get(f"/api/tasks/{tid2}").json()["state"] == "published"
    t.join(timeout=30)
    assert result["body"] == data
