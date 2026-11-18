"""Helpers for the live uvicorn tests (real server processes)."""
import hashlib
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
CHUNK = 1024 * 1024


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    def __init__(self, data_dir: Path, log_dir: Path):
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        env = dict(os.environ, DATA_DIR=str(data_dir), PYTHONPATH=str(ROOT))
        self.log = open(log_dir / f"server-{self.port}.log", "wb")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            env=env, cwd=str(ROOT), stdout=self.log, stderr=subprocess.STDOUT,
        )

    def wait_ready(self, timeout=30.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server on {self.port} exited early")
            try:
                if httpx.get(self.base + "/api/health", timeout=1).status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(0.2)
        raise TimeoutError(f"server on {self.port} not ready")

    def kill(self):
        self.proc.kill()
        self.proc.wait()
        self.log.close()

    def stop(self):
        self.proc.terminate()
        self.proc.wait(timeout=10)
        self.log.close()


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def create_task(client: httpx.Client, base: str, data: bytes) -> str:
    r = client.post(f"{base}/api/tasks", json={
        "filename": "diag.tar.gz", "size": len(data), "sha256": sha256(data)})
    assert r.status_code == 201, r.text
    return r.json()["task_id"]


def put_chunk(client: httpx.Client, base: str, tid: str, i: int, payload: bytes):
    return client.put(f"{base}/api/tasks/{tid}/chunks/{i}", content=payload)


def publish(client: httpx.Client, base: str, data: bytes) -> str:
    tid = create_task(client, base, data)
    for i in range(0, len(data), CHUNK):
        r = put_chunk(client, base, tid, i // CHUNK, data[i:i + CHUNK])
        assert r.status_code == 200, r.text
    r = client.post(f"{base}/api/tasks/{tid}/finalize")
    assert r.status_code == 200 and r.json()["state"] == "published", r.text
    return tid


def set_rate(client: httpx.Client, base: str, rate_bps: int, burst: int):
    r = client.put(f"{base}/api/settings",
                   json={"rate_bps": rate_bps, "burst_bytes": burst})
    assert r.status_code == 200, r.text
    return r.json()
