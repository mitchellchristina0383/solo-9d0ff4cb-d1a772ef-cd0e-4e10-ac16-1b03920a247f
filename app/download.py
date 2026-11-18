"""Streaming download responses that obey the service-wide byte budget.

The body is streamed from the published file in bounded chunks.  Before each
chunk is handed to the ASGI server the response asks the shared token bucket
(:mod:`app.ratelimit`) for permission; when the budget is exhausted it simply
waits, so a throttled transfer is ordinary backpressure and never an error.

Two properties matter for the callers:

* every connection reads its own file handle at its own offset, so concurrent
  downloads of the same package each receive exactly the published bytes, in
  order;
* the budget is requested per chunk (64 KiB), not per file, so a settings
  change is observed by transfers that are already running, and a client that
  disappears only ever strands at most one small chunk worth of tokens.

Disconnects are always detected here, regardless of the ASGI spec version the
server advertises: a task listens on ``receive()`` for ``http.disconnect`` and
cancels the sender.  Otherwise a dead connection could keep draining the
shared budget for the rest of the file and delay everybody else.
"""
from __future__ import annotations

import asyncio
import os
from urllib.parse import quote

import anyio
from starlette.responses import Response

DOWNLOAD_CHUNK = 64 * 1024


def content_disposition(filename: str) -> str:
    quoted = quote(filename)
    if quoted != filename:
        return f"attachment; filename*=utf-8''{quoted}"
    return f'attachment; filename="{filename}"'


def parse_single_range(header: str | None, size: int) -> tuple[int, int] | None:
    """Parse a single ``bytes=`` range into ``[start, end)``.

    Returns ``None`` for anything unsupported; callers then serve the whole
    file with ``200`` (which HTTP permits).  Multiple ranges are deliberately
    ignored: the product only needs whole-package streaming plus simple
    resume, and a multi-range response would be a much larger surface.
    """
    if not header or size <= 0:
        return None
    unit, sep, spec = header.partition("=")
    if not sep or unit.strip().lower() != "bytes" or "," in spec:
        return None
    spec = spec.strip()
    if "-" not in spec:
        return None
    first, last = (part.strip() for part in spec.split("-", 1))
    try:
        if not first:
            if not last:
                return None
            suffix = int(last)
            if suffix <= 0:
                return None
            return max(0, size - suffix), size
        start = int(first)
        if start < 0 or start >= size:
            return None
        if not last:
            return start, size
        end = int(last) + 1
        if end <= start:
            return None
        return start, min(end, size)
    except ValueError:
        return None


def range_is_unsatisfiable(header: str | None, size: int) -> bool:
    """True for a syntactically valid single range that starts past the end."""
    if not header or size <= 0:
        return False
    unit, sep, spec = header.partition("=")
    if not sep or unit.strip().lower() != "bytes" or "," in spec:
        return False
    spec = spec.strip()
    if "-" not in spec:
        return False
    first, _ = (part.strip() for part in spec.split("-", 1))
    if not first:
        return False
    try:
        return int(first) >= size
    except ValueError:
        return False


class RateLimitedFileResponse(Response):
    """Stream a file at most as fast as the shared bucket allows."""

    chunk_size = DOWNLOAD_CHUNK

    def __init__(
        self,
        path,
        filename: str,
        limiter,
        request=None,
        media_type: str = "application/octet-stream",
    ) -> None:
        self.path = os.fspath(path)
        self.limiter = limiter
        stat_result = os.stat(self.path)
        total = stat_result.st_size

        start, length, status = 0, total, 200
        extra_headers: dict[str, str] = {}
        range_header = request.headers.get("range") if request is not None else None
        if range_is_unsatisfiable(range_header, total):
            status = 416
            length = 0
            extra_headers["content-range"] = f"bytes */{total}"
        else:
            parsed = parse_single_range(range_header, total)
            if parsed is not None:
                start, end = parsed
                length = end - start
                status = 206
                extra_headers["content-range"] = f"bytes {start}-{end - 1}/{total}"

        headers = {
            "content-length": str(length),
            "content-disposition": content_disposition(filename),
            "accept-ranges": "bytes",
            **extra_headers,
        }
        super().__init__(
            status_code=status, headers=headers, media_type=media_type
        )
        self.offset = start
        self.length = length
        self.total_size = total

    # ------------------------------------------------------------- sending
    async def _iter_body(self, start: int, length: int):
        if length <= 0:
            return
        # One raw (unbuffered) handle per connection: offsets are private, so
        # N clients can read the same package concurrently without
        # interfering, and each read is exactly one syscall of the granted
        # size.  Reads stay on the event loop deliberately: they are at most
        # 64 KiB from a local file, while doing them in a worker thread would
        # leave an in-flight read behind when the client disconnects.
        file = open(self.path, "rb", buffering=0)
        leased = 0
        try:
            file.seek(start)
            remaining = length
            while remaining > 0:
                # Waiting for budget is never an error; it is the normal
                # behaviour of a throttled stream.  A disconnect cancels this
                # coroutine, so a vanished client stops asking immediately.
                granted = await self.limiter.acquire(min(self.chunk_size, remaining))
                leased = granted
                data = file.read(granted)
                if not data:
                    # Published files are immutable; a short file is a real
                    # storage fault, not throttling.  Aborting the response
                    # keeps the client from mistaking truncation for success.
                    raise RuntimeError("published file shorter than expected")
                remaining -= len(data)
                yield data
                leased = 0
        finally:
            file.close()
            if leased > 0:
                # Bytes were leased but the client went away before they
                # could be sent: return them to the shared bucket so other
                # downloads are not held back by an abandoned connection.
                try:
                    await asyncio.to_thread(self.limiter.refund, leased)
                except Exception:
                    pass

    async def _send_body(self, send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self.raw_headers,
            }
        )
        iterator = self._iter_body(self.offset, self.length)
        try:
            async for chunk in iterator:
                await send(
                    {"type": "http.response.body", "body": chunk, "more_body": True}
                )
        finally:
            # Close the generator (and its file handle) even when cancelled.
            with anyio.CancelScope(shield=True):
                await iterator.aclose()
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") == "http" and scope.get("method", "").upper() == "HEAD":
            # A HEAD must neither stream nor consume budget.
            await send(
                {
                    "type": "http.response.start",
                    "status": self.status_code,
                    "headers": self.raw_headers,
                }
            )
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return

        if scope.get("type") != "http":
            await self._send_body(send)
            return

        async with anyio.create_task_group() as task_group:

            async def stream() -> None:
                try:
                    await self._send_body(send)
                except OSError:
                    # The transport went away mid-write; nothing to report.
                    pass
                finally:
                    task_group.cancel_scope.cancel()

            async def watch_disconnect() -> None:
                while not task_group.cancel_scope.cancel_called:
                    try:
                        message = await receive()
                    except Exception:
                        # A broken/closed channel is equivalent to the peer
                        # going away: stop sending instead of retrying.
                        task_group.cancel_scope.cancel()
                        return
                    if message["type"] == "http.disconnect":
                        task_group.cancel_scope.cancel()
                        return

            task_group.start_soon(stream)
            task_group.start_soon(watch_disconnect)
