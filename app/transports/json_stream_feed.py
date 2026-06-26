"""HTTP stream feed for .jsonStream endpoints (CarData.z, Position.z).

These are NOT SignalR topics - they are HTTP REST .jsonStream files
served from the F1 live timing static server. Data is continuously
appended to these files during a session. We use HTTP streaming with
byte-range tracking to pick up new data as it arrives.

Usage:
    feed = JsonStreamFeed(token=token, session_path=path, on_message=callback)
    await feed.start()  # Starts two long-lived streaming connections
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import zlib
from typing import Callable, Optional
from .base import BaseFeed

import httpx

logger = logging.getLogger(__name__)

API_BASE = "https://livetiming.formula1.com/static"
# Mirror used by FastF1 as fallback; uncomment if needed
# API_BASE = "https://livetiming-mirror.fastf1.dev/static"


def _build_headers(token: str) -> dict:
    """Build HTTP headers with auth token (both Bearer and cookie)."""
    headers = {
        "User-Agent": "BestHTTP",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, identity",
        "Origin": "https://www.formula1.com",
        "Authorization": f"Bearer {token}",
    }
    if token:
        headers["Cookie"] = f"login-session={token}"
    return headers


def _parse_line(line: bytes):
    """Parse a single line from a .jsonStream endpoint.

    Format per line:
        12-byte ASCII timestamp (00:00:00:000) + zlib-compressed JSON

    Returns (timestamp_str, decoded_dict) or (None, None) on failure.
    """
    line = line.strip()
    if not line:
        return None, None
    try:
        ts_str = line[:12].decode("ascii", errors="replace")
        payload = line[12:]
        if not payload:
            return ts_str, None
        # CarData.z / Position.z data is zlib-compressed
        try:
            decompressed = zlib.decompress(payload, -zlib.MAX_WBITS)
            return ts_str, json.loads(decompressed)
        except (zlib.error, json.JSONDecodeError):
            pass
        # Some lines might be plain JSON (non- .z)
        try:
            return ts_str, json.loads(payload)
        except json.JSONDecodeError:
            return ts_str, None
    except Exception:
        return None, None


def _convert_position(positions_data: dict) -> dict:
    """Convert Position.z.jsonStream format to SignalR message format.

    Server format:
        {"Position": [{"Date": "...", "X": 1, "Y": 2, "Z": 3,
                       "Status": "OnTrack", "DriverNumber": 44}, ...]}

    SignalR format (what DataProcessor._handle_Position.z expects):
        {"Entries": [{"Cars": {"44": {"X": 1, "Y": 2, "Z": 3,
                                      "Status": "OnTrack"}, ...}}]}
    """
    samples = positions_data.get("Position", [])
    if not samples:
        return None
    cars = {}
    for s in samples:
        dn = s.get("DriverNumber")
        if dn is not None:
            cars[str(dn)] = {
                "X": s.get("X", 0),
                "Y": s.get("Y", 0),
                "Z": s.get("Z", 0),
                "Status": s.get("Status", "OnTrack"),
            }
    if not cars:
        return None
    return {"Entries": [{"Cars": cars}]}


class JsonStreamFeed(BaseFeed):
    """Continuously stream CarData.z.jsonStream and Position.z.jsonStream.

    Uses HTTP persistent connections with byte-range tracking so each
    poll only fetches new data (efficient even at high frequency).

    Args:
        token: F1 subscription token (F1_TOKEN)
        session_path: relative path from session info,
            e.g. "2026/2026-06-28_Austrian_Grand_Prix/2026-06-26_Practice_1/"
        on_message: callback(topic, data, timestamp) - same interface as
            F1SignalRClient's on_message
    """

    def __init__(
        self,
        token: str = "",
        session_path: str = "",
        on_message: Optional[Callable] = None,
    ):
        self.token = token
        self.session_path = session_path.strip("/")
        self.on_message = on_message
        self._running = False
        self._headers = _build_headers(token)
        # Track byte offsets for Range requests
        self._car_offset: int = 0
        self._pos_offset: int = 0
        # Completed line tracking (in case Range is not supported)
        self._car_line_count: int = 0
        self._pos_line_count: int = 0

    @property
    def is_connected(self) -> bool:
        return self._running

    @property
    def name(self) -> str:
        return "json_stream"

    @property
    def base_url(self) -> str:
        return f"{API_BASE}/{self.session_path}"

    @property
    def car_data_url(self) -> str:
        return f"{self.base_url}/CarData.z.jsonStream"

    @property
    def position_url(self) -> str:
        return f"{self.base_url}/Position.z.jsonStream"

    async def start(self):
        """Start streaming both endpoints concurrently."""
        self._running = True
        # Wait for session path
        if not self.session_path:
            logger.info("JsonStreamFeed: waiting for session_path from SignalR...")
            while self._running and not self.session_path:
                await asyncio.sleep(0.5)
            if not self._running:
                return

        logger.info(
            f"JsonStreamFeed starting for {self.session_path}"
        )
        tasks = [
            asyncio.create_task(self._stream_loop("CarData.z", self.car_data_url)),
            asyncio.create_task(self._stream_loop("Position.z", self.position_url)),
        ]
        self._tasks = tasks
        # Wait for both to complete (they run until cancelled)
        await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self):
        self._running = False
        for t in getattr(self, "_tasks", []):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    def update_session_path(self, path: str):
        """Update session path when SessionInfo arrives."""
        if path and path != self.session_path:
            self.session_path = path.strip("/")
            self._car_offset = 0
            self._pos_offset = 0
            self._car_line_count = 0
            self._pos_line_count = 0
            logger.info(f"JsonStreamFeed: session_path updated to {path}")

    async def _stream_loop(self, topic: str, url: str):
        """Stream one .jsonStream endpoint with reconnection."""
        convert = _convert_position if topic == "Position.z" else None

        while self._running:
            try:
                await self._stream_once(topic, url, convert)
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._running:
                    logger.warning(
                        f"{topic} stream error: {e}, reconnecting in 2s..."
                    )
                    await asyncio.sleep(2)

    async def _stream_once(self, topic: str, url: str, convert=None):
        """Open a streaming connection and process incoming lines.

        Uses Range header to only fetch data from the last known offset.
        Falls back to full download if Range is not supported.
        """
        offset = (
            self._car_offset if "CarData" in url else self._pos_offset
        )
        headers = dict(self._headers)
        if offset > 0:
            headers["Range"] = f"bytes={offset}-"

        async with httpx.AsyncClient(
            headers=headers, timeout=httpx.Timeout(30.0, connect=10.0)
        ) as client:
            async with client.stream("GET", url) as resp:
                if resp.status_code == 416:
                    # Range not satisfiable - file hasn't grown
                    return
                if resp.status_code not in (200, 206):
                    logger.warning(
                        f"{topic} HTTP {resp.status_code} for {url}"
                    )
                    await asyncio.sleep(1)
                    return

                # Update offset from Content-Range or Content-Length
                content_range = resp.headers.get("content-range")
                if content_range:
                    # "bytes {start}-{end}/{total}" -> update offset
                    parts = content_range.split("/")[0].split("-")
                    if len(parts) == 2:
                        new_offset = int(parts[1]) + 1
                        if "CarData" in url:
                            self._car_offset = new_offset
                        else:
                            self._pos_offset = new_offset
                elif resp.status_code == 200:
                    # Full response, no Range - reset offset tracking
                    content_len = resp.headers.get("content-length")
                    if content_len:
                        total = int(content_len)
                        if "CarData" in url:
                            self._car_offset = total
                        else:
                            self._pos_offset = total

                # Process chunks as they arrive (streaming)
                buf = b""
                async for chunk in resp.aiter_bytes():
                    if not self._running:
                        break
                    buf += chunk
                    lines = buf.split(b"\n")
                    buf = lines[-1]  # Keep incomplete trailing line

                    for line_bytes in lines[:-1]:
                        if not line_bytes.strip():
                            continue
                        ts_str, data = _parse_line(line_bytes)
                        if data is None:
                            continue

                        if convert:
                            data = convert(data)
                            if data is None:
                                continue

                        if self.on_message:
                            try:
                                self.on_message(topic, data, time.time())
                            except Exception as e:
                                logger.error(
                                    f"{topic} on_message error: {e}"
                                )

                # Stream ended - save any remaining partial line
                if buf.strip():
                    ts_str, data = _parse_line(buf)
                    if data and self.on_message:
                        if convert:
                            data = convert(data)
                        if data:
                            self.on_message(topic, data, time.time())

    def _reset_offsets(self):
        """Reset byte offsets (e.g. on new session)."""
        self._car_offset = 0
        self._pos_offset = 0
        self._car_line_count = 0
        self._pos_line_count = 0

    @staticmethod
    def get_url_for(path: str, topic: str) -> str:
        """Build the HTTP URL for a given topic and session path."""
        name = "CarData.z.jsonStream" if topic == "CarData.z" else "Position.z.jsonStream"
        return f"{API_BASE}/{path.strip('/')}/{name}"
