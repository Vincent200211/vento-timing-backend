"""Replay recorded F1 SignalR JSONL data through the live pipeline.

Reads JSONL files (same format as recorded by record_f1_live.py) and
feeds them through handle_f1_message exactly like the live SignalR feed.

Usage:
    $env:LIVE_REPLAY_MODE=1; $env:LIVE_REPLAY_SPEED=2; python run.py
    $env:LIVE_REPLAY_MODE="recordings/2026-06-26"; python run.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Callable, Optional

from .base import BaseFeed

logger = logging.getLogger(__name__)


class LiveReplayFeed(BaseFeed):
    """Replays recorded F1 SignalR JSONL data through the live pipeline.

    Mirrors F1SignalRClient interface so main.py can swap it in
    via LIVE_REPLAY_MODE env var. Respects LIVE_REPLAY_SPEED.
    """

    def __init__(
        self,
        on_message: Optional[Callable] = None,
        data_dir: str = "",
        speed: float = 1.0,
        start_offset: float = 0.0,
    ):
        self.on_message = on_message
        self.data_dir = str(data_dir) if data_dir else ""
        self.speed = float(speed) if speed > 0 else 1.0
        self.start_offset = max(0.0, float(start_offset))
        self._running = False
        self._records: list[tuple[float, str, dict]] = []
        self._start_ts: float = 0.0
        self._errors = 0
        self._processed = 0

    @property
    def is_connected(self) -> bool:
        return self._running

    @property
    def name(self) -> str:
        return "live_replay"

    def _load(self):
        """Load all JSONL files from the recording directory, sorted by timestamp."""
        self._records = []
        if not os.path.isdir(self.data_dir):
            raise FileNotFoundError(f"Recording directory not found: {self.data_dir}")

        fnames = sorted(f for f in os.listdir(self.data_dir) if f.endswith(".jsonl"))
        if not fnames:
            logger.warning(f"No .jsonl files found in {self.data_dir}")
            return

        for fname in fnames:
            path = os.path.join(self.data_dir, fname)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                            self._records.append((
                                float(rec["ts"]),
                                str(rec["topic"]),
                                rec.get("data", {}),
                            ))
                        except (KeyError, ValueError, json.JSONDecodeError):
                            continue
            except (OSError, IOError):
                logger.warning(f"Failed to read {path}")
                continue

        self._records.sort(key=lambda x: x[0])
        # Apply start_offset: skip records before desired start time
        if self.start_offset > 0 and self._records:
            base_ts = self._records[0][0]
            cut_ts = base_ts + self.start_offset
            # Keep metadata topics regardless of offset (needed for drivers, session, circuit info)
            _META_TOPICS = {"SessionInfo", "DriverList", "SessionData"}
            skipped = sum(1 for r in self._records if r[0] < cut_ts)
            self._records = [r for r in self._records if r[0] >= cut_ts or r[1] in _META_TOPICS]
            # Start pacing from cut_ts so we don't wait for metadata before the offset
            self._start_ts = cut_ts
            if skipped > 0:
                logger.info(f"LiveReplayFeed: skipped {skipped} records ({self.start_offset:.0f}s offset)")
            else:
                logger.warning(f"LiveReplayFeed: start_offset={self.start_offset}s but no records before that time")
        else:
            self._start_ts = self._records[0][0] if self._records else 0
        logger.info(f"LiveReplayFeed loaded {len(self._records)} records from {len(fnames)} files")

    async def start(self):
        """Start replaying recorded data through the pipeline."""
        self._running = True
        self._errors = 0
        self._processed = 0

        try:
            self._load()
        except Exception as e:
            logger.error(f"LiveReplayFeed load failed: {e}")
            self._running = False
            return

        if not self._records:
            logger.warning("LiveReplayFeed: no records to replay")
            self._running = False
            return

        first_ts = self._start_ts
        start_wall = time.perf_counter()
        total = len(self._records)
        status_interval = max(1, total // 10)

        logger.info(f"LiveReplayFeed: {total} records, {self.speed}x speed")

        for i, (ts, topic, data) in enumerate(self._records):
            if not self._running:
                break

            # Wall-clock pacing relative to recording timestamps
            elapsed_rec = ts - first_ts
            target_wall = elapsed_rec / self.speed
            actual_wall = time.perf_counter() - start_wall
            to_sleep = target_wall - actual_wall

            if to_sleep > 0.002:
                await asyncio.sleep(to_sleep)
            else:
                await asyncio.sleep(0)  # yield to event loop

            if self.on_message:
                try:
                    self.on_message(topic, data, ts)
                    self._processed += 1
                except Exception as e:
                    self._errors += 1
                    if self._errors <= 3:
                        logger.warning(f"on_message error [{topic}]: {e}")

            # Periodic status update
            count = i + 1
            if count % status_interval == 0:
                pct = count * 100 // total
                logger.info(f"LiveReplayFeed: {count}/{total} ({pct}%), {self._errors} errors")

        logger.info(
            f"LiveReplayFeed complete: {self._processed}/{total} "
            f"({self._errors} errors)"
        )
        self._running = False

    async def stop(self):
        """Stop the replay."""
        self._running = False
        logger.info("LiveReplayFeed stopped")
