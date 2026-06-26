"""Local data storage with auto-cleanup (7 days retention)."""
from __future__ import annotations
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "data")


def cleanup_old_data(days: int = 7):
    if not os.path.exists(DATA_DIR):
        return
    cutoff = time.time() - days * 86400
    for name in os.listdir(DATA_DIR):
        dirpath = os.path.join(DATA_DIR, name)
        if os.path.isdir(dirpath):
            try:
                t = datetime.strptime(name, "%Y-%m-%d").timestamp()
                if t < cutoff:
                    import shutil
                    shutil.rmtree(dirpath)
                    logger.info(f"Cleaned up: {name}")
            except ValueError:
                continue


class SessionRecorder:
    def __init__(self):
        self._writers: dict[str, Any] = {}
        self._today = ""
        self._write_count = 0
        self._flush_interval = 50
        cleanup_old_data()

    def _ensure_dir(self):
        self._today = datetime.now().strftime("%Y-%m-%d")
        os.makedirs(os.path.join(DATA_DIR, self._today), exist_ok=True)

    def _get_writer(self, topic: str):
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self._today:
            self._ensure_dir()
        if topic not in self._writers:
            path = os.path.join(DATA_DIR, self._today, f"{topic}.jsonl")
            self._writers[topic] = open(path, "a", encoding="utf-8")
        return self._writers[topic]

    def record(self, topic: str, data: Any, timestamp: float):
        try:
            w = self._get_writer(topic)
            w.write(json.dumps({"t": timestamp, "topic": topic, "data": data}, default=str) + "\n")
            self._write_count += 1
            if self._write_count >= self._flush_interval:
                for w in self._writers.values():
                    w.flush()
                self._write_count = 0
        except Exception as e:
            logger.error(f"Record failed: {e}")

    def close(self):
        for w in self._writers.values():
            try: w.close()
            except: pass
        self._writers.clear()
