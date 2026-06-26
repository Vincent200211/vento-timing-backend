"""F1 Live Timing SignalR Core Client using signalrcore library."""
from __future__ import annotations
import asyncio
import logging
import os
import time
from typing import Any, Callable, Optional
import httpx
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.messages.completion_message import CompletionMessage
from ..codec.decoder import decode_topic_data
from .base import BaseFeed
logger = logging.getLogger(__name__)
F1_WS_URL = "wss://livetiming.formula1.com/signalrcore"
F1_NEGOTIATE_URL = "https://livetiming.formula1.com/signalrcore/negotiate?negotiateVersion=1"
TOPICS = [
    "Heartbeat", "CarData.z", "Position.z", "ExtrapolatedClock",
    "TopThree", "TimingStats", "TimingAppData", "WeatherData",
    "TrackStatus", "DriverList", "RaceControlMessages",
    "SessionInfo", "SessionData", "LapCount", "TimingData",
    "TeamRadio", "AudioStreams", "ContentStreams",
]
class F1SignalRClient(BaseFeed):
    def __init__(self, token: str = "", on_message: Optional[Callable] = None, topics: Optional[list[str]] = None):
        self.token = token or os.environ.get("F1_TOKEN", "")
        self.on_message = on_message
        self.topics = topics or TOPICS
        self._connection = None
        self._running = False
    @property
    def is_connected(self) -> bool: return self._running and self._connection is not None
    @property
    def name(self) -> str: return "signalr"
    async def start(self):
        self._running = True
        loop = asyncio.get_running_loop()
        await asyncio.to_thread(self._run_sync, loop)
    def _run_sync(self, loop: asyncio.AbstractEventLoop):
        aws_cookie = ""
        try:
            resp = httpx.options(F1_NEGOTIATE_URL, headers={"User-Agent": "BestHTTP"}, timeout=5)
            aws_cookie = resp.cookies.get("AWSALBCORS", "")
        except Exception as e:
            logger.warning(f"AWSALBCORS preflight failed: {e}")
        options = {"access_token_factory": lambda: self.token, "headers": {}}
        if aws_cookie:
            options["headers"]["Cookie"] = f"AWSALBCORS={aws_cookie}"
        builder = HubConnectionBuilder().with_url(F1_WS_URL, options=options)
        builder.keep_alive_interval = 5
        self._connection = builder.build()
        def on_feed(args):
            loop.call_soon_threadsafe(self._handle_feed, args)
        def on_connect():
            logger.info("SignalR connected, subscribing...")
            self._connection.send("Subscribe", [self.topics],
                on_invocation=lambda msg: loop.call_soon_threadsafe(self._handle_invocation, msg))
        self._connection.on("feed", on_feed)
        self._connection.on_open(on_connect)
        logger.info("SignalR starting (thread blocks)...")
        self._connection.start()
        logger.info("SignalR connection ended")
    def _handle_feed(self, args: Any):
        if not isinstance(args, list) or len(args) < 2: return
        topic, raw_data = args[0], args[1]
        try:
            decoded = decode_topic_data(raw_data) if isinstance(raw_data, str) else raw_data
        except Exception:
            decoded = raw_data
        if self.on_message:
            try: self.on_message(topic, decoded, time.time())
            except Exception as e: logger.error(f"on_message error for {topic}: {e}")
    def _handle_invocation(self, msg: Any):
        if isinstance(msg, CompletionMessage):
            result = msg.result
            if isinstance(result, dict):
                for topic, data in result.items():
                    if data and isinstance(data, (dict, list)) and len(data) > 0:
                        if self.on_message:
                            try: self.on_message(topic, data if isinstance(data, dict) else {"_list": data}, time.time())
                            except Exception as e: logger.error(f"invocation error for {topic}: {e}")
        elif hasattr(msg, "item"):
            item = msg.item
            if isinstance(item, dict):
                topic = item.get("topic", item.get("Topic", ""))
                sdata = item.get("data", item)
                if topic and self.on_message:
                    try: self.on_message(topic, sdata, time.time())
                    except Exception as e: logger.error(f"stream error for {topic}: {e}")
    async def stop(self):
        self._running = False
        if self._connection:
            try: self._connection.stop()
            except Exception as e: logger.warning(f"SignalR stop error: {e}")
            self._connection = None
