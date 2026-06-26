"""Simple async event bus for F1 data topics."""
from __future__ import annotations
import asyncio
import logging
from collections import defaultdict
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)

EventHandler = Callable[[str, dict, float], Awaitable[None]]


class EventBus:
    """Async pub/sub event bus for F1 data topics."""

    def __init__(self):
        self._handlers: dict[str, list[EventHandler]] = defaultdict(list)
        self._lock = asyncio.Lock()

    def subscribe(self, topic: str, handler: EventHandler):
        """Subscribe to a specific topic."""
        self._handlers[topic].append(handler)

    def subscribe_all(self, handler: EventHandler):
        """Subscribe to ALL topics (wildcard)."""
        self._handlers["*"].append(handler)

    async def emit(self, topic: str, data: dict, ts: float):
        """Emit an event to all matching subscribers."""
        handlers = list(self._handlers.get(topic, [])) + list(self._handlers.get("*", []))
        for handler in handlers:
            try:
                await handler(topic, data, ts)
            except Exception as e:
                logger.exception(f"EventBus handler error for '{topic}': {e}")

    def unsubscribe(self, topic: str, handler: EventHandler):
        if handler in self._handlers[topic]:
            self._handlers[topic].remove(handler)
