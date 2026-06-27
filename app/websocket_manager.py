"""WebSocket connection manager."""
from __future__ import annotations
import json
import asyncio
from typing import Any
from fastapi import WebSocket


class WebSocketManager:
    """Manages multiple WebSocket client connections."""

    def __init__(self):
        self._connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self._connections.append(websocket)

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self._connections:
                self._connections.remove(websocket)

    async def broadcast(self, msg_type: str, data: Any):
        import logging as _logging
        try:
            message = json.dumps({
                "type": msg_type,
                "data": data,
                "timestamp": __import__('time').time(),
            }, default=str)
        except (MemoryError, OverflowError) as _e:
            _logging.getLogger(__name__).warning(f"Broadcast [{msg_type}] too large, skipped ({_e})")
            return
        async with self._lock:
            connections = list(self._connections)
        results = await asyncio.gather(
            *[ws.send_text(message) for ws in connections],
            return_exceptions=True,
        )
        dead = [ws for ws, r in zip(connections, results) if isinstance(r, Exception)]
        if dead:
            async with self._lock:
                for ws in dead:
                    if ws in self._connections:
                        self._connections.remove(ws)

    @property
    def active_connections(self) -> int:
        return len(self._connections)


manager = WebSocketManager()
