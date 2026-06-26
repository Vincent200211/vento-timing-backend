"""Abstract base class for all F1 data feeds."""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class FeedMessage:
    """A decoded message from an F1 data feed."""
    topic: str
    data: dict
    timestamp: float


class BaseFeed(ABC):
    """Abstract base class for all F1 data feed sources."""

    @abstractmethod
    async def start(self):
        """Start the feed (connect / begin streaming)."""
        ...

    @abstractmethod
    async def stop(self):
        """Stop the feed and clean up resources."""
        ...

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Whether the feed is currently connected."""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable feed name (e.g. 'signalr', 'replay', 'mock')."""
        ...
