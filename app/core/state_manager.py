"""State manager — aggregates EventBus events and delegates to sub-modules."""
from __future__ import annotations
import logging
from typing import Any, Optional

from .event_bus import EventBus
from ..data_processor import DataProcessor
from .timing_state import TimingState
from .position_state import PositionState
from .telemetry_state import TelemetryState
from .weather_state import WeatherState
from .session_state import SessionState
from .race_control_state import RaceControlState

logger = logging.getLogger(__name__)


class StateManager:
    """Aggregates EventBus events and provides unified data access.

    Holds all domain-specific state modules and falls back to
    DataProcessor for complex logic not yet migrated.
    """

    def __init__(self, event_bus: EventBus):
        self._bus = event_bus
        from ..data_processor import DataProcessor
        self._processor = DataProcessor()

        # Independent state modules (event-driven)
        self.timing_state = TimingState()
        self.position_state = PositionState()
        self.telemetry_state = TelemetryState()
        self.weather_module = WeatherState()
        self.session_module = SessionState()
        self.race_control_module = RaceControlState()

    def connect_event_bus(self):
        """Subscribe all state modules to EventBus topics."""
        async def _wrap(fn, topic, data, ts):
            fn(topic, data, ts)
        subs = [
            ("TimingData", self.timing_state),
            ("Position", self.position_state),
            ("CarData", self.telemetry_state),
            ("WeatherData", self.weather_module),
            ("TrackStatus", self.weather_module),
            ("SessionInfo", self.session_module),
            ("DriverList", self.session_module),
            ("LapCount", self.session_module),
            ("RaceControlMessages", self.race_control_module),
            ("TimingAppData", self.race_control_module),
        ]
        for topic, mod in subs:
            def _make_handler(m):
                async def h(t, d, ts): m.apply_event(t, d, ts)
                return h
            self._bus.subscribe(topic, _make_handler(mod))
        # Also subscribe internal DataProcessor to ALL topics (safety net)
        async def _proc_h(t, d, ts):
            self._processor.process_message(t, d, ts)
        self._bus.subscribe_all(_proc_h)
        logger.info(f"StateManager: subscribed {len(subs)} handlers + processor")

    def register_state(self, topic: str, module: Any):
        """Register a sub-state module for a topic (legacy)."""
        pass

    # ── Backward-compatible DataProcessor interface ──

    def get_snapshot(self) -> dict:
        """Return snapshot from internal DataProcessor (safe fallback)."""
        return self._processor.get_snapshot()
        def d(v):
            if hasattr(v, 'model_dump'): return v.model_dump()
            if hasattr(v, '__dict__'): return v.__dict__
            return v
        snap = self._processor.get_snapshot()
        snap['session'] = d(self.session_module.session_info)
        snap['drivers'] = {n: d(drv) for n, drv in self.session_module.drivers.items()}
        snap['weather'] = d(self.weather_module.weather) if self.weather_module.weather else None
        snap['track_status'] = d(self.weather_module.track_status) if self.weather_module.track_status else None
        snap['lap_count'] = d(self.session_module.lap_count) if self.session_module.lap_count else None
        snap['race_control'] = [d(m) for m in self.race_control_module.race_control_messages]
        return snap

    def get_field(self, btype: str):
        return self._processor.get_field(btype)

    def get_sorted_timing(self) -> list[dict]:
        return self._processor.get_sorted_timing()

    def get_track_positions(self) -> list[dict]:
        return self._processor.get_track_positions()

    @property
    def session_info(self):
        return self._processor.session_info

    @property
    def drivers(self):
        return self._processor.drivers

    @property
    def timing(self):
        return self._processor.timing

    @property
    def app_data(self):
        return self._processor.app_data

    @property
    def car_data(self):
        return self._processor.car_data

    @property
    def positions(self):
        return self._processor.positions

    @property
    def weather(self):
        return self._processor.weather

    @property
    def track_status(self):
        return self._processor.track_status

    @property
    def lap_count(self):
        return self._processor.lap_count

    @property
    def race_control_messages(self):
        return self._processor.race_control_messages

    @property
    def current_circuit(self):
        return self._processor.current_circuit

    @property
    def circuits(self):
        return self._processor.circuits

    def reset(self):
        self._processor.reset()

    def full_reset(self):
        self._processor.reset()
        for mod in [self.timing_state]:
            if hasattr(mod, '__init__'):
                mod.__init__()
        logger.info("StateManager: full reset complete")
        def _d(v):
            if hasattr(v, 'model_dump'): return v.model_dump()
            if hasattr(v, '__dict__'): return v.__dict__
            return v
        snap = self._processor.get_snapshot()
        snap['session'] = _d(self.session_module.session_info)
        snap['drivers'] = {n: _d(drv) for n, drv in self.session_module.drivers.items()}
        snap['weather'] = _d(self.weather_module.weather) if self.weather_module.weather else None
        snap['track_status'] = _d(self.weather_module.track_status) if self.weather_module.track_status else None
        snap['lap_count'] = _d(self.session_module.lap_count) if self.session_module.lap_count else None
        snap['race_control'] = [_d(m) for m in self.race_control_module.race_control_messages]
        return snap
