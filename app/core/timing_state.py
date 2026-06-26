"""Timing state — extracted from DataProcessor._handle_TimingData."""
from __future__ import annotations
import logging
from ..models import TimingEntry

logger = logging.getLogger(__name__)


class TimingState:
    """Manages driver timing state independently of DataProcessor."""

    def __init__(self):
        self.timing: dict[int, TimingEntry] = {}

    def apply_event(self, topic: str, data: dict, ts: float):
        """Process a TimingData event from EventBus."""
        if topic != "TimingData":
            return
        lines = data.get("Lines", {})
        if not lines:
            return
        for num_str, td in lines.items():
            try:
                num = int(num_str)
            except ValueError:
                continue
            entry = self.timing.get(num, TimingEntry(driver_number=num))
            for k, v in td.items():
                if k == "Position":
                    entry.position = v
                elif k == "LapNumber":
                    entry.lap_number = v
                elif k == "GapToLeader":
                    entry.gap_to_leader = v if v and v != "None" else None
                elif k == "Interval":
                    entry.interval = v if v and v != "None" else None
                elif k == "LastLapTime":
                    entry.last_lap_time = v if v and v != "None" else None
                elif k == "BestLapTime":
                    entry.best_lap_time = v if v and v != "None" else None
                elif k == "Sector1":
                    entry.sector1 = v if v and v != "None" else None
                elif k == "Sector2":
                    entry.sector2 = v if v and v != "None" else None
                elif k == "Sector3":
                    entry.sector3 = v if v and v != "None" else None
                elif k == "NumberOfPitStops":
                    entry.pit_stop_count = v
                elif k == "Retired":
                    entry.retired = v
                elif k == "Segments":
                    entry.segments = v if isinstance(v, list) else []
            self.timing[num] = entry

    def get_snapshot(self) -> dict[int, dict]:
        return {n: t.__dict__ for n, t in self.timing.items()}
