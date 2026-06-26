"""Race control messages state."""
from __future__ import annotations
import logging
from typing import Any, Optional
from ..models import RaceControlMessage

logger = logging.getLogger(__name__)


class RaceControlState:
    """Tracks race control messages and timing app data."""

    def __init__(self):
        self.race_control_messages: list[RaceControlMessage] = []
        self.app_data: dict[int, dict] = {}
        self.stint_history: dict[int, list] = {}

    def apply_event(self, topic: str, data: dict, ts: float):
        if topic == "RaceControlMessages":
            messages = data.get("Messages", {})
            for msg_id, msg in messages.items():
                rcm = RaceControlMessage(
                    timestamp=ts, category=msg.get("Category", ""),
                    message=msg.get("Message", ""), flag=msg.get("Flag"),
                    lap=msg.get("Lap"),
                )
                if not any(m.message == rcm.message and m.lap == rcm.lap for m in self.race_control_messages):
                    self.race_control_messages.append(rcm)
                if len(self.race_control_messages) > 200:
                    self.race_control_messages = self.race_control_messages[-100:]
        elif topic == "TimingAppData":
            lines = data.get("Lines", {})
            for num_str, ad in lines.items():
                try:
                    num = int(num_str)
                except ValueError:
                    continue
                if num not in self.app_data:
                    self.app_data[num] = {}
                if "Stint" in ad:
                    self.app_data[num]["stint"] = ad["Stint"]
                if "TyreAge" in ad:
                    self.app_data[num]["tyre_age"] = ad["TyreAge"]
                if "FreshTyre" in ad:
                    self.app_data[num]["fresh_tyre"] = ad["FreshTyre"]
                if "Compound" in ad:
                    new_compound = ad["Compound"]
                    old_compound = self.app_data[num].get("compound")
                    if new_compound != old_compound and old_compound is not None:
                        cur_lap = 0  # will be overridden by session late
                        if num not in self.stint_history:
                            self.stint_history[num] = []
                        self.stint_history[num].append({
                            "compound": old_compound,
                            "lap_count": cur_lap,
                        })
                    self.app_data[num]["compound"] = new_compound

    def get_snapshot(self) -> dict:
        def d(v):
            return v.__dict__ if hasattr(v, '__dict__') else v
        return {
            "race_control": [d(m) for m in self.race_control_messages],
            "app_data": self.app_data,
            "stint_history": self.stint_history,
        }
