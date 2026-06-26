"""Weather + track status state."""
from __future__ import annotations
import logging
from typing import Any, Optional
from ..models import WeatherData, TrackStatus

logger = logging.getLogger(__name__)


class WeatherState:
    """Tracks weather conditions and track status independently."""

    def __init__(self):
        self.weather: Optional[WeatherData] = None
        self.track_status: Optional[TrackStatus] = None

    def apply_event(self, topic: str, data: dict, ts: float):
        if topic == "WeatherData":
            self.weather = WeatherData(
                timestamp=ts,
                air_temperature=data.get("AirTemp", 0),
                track_temperature=data.get("TrackTemp", 0),
                humidity=data.get("Humidity", 0),
                pressure=data.get("Pressure", 0),
                wind_speed=data.get("WindSpeed", 0),
                wind_direction=data.get("WindDirection", 0),
                rainfall=data.get("Rainfall", False),
            )
        elif topic == "TrackStatus":
            status_map = {
                "1": "Green", "2": "Yellow", "3": "Yellow",
                "4": "Yellow", "5": "Red", "6": "Red",
                "7": "SafetyCar", "8": "VirtualSafetyCar",
            }
            code = str(data.get("Status", ""))
            status = status_map.get(code, f"Code_{code}")
            self.track_status = TrackStatus(
                status=status,
                message=data.get("Message", ""),
            )

    def get_snapshot(self) -> dict:
        def d(v):
            return v.__dict__ if hasattr(v, '__dict__') else v
        return {
            "weather": d(self.weather) if self.weather else None,
            "track_status": d(self.track_status) if self.track_status else None,
        }
