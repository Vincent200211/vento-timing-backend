"""Session state — session info, driver list, lap count."""
from __future__ import annotations
import logging
from typing import Any, Optional
from ..models import SessionInfo, DriverInfo, LapCount
from ..circuit_data import match_circuit, load_circuits

logger = logging.getLogger(__name__)


class SessionState:
    """Tracks session metadata, driver list, and lap count."""

    def __init__(self):
        self.session_info = SessionInfo()
        self.drivers: dict[int, DriverInfo] = {}
        self.lap_count: Optional[LapCount] = None
        self.circuits = load_circuits()
        self.current_circuit = None

    def apply_event(self, topic: str, data: dict, ts: float):
        if topic == "SessionInfo":
            meeting = data.get("Meeting", {})
            self.session_info = SessionInfo(
                meeting_key=meeting.get("Key"),
                session_key=data.get("Key"),
                meeting_name=meeting.get("Name", ""),
                session_name=data.get("Name", ""),
                circuit_short=meeting.get("Circuit", {}).get("ShortName", ""),
                country=meeting.get("Country", {}).get("Name", ""),
                session_status=data.get("SessionStatus", ""),
                session_type=data.get("Type", ""),
                start_date=data.get("StartDate"),
                end_date=data.get("EndDate"),
                gmt_offset=data.get("GmtOffset"),
            )
            if meeting.get("Name") or meeting.get("Circuit", {}).get("ShortName"):
                matched = match_circuit(meeting.get("Name", ""), meeting.get("Circuit", {}).get("ShortName", ""), self.circuits)
                if matched:
                    self.current_circuit = matched
        elif topic == "DriverList":
            lines = data.get("Lines", data)
            for num_str, driver in lines.items():
                if not num_str.isdigit():
                    continue
                num = int(num_str)
                self.drivers[num] = DriverInfo(
                    driver_number=num,
                    racing_number=driver.get("RacingNumber", num),
                    broadcast_name=driver.get("BroadcastName", ""),
                    full_name=driver.get("FullName", ""),
                    first_name=driver.get("FirstName", ""),
                    last_name=driver.get("LastName", ""),
                    team_name=driver.get("TeamName", ""),
                    team_colour=driver.get("TeamColour", "#cccccc"),
                    headshot_url=driver.get("HeadshotUrl"),
                    country_code=driver.get("CountryCode") or "",
                    tla=driver.get("Tla", ""),
                )
        elif topic == "LapCount":
            self.lap_count = LapCount(
                total_laps=data.get("TotalLaps", 0),
                current_lap=data.get("CurrentLap", 0),
            )

    def get_snapshot(self) -> dict:
        def d(v):
            return v.__dict__ if hasattr(v, '__dict__') else v
        return {
            "session": d(self.session_info),
            "drivers": {n: d(drv) for n, drv in self.drivers.items()},
            "lap_count": d(self.lap_count) if self.lap_count else None,
        }
