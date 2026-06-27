"""Data models for F1 live timing data."""
from __future__ import annotations
from typing import Optional, Any
from pydantic import BaseModel, Field


class CarTelemetry(BaseModel):
    """Car telemetry data point from CarData.z"""
    timestamp: float
    driver_number: int
    rpm: int
    speed: float
    throttle: float
    brake: float = 0.0
    drs: Optional[int] = None
    n_gear: int = 0
    speed_ms: Optional[float] = None
    acceleration: Optional[float] = None
    angle: Optional[float] = None
    lap: Optional[int] = None
    track_percentage: Optional[float] = None
    distance: Optional[float] = None


class CarPosition(BaseModel):
    """GPS position data from Position.z"""
    timestamp: float
    driver_number: int
    x: float
    y: float
    z: Optional[float] = None
    status: Optional[str] = None
    speed_ms: Optional[float] = None
    lateral_g: Optional[float] = None
    longitudinal_g: Optional[float] = None
    angle: float = 0.0
    lap: int = 0
    cumulative_angle: float = 0.0
    track_percentage: float = 0.0
    distance: float = 0.0


class TimingEntry(BaseModel):
    """Timing data for a single driver"""
    driver_number: int
    position: int | str = 0
    gap_to_leader: Optional[str] = None
    interval: Optional[str] = None
    lap_number: int = 0
    last_lap_time: Optional[dict | str] = None
    best_lap_time: Optional[dict | str] = None
    sector1: Optional[str] = None
    sector2: Optional[str] = None
    sector3: Optional[str] = None
    pit_stop_count: Optional[int] = None
    retired: bool = False
    # Segments: mini-sector colors from online feed (2049=green, 2050=yellow, 2051=red)
    segments: list[Optional[int]] = Field(default_factory=list)


class TimingAppDataEntry(BaseModel):
    """Per-driver application data (tyres, stints)"""
    driver_number: int
    stint: int = 0
    compound: Optional[str] = None
    tyre_age: int = 0
    fresh_tyre: bool = False


class DriverInfo(BaseModel):
    """Driver information"""
    driver_number: int
    racing_number: int = 0
    broadcast_name: str = ''
    full_name: str = ''
    first_name: str = ''
    last_name: str = ''
    team_name: str = ''
    team_colour: str = ''
    headshot_url: Optional[str] = None
    country_code: str = ''
    tla: str = ''


class SessionInfo(BaseModel):
    """Current session information"""
    meeting_key: Optional[int] = None
    session_key: Optional[int] = None
    meeting_name: str = ''
    session_name: str = ''
    circuit_short: str = ''
    country: str = ''
    session_status: str = ''
    session_type: str = ''
    start_date: Optional[str] = None
    end_date: Optional[str] = None
   gmt_offset: Optional[str] = None
    remaining: Optional[str] = None


class WeatherData(BaseModel):
    """Weather data point"""
    timestamp: float
    air_temperature: float = 0.0
    track_temperature: float = 0.0
    humidity: float = 0.0
    pressure: float = 0.0
    wind_speed: float = 0.0
    wind_direction: float = 0.0
    rainfall: bool = False


class TrackStatus(BaseModel):
    """Track status"""
    status: str
    message: str = ''


class RaceControlMessage(BaseModel):
    """Race control message"""
    timestamp: float
    category: str = ''
    message: str = ''
    flag: Optional[str] = None
    driver_numbers: Optional[list[int]] = None
    lap: Optional[int] = None


class LapCount(BaseModel):
    total_laps: int = 0
    current_lap: int = 0


class SessionSnapshot(BaseModel):
    """Complete snapshot of current session state"""
    session: SessionInfo = SessionInfo()
    drivers: dict[int, DriverInfo] = {}
    timing: dict[int, TimingEntry] = {}
    app_data: dict[int, TimingAppDataEntry] = {}
    car_data: dict[int, list[CarTelemetry]] = {}
    positions: dict[int, list[CarPosition]] = {}
    weather: Optional[WeatherData] = None
    track_status: Optional[TrackStatus] = None
    lap_count: Optional[LapCount] = None
    race_control_messages: list[RaceControlMessage] = []
