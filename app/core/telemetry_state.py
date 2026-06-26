"""Telemetry state — processes CarData.z into telemetry data."""
from __future__ import annotations
import logging
import math
from collections import defaultdict
from typing import Any, Optional
from ..models import CarTelemetry

logger = logging.getLogger(__name__)


class TelemetryState:
    """Tracks car telemetry data (speed, RPM, throttle, etc.)."""

    def __init__(self):
        self.car_data: dict[int, list[CarTelemetry]] = defaultdict(list)
        self._car_prev_speed_ms: dict[int, float] = {}
        self._car_prev_ts: dict[int, float] = {}
        self._lon_g_buf: dict[int, list[float]] = {}
        self._latest_lon_g: dict[int, float] = {}
        self._car_data_distance: dict[int, float] = {}
        self._car_data_prev_time: dict[int, float] = {}
        self._car_lap_counter: dict[int, int] = {}
        self._last_race_ts: float = 0.0

    def apply_event(self, topic: str, data: dict, ts: float):
        if topic != "CarData":
            return
        entries = data.get("Entries", [])
        for entry in entries:
            cars = entry.get("Cars", {})
            for num_str, car in cars.items():
                try:
                    num = int(num_str)
                except ValueError:
                    continue
                channels = car.get("Channels")
                if channels is not None and isinstance(channels, dict):
                    car["RPM"] = int(float(channels.get("0", 0) or 0))
                    car["Speed"] = float(channels.get("2", 0) or 0)
                    car["nGear"] = int(float(channels.get("3", 0) or 0))
                    car["Throttle"] = float(channels.get("4", 0) or 0)
                    car["Brake"] = float(channels.get("5", 0) or 0)
                elif car.get("Speed") is None:
                    car["Speed"] = 0
                    car["RPM"] = 0
                    car["nGear"] = 0
                    car["Throttle"] = 0
                    car["Brake"] = 0

                speed_kmh = float(car.get("Speed") or 0)
                speed_ms = speed_kmh / 3.6
                raw_ts = ts
                date_str = car.get("date")
                if date_str:
                    try:
                        from datetime import datetime
                        raw_ts = datetime.fromisoformat(date_str.replace("Z", "+00:00")).timestamp()
                    except Exception:
                        pass
                if raw_ts > self._last_race_ts:
                    self._last_race_ts = raw_ts

                prev_spd = self._car_prev_speed_ms.get(num)
                prev_ts = self._car_prev_ts.get(num)
                acceleration = None
                if prev_spd is not None and prev_ts is not None:
                    dt = raw_ts - prev_ts
                    if 0.05 < dt < 2.0:
                        dv = speed_ms - prev_spd
                        raw_g = (dv / dt) / 9.81
                        buf = self._lon_g_buf.get(num, [])
                        buf.append(raw_g)
                        if len(buf) > 3:
                            buf.pop(0)
                        self._lon_g_buf[num] = buf
                        self._latest_lon_g[num] = sum(buf) / len(buf)
                        acceleration = dv / dt
                self._car_prev_speed_ms[num] = speed_ms
                self._car_prev_ts[num] = raw_ts

                prev_odo_ts = self._car_data_prev_time.get(num)
                if prev_odo_ts is not None:
                    dt = raw_ts - prev_odo_ts
                    if 0 < dt < 3.0:
                        self._car_data_distance[num] = self._car_data_distance.get(num, 0.0) + speed_ms * dt
                else:
                    self._car_data_distance[num] = self._car_data_distance.get(num, 0.0)
                _circuit_len = 4655
                if _circuit_len > 0 and self._car_data_distance.get(num, 0) >= _circuit_len:
                    self._car_data_distance[num] -= _circuit_len
                    self._car_lap_counter[num] = self._car_lap_counter.get(num, 0) + 1
                self._car_data_prev_time[num] = raw_ts

                raw_brake = car.get("Brake", 0)
                if isinstance(raw_brake, bool):
                    brake_val = 1.0 if raw_brake else 0.0
                elif isinstance(raw_brake, (int, float)):
                    brake_val = float(raw_brake)
                    if brake_val > 1:
                        brake_val = brake_val / 100.0
                else:
                    brake_val = 0.0
                brake_val = max(0.0, min(1.0, brake_val))

                tm = CarTelemetry(
                    timestamp=ts, driver_number=num, speed=speed_kmh,
                    rpm=car.get("RPM", 0),
                    throttle=car.get("Throttle", 0),
                    brake=brake_val,
                    drs=car.get("DRS"), n_gear=car.get("nGear", 0),
                    speed_ms=round(speed_ms, 2),
                    acceleration=round(acceleration, 3) if acceleration is not None else None,
                    distance=self._car_data_distance.get(num, 0.0),
                    lap=self._car_lap_counter.get(num, 0),
                )
                self.car_data[num].append(tm)
                if len(self.car_data[num]) > 2000:
                    self.car_data[num] = self.car_data[num][-2000:]

    def get_snapshot(self) -> dict[int, list[dict]]:
        return {n: [c.__dict__ for c in pts[-3:]] for n, pts in self.car_data.items()}

    def get_car_data(self, dn: int, limit: int = 3000) -> list[dict]:
        pts = self.car_data.get(dn, [])
        return [c.__dict__ for c in pts[-limit:]]
