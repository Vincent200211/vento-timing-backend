"""Position state — processes Position.z into GPS + G-force + lap tracking."""
from __future__ import annotations
import logging
import math
from collections import defaultdict
from typing import Any, Optional
from ..models import CarPosition

logger = logging.getLogger(__name__)


def _haversine_distance(x1, y1, x2, y2) -> float:
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)


class PositionState:
    """Tracks car GPS positions independently of DataProcessor."""

    def __init__(self):
        self.positions: dict[int, list[CarPosition]] = defaultdict(list)
        self.prev_positions: dict[int, CarPosition] = {}
        self._track_center_x = 0.0
        self._track_center_y = 0.0
        self._center_samples = 0
        self._center_sum_x = 0.0
        self._center_sum_y = 0.0
        self._cumulative_angle: dict[int, float] = {}
        self._pos_buf: dict[int, list[tuple[float, float]]] = {}
        self._smooth_pos: dict[int, tuple[float, float]] = {}
        self._smooth_chain: dict[int, list[tuple[float, float, float]]] = {}
        self._lat_buf: dict[int, list[float]] = {}
        self._gps_prev_speed_ms: dict[int, float] = {}
        self._gps_prev_ts: dict[int, float] = {}
        self._gps_lon_g_buf: dict[int, list[float]] = {}
        self._last_race_ts: float = 0.0

    def _compute_angle(self, x: float, y: float) -> float:
        if self._center_samples < 10:
            if self._center_samples == 0:
                self._center_sum_x = 0.0
                self._center_sum_y = 0.0
            self._center_sum_x += x
            self._center_sum_y += y
            self._center_samples += 1
            if self._center_samples == 10:
                self._track_center_x = self._center_sum_x / 10.0
                self._track_center_y = self._center_sum_y / 10.0
            return 0.0
        angle_rad = math.atan2(y - self._track_center_y, x - self._track_center_x)
        angle_deg = math.degrees(angle_rad)
        if angle_deg < 0:
            angle_deg += 360
        return angle_deg

    def _update_cumulative_angle(self, dn: int, angle_deg: float) -> float:
        prev = self._cumulative_angle.get(dn)
        if prev is None:
            self._cumulative_angle[dn] = angle_deg
            return angle_deg
        prev_mod = prev % 360
        diff = angle_deg - prev_mod
        if diff < -180:
            diff += 360
        cum = prev + diff
        self._cumulative_angle[dn] = cum
        return cum

    def apply_event(self, topic: str, data: dict, ts: float):
        if topic != "Position":
            return
        if "Position" in data:
            positions = data["Position"]
            entries_list = []
            for pos_entry in positions if isinstance(positions, list) else [positions]:
                cars_dict = {}
                for dn_str, cd in pos_entry.get("Entries", {}).items():
                    cars_dict[dn_str] = {
                        "X": cd.get("X", 0),
                        "Y": cd.get("Y", 0),
                        "Z": cd.get("Z", 0),
                        "Status": cd.get("Status", ""),
                    }
                entries_list.append({"Cars": cars_dict})
            data = {"Entries": entries_list}
        entries = data.get("Entries", [])
        for entry in entries:
            cars = entry.get("Cars", {})
            for num_str, pos in cars.items():
                try:
                    num = int(num_str)
                except ValueError:
                    continue
                pos_ts = self._last_race_ts if self._last_race_ts > 0 else ts
                cp = CarPosition(
                    timestamp=pos_ts, driver_number=num,
                    x=pos.get("X", 0), y=pos.get("Y", 0),
                    z=pos.get("Z"), status=pos.get("Status"),
                )
                cp.angle = self._compute_angle(cp.x, cp.y)
                cp.cumulative_angle = self._update_cumulative_angle(num, cp.angle)
                cp.lap = int(cp.cumulative_angle / 360) if cp.cumulative_angle else 0

                buf = self._pos_buf.get(num, [])
                buf.append((cp.x, cp.y))
                if len(buf) > 7:
                    buf.pop(0)
                self._pos_buf[num] = buf
                sx = sum(p[0] for p in buf) / len(buf)
                sy = sum(p[1] for p in buf) / len(buf)
                self._smooth_pos[num] = (sx, sy)

                s_chain = self._smooth_chain.get(num, [])
                if s_chain:
                    s1 = s_chain[-1]
                    dt = pos_ts - s1[2]
                    if dt > 0:
                        dist = _haversine_distance(s1[0], s1[1], sx, sy)
                        cp.speed_ms = dist / dt
                    else:
                        cp.speed_ms = 0

                    if len(s_chain) >= 7:
                        s0 = s_chain[-7]
                        s2 = s_chain[-4]
                        dt1 = s2[2] - s0[2]
                        dt2 = pos_ts - s2[2]
                        if 0.1 < dt1 < 4.0 and 0.1 < dt2 < 4.0:
                            v1x = (s2[0] - s0[0]) / dt1
                            v1y = (s2[1] - s0[1]) / dt1
                            v2x = (sx - s2[0]) / dt2
                            v2y = (sy - s2[1]) / dt2
                            speed1 = cp.speed_ms if cp.speed_ms > 1 else math.sqrt(v1x**2 + v1y**2)

                            ax, ay = s0[0], s0[1]
                            bx, by = s2[0], s2[1]
                            cx, cy = sx, sy
                            dx1 = bx - ax; dy1 = by - ay
                            dx2 = cx - bx; dy2 = cy - by
                            ab = math.sqrt(dx1*dx1 + dy1*dy1)
                            bc = math.sqrt(dx2*dx2 + dy2*dy2)
                            cross = dx1*dy2 - dy1*dx2
                            area2 = abs(cross)
                            if area2 > 0.1:
                                ac = math.sqrt((cx-ax)*(cx-ax) + (cy-ay)*(cy-ay))
                                R = ab * bc * ac / (2.0 * area2)
                                lat_accel = speed1 * speed1 / R
                                if cross < 0:
                                    lat_accel = -lat_accel
                            else:
                                lat_accel = 0.0
                            raw_g = lat_accel / 9.81
                            lbuf = self._lat_buf.get(num, [])
                            lbuf.append(raw_g)
                            if len(lbuf) > 3:
                                lbuf.pop(0)
                            self._lat_buf[num] = lbuf
                            cp.lateral_g = sum(lbuf) / len(lbuf)

                            prev_spd = self._gps_prev_speed_ms.get(num)
                            prev_ts2 = self._gps_prev_ts.get(num)
                            if prev_spd is not None and prev_ts2 is not None:
                                gps_dt = pos_ts - prev_ts2
                                if 0.05 < gps_dt < 4.0:
                                    gps_dv = speed1 - prev_spd
                                    gps_lon_raw = (gps_dv / gps_dt) / 9.81
                                    glb = self._gps_lon_g_buf.get(num, [])
                                    glb.append(gps_lon_raw)
                                    if len(glb) > 3:
                                        glb.pop(0)
                                    self._gps_lon_g_buf[num] = glb
                                    cp.longitudinal_g = sum(glb) / len(glb)
                            self._gps_prev_speed_ms[num] = speed1
                            self._gps_prev_ts[num] = pos_ts
                else:
                    cp.speed_ms = 0

                s_chain.append((sx, sy, pos_ts))
                if len(s_chain) > 7:
                    s_chain.pop(0)
                self._smooth_chain[num] = s_chain

                if cp.cumulative_angle:
                    cp.track_percentage = (cp.cumulative_angle % 360) / 360.0

                if pos_ts > self._last_race_ts:
                    self._last_race_ts = pos_ts

                self.prev_positions[num] = cp
                self.positions[num].append(cp)
                if len(self.positions[num]) > 1000:
                    self.positions[num] = self.positions[num][-1000:]

    def get_snapshot(self) -> dict[int, list[dict]]:
        return {n: [p.__dict__ for p in pts[-3:]] for n, pts in self.positions.items()}

    def get_positions(self, dn: int, limit: int = 200) -> list[dict]:
        pts = self.positions.get(dn, [])
        return [p.__dict__ for p in pts[-limit:]]
