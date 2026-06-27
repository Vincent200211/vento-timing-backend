"""Data processor for F1 live timing streams."""
from __future__ import annotations
import os
import logging
import math
import threading
from collections import defaultdict
from typing import Any, Optional

from .models import (
    CarTelemetry, CarPosition, TimingEntry, TimingAppDataEntry,
    DriverInfo, SessionInfo, WeatherData, TrackStatus,
    RaceControlMessage, LapCount,
)
from .circuit_data import load_circuits, match_circuit
from .track_centerline import load_centerline, project_onto_centerline

logger = logging.getLogger(__name__)


def haversine_distance(x1, y1, x2, y2) -> float:
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)


PIT_LANE_SPEEDS = {
    "Monaco": 60, "Melbourne": 60, "Albert Park": 60,
    "Zandvoort": 60, "Singapore": 60, "Marina Bay": 60, "Miami": 60,
    "Baku": 80, "Jeddah": 80, "Bahrain": 80, "Imola": 80,
    "Barcelona": 80, "Catalunya": 80, "Montreal": 80, "Silverstone": 80,
    "Spa": 80, "Suzuka": 80, "Monza": 80, "Interlagos": 80, "Sao Paulo": 80,
    "Yas Marina": 80, "Abu Dhabi": 80, "Budapest": 80, "Hungaroring": 80,
    "Red Bull Ring": 80, "Spielberg": 80, "Losail": 80, "Lusail": 80,
    "Austin": 80, "COTA": 80, "Mexico": 80, "Hermanos Rodriguez": 80,
    "Las Vegas": 80, "Shanghai": 80, "Portimao": 80, "Istanbul": 80
}

class DataProcessor:
    """Processes incoming F1 data streams into structured state."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.session_info = SessionInfo()
        self.drivers: dict[int, DriverInfo] = {}
        self.timing: dict[int, TimingEntry] = {}
        self.app_data: dict[int, TimingAppDataEntry] = {}
        self.car_data: dict[int, list[CarTelemetry]] = defaultdict(list)
        self.positions: dict[int, list[CarPosition]] = defaultdict(list)
        self.prev_positions: dict[int, CarPosition] = {}
        self.weather: Optional[WeatherData] = None
        self.track_status: Optional[TrackStatus] = None
        self.lap_count: Optional[LapCount] = None
        self.race_control_messages: list[RaceControlMessage] = []
        self.circuits = load_circuits()
        self.current_circuit = None
        # Track center for polar angle (lap detection + rankings)
        self._track_center_x = 0.0
        self._track_center_y = 0.0
        self._center_samples = 0
        self._cumulative_angle: dict[int, float] = {}
        # Speed-based distance tracking (odometry)
        self._car_data_distance: dict[int, float] = {}
        self._car_data_prev_time: dict[int, float] = {}
        self._car_lap_counter: dict[int, int] = {}
        self._car_data_prev_lap: dict[int, int] = {}
        self._smooth_pos: dict[int, tuple] = {}
        self._smooth_chain: dict[int, list] = {}
        self._last_race_ts: float = 0.0
        self._latest_lon_g: dict[int, float] = {}
        self._live_segments: dict[int, list[int]] = {}
        self._car_prev_speed_ms: dict[int, float] = {}
        self._car_prev_ts: dict[int, float] = {}
        self._lon_g_buf: dict[int, list] = {}
        self._lat_buf: dict[int, list] = {}
        self._pos_buf: dict[int, list] = {}
        self._latest_speed_ms: dict[int, float] = {}
        # ---- NEW: Dynamic segment color system ----
        # Currently displayed segments per driver: [22 ints], 2048=unlit
        self._cur_segments: dict[int, list[int]] = {}
        # Full (non-progressive) colors per driver - 22-element array
        self._seg_colors: dict[int, list[int]] = {}
        # Lap sector time caches for comparison
        self._lap_curr: dict[int, dict] = {}
        self._lap_prev: dict[int, dict] = {}
        # Per-sector self-comparison color (before purple override)
        self._sector_self: dict[int, list[int]] = {}
        # Fastest sector times per lap: only keep last 2 laps
        self._sector_best: dict[int, dict] = {}
        # Track lap numbers to prune old data
        self._sector_best_laps: list[int] = []
        self._lap_scale: dict[int, float] = {}
        self._red_flag_active: bool = False
        # Monotonic distance for track ring (never reset, speed*dt integral)
        self._ring_distance: dict[int, float] = {}
        # Finish line capture (first lead car lap+1 XY)
        self._finish_line_x: Optional[float] = None
        self._finish_line_y: Optional[float] = None
        self._finish_last_near: dict[int, bool] = {}
        self._pit_car_distance: dict[int, float] = {}
        # Pit stop tracking
        self._pit_entry_time: dict[int, float] = {}
        self._pit_prev_status: dict[int, str] = {}
        self._pit_durations: list[float] = []
        self._pit_median: Optional[float] = None
        self._pit_max_duration: float = 180.0
        self._pit_entry_distance: dict[int, float] = {}
        self._pit_entry_distance_list: list[float] = []
        self._pit_entry_distance_median: Optional[float] = None
        self._pit_window_delta: Optional[float] = None
        self._pit_entry_speed_default: float = 80.0
        self._field_median_speed_ms: Optional[float] = None
        # Learned pit parameters (from observed pit stops)
        self._pit_entry_ring_distance: dict[int, float] = {}
        self._pit_sector_observations: list[float] = []
        self._pit_sector_length: Optional[float] = None
        self._pit_entry_pos_observations: list[float] = []
        self._pit_exit_pos_observations: list[float] = []
        self._pit_entry_pos_median: Optional[float] = None
        self._pit_exit_pos_median: Optional[float] = None
        self._pit_speed_observations: list[float] = []
        self._pit_session_count: int = 0
        self._stint_history: dict[int, list] = {}
        self._stint_start_laps: dict[int, int] = {}
        # circuits_xy.json lookup: circuit name -> circuit_key
        # Pit window smoothing (EMA)
        self._car_speed_history: dict[int, list] = {}
        self._pit_min_per_lap: dict[int, float] = {}
        self._pit_assessment_cache: dict[int, float] = {}
        self._intra_lap_active = False
        self._current_assessment_lap = 0
        self._circuits_xy: dict = {}
        self._circuit_name_to_key: dict[str, int] = {}
        _xy_path = os.path.join(os.path.dirname(__file__), "circuits_xy.json")
        if os.path.exists(_xy_path):
            import json as _json
            with open(_xy_path) as _f:
                self._circuits_xy = _json.load(_f)
            for ck_str, xy_data in self._circuits_xy.items():
                cname = xy_data.get("circuit_name", "")
                if cname:
                    self._circuit_name_to_key[cname.lower()] = int(ck_str)

        self._load_pit_data()
        # Centerline auto-recording (for new circuits without pre-built centerline)
        self._recording_active: bool = False
        self._recording_positions: list = []
        self._recording_start_angle: float = 0.0
        self._recording_driver: int = -1

        # Centerline projection data (loaded from cache when circuit is matched)
        self._centerline: Optional[dict] = None
        self._proj_idx: dict[int, int] = {}



    @staticmethod
    def _parse_sector_time(time_str) -> float | None:
        """Parse sector time string like '30.500' to float seconds."""
        if time_str is None or time_str == "None":
            return None
        try:
            return float(str(time_str).lstrip("+"))
        except (ValueError, TypeError):
            return None

    def _load_pit_data(self):
        """Load existing pit durations from pit.json for baseline data."""
        try:
            import json, os
            pit_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "archive", "design_prototypes", "data", "pit.json")
            if os.path.exists(pit_path):
                with open(pit_path, "r", encoding="utf-8") as f:
                    pits = json.load(f)
                for rec in pits:
                    dur = rec.get("pit_duration")
                    if dur is not None and 2.0 < dur < self._pit_max_duration:
                        self._pit_durations.append(float(dur))
                if self._pit_durations:
                    self._update_pit_median()
                    logger.info(f'Loaded {len(pits)} pit stops from file, median: {self._pit_median:.1f}s')
        except Exception as e:
            logger.warning(f"Failed to load pit data: {e}")

    def _update_pit_median(self):
        """Sort all pit durations and calculate median."""
        if not self._pit_durations:
            self._pit_median = None
            return
        durs = sorted(self._pit_durations)
        n = len(durs)
        m = n // 2
        if n % 2 == 0:
            self._pit_median = (durs[m - 1] + durs[m]) / 2.0
        else:
            self._pit_median = durs[m]
        if self._pit_median is not None:
            self._recalc_pit_windows()

    def _recalc_pit_windows(self):
        if self._pit_median is None or self._field_median_speed_ms is None:
            return
        self._pit_window_delta = self._pit_median * self._field_median_speed_ms

    def _update_field_median_speed(self):
        """Calculate median speed across all cars on track (pit entry speeds excluded)."""
        speeds = [s for s in self._latest_speed_ms.values() if s > 5.0]
        if not speeds:
            return
        sorted_spd = sorted(speeds)
        n = len(sorted_spd)
        m = n // 2
        if n % 2 == 0:
            self._field_median_speed_ms = (sorted_spd[m-1] + sorted_spd[m]) / 2.0
        else:
            self._field_median_speed_ms = sorted_spd[m]

    def _update_pit_speed(self):
        # Recalculate default pit entry speed from observations (median)
        if not self._pit_speed_observations:
            return
        sorted_spd = sorted(self._pit_speed_observations)
        n = len(sorted_spd)
        m = n // 2
        if n % 2 == 0:
            self._pit_entry_speed_default = (sorted_spd[m - 1] + sorted_spd[m]) / 2.0
        else:
            self._pit_entry_speed_default = sorted_spd[m]
        pass  # window delta handled by _recalc_pit_windows
    def _build_driver_speed_profile(self, driver_number, track_len=None, num_bins=100):
        """Build a per-driver speed profile from car telemetry data."""
        data = self.car_data.get(driver_number, [])
        if not data:
            return None
        if track_len is None:
            track_len = self.current_circuit.get("length_m", 4655) if self.current_circuit else 4655
        bins = [[] for _ in range(num_bins)]
        for tm in data:
            if tm.speed <= 5:
                continue
            pos = (tm.distance % track_len) if tm.distance is not None else 0
            idx = min(num_bins - 1, int(pos / track_len * num_bins))
            bins[idx].append(tm.speed / 3.6)
        positions = []
        speeds = []
        for i, vals in enumerate(bins):
            if vals:
                s = sorted(vals)
                speeds.append(s[len(s)//2])
            else:
                speeds.append(None)
            positions.append(i * track_len / num_bins)
        positions.append(track_len)
        speeds.append(speeds[-1] if speeds[-1] is not None else 0)
        first = next((i for i, v in enumerate(speeds) if v is not None), None)
        if first is not None:
            fill = speeds[first]
            for i in range(first, len(speeds)):
                if speeds[i] is not None:
                    fill = speeds[i]
                speeds[i] = fill
        class SpeedProfile:
            pass
        sp = SpeedProfile()
        sp.positions = positions
        sp.speeds = speeds
        return sp

    @property
    def _is_safety_car_active(self):
        """Returns True if SafetyCar or VirtualSafetyCar is active."""
        if self.track_status is None:
            return False
        return self.track_status.status in ("SafetyCar", "VirtualSafetyCar")

    def _get_smoothed_speed(self, driver_number):
        """3-second moving average speed for a driver. Falls back to latest speed."""
        history = self._car_speed_history.get(driver_number, [])
        if history:
            speeds = [s for _, s in history]
            return sum(speeds) / len(speeds)
        return self._latest_speed_ms.get(driver_number, 0.0)

    def run_pit_assessment(self, ego_driver_number, max_cars_behind=None):
        """Run pit window assessment for a given driver.
        Returns the assessment dict, or None if data insufficient."""
        from .pit_assessment import assess_instant_pit_window, _calc_travel_time
        if self.current_circuit is None or self._pit_entry_pos_median is None or self._pit_exit_pos_median is None:
            return None
        lap_len = self.current_circuit.get("length_m", 4655)
        track_params = {
            "lap_length": lap_len,
            "pit_entry_pos": self._pit_entry_pos_median,
            "pit_exit_pos": self._pit_exit_pos_median,
        }
        class _Car:
            pass
        ego = _Car()
        ego.car_id = str(ego_driver_number)
        dist = self._ring_distance.get(ego_driver_number, 0.0)
        ego.track_pos = dist % lap_len
        ego.speed_profile = self._build_driver_speed_profile(ego_driver_number, lap_len)
        if ego.speed_profile is None:
            return None
        gap_str = self.timing.get(ego_driver_number, {}).gap_to_leader
        if gap_str is None or gap_str == "None" or gap_str == "":
            ego.gap_to_leader = 0.0
        else:
            try: ego.gap_to_leader = float(str(gap_str).lstrip("+"))
            except: ego.gap_to_leader = 0.0
        pit_loss = 0.0
        if self._pit_median is not None:
            try:
                Tn = _calc_travel_time(ego.speed_profile, self._pit_entry_pos_median, self._pit_exit_pos_median, lap_len)
                pit_loss = max(0.0, self._pit_median - Tn)
            except:
                pass
        ego.pit_loss = pit_loss
        # SC/VSC: recalc pit_loss using current slow speed
        if self._is_safety_car_active:
            sc_speed = self._get_smoothed_speed(ego_driver_number)
            if sc_speed > 5.0:
                pit_sector_length = (self._pit_exit_pos_median - self._pit_entry_pos_median) % lap_len
                if pit_sector_length > 10:
                    Tn_sc = pit_sector_length / sc_speed
                    ego.pit_loss = max(0.0, self._pit_median - Tn_sc)

        # ?? Build time_diff_to_ego from sorted timing (interval accumulation) ??
        time_diff_to_ego: dict[int, float] = {}
        sorted_timing_list = self.get_sorted_timing()
        ego_idx = None
        for i, t in enumerate(sorted_timing_list):
            if t["driver_number"] == ego_driver_number:
                ego_idx = i
                break
        if ego_idx is not None:
            cum = 0.0
            for i in range(ego_idx + 1, len(sorted_timing_list)):
                entry = sorted_timing_list[i]
                dn = entry["driver_number"]
                interval = entry.get("interval")
        if interval is not None:
            try:
                ival = float(str(interval).lstrip("+"))
                if ival > 0:
                    cum += ival
            except (ValueError, TypeError):
                pass
        # ── 基于官方 position 筛选身后车（准确处理套圈，无漂移）──
        ego_entry = self.timing.get(ego_driver_number)
        ego_pos = getattr(ego_entry, 'position', None)

        if ego_pos is not None:
            # Use official race position to determine cars behind
            behind_candidates = []
            for dn, data in self.timing.items():
                if dn == ego_driver_number:
                    continue
                car_pos = getattr(data, 'position', None)
                if car_pos is not None and car_pos > ego_pos:
                    behind_candidates.append((dn, car_pos))

            # Sort by position ascending (closest behind first)
            behind_candidates.sort(key=lambda x: x[1])

            # Take up to max_cars_behind
            limit = max_cars_behind if max_cars_behind else 3
            cars_behind = []
            for dn, _ in behind_candidates[:limit]:
                behind = _Car()
                behind.car_id = str(dn)
                behind.track_pos = self._ring_distance.get(dn, 0.0) % lap_len
                behind.speed_profile = self._build_driver_speed_profile(dn, lap_len)
                if behind.speed_profile is None:
                    continue
                behind.current_speed = self._latest_speed_ms.get(dn, 80.0)
                behind.time_diff_to_ego = time_diff_to_ego.get(dn, 0.0)

                gs = getattr(self.timing.get(dn, {}), 'gap_to_leader', None)
                if gs in (None, "None", ""):
                    behind.gap_to_leader = 0.0
                else:
                    try:
                        behind.gap_to_leader = float(str(gs).lstrip("+"))
                    except:
                        behind.gap_to_leader = 0.0
                cars_behind.append(behind)

        else:
            # ── Fallback: use physical distance when position unavailable ──
            ego_total_dist = self._ring_distance.get(ego_driver_number, 0.0)
            behind_candidates = []
            for dn, d_total in self._ring_distance.items():
                if dn == ego_driver_number:
                    continue
                diff = ego_total_dist - d_total
                if 0 < diff < lap_len:
                    behind_candidates.append((dn, diff))

            behind_candidates.sort(key=lambda x: x[1])
            limit = max_cars_behind if max_cars_behind else 3
            cars_behind = []
            for dn, diff in behind_candidates[:limit]:
                behind = _Car()
                behind.car_id = str(dn)
                behind.track_pos = self._ring_distance.get(dn, 0.0) % lap_len
                behind.speed_profile = self._build_driver_speed_profile(dn, lap_len)
                if behind.speed_profile is None:
                    continue
                behind.current_speed = self._latest_speed_ms.get(dn, 80.0)
                behind.time_diff_to_ego = time_diff_to_ego.get(dn, 0.0)
                gs = getattr(self.timing.get(dn, {}), 'gap_to_leader', None)
                if gs in (None, "None", ""):
                    behind.gap_to_leader = 0.0
                else:
                    try:
                        behind.gap_to_leader = float(str(gs).lstrip("+"))
                    except:
                        behind.gap_to_leader = 0.0
                cars_behind.append(behind)
        class _Leader:
            pass
        leader = _Leader()
        leader.track_pos = 0.0
        leader.gap_to_leader = 0.0
        safety_car_active = self._is_safety_car_active
        if safety_car_active:
            ego.current_speed = self._get_smoothed_speed(ego_driver_number)
        result = assess_instant_pit_window(ego, cars_behind, track_params, leader, safety_car_active=safety_car_active)
        result["pit_loss"] = getattr(ego, "pit_loss", 0.0)
        return result
     # Segment color constants
    SEG_EMPTY = 0  # sentinel for not-set
    SEG_GREEN = 2049
    SEG_YELLOW = 2048
    SEG_RED = 2048   # OpenF1: slower = yellow (no separate red)
    SEG_PURPLE = 2051 # OpenF1: 2051=purple
    _SECTOR_RANGES = [(0, 7), (7, 15), (15, 22)]
    _SECTOR_NAMES = ["s1", "s2", "s3"]
    _SAME_THRESHOLD = 0.3

    def _apply_sector_color(self, dn: int, sector_idx: int, color: int):
        """Store sector color in self-color and full-color array. Does NOT write to progressive reveal."""
        if dn not in self._sector_self:
            self._sector_self[dn] = [0, 0, 0]
        self._sector_self[dn][sector_idx] = color
        # Store in the full-color array (source of truth for _get_segment_colors)
        if dn not in self._seg_colors:
            self._seg_colors[dn] = [0] * 22
        start, end = self._SECTOR_RANGES[sector_idx]
        for si in range(start, end):
            self._seg_colors[dn][si] = color

    def _on_sector_completed(self, dn: int, sector_idx: int, sector_time: float, lap: int):
        """
        Called when a driver completes a sector.
        - Compares vs previous lap -> Green/Red/Yellow
        - Compares inter-driver -> Purple if fastest
        """
        curr = self._lap_curr.get(dn, {"lap": lap})
        if curr.get("lap") != lap:
            if curr.get("lap") is not None:
                self._lap_prev[dn] = curr
            curr = {"lap": lap}
        curr[self._SECTOR_NAMES[sector_idx]] = sector_time
        self._lap_curr[dn] = curr

        prev = self._lap_prev.get(dn, {})
        prev_time = prev.get(self._SECTOR_NAMES[sector_idx])

        # Self-comparison: green/red/yellow
        if prev_time is not None and sector_time > 0:
            diff = sector_time - prev_time
            if diff > self._SAME_THRESHOLD:
                self_color = self.SEG_RED
            elif diff < -self._SAME_THRESHOLD:
                self_color = self.SEG_GREEN
            else:
                self_color = self.SEG_YELLOW
        else:
            self_color = self.SEG_GREEN

        # Inter-driver best: purple
        if lap not in self._sector_best:
            self._sector_best[lap] = {}
            self._sector_best_laps.append(lap)
            # Keep only last 2 laps
            while len(self._sector_best_laps) > 2:
                old_lap = self._sector_best_laps.pop(0)
                self._sector_best.pop(old_lap, None)
        best_info = self._sector_best[lap].get(sector_idx)

        if best_info is None or sector_time < best_info["time"]:
            prev_best_dn = best_info["dn"] if best_info else None
            self._sector_best[lap][sector_idx] = {"dn": dn, "time": sector_time}
            final_color = self.SEG_PURPLE

            # Revert previous fastest driver back to self-color
            if prev_best_dn is not None and prev_best_dn != dn:
                prev_self = self._sector_self.get(prev_best_dn, [None, None, None])
                revert_color = prev_self[sector_idx]
                if revert_color not in (0, None):
                    start, end = self._SECTOR_RANGES[sector_idx]
                    segs = self._cur_segments.get(prev_best_dn)
                    if segs:
                        for si in range(start, end):
                            if segs[si] == self.SEG_PURPLE:
                                segs[si] = revert_color
        elif best_info["dn"] == dn:
            final_color = self.SEG_PURPLE
        else:
            final_color = self_color

        self._apply_sector_color(dn, sector_idx, final_color)
        if final_color == self.SEG_PURPLE:
            start, end = self._SECTOR_RANGES[sector_idx]
            segs = self._cur_segments.get(dn)
            if segs:
                for si in range(start, end):
                    segs[si] = self.SEG_PURPLE

    def _get_segment_colors(self, dn: int, lap: int):
        """Get segments as-is from data. Priority: 1) _live_segments, 2) _seg_colors, 3) None."""
        live = self._live_segments.get(dn)
        if live and len(live) == 22:
            return live
        colors = self._seg_colors.get(dn)
        if colors and len(colors) == 22 and any(c != 0 for c in colors):
            return colors
        return None

    def _update_segments(self, num: int):
        """Display segments as-is from the data stream.
        S1 event -> first 7 filled, S2 -> first 15, S3 -> full 22.
        No interpolation - data tells us what to show."""
        if num not in self.timing:
            return
        entry = self.timing[num]
        full_colors = self._get_segment_colors(num, self.lap_count.current_lap if self.lap_count else 0)
        if full_colors is None:
            shown = [0] * 22
        else:
            shown = list(full_colors)
        self._cur_segments[num] = shown
        entry.segments = shown
        self.timing[num] = entry
    def process_message(self, topic: str, data: Any, timestamp: float):
        clean_topic = topic[:-2] if topic.endswith(".z") else topic
        handler = getattr(self, f"_handle_{clean_topic}", None)
        if handler:
            try:
                handler(data, timestamp)
            except Exception as e:
                logger.warning(f"Error processing {topic}: {e}")

    def _handle_SessionInfo(self, data: dict, ts: float):
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
            remaining=self.session_info.remaining,
        )
        # Try to match circuit data from the CSV
        if meeting.get("Name") or meeting.get("Circuit", {}).get("ShortName"):
            matched = match_circuit(meeting.get("Name", ""), meeting.get("Circuit", {}).get("ShortName", ""), self.circuits)
            if matched:
                self.current_circuit = matched
            elif self.current_circuit:
                pass  # keep previous match
            else:
                cshort = meeting.get("Circuit", {}).get("ShortName", "").lower()
                ck = self._circuit_name_to_key.get(cshort)
                if ck is not None:
                    cl_data = load_centerline(ck)
                    if cl_data:
                        self._centerline = cl_data
                        self.current_circuit = {
                            "circuit": meeting.get("Circuit", {}).get("ShortName", ""),
                            "length_m": int(cl_data["length"]),
                        }
        circuit_short = meeting.get("Circuit", {}).get("ShortName", "")
        # Unified centerline lookup: use circuit_short from meeting data
        if self._centerline is None and circuit_short:
            ck = self._circuit_name_to_key.get(circuit_short.lower())
            if ck is not None:
                cl_data = load_centerline(ck)
                if cl_data:
                    self._centerline = cl_data
                    if self.current_circuit is None:
                        self.current_circuit = {"circuit": circuit_short, "length_m": int(cl_data["length"])}
        # Fallback: use circuit_key from Meeting data directly (bypasses circuits_xy.json which has limited coverage)
        if self._centerline is None:
            ck = meeting.get("Circuit", {}).get("Key")
            if ck is not None:
                cl_data = load_centerline(int(ck))
                if cl_data:
                    self._centerline = cl_data
                    circuit_short_name = meeting.get("Circuit", {}).get("ShortName", str(ck))
                    if self.current_circuit is None:
                        self.current_circuit = {"circuit": circuit_short_name, "length_m": int(cl_data["length"])}
        # Centerline status
        if self._centerline is not None:
            logger.info("Centerline loaded: %s (key=%s)", circuit_short,
                        self._circuit_name_to_key.get(circuit_short.lower(), "?"))
        else:
            logger.info("Centerline unavailable for %s, using speed integral", circuit_short)
            # Activate auto-recording: build centerline from 3 leader laps
            if self.current_circuit:
                self._recording_active = True
                self._recording_positions.clear()
                self._recording_driver = -1
                logger.info("Auto-recording activated for %s", circuit_short)
        speed_kmh = PIT_LANE_SPEEDS.get(circuit_short, 80)
        self._pit_entry_speed_default = speed_kmh / 3.6
 
    def _handle_ExtrapolatedClock(self, data: dict, ts: float):
        remaining = data.get("Remaining")
        if remaining and isinstance(remaining, str) and ":" in remaining:
            self.session_info.remaining = remaining

    def _handle_DriverList(self, data: dict, ts: float):
        lines = data.get("Lines", data)
        for num_str, driver in lines.items():
            if not num_str.isdigit():
                continue
            num = int(num_str)
            # Update existing DriverInfo in-place (handles partial delta messages)
            if num not in self.drivers:
                self.drivers[num] = DriverInfo(driver_number=num)
            drv = self.drivers[num]
            for k, v in driver.items():
                if k == "RacingNumber":
                    drv.racing_number = int(v) if v else 0
                elif k == "BroadcastName":
                    drv.broadcast_name = v if v else ""
                elif k == "FullName":
                    drv.full_name = v if v else ""
                elif k == "FirstName":
                    drv.first_name = v if v else ""
                elif k == "LastName":
                    drv.last_name = v if v else ""
                elif k == "TeamName":
                    drv.team_name = v if v else ""
                elif k == "TeamColour":
                    tc = str(v).strip() if v else ""
                    if tc and not tc.startswith("#"):
                        tc = "#" + tc
                    drv.team_colour = tc or "#cccccc"
                elif k == "HeadshotUrl":
                    drv.headshot_url = v
                elif k == "CountryCode":
                    drv.country_code = v if v else ""
                elif k == "Tla":
                    drv.tla = v if v else ""

    def _handle_TimingData(self, data: dict, ts: float):
        lines = data.get("Lines", data)
        for num_str, td in lines.items():
            try:
                num = int(num_str)
            except ValueError:
                continue
            entry = self.timing.get(num, TimingEntry(driver_number=num))
            sector_completed = None
            entry_lap = entry.lap_number or (self.lap_count.current_lap if self.lap_count else 0)
            for k, v in td.items():
                if k == "Position":
                    entry.position = v
                elif k == "LapNumber":
                    entry.lap_number = v
                    entry_lap = v
                elif k == "NumberOfLaps":
                    entry.lap_number = v
                    entry_lap = v
                elif k == "GapToLeader":
                    entry.gap_to_leader = v if v and v != "None" else None
                elif k == "TimeDiffToFastest":
                    entry.gap_to_leader = v if v and v != "None" else None
                elif k == "Interval":
                    entry.interval = v if v and v != "None" else None
                elif k == "TimeDiffToPositionAhead":
                    entry.interval = v if v and v != "None" else None
                elif k == "LastLapTime":
                    entry.last_lap_time = v if v and v != "None" else None
                elif k == "BestLapTime":
                    entry.best_lap_time = v if v and v != "None" else None
                elif k == "Sector1":
                    entry.sector1 = v if v and v != "None" else None
                    t = DataProcessor._parse_sector_time(v)
                    if t is not None:
                        sector_completed = (0, t, entry_lap)
                elif k == "Sector2":
                    entry.sector2 = v if v and v != "None" else None
                    t = DataProcessor._parse_sector_time(v)
                    if t is not None:
                        sector_completed = (1, t, entry_lap)
                elif k == "Sector3":
                    entry.sector3 = v if v and v != "None" else None
                    t = DataProcessor._parse_sector_time(v)
                    if t is not None:
                        sector_completed = (2, t, entry_lap)
                elif k == "NumberOfPitStops":
                    entry.pit_stop_count = v
                elif k == "Retired":
                    entry.retired = v
                elif k == "Segments":
                    if isinstance(v, list):
                        self._live_segments[num] = v
                        entry.segments = v
            if sector_completed:
                sec_idx, sec_time, sec_lap = sector_completed
                self._on_sector_completed(num, sec_idx, sec_time, sec_lap)
            self._update_segments(num)
            self.timing[num] = entry
        # Derive lap_count from max NumberOfLaps (handles practice/qualifying where LapCount topic isnt sent)
        max_lap = 0
        for entry in self.timing.values():
            if entry.lap_number and entry.lap_number > max_lap:
                max_lap = entry.lap_number
        if max_lap > (self.lap_count.current_lap if self.lap_count else 0):
            total = self.lap_count.total_laps if self.lap_count else 0
            self.lap_count = LapCount(total_laps=total, current_lap=max_lap)

    def _handle_TimingAppData(self, data: dict, ts: float):
        lines = data.get("Lines", {})
        for num_str, ad in lines.items():
            try:
                num = int(num_str)
            except ValueError:
                continue
            entry = self.app_data.get(num, TimingAppDataEntry(driver_number=num))
            # Extract stint data from Stints dict format (live timing signalrcore protocol)
            if "Stints" in ad and isinstance(ad["Stints"], dict) and ad["Stints"]:
                stint_keys = sorted(ad["Stints"].keys())
                last_stint = ad["Stints"][stint_keys[-1]]
                if isinstance(last_stint, dict):
                    sc = str(last_stint.get("Compound", ""))
                    if sc and sc != "UNKNOWN":
                        if sc.startswith("COMPOUND_"):
                            sc = sc[9:]
                        ad["Compound"] = sc
                    if "New" in last_stint:
                        nv = last_stint["New"]
                        if isinstance(nv, str):
                            ad["FreshTyre"] = nv.lower() in ("true", "1")
                        else:
                            ad["FreshTyre"] = bool(nv)
                    if "TotalLaps" in last_stint and last_stint["TotalLaps"] is not None:
                        ad["TyreAge"] = int(last_stint["TotalLaps"])
            if "Stint" in ad: entry.stint = ad["Stint"]
            if "TyreAge" in ad: entry.tyre_age = ad["TyreAge"]
            if "FreshTyre" in ad: entry.fresh_tyre = ad["FreshTyre"]
            if "Compound" in ad:
                new_compound = ad["Compound"]
                old_compound = entry.compound
                if new_compound != old_compound:
                    current_lap = self.lap_count.current_lap if self.lap_count else 0
                    if old_compound is not None and num in self._stint_start_laps:
                        stint_laps = current_lap - self._stint_start_laps[num]
                        if stint_laps > 0:
                            if num not in self._stint_history:
                                self._stint_history[num] = []
                            self._stint_history[num].append({
                                "compound": old_compound,
                                "lap_count": stint_laps,
                            })
                    if num not in self._stint_history:
                        self._stint_history[num] = []
                    self._stint_start_laps[num] = current_lap
                entry.compound = new_compound
            self.app_data[num] = entry

    def _handle_CarData(self, data, ts: float):
        # TODO: migrate GG accumulation & lap detection logic to core/telemetry_state.py
        # TODO: migrate lap detection & distance calc to core/position_state.py
        # Handle string from jsonStream
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (json.JSONDecodeError, ValueError, TypeError):
                return
        if not isinstance(data, dict):
            return
        # Normalize Position.z format: signalrcore sends Position[].Entries.driver.{X,Y,Z,Status}
        # Convert to: Entries[].Cars.driver.{X,Y,Z,Status}
        if "Position" in data:
            positions = data["Position"]
            entries_list = []
            for pos_entry in positions if isinstance(positions, list) else [positions]:
                cars_dict = {}
                for dn_str, car_data in pos_entry.get("Entries", {}).items():
                    cars_dict[dn_str] = {
                        "X": car_data.get("X", 0),
                        "Y": car_data.get("Y", 0),
                        "Z": car_data.get("Z", 0),
                        "Status": car_data.get("Status", ""),
                    }
                entries_list.append({"Cars": cars_dict})
            data = {"Entries": entries_list}
        # Normalize: flat {"1":{...}} instead of {"Entries":[{"Cars":{...}}]}
        if "Entries" not in data or not data.get("Entries"):
            data = {"Entries": [{"Cars": data}]}
        entries = data.get("Entries", [])
        for entry in entries:
            cars = entry.get("Cars", {})
            utc_str = entry.get("Utc", "")
            for num_str, car in cars.items():
                try:
                    num = int(num_str)
                except ValueError:
                    continue
                if utc_str and isinstance(car, dict):
                    car["date"] = utc_str
                # Convert Channels numeric format to named fields (F1 SignalR protocol)
                channels = car.get("Channels")
                if channels is not None and isinstance(channels, dict):
                    # Channel 0 = RPM, Channel 2 = Speed (km/h)
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
                # Use original race timestamp from data['date'] when available
                # (important for replay at any speed - wall clock ts != race time)
                raw_ts = ts
                date_str = car.get("date")
                if date_str:
                    try:
                        from datetime import datetime
                        raw_ts = datetime.fromisoformat(date_str.replace("Z", "+00:00")).timestamp()
                    except Exception:
                        pass  # fallback to wall clock ts
                if raw_ts > self._last_race_ts:
                    self._last_race_ts = raw_ts
                # Wheel speed ?longitudinal G (primary source, much more accurate)
                speed_ms = speed_kmh / 3.6
                prev_car_spd = self._car_prev_speed_ms.get(num)
                prev_car_ts = self._car_prev_ts.get(num)
                if prev_car_spd is not None and prev_car_ts is not None:
                    dt = raw_ts - prev_car_ts
                    if 0.05 < dt < 2.0:
                        dv = speed_ms - prev_car_spd
                        raw_g = (dv / dt) / 9.81
                        buf = self._lon_g_buf.get(num, [])
                        buf.append(raw_g)
                        if len(buf) > 3:
                            buf.pop(0)
                        self._lon_g_buf[num] = buf
                        self._latest_lon_g[num] = sum(buf) / len(buf)
                self._car_prev_speed_ms[num] = speed_ms
                self._latest_speed_ms[num] = speed_ms
                # 3-second rolling speed history for Safety Car / VSC mode
                ts_list = self._car_speed_history.setdefault(num, [])
                ts_list.append((raw_ts, speed_ms))
                cutoff = raw_ts - 3.0
                while ts_list and ts_list[0][0] < cutoff:
                    ts_list.pop(0)
                self._car_prev_ts[num] = raw_ts
                # Speed-based odometry: smooth and no GPS noise
                prev_ts = self._car_data_prev_time.get(num)
                if prev_ts is not None:
                    dt = raw_ts - prev_ts
                    if 0 < dt < 3.0:
                        self._car_data_distance[num] = self._car_data_distance.get(num, 0.0) + (speed_kmh / 3.6) * dt
                else:
                    self._car_data_distance[num] = self._car_data_distance.get(num, 0.0)
                # Distance-based lap detection (corrects distance only; lap counter handled by angle system)
                _circuit_len = self.current_circuit.get("length_m") if self.current_circuit else 0
                if _circuit_len > 0 and self._car_data_distance.get(num, 0) >= _circuit_len:
                    _actual_dist = self._car_data_distance.get(num, 0.0)
                    self._car_data_distance[num] -= _circuit_len
                    self._car_data_prev_time[num] = raw_ts
                    self._car_lap_counter[num] = self._car_lap_counter.get(num, 0) + 1

                # Ring distance: only integrate during green flag racing
                if not self._red_flag_active:
                    if prev_ts is not None:
                        dt = raw_ts - prev_ts
                        if 0 < dt < 3.0:
                            self._ring_distance[num] = self._ring_distance.get(num, 0.0) + (speed_kmh / 3.6) * dt
                    else:
                        self._ring_distance[num] = self._ring_distance.get(num, 0.0)
                # Pit car distance (per-lap, resets at GPS finish line)
                if prev_ts is not None:
                    dt = raw_ts - prev_ts
                    if 0 < dt < 3.0:
                        self._pit_car_distance[num] = self._pit_car_distance.get(num, 0.0) + (speed_kmh / 3.6) * dt
                else:
                    self._pit_car_distance[num] = self._pit_car_distance.get(num, 0.0)

                self._car_data_prev_time[num] = raw_ts


                # Normalize brake pressure to [0, 1] (handles bool, 0-100%, or 0-1 float)
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
                self._update_field_median_speed()

                tm = CarTelemetry(
                    timestamp=raw_ts, driver_number=num,
                    rpm=car.get("RPM", 0), speed=speed_kmh,
                    throttle=car.get("Throttle", 0),
                    brake=brake_val,
                    drs=car.get("DRS"), n_gear=car.get("nGear", 0),
                    distance=self._car_data_distance.get(num, 0.0),
                    lap=self._car_lap_counter.get(num, 0),
                )
                # Lap is now set by distance-based detection above
                self.car_data[num].append(tm)
                if len(self.car_data[num]) > 2000:
                    self.car_data[num] = self.car_data[num][-2000:]

    def _handle_Position(self, data, ts: float):
        # TODO: migrate lap detection & distance calc to core/position_state.py
        # TODO: migrate pit assessment trigger to core/pit_state.py
        # Handle string from jsonStream
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (json.JSONDecodeError, ValueError, TypeError):
                return
        if not isinstance(data, dict):
            return
        # Normalize Position.z format: signalrcore sends Position[].Entries.driver.{X,Y,Z,Status}
        # Convert to: Entries[].Cars.driver.{X,Y,Z,Status}
        if "Position" in data:
            positions = data["Position"]
            entries_list = []
            for pos_entry in positions if isinstance(positions, list) else [positions]:
                cars_dict = {}
                for dn_str, car_data in pos_entry.get("Entries", {}).items():
                    cars_dict[dn_str] = {
                        "X": car_data.get("X", 0),
                        "Y": car_data.get("Y", 0),
                        "Z": car_data.get("Z", 0),
                        "Status": car_data.get("Status", ""),
                    }
                entries_list.append({"Cars": cars_dict})
            data = {"Entries": entries_list}
        # Normalize: flat {"1":{...}} instead of {"Entries":[{"Cars":{...}}]}
        if "Entries" not in data or not data.get("Entries"):
            data = {"Entries": [{"Cars": data}]}
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
                # Compute angle from track center (for lap detection + rankings)
                cp.angle = self._compute_angle(cp.x, cp.y)
                cp.cumulative_angle = self._update_cumulative_angle(num, cp.angle)
                cp.lap = int(cp.cumulative_angle / 360) if cp.cumulative_angle else 0

                # Lap boundary via cumulative angle (skip if in pit)
                # Finish line: capture from timing data first lap increment
                if self._finish_line_x is None:
                    te = self.timing.get(num)
                    if te and te.lap_number is not None and int(te.lap_number) > self._car_data_prev_lap.get(num, 0):
                        self._finish_line_x = cp.x
                        self._finish_line_y = cp.y
                        self._car_data_prev_lap[num] = te.lap_number
                # Lap detection via finish line proximity
                _just_crossed = False
                if self._finish_line_x is not None:
                    dx = cp.x - self._finish_line_x
                    dy = cp.y - self._finish_line_y
                    fd = dx*dx + dy*dy
                    was_near = self._finish_last_near.get(num, False)
                    if fd < 25.0 and not was_near:
                        self._pos_buf[num] = []
                        _just_crossed = True
                        self._finish_last_near[num] = True
                    elif fd >= 25.0 and was_near:
                        self._finish_last_near[num] = False

                # ---- G-force from three-point trajectory ----
                # 7-point SMA: outlier zero-residual after 0.7s
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
                        dist = haversine_distance(s1[0], s1[1], sx, sy)
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

                            speed1 = self._latest_speed_ms.get(num)
                            if speed1 is None or speed1 <= 1:
                                speed1 = math.sqrt(v1x ** 2 + v1y ** 2)  # fallback to GPS
                            speed2 = math.sqrt(v2x ** 2 + v2y ** 2)
                            dt_avg = (dt1 + dt2) / 2

                            # Longitudinal G from wheel speed (CarData.z)
                            lon_g = self._latest_lon_g.get(num)
                            if lon_g is not None:
                                cp.longitudinal_g = lon_g

                            # Lateral G via circumradius of triangle A(s[-7]) B(s[-4]) C(s_now)
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
                            buf = self._lat_buf.get(num, [])
                            buf.append(raw_g)
                            if len(buf) > 3:
                                buf.pop(0)
                            self._lat_buf[num] = buf
                            cp.lateral_g = sum(buf) / len(buf)
                else:
                    cp.speed_ms = 0

                self.prev_positions[num] = cp
                # Centerline auto-recording: accumulate GPS for 3 laps
                if self._recording_active and cp.status == "OnTrack":
                    if self._recording_driver < 0:
                        self._recording_driver = num
                        self._recording_start_angle = cp.cumulative_angle
                        self._recording_positions = [(cp.x, cp.y)]
                    elif self._recording_driver == num:
                        if self._recording_positions:
                            lx, ly = self._recording_positions[-1]
                            if (cp.x - lx)**2 + (cp.y - ly)**2 >= 0.5:
                                self._recording_positions.append((cp.x, cp.y))
                        laps_done = (cp.cumulative_angle - self._recording_start_angle) / 360.0
                        if laps_done >= 3.0 and len(self._recording_positions) > 500:
                            self._recording_active = False
                            self._process_recorded_centerline()
                # --- Pit stop duration tracking ---
                prev_status = self._pit_prev_status.get(num)
                curr_status = cp.status or ""
                if prev_status == "InPit" and curr_status != "InPit":
                    entry_ts = self._pit_entry_time.get(num)
                    if entry_ts is not None and entry_ts > 0:
                        duration = pos_ts - entry_ts
                        if 2.0 < duration < self._pit_max_duration:
                            self._pit_durations.append(duration)
                            self._pit_session_count += 1
                            self._update_pit_median()
                            # Learn pit sector length from ring distance change
                            _entry_ring = self._pit_entry_ring_distance.pop(num, None)
                            if _entry_ring is not None and _entry_ring > 0:
                                _exit_ring = self._ring_distance.get(num, 0.0)
                                _sector = _exit_ring - _entry_ring
                                if 100 < _sector < 2000:
                                    self._pit_sector_observations.append(_sector)
                                    if len(self._pit_sector_observations) <= 50:
                                        _sd = sorted(self._pit_sector_observations)
                                        self._pit_sector_length = _sd[len(_sd)//2]
                                    _lap_len = self.current_circuit.get("length_m", 4655) if self.current_circuit else 4655
                                    self._pit_entry_pos_observations.append(_entry_ring % _lap_len)
                                    self._pit_exit_pos_observations.append(_exit_ring % _lap_len)
                                    if len(self._pit_sector_observations) <= 50:
                                        self._pit_entry_pos_median = sorted(self._pit_entry_pos_observations)[len(self._pit_entry_pos_observations)//2]
                                        self._pit_exit_pos_median = sorted(self._pit_exit_pos_observations)[len(self._pit_exit_pos_observations)//2]
                            entry_dist = self._pit_entry_distance.get(num)
                            if entry_dist is not None and entry_dist > 0:
                                exit_dist = self._ring_distance.get(num, 0.0)
                                pit_dist = exit_dist - entry_dist
                                if pit_dist > 10:
                                    avg_speed = pit_dist / duration
                                    self._pit_speed_observations.append(avg_speed)
                                    self._update_pit_speed()
                    self._pit_entry_time.pop(num, None)
                elif curr_status == "InPit" and prev_status != "InPit":
                    self._pit_entry_time[num] = pos_ts
                    self._pit_entry_distance[num] = self._pit_car_distance.get(num, 0.0)
                    self._pit_entry_ring_distance[num] = self._ring_distance.get(num, 0.0)
                    self._pit_entry_distance_list.append(self._pit_entry_distance[num])
                    # Keep sliding window of last 50 entries
                    if len(self._pit_entry_distance_list) > 50:
                        self._pit_entry_distance_list.pop(0)
                    sd = sorted(self._pit_entry_distance_list)
                    n = len(sd)
                    m = n // 2
                    if n % 2 == 0:
                        self._pit_entry_distance_median = (sd[m-1] + sd[m]) / 2.0
                    else:
                        self._pit_entry_distance_median = sd[m]
                self._pit_prev_status[num] = curr_status
                if _just_crossed:
                    self._pit_car_distance[num] = 0.0
                # Update smoothed position chain
                s_chain.append((sx, sy, pos_ts))
                if len(s_chain) > 7:
                    s_chain.pop(0)
                self._smooth_chain[num] = s_chain
                self.positions[num].append(cp)
                if len(self.positions[num]) > 1000:
                    self.positions[num] = self.positions[num][-1000:]

    def _handle_WeatherData(self, data: dict, ts: float):
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

    def _handle_TrackStatus(self, data: dict, ts: float):
        status_map = {
            "1": "Green", "2": "Yellow", "3": "Yellow",
            "4": "Yellow", "5": "Red", "6": "Red",
            "7": "SafetyCar", "8": "VirtualSafetyCar",
        }
        code = str(data.get("Status", ""))
        status = status_map.get(code, f"Code_{code}")
        prev = self.track_status.status if self.track_status else ""
        # Red flag start: clear ring distance for fresh restart
        if "Red" in status and "Red" not in prev:
            self._ring_distance.clear()
            self._red_flag_active = True
        elif "Red" not in status and "Red" in prev:
            self._red_flag_active = False
        self.track_status = TrackStatus(
            status=status,
            message=data.get("Message", ""),
        )

    def _handle_RaceControlMessages(self, data: dict, ts: float):
        messages = data.get("Messages", {})
        if isinstance(messages, list):
            for msg in messages:
                rcm = RaceControlMessage(
                    timestamp=ts, category=msg.get("Category", ""),
                    message=msg.get("Message", ""), flag=msg.get("Flag"),
                    lap=msg.get("Lap"),
                )
                if not any(m.message == rcm.message and m.lap == rcm.lap for m in self.race_control_messages):
                    self.race_control_messages.append(rcm)
                if len(self.race_control_messages) > 200:
                    self.race_control_messages = self.race_control_messages[-100:]
            return
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

    def _handle_LapCount(self, data: dict, ts: float):
        prev_lap = self.lap_count.current_lap if self.lap_count else 0
        self.lap_count = LapCount(
            total_laps=data.get("TotalLaps", 0),
            current_lap=data.get("CurrentLap", 0),
        )
        self._recalc_pit_windows()
        # New lap: start per-lap assessment loop
        if self.lap_count.current_lap != prev_lap:
            self._current_assessment_lap = self.lap_count.current_lap
            self._pit_min_per_lap.clear()
            if not self._intra_lap_active:
                self._intra_lap_active = True
                t = threading.Thread(target=self._intra_lap_assessment_loop, daemon=True)
                t.start()


    def _intra_lap_assessment_loop(self):
        """Background loop: evaluate pit window for top drivers every 8 seconds,
        collecting per-lap minimum critical_distance_m for stable arc display."""
        import time
        current_lap = self._current_assessment_lap
        try:
            while self._intra_lap_active and self.lap_count and self.lap_count.current_lap == current_lap:
                sorted_timing = self.get_sorted_timing()
                # Assess only top 5 drivers for performance
                assess_drivers = [t["driver_number"] for t in sorted_timing[:5] if t.get("driver_number")]
                for dn in assess_drivers:
                    try:
                        result = self.run_pit_assessment(dn, max_cars_behind=3)
                    except Exception:
                        result = None
                    critical_val = result["critical_distance_m"] if (result and "critical_distance_m" in result) else 0.0
                    pit_loss_val = result.get("pit_loss", 0.0) if result else 0.0
                    old_min = self._pit_min_per_lap.get(dn)
                    if old_min is None or critical_val < old_min:
                        self._pit_min_per_lap[dn] = critical_val
                        self._pit_assessment_cache[dn] = {"critical": critical_val, "pit_loss": pit_loss_val}
                time.sleep(1)
        finally:
            self._intra_lap_active = False

    def get_sorted_timing(self) -> list[dict]:
        raw_entries = [e for e in self.timing.values() if not e.retired and e.position is not None]
        valid = [e for e in raw_entries if str(e.position).strip().lstrip("-").isdigit() and int(str(e.position).strip()) > 0]
        entries = sorted(valid, key=lambda e: int(e.position))
        if not entries and raw_entries:
            entries = sorted(raw_entries, key=lambda e: e.driver_number)
        result = []
        for i, e in enumerate(entries, 1):
            d = dict(e.__dict__)
            d['position'] = i
            d['in_pit'] = self._pit_prev_status.get(e.driver_number, "") == "InPit"
            result.append(d)
        # Retired/position-0 drivers at the bottom
        retired = sorted(
            [e for e in self.timing.values() if e.retired or (e.position is not None and str(e.position).strip() == "0")],
            key=lambda e: e.driver_number,
        )
        for e in retired:
            d = dict(e.__dict__)
            d['position'] = 0
            d['in_pit'] = self._pit_prev_status.get(e.driver_number, "") == "InPit"
            result.append(d)
        return result

    def _compute_angle(self, x: float, y: float) -> float:
        """Compute polar angle (0-360) from track center for a given X,Y.
        Center is frozen after 10 samples; no long warm-up needed."""
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

    def _update_cumulative_angle(self, driver_number: int, angle_deg: float) -> float:
        """Track unwrapped cumulative angle, detecting lap boundaries."""
        prev = self._cumulative_angle.get(driver_number)
        if prev is None:
            self._cumulative_angle[driver_number] = angle_deg
            return angle_deg

        prev_mod = prev % 360
        diff = angle_deg - prev_mod
        if diff < -180:
            diff += 360

        cum = prev + diff
        self._cumulative_angle[driver_number] = cum
        return cum

    def get_track_positions(self) -> list[dict]:
        """Return all drivers sorted by track position on the ring.

        Uses cached centerline projection when available: projects each car's
        GPS X,Y onto the pre-recorded centerline segments to compute accurate
        track distance and ring angle. Falls back to cumulative polar angle
        when centerline is unavailable.
        """
        clen = self.current_circuit.get("length_m") if self.current_circuit else 4657
        has_cl = self._centerline is not None
        cl_segs = self._centerline["segs"] if has_cl else None
        cl_len = self._centerline["length"] if has_cl else clen
        entries = []
        for dn in list(self.drivers.keys()):
            timing_entry = self.timing.get(dn)
            driver = self.drivers.get(dn)

            # ---- Centerline projection (primary) ----
            cp = self.prev_positions.get(dn)
            if has_cl and cp is not None and hasattr(cp, 'x'):
                cum_dist, best_idx = project_onto_centerline(
                    cp.x, cp.y, cl_segs,
                    self._proj_idx.get(dn, 0),
                )
                self._proj_idx[dn] = best_idx
                pos_on_track = cum_dist % cl_len
                angle = (pos_on_track / cl_len) * 360.0
                lap = int(self._cumulative_angle.get(dn, 0.0) / 360)
                sort_key = lap * cl_len + pos_on_track
            else:
                # ---- Fallback: speed-integral ring distance ----
                dist = self._ring_distance.get(dn, 0.0)
                pos_on_track = dist % clen if clen > 0 else 0
                angle = (pos_on_track / clen) * 360.0 if clen > 0 else 0.0
                lap = int(dist / clen) if clen > 0 else 0
                sort_key = dist

            # Prefer official lap number from timing data when available
            if timing_entry and timing_entry.lap_number:
                lap = timing_entry.lap_number
            # Determine pit_window_delta from per-lap range cache or fallback
            # Use smoothed pit assessment value when available, fallback to global window
            cached = self._pit_assessment_cache.get(dn)
            if isinstance(cached, dict):
                pit_delta = cached.get("critical", 0) if cached.get("critical", 0) > 0 else (self._pit_window_delta or 0)
                pit_loss_val_cache = cached.get("pit_loss", self._pit_median or 0)
            elif cached is not None and cached > 0:
                pit_delta = cached
                pit_loss_val_cache = self._pit_median or 0
            else:
                pit_delta = self._pit_window_delta or 0
                pit_loss_val_cache = self._pit_median or 0
            entry = {
                "driver_number": dn,
                "ring_distance": self._ring_distance.get(dn, 0.0),
                "lap": lap,
                "angle": angle,
                "tla": driver.tla if driver else "",
                "team_colour": driver.team_colour if driver else "",
                "team_name": driver.team_name if driver else "",
                "pit_window_delta": pit_delta,
                "gap_to_leader": timing_entry.gap_to_leader if timing_entry else None,
                "interval": timing_entry.interval if timing_entry else None,
                "pit_loss": round(pit_loss_val_cache, 1),
                "speed_ms": round(self._get_smoothed_speed(dn), 1),
                "_sort": sort_key,
            }
            entries.append(entry)
        entries.sort(key=lambda e: -e["_sort"])
        for i, e in enumerate(entries):
            e["position"] = i + 1
            e.pop("_sort", None)
        return entries

    def _process_recorded_centerline(self):
        """Process accumulated GPS positions into a centerline (3 laps collected)."""
        pts = self._recording_positions
        if len(pts) < 1000:
            logger.warning("Auto-recording insufficient: %d pts", len(pts))
            return
        third = len(pts) // 3
        one_lap = pts[third:third*2]
        track_shape = [{"x": float(x), "y": float(y)} for x, y in one_lap]
        from .track_centerline import build_from_track_shape
        clen = self.current_circuit["length_m"] if self.current_circuit and "length_m" in self.current_circuit else 5000
        try:
            cl_data = build_from_track_shape(track_shape, clen)
            self._centerline = cl_data
            logger.info("Auto-recorded centerline from #%d: %d pts -> %d segs, %.0fm",
                        self._recording_driver, len(pts), len(cl_data["segs"]), cl_data["length"])
        except Exception as e:
            logger.warning("Auto-recording failed: %s", e)




    def get_snapshot(self) -> dict:
        def d(v):
            if hasattr(v, 'model_dump'):
                return v.model_dump()
            if hasattr(v, '__dict__'):
                return v.__dict__
            return v
        # Fallback: try matching circuit from session info if not yet set
        if self.current_circuit is None and self.session_info.circuit_short:
            matched = match_circuit(self.session_info.meeting_name, self.session_info.circuit_short, self.circuits)
            if matched:
                self.current_circuit = matched
        snap = {
            "session": d(self.session_info),
            "drivers": {n: d(drv) for n, drv in self.drivers.items()},
            "timing": {n: d(t) for n, t in self.timing.items()},
            "timing_sorted": self.get_sorted_timing(),
            "track_positions": self.get_track_positions(),
            "app_data": {n: d(a) for n, a in self.app_data.items()},
            "car_data": {n: [d(c) for c in pts[-3:]] for n, pts in self.car_data.items()},
            "positions": {n: [d(p) for p in pts[-3:]] for n, pts in self.positions.items()},
            "weather": d(self.weather) if self.weather else None,
             "track_status": d(self.track_status) if self.track_status else None,
             "lap_count": d(self.lap_count) if self.lap_count else None,
             "pit_median": self._pit_median,
             "pit_loss_available": self._pit_median is not None and self._pit_entry_distance_median is not None and self._pit_window_delta is not None,
             "stint_history": self._stint_history,
            "race_control": [d(m) for m in self.race_control_messages],
            "circuit": self.current_circuit,
        }

        return snap

    def get_field(self, btype: str):
        def d(v):
            if hasattr(v, 'model_dump'): return v.model_dump()
            if hasattr(v, '__dict__'): return v.__dict__
            return v
        if btype == "session": return d(self.session_info)
        if btype == "drivers": return {n: d(drv) for n, drv in self.drivers.items()}
        if btype == "timing": return self.get_sorted_timing()
        if btype == "timing_sorted": return self.get_sorted_timing()
        if btype == "track_positions": return self.get_track_positions()
        if btype == "app_data": return {n: d(a) for n, a in self.app_data.items()}
        if btype == "car_data": return {n: [d(c) for c in pts[-100:]] for n, pts in self.car_data.items()}
        if btype == "positions": return {n: [d(p) for p in pts[-20:]] for n, pts in self.positions.items()}
        if btype == "weather": return d(self.weather) if self.weather else None
        if btype == "track_status": return d(self.track_status) if self.track_status else None
        if btype == "lap_count": return d(self.lap_count) if self.lap_count else None
        if btype == "stint_history": return self._stint_history
        if btype == "race_control": return [d(m) for m in self.race_control_messages]
        if btype == "circuit": return self.current_circuit
        if btype == "pit_median": return self._pit_median
        return None

