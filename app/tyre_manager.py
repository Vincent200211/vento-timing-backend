"""Tyre Manager — 独立轮胎系统模块

完整的轮胎数据模型、生命周期跟踪、退化计算。
设计原则：
1. 独立 — 不依赖 DataProcessor，自有 state
2. 可插拔 — process_message() 接口，main.py handle_f1_message 中一行接入
3. 实时 — 基于 TimingAppData / CarData.z / LapCount / TrackStatus 更新
4. 全生命周期 — stint 切换、胎龄增长、退化曲线、历史记录
"""

from __future__ import annotations
import logging
from collections import defaultdict
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── 配方枚举与常量 ───────────────────────────────────────────────────
COMPOUND_ORDER = ["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"]
COMPOUND_WEAR_RATES = {
    "SOFT": 1.20,
    "MEDIUM": 0.70,
    "HARD": 0.35,
    "INTERMEDIATE": 0.50,
    "WET": 0.20,
}
COMPOUND_TEMP_RANGES = {
    "SOFT": (85, 110, 130),
    "MEDIUM": (90, 115, 140),
    "HARD": (95, 120, 145),
    "INTERMEDIATE": (80, 105, 130),
    "WET": (70, 95, 120),
}
COMPOUND_COLORS = {
    "SOFT": "#da291c", "MEDIUM": "#ffffff", "HARD": "#ffd700",
    "INTERMEDIATE": "#4caf50", "WET": "#2196f3",
}
COMPOUND_BG = {
    "SOFT": "#da291c22", "MEDIUM": "#ffd70022", "HARD": "#ffffff11",
    "INTERMEDIATE": "#4caf5022", "WET": "#2196f322",
}


class TyreState:
    """车手当前轮胎完整状态"""
    __slots__ = (
        "driver_number", "stint", "compound", "tyre_age",
        "fresh_tyre", "lap_started", "total_laps_on_compound",
        "degradation", "speed_loss_pct", "grip_loss_pct",
        "overheating", "critical_age",
    )

    def __init__(self, driver_number: int):
        self.driver_number = driver_number
        self.stint: int = 0
        self.compound: Optional[str] = None
        self.tyre_age: int = 0
        self.fresh_tyre: bool = False
        self.lap_started: int = 0
        self.total_laps_on_compound: int = 0
        self.degradation: float = 0.0
        self.speed_loss_pct: float = 0.0
        self.grip_loss_pct: float = 0.0
        self.overheating: bool = False
        self.critical_age: int = 30

    def to_dict(self) -> dict:
        return {
            "driver_number": self.driver_number,
            "stint": self.stint,
            "compound": self.compound,
            "tyre_age": self.tyre_age,
            "fresh_tyre": self.fresh_tyre,
            "lap_started": self.lap_started,
            "total_laps_on_compound": self.total_laps_on_compound,
            "degradation": round(self.degradation, 4),
            "speed_loss_pct": round(self.speed_loss_pct, 2),
            "grip_loss_pct": round(self.grip_loss_pct, 2),
            "overheating": self.overheating,
            "critical_age": self.critical_age,
        }


class StintRecord:
    """单个 stint 完整记录"""
    def __init__(self, driver_number: int, stint_number: int,
                 compound: str, lap_start: int, tyre_age_at_start: int = 0):
        self.driver_number = driver_number
        self.stint_number = stint_number
        self.compound = compound
        self.lap_start = lap_start
        self.lap_end: Optional[int] = None
        self.total_laps: int = 0
        self.tyre_age_at_start = tyre_age_at_start
        self.peak_degradation: float = 0.0
        self.pit_duration: Optional[float] = None

    def close(self, lap_end: int):
        self.lap_end = lap_end
        self.total_laps = lap_end - self.lap_start

    def to_dict(self) -> dict:
        return {
            "stint_number": self.stint_number,
            "compound": self.compound,
            "lap_start": self.lap_start,
            "lap_end": self.lap_end,
            "total_laps": self.total_laps,
            "tyre_age_at_start": self.tyre_age_at_start,
            "peak_degradation": round(self.peak_degradation, 4),
        }


class TyreManager:
    """独立轮胎系统核心。"""

    def __init__(self):
        self.reset()

    def reset(self):
        self._states: dict[int, TyreState] = {}
        self._stint_history: dict[int, list[StintRecord]] = defaultdict(list)
        self._current_lap: int = 0
        self._total_laps: int = 0
        self._air_temp: float = 20.0
        self._track_temp: float = 25.0
        self._rainfall: bool = False
        self._safety_car: bool = False

    # ── 消息处理入口 ──────────────────────────────────────────────────
    def process_message(self, topic: str, data: Any, ts: float) -> None:
        topic_clean = topic[:-2] if topic.endswith(".z") else topic
        if topic_clean == "TimingAppData" and isinstance(data, dict):
            self._handle_timing_app_data(data)
        elif topic_clean == "LapCount" and isinstance(data, dict):
            self._handle_lap_count(data)
        elif topic_clean == "WeatherData" and isinstance(data, dict):
            self._handle_weather(data)
        elif topic_clean == "TrackStatus" and isinstance(data, dict):
            self._handle_track_status(data)
        elif topic_clean == "CarData" and isinstance(data, dict):
            self._handle_car_data(data)
        elif topic_clean == "SessionInfo":
            self.reset()

    # ── Handler ────────────────────────────────────────────────────────

    def _handle_timing_app_data(self, data: dict):
        lines = data.get("Lines", {})
        for num_str, ad in lines.items():
            try:
                dn = int(num_str)
            except ValueError:
                continue
            state = self._states.get(dn)
            if state is None:
                state = TyreState(dn)
                self._states[dn] = state
                # First message for this driver: create initial stint record
                init_compound = ad.get('Compound', None)
                if init_compound is not None:
                    self._stint_history[dn].append(StintRecord(
                        driver_number=dn,
                        stint_number=ad.get('Stint', 1),
                        compound=init_compound,
                        lap_start=self._current_lap,
                        tyre_age_at_start=ad.get('TyreAge', 0),
                    ))
            old_compound = state.compound
            old_age = state.tyre_age
            if "Stint" in ad:
                state.stint = ad["Stint"]
            if "Compound" in ad:
                state.compound = ad["Compound"]
            if "TyreAge" in ad:
                state.tyre_age = ad["TyreAge"]
            if "FreshTyre" in ad:
                state.fresh_tyre = ad["FreshTyre"]
            # 检测轮胎更换
            compound_changed = (
                ad.get("Compound") is not None
                and old_compound is not None
                and ad["Compound"] != old_compound
            )
            age_reset = (
                ad.get("TyreAge") is not None
                and old_age is not None
                and int(ad["TyreAge"]) < old_age - 1
                and old_age > 1
            )
            if compound_changed or age_reset:
                if old_compound is not None:
                    prev_stints = self._stint_history.get(dn, [])
                    if prev_stints:
                        prev_stints[-1].close(self._current_lap)
                        prev_stints[-1].peak_degradation = self._calc_degradation(
                            old_compound, prev_stints[-1].total_laps
                        )[0]
                new_compound = ad.get("Compound", old_compound or "UNKNOWN")
                self._stint_history[dn].append(StintRecord(
                    driver_number=dn, stint_number=ad.get("Stint", state.stint),
                    compound=new_compound, lap_start=self._current_lap,
                    tyre_age_at_start=ad.get("TyreAge", 0),
                ))
                state.lap_started = self._current_lap
                state.total_laps_on_compound = 0
                state.degradation = 0.0
                state.speed_loss_pct = 0.0
                state.grip_loss_pct = 0.0
            self._recalc_degradation(state)

    def _handle_lap_count(self, data: dict):
        new_lap = data.get("CurrentLap", 0)
        prev_lap = self._current_lap
        self._current_lap = new_lap
        self._total_laps = data.get("TotalLaps", 0)
        if new_lap <= prev_lap:
            return
        laps_advanced = new_lap - prev_lap
        for state in self._states.values():
            if state.compound is None:
                continue
            state.tyre_age += laps_advanced
            state.total_laps_on_compound += laps_advanced
            if state.tyre_age > 0:
                self._recalc_degradation(state)
        for records in self._stint_history.values():
            for rec in records:
                if rec.lap_end is None:
                    rec.total_laps = new_lap - rec.lap_start

    def _handle_weather(self, data: dict):
        self._air_temp = data.get("AirTemp", self._air_temp)
        self._track_temp = float(data.get("TrackTemp", self._track_temp))
        self._rainfall = data.get("Rainfall", self._rainfall)
        for state in self._states.values():
            if state.compound is not None:
                self._recalc_degradation(state)

    def _handle_track_status(self, data: dict):
        code = str(data.get("Status", ""))
        self._safety_car = code in ("7", "8")
        if code in ("5", "6"):
            for state in self._states.values():
                state.overheating = False

    def _handle_car_data(self, data: dict):
        entries = data.get("Entries", [])
        for entry in entries:
            cars = entry.get("Cars", {})
            for num_str, car in cars.items():
                try:
                    dn = int(num_str)
                except ValueError:
                    continue
                state = self._states.get(dn)
                if state is None or state.compound is None:
                    continue
                brake = float(car.get("Brake", 0) or 0)
                speed = float(car.get("Speed", 0) or 0)
                if brake > 80 and speed < 30:
                    state.degradation = min(1.0, state.degradation + 0.004)
                    state.speed_loss_pct = self._deg_curve_speed(state.compound, state.degradation)
                    state.grip_loss_pct = self._deg_curve_grip(state.compound, state.degradation)

    # ── 退化计算 ──────────────────────────────────────────────────────
    def _recalc_degradation(self, state: TyreState):
        if state.compound is None or state.compound not in COMPOUND_WEAR_RATES:
            state.degradation = 0.0
            state.speed_loss_pct = 0.0
            state.grip_loss_pct = 0.0
            state.overheating = False
            return
        compound = state.compound
        age = state.tyre_age
        base_rate = COMPOUND_WEAR_RATES[compound]
        # 温度修正
        temp_penalty = 0.0
        temp_range = COMPOUND_TEMP_RANGES.get(compound, (80, 120, 140))
        opt_low, _, overheat_thresh = temp_range
        if self._track_temp < opt_low:
            temp_penalty = (opt_low - self._track_temp) * 0.02
        elif self._track_temp > overheat_thresh:
            temp_penalty = (self._track_temp - overheat_thresh) * 0.05
            state.overheating = True
        else:
            state.overheating = False
        # 降雨修正
        rain_penalty = 1.5 if (self._rainfall and compound not in ("INTERMEDIATE", "WET")) else 0.0
        sc_factor = 0.6 if self._safety_car else 1.0
        effective_age = age * sc_factor
        raw_deg = min(1.0, (base_rate + temp_penalty + rain_penalty) * (effective_age ** 1.4 / 100.0))
        if age <= 3:
            raw_deg *= (age / 3.0) ** 0.5
        state.degradation = raw_deg
        state.speed_loss_pct = self._deg_curve_speed(compound, raw_deg)
        state.grip_loss_pct = self._deg_curve_grip(compound, raw_deg)

    @staticmethod
    def _deg_curve_speed(compound: str, deg: float) -> float:
        severity = {"SOFT": 1.4, "MEDIUM": 1.0, "HARD": 0.7,
                    "INTERMEDIATE": 0.9, "WET": 0.5}
        return deg * 6.0 * severity.get(compound, 1.0)

    @staticmethod
    def _deg_curve_grip(compound: str, deg: float) -> float:
        severity = {"SOFT": 1.3, "MEDIUM": 1.0, "HARD": 0.8,
                    "INTERMEDIATE": 0.9, "WET": 0.6}
        return deg * 8.0 * severity.get(compound, 1.0)

    @staticmethod
    def _calc_degradation(compound: str, laps: int) -> tuple:
        rate = COMPOUND_WEAR_RATES.get(compound, 0.5)
        deg = min(1.0, rate * (laps ** 1.4 / 100.0))
        sev_s = {"SOFT": 1.4, "MEDIUM": 1.0, "HARD": 0.7,
                 "INTERMEDIATE": 0.9, "WET": 0.5}
        sev_g = {"SOFT": 1.3, "MEDIUM": 1.0, "HARD": 0.8,
                 "INTERMEDIATE": 0.9, "WET": 0.6}
        return round(deg, 4), round(deg * 6.0 * sev_s.get(compound, 1.0), 2), \
               round(deg * 8.0 * sev_g.get(compound, 1.0), 2)

    # ── 数据导出 ──────────────────────────────────────────────────────
    def get_current_states(self) -> dict[int, dict]:
        return {dn: st.to_dict() for dn, st in self._states.items()}

    def get_stint_history(self) -> dict[int, list[dict]]:
        return {dn: [r.to_dict() for r in records] for dn, records in self._stint_history.items()}

    def get_driver_state(self, driver_number: int) -> Optional[dict]:
        state = self._states.get(driver_number)
        return state.to_dict() if state else None

    def get_snapshot(self) -> dict:
        return {
            "tyre_states": self.get_current_states(),
            "stint_history": self.get_stint_history(),
            "current_lap": self._current_lap,
            "total_laps": self._total_laps,
            "track_temp": self._track_temp,
            "air_temp": self._air_temp,
            "rainfall": self._rainfall,
            "safety_car": self._safety_car,
        }

    def get_compound_summary(self) -> list[dict]:
        summary: dict[str, dict] = {}
        for dn, st in self._states.items():
            c = st.compound or "UNKNOWN"
            if c not in summary:
                summary[c] = {"compound": c, "drivers": [], "avg_age": 0.0, "max_age": 0, "count": 0}
            summary[c]["drivers"].append(dn)
            summary[c]["count"] += 1
            summary[c]["avg_age"] += st.tyre_age
            summary[c]["max_age"] = max(summary[c]["max_age"], st.tyre_age)
        for c in summary:
            cnt = summary[c]["count"]
            summary[c]["avg_age"] = round(summary[c]["avg_age"] / cnt, 1) if cnt > 0 else 0.0
        return list(summary.values())

    def get_pit_window_adjustment(self, driver_number: int) -> dict:
        state = self._states.get(driver_number)
        if state is None or state.degradation is None:
            return {"adjustment": 0.0, "reason": "no_data"}
        if state.degradation > 0.8:
            adj, reason = -0.4, "critical_degradation"
        elif state.degradation > 0.6:
            adj, reason = -0.25, "high_degradation"
        elif state.degradation > 0.4:
            adj, reason = -0.1, "moderate_degradation"
        else:
            adj, reason = 0.0, "good_condition"
        return {"adjustment": adj, "reason": reason, "degradation": round(state.degradation, 4),
                "speed_loss_pct": state.speed_loss_pct, "grip_loss_pct": state.grip_loss_pct,
                "tyre_age": state.tyre_age, "compound": state.compound}

    def predict_optimal_pit_lap(self, driver_number: int,
                                target_window_start: int, target_window_end: int) -> Optional[int]:
        state = self._states.get(driver_number)
        if state is None or state.compound is None:
            return None
        compound, base_age = state.compound, state.tyre_age
        best_lap, best_score = target_window_end, float("inf")
        for lap in range(target_window_start, target_window_end + 1):
            projected_age = base_age + (lap - self._current_lap)
            projected_deg = min(1.0, COMPOUND_WEAR_RATES.get(compound, 0.5) * (projected_age ** 1.4 / 100.0))
            score = self._deg_curve_speed(compound, projected_deg) * 1.2 + self._deg_curve_grip(compound, projected_deg) * 1.5
            if score < best_score:
                best_score, best_lap = score, lap
        return best_lap


# ── 单例 ──────────────────────────────────────────────────────────────────
_tyre_manager_instance: Optional[TyreManager] = None

def get_tyre_manager() -> TyreManager:
    global _tyre_manager_instance
    if _tyre_manager_instance is None:
        _tyre_manager_instance = TyreManager()
    return _tyre_manager_instance

def process_tyre_message(topic: str, data: Any, ts: float) -> None:
    """一行接入 main.py handle_f1_message"""
    get_tyre_manager().process_message(topic, data, ts)


if __name__ == "__main__":
    import json
    mgr = TyreManager()
    mgr.process_message("TimingAppData", {
        "Lines": {
            "1": {"Stint": 1, "Compound": "SOFT", "TyreAge": 0, "FreshTyre": True},
            "16": {"Stint": 1, "Compound": "MEDIUM", "TyreAge": 0, "FreshTyre": True},
            "44": {"Stint": 2, "Compound": "HARD", "TyreAge": 5, "FreshTyre": False},
        }
    }, 1000.0)
    for lap in range(1, 13):
        mgr.process_message("LapCount", {"CurrentLap": lap, "TotalLaps": 66}, 1000.0 + lap * 90)
        if lap % 5 == 0:
            print(f"--- Lap {lap} ---")
            for dn in [1, 16, 44]:
                s = mgr.get_driver_state(dn)
                print(f"  #{dn}: {s['compound']} age={s['tyre_age']} deg={s['degradation']:.3f} speed_loss={s['speed_loss_pct']:.1f}% grip_loss={s['grip_loss_pct']:.1f}%")
    print("\n=== Snapshot ===")
    print(json.dumps(mgr.get_snapshot(), indent=2, default=str))
    print("\n=== Compound Summary ===")
    print(json.dumps(mgr.get_compound_summary(), indent=2))
    print("\n=== Stint History ===")
    print(json.dumps(mgr.get_stint_history(), indent=2))
    print("\n=== Pit Window Adj (driver 16) ===")
    print(json.dumps(mgr.get_pit_window_adjustment(16), indent=2))
    print("\n=== Optimal Pit Lap (driver 16, window 20-30) ===")
    print(mgr.predict_optimal_pit_lap(16, 20, 30))

