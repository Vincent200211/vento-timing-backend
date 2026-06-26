"""
Tyre Database — 轮胎圈速数据查询层

从 OpenF1 回放数据 (JSON) 构建 tyre_laps 数据集:
合并 laps.json + stints.json → 每条记录带 tyre_age, compound, stint_number

提供查询方法:
- get_stint_laps(session_id, driver, stint)
- get_driver_compound_history(session_id, driver)
- get_compound_comparison(session_id, compounds)
- build_tyre_laps(session_id) — 构建完整数据集
"""

from __future__ import annotations
import json
import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

REPLAY_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "Vento timing design", "data",
)

STINT_BASELINE_WINDOW = 3  # 前 N 圈取最快作为基准
MIN_LAPS_FOR_DEGRADATION = 4


class TyreLapRecord:
    """一圈的完整数据(含轮胎信息)"""
    __slots__ = (
        "driver_number", "lap_number", "lap_duration",
        "stint_number", "compound", "tyre_age", "lap_in_stint",
        "is_pit_out", "is_pit_in",
        "sector1", "sector2", "sector3",
        "i1_speed", "i2_speed", "st_speed",
        "stint_baseline", "degradation_est_raw", "degradation_est",
        "is_outlier",
    )

    def __init__(self, driver_number: int, lap_data: dict, stint_data: dict):
        self.driver_number = driver_number
        self.lap_number = lap_data["lap_number"]
        self.lap_duration = lap_data.get("lap_duration", 0)
        self.stint_number = stint_data.get("stint_number", 0)
        self.compound = stint_data.get("compound", "UNKNOWN")
        self.lap_in_stint = lap_data["lap_number"] - stint_data.get("lap_start", 0)
        self.tyre_age = stint_data.get("tyre_age_at_start", 0) + self.lap_in_stint
        self.is_pit_out = lap_data.get("is_pit_out_lap", False)
        # pit_in 检测: 最后一圈 or 圈速异常大
        self.is_pit_in = False
        self.sector1 = lap_data.get("duration_sector_1")
        self.sector2 = lap_data.get("duration_sector_2")
        self.sector3 = lap_data.get("duration_sector_3")
        self.i1_speed = lap_data.get("i1_speed")
        self.i2_speed = lap_data.get("i2_speed")
        self.st_speed = lap_data.get("st_speed")
        self.stint_baseline = 0.0
        self.degradation_est_raw = 0.0
        self.degradation_est = 0.0
        self.is_outlier = False

    def to_dict(self) -> dict:
        return {
            "driver_number": self.driver_number,
            "lap_number": self.lap_number,
            "lap_duration": self.lap_duration,
            "stint_number": self.stint_number,
            "compound": self.compound,
            "tyre_age": self.tyre_age,
            "lap_in_stint": self.lap_in_stint,
            "is_pit_out": self.is_pit_out,
            "is_pit_in": self.is_pit_in,
            "sector1": self.sector1,
            "sector2": self.sector2,
            "sector3": self.sector3,
            "i1_speed": self.i1_speed,
            "i2_speed": self.i2_speed,
            "st_speed": self.st_speed,
            "stint_baseline": round(self.stint_baseline, 4),
            "degradation_est_raw": round(self.degradation_est_raw, 4),
            "degradation_est": round(self.degradation_est, 4),
            "is_outlier": self.is_outlier,
        }

    def __repr__(self) -> str:
        return (
            f"TyreLap(#{self.driver_number} st.{self.stint_number} "
            f"L{self.lap_number} {self.compound} "
            f"age={self.tyre_age} lap_dur={self.lap_duration})"
        )


class StintInfo:
    """单个 stint 的汇总信息"""
    def __init__(self, stint_data: dict, laps: list[TyreLapRecord]):
        self.stint_number = stint_data["stint_number"]
        self.compound = stint_data["compound"]
        self.driver_number = stint_data["driver_number"]
        self.lap_start = stint_data["lap_start"]
        self.lap_end = stint_data.get("lap_end")
        self.laps = sorted(laps, key=lambda x: x.lap_number)
        self.baseline = 0.0
        self._compute_baseline()

    def _compute_baseline(self):
        valid = [l for l in self.laps
                 if not l.is_pit_out and not l.is_pit_in and not l.is_outlier
                 and l.lap_duration > 0]
        if len(valid) >= 2:
            self.baseline = min(l.lap_duration for l in valid[:3])
        elif valid:
            self.baseline = valid[0].lap_duration

    def to_dict(self) -> dict:
        return {
            "stint_number": self.stint_number,
            "compound": self.compound,
            "driver_number": self.driver_number,
            "lap_start": self.lap_start,
            "lap_end": self.lap_end,
            "baseline": round(self.baseline, 4),
            "lap_count": len(self.laps),
            "lap_range": f"{self.lap_start}-{self.lap_end or 'running'}",
        }


class TyreDatabase:
    """轮胎圈速数据库。

    从 replay JSON 数据构建 tyre_laps 数据集,
    支持按 session/driver/compound/stint 查询.
    """

    def __init__(self, data_dir: str = ""):
        self.data_dir = data_dir or REPLAY_DATA_DIR
        self._cached_laps: dict[int, list[TyreLapRecord]] = {}
        self._cached_stints: dict[int, list[StintInfo]] = {}
        self._current_session_key: Optional[int] = None

    # ── 数据加载 ──────────────────────────────────────────────────────

    def load_session(self, session_key: Optional[int] = None) -> None:
        """从 JSON 文件加载并构建 tyre_laps 数据集"""
        if not os.path.exists(self.data_dir):
            logger.warning(f"Data dir not found: {self.data_dir}")
            return

        laps_raw = self._load_json("laps")
        stints_raw = self._load_json("stints")
        if not laps_raw or not stints_raw:
            return

        # 确定 session_key
        if session_key is None and laps_raw:
            self._current_session_key = laps_raw[0].get("session_key")
        else:
            self._current_session_key = session_key

        # 按 session 过滤
        if self._current_session_key:
            laps_raw = [l for l in laps_raw
                        if l.get("session_key") == self._current_session_key]
            stints_raw = [s for s in stints_raw
                          if s.get("session_key") == self._current_session_key]

        # 按 driver 分组
        by_driver: dict[int, list] = {}
        for l in laps_raw:
            by_driver.setdefault(l["driver_number"], []).append(l)

        # 对每个 driver 构建 TyreLapRecord
        for dn, driver_laps in by_driver.items():
            driver_stints = [s for s in stints_raw if s["driver_number"] == dn]
            driver_stints.sort(key=lambda s: s["stint_number"])

            stint_map: dict[int, dict] = {}
            stint_lap_map: dict[int, list[TyreLapRecord]] = {}

            for s in driver_stints:
                sn = s["stint_number"]
                stint_map[sn] = s
                stint_lap_map[sn] = []
                for l in driver_laps:
                    if (l["lap_number"] >= s["lap_start"]
                            and (s.get("lap_end") is None
                                 or l["lap_number"] <= s["lap_end"])):
                        record = TyreLapRecord(dn, l, s)
                        stint_lap_map[sn].append(record)

                # 标记 pit_in 和异常
                self._mark_pit_in(stint_lap_map[sn])
                self._mark_outliers(stint_lap_map[sn])

                # 计算 stint 基准
                self._compute_baselines(stint_lap_map[sn])

            # 收集所有记录
            all_records = []
            for sn in sorted(stint_lap_map.keys()):
                all_records.extend(stint_lap_map[sn])
            self._cached_laps[dn] = all_records

            # 构建 StintInfo
            self._cached_stints[dn] = [
                StintInfo(stint_map[sn], stint_lap_map[sn])
                for sn in sorted(stint_map.keys())
            ]

        logger.info(
            f"Loaded {len(self._cached_laps)} drivers, "
            f"{sum(len(v) for v in self._cached_laps.values())} laps"
        )

    def _load_json(self, name: str) -> list:
        path = os.path.join(self.data_dir, f"{name}.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return []

    @staticmethod
    def _mark_pit_in(laps: list[TyreLapRecord]):
        """标记进站圈: 最后一圈 or 比前一圈快很多且圈速异常大"""
        if not laps:
            return
        sorted_laps = sorted(laps, key=lambda x: x.lap_number)
        for i, l in enumerate(sorted_laps):
            if l.is_pit_out:
                continue
            # 最后一圈标记为 pit_in
            if i == len(sorted_laps) - 1 and l.lap_duration:
                if l.lap_duration > 0:
                    avg = sum(x.lap_duration for x in sorted_laps[:-1]
                              if x.lap_duration > 0) / max(len(sorted_laps) - 1, 1)
                    if l.lap_duration > avg * 1.5:
                        l.is_pit_in = True

    @staticmethod
    def _mark_outliers(laps: list[TyreLapRecord]):
        """标记异常圈:
        1. pit_in/pit_out/第一圈
        2. 圈速比同类中位数偏差 > 3*IQR
        3. 圈速 > stint 中位数的 115%
        4. pit_out lap 之前的圈
        """
        valid = [l for l in laps if l.lap_duration > 0 and not l.is_pit_out]
        if len(valid) < 3:
            return

        values = sorted(l.lap_duration for l in valid)
        n = len(values)
        q1 = values[n // 4]
        q3 = values[3 * n // 4]
        iqr = q3 - q1
        upper = q3 + 2.0 * iqr
        median = values[n // 2]

        for l in laps:
            if l.is_pit_out or l.is_pit_in or l.lap_in_stint <= 0:
                continue
            if l.lap_duration > upper or l.lap_duration > median * 1.20:
                l.is_outlier = True
                logger.debug(
                    f"Outlier: #{l.driver_number} L{l.lap_number} "
                    f"stint={l.stint_number} {l.lap_duration}s"
                )

    @staticmethod
    def _compute_baselines(laps: list[TyreLapRecord]):
        """计算 stint 基准圈速 (前 3 个有效圈中最快)"""
        valid = [l for l in laps
                 if l.lap_duration > 0 and not l.is_pit_out
                 and not l.is_outlier and not l.is_pit_in]
        if len(valid) < 2:
            return
        fastest = min(valid[:STINT_BASELINE_WINDOW],
                      key=lambda x: x.lap_duration)
        baseline = fastest.lap_duration

        for l in laps:
            l.stint_baseline = baseline
            l.degradation_est_raw = l.lap_duration - baseline
            # 异常圈的退化置 0 (不参与拟合)
            if l.is_outlier or l.is_pit_out or l.is_pit_in:
                l.degradation_est = 0.0
            else:
                l.degradation_est = l.degradation_est_raw  # 保留负值(油耗减重), 清洗层会过滤异常值

    # ── 查询方法 ──────────────────────────────────────────────────────

    def get_stint_laps(self, driver_number: int,
                       stint_number: Optional[int] = None) -> list[TyreLapRecord]:
        """获取车手的一个或多个 stint 的圈速数据"""
        records = self._cached_laps.get(driver_number, [])
        if stint_number is not None:
            records = [r for r in records if r.stint_number == stint_number]
        return sorted(records, key=lambda r: r.lap_number)

    def get_driver_stints(self, driver_number: int) -> list[StintInfo]:
        """获取车手的所有 stint 摘要"""
        return self._cached_stints.get(driver_number, [])

    def get_driver_names(self) -> dict[int, str]:
        """获取已知车手名字列表"""
        return {
            dn: f"#{dn}"
            for dn in sorted(self._cached_laps.keys())
        }

    def get_degradation_data(self, driver_number: int,
                             compound_filter: Optional[str] = None,
                             stint_filter: Optional[int] = None
                             ) -> dict:
        """获取车手的退化数据(用于模型拟合和前端绘图)

        返回:
        {
            "driver_number": int,
            "stints": [...],
            "points": [{"tyre_age": n, "degradation": f, "lap_duration": f, "compound": s, "stint": n}, ...],
            "driver_name": str (optional)
        }
        """
        records = self._cached_laps.get(driver_number, [])
        if stint_filter is not None:
            records = [r for r in records if r.stint_number == stint_filter]
        if compound_filter:
            records = [r for r in records if r.compound == compound_filter]

        stints = self._cached_stints.get(driver_number, [])

        # 有效数据点(用于拟合)
        points = []
        for r in records:
            if r.lap_duration > 0 and not r.is_outlier and not r.is_pit_out and not r.is_pit_in:
                points.append({
                    "tyre_age": r.tyre_age,
                    "lap_in_stint": r.lap_in_stint,
                    "degradation": r.degradation_est,
                    "lap_duration": r.lap_duration,
                    "compound": r.compound,
                    "stint": r.stint_number,
                    "lap_number": r.lap_number,
                })

        return {
            "driver_number": driver_number,
            "stints": [s.to_dict() for s in stints],
            "points": points,
        }

    def get_comparison_data(self, driver_numbers: list[int]) -> dict:
        """多车手对比数据"""
        return {
            dn: self.get_degradation_data(dn) for dn in driver_numbers
            if dn in self._cached_laps
        }

    def get_compound_summary(self) -> list[dict]:
        """按 compound 汇总"""
        summary: dict[str, dict] = {}
        for dn, records in self._cached_laps.items():
            for r in records:
                if r.is_outlier or r.is_pit_out or r.is_pit_in:
                    continue
                c = r.compound
                if c not in summary:
                    summary[c] = {
                        "compound": c,
                        "samples": 0,
                        "drivers": set(),
                        "avg_degradation_per_lap": 0.0,
                        "total_laps": 0,
                    }
                summary[c]["samples"] += 1
                summary[c]["drivers"].add(r.driver_number)
                summary[c]["avg_degradation_per_lap"] += r.degradation_est
        for c in summary.values():
            s = c["samples"]
            c["avg_degradation_per_lap"] = round(c["avg_degradation_per_lap"] / s, 4) if s > 0 else 0.0
            c["drivers"] = list(c["drivers"])
        return list(summary.values())


# ── 单例 ──────────────────────────────────────────────────────────────────
_db_instance: Optional[TyreDatabase] = None


def get_tyre_database(data_dir: str = "") -> TyreDatabase:
    global _db_instance
    if _db_instance is None:
        _db_instance = TyreDatabase(data_dir)
    return _db_instance


# ── Demo ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    db = TyreDatabase()
    db.load_session()

    print("=== Drivers ===")
    for dn in sorted(db._cached_laps.keys()):
        stints = db.get_driver_stints(dn)
        print(f"  #{dn}: {len(stints)} stints, {len(db._cached_laps[dn])} laps")

    # VER degradation data
    print("\n=== VER (#1) Degradation ===")
    data = db.get_degradation_data(1)
    print(f"  Points: {len(data['points'])}")
    for s in data["stints"]:
        print(f"  Stint {s['stint_number']}: {s['compound']} "
              f"laps {s['lap_start']}-{s['lap_end']}, "
              f"baseline={s['baseline']:.3f}s")

    print("\n  First 10 points:")
    for p in data["points"][:10]:
        print(f"    age={p['tyre_age']} stint={p['stint']} "
              f"deg={p['degradation']:.3f}s lap={p['lap_duration']:.2f}s")

    # 用 degradation_model 拟合
    print("\n=== Model Fit ===")
    from degradation_model import fit_best_model, model_to_dict
    import json

    for stint_num in [1, 2]:
        sd = db.get_degradation_data(1, stint_filter=stint_num)
        ages = [p["tyre_age"] for p in sd["points"]]
        degs = [p["degradation"] for p in sd["points"]]
        if len(ages) < 3:
            continue
        model = fit_best_model(ages, degs)
        print(f"  Stint {stint_num}: {type(model).__name__} "
              f"R2={model.r_squared:.3f}, "
              f"pred(15)={model.predict(15):.3f}s")
