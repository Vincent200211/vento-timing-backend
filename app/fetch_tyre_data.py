"""
从 OpenF1 API 拉取全周末数据（FP1→Qualifying），存入 tyre_raw.db
正赛数据（Race）不拉取——模拟正赛前收集所有已知数据
"""
from __future__ import annotations
import httpx
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.tyre_raw_db import RawTyreDB

DATA_DIR = "D:/Vento_Timing/Vento timing design/data"
DB_PATH = os.environ.get("TYRE_DB_PATH") or "D:/Vento_Timing/tyre_raw.db"
OPENF1_BASE = "https://api.openf1.org/v1"

# 只拉取 FP1→Qualifying (Race = 11307 跳过)
SESSION_KEYS = [11300, 11301, 11302, 11303]
SESSION_NAMES = {11300: "Practice 1", 11301: "Practice 2", 11302: "Practice 3", 11303: "Qualifying"}
SESSION_TYPES = {11300: "Practice", 11301: "Practice", 11302: "Practice", 11303: "Qualifying"}


def openf1_fetch(endpoint: str, params: dict = None, retries: int = 3) -> list:
    """从 OpenF1 API 获取数据，带重试"""
    url = f"{OPENF1_BASE}/{endpoint}"
    for attempt in range(retries):
        try:
            r = httpx.get(url, params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
            print(f"  WARN: {endpoint} {params} -> HTTP {r.status_code}")
            if r.status_code == 429:
                time.sleep(5)
                continue
            return []
        except Exception as e:
            print(f"  ERROR fetching {endpoint} {params}: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return []


def fetch_all():
    """主流程"""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed old DB: {DB_PATH}")

    db = RawTyreDB(DB_PATH)
    print(f"Created: {DB_PATH}")

    # ── 1. 从本地 JSON 加载会议元数据 ────────────────────────────────
    meeting = json.load(open(os.path.join(DATA_DIR, "meeting.json")))
    sessions_meta = json.load(open(os.path.join(DATA_DIR, "sessions.json")))
    meeting_name = meeting[0].get("meeting_name", "Barcelona Grand Prix") if meeting else "Barcelona GP"

    # ── 2. 插入 Sessions (11300-11303) ───────────────────────────────
    for sk in SESSION_KEYS:
        meta = next((s for s in sessions_meta if s["session_key"] == sk), {})
        db.get_or_create_session(
            sk, meeting_name, SESSION_TYPES[sk], SESSION_NAMES[sk],
            meta.get("date_start", ""),
            meta.get("circuit_short_name", "Catalunya"),
        )
    print(f"Sessions: {len(SESSION_KEYS)} ({', '.join(SESSION_NAMES.values())})")

    # ── 3. 拉取 Drivers ─────────────────────────────────────────────
    all_drivers = {}  # driver_number -> info
    for sk in SESSION_KEYS:
        drivers_data = openf1_fetch("drivers", {"session_key": sk})
        for d in drivers_data:
            dn = d["driver_number"]
            if dn not in all_drivers:
                tc = d.get("team_colour", "#888888")
                if tc and not tc.startswith("#"):
                    tc = "#" + tc
                all_drivers[dn] = {
                    "name": d.get("name_acronym", d.get("broadcast_name", "")),
                    "team": d.get("team_name", ""),
                    "color": tc,
                }
    for dn, info in sorted(all_drivers.items()):
        db.upsert_driver(dn, info["name"], info["team"], info["color"])
    print(f"Drivers: {len(all_drivers)}")

    # ── 4. 拉取 Stints + Laps + Pit ─────────────────────────────────
    total_stints = 0
    total_laps = 0
    total_pit = 0

    for sk in SESSION_KEYS:
        sname = SESSION_NAMES[sk]
        sess_id = db.conn.execute(
            "SELECT id FROM sessions WHERE session_key=?", (sk,)
        ).fetchone()
        if not sess_id:
            print(f"  ERROR: session {sk} not in DB")
            continue
        sess_id = sess_id[0]
        print(f"\n{sname} (session_key={sk}):")

        # ── Stints ──────────────────────────────────────────────────
        stints_data = openf1_fetch("stints", {"session_key": sk})
        stint_cache = {}
        for s in stints_data:
            dn = s["driver_number"]
            sn = s["stint_number"]
            stint_id = db.insert_stint(
                sess_id, dn, sn, s["compound"],
                s.get("tyre_age_at_start", 0),
                s.get("lap_start"),
            )
            if stint_id < 0:
                stint_id = db.get_stint_id(sess_id, dn, sn)
            stint_cache[(dn, sn)] = stint_id
        print(f"  Stints: {len(stints_data)}")

        # ── Laps ────────────────────────────────────────────────────
        laps_data = openf1_fetch("laps", {"session_key": sk})
        lap_count = 0

        # 按 driver 分组处理
        by_driver = {}
        for l in laps_data:
            by_driver.setdefault(l["driver_number"], []).append(l)

        for dn, driver_laps in sorted(by_driver.items()):
            driver_stints = sorted(
                [s for s in stints_data if s["driver_number"] == dn],
                key=lambda x: x["stint_number"],
            )
            if not driver_stints:
                continue

            for l in sorted(driver_laps, key=lambda x: x["lap_number"]):
                matched = None
                for stint in driver_stints:
                    ls = stint.get("lap_start", 0)
                    le = stint.get("lap_end", 999)
                    if ls <= l["lap_number"] <= le:
                        matched = stint
                        break
                if not matched:
                    continue

                sn = matched["stint_number"]
                stint_id = stint_cache.get((dn, sn))
                if not stint_id or stint_id < 0:
                    continue

                lap_in_stint = l["lap_number"] - matched.get("lap_start", 0)
                tyre_age = matched.get("tyre_age_at_start", 0) + max(0, lap_in_stint)
                is_outlap = 1 if l["lap_number"] == matched.get("lap_start", 0) else 0
                is_inlap = 1 if (l["lap_number"] == matched.get("lap_end", 999)
                                 or l.get("is_pit_out_lap", False)) else 0

                db.insert_lap(
                    session_id=sess_id,
                    driver_number=dn,
                    stint_id=stint_id,
                    lap_number=l["lap_number"],
                    lap_in_stint=lap_in_stint,
                    tyre_age=tyre_age,
                    lap_time=l.get("lap_duration"),
                    sector1=l.get("duration_sector_1"),
                    sector2=l.get("duration_sector_2"),
                    sector3=l.get("duration_sector_3"),
                    speed_i1=l.get("i1_speed"),
                    speed_i2=l.get("i2_speed"),
                    speed_st=l.get("st_speed"),
                    segments_s1=l.get("segments_sector_1", []),
                    segments_s2=l.get("segments_sector_2", []),
                    segments_s3=l.get("segments_sector_3", []),
                    is_outlap=is_outlap,
                    is_inlap=is_inlap,
                    track_status="Green",
                )
                lap_count += 1

        # ── 关闭 Stints ─────────────────────────────────────────────
        for s in stints_data:
            if s.get("lap_end"):
                db.close_stint(sess_id, s["driver_number"], s["stint_number"], s["lap_end"])

        # ── 更新 Session end_time ────────────────────────────────────
        if laps_data:
            max_lap = max(l["lap_number"] for l in laps_data)
            db.update_session_end(sess_id, None, max_lap)

        print(f"  Laps: {lap_count}")
        total_stints += len(stints_data)
        total_laps += lap_count

        # ── Pit Events ──────────────────────────────────────────────
        pit_data = openf1_fetch("pit", {"session_key": sk})
        for p in pit_data:
            db.insert_pit_event(
                sess_id, p["driver_number"],
                lap_number=p.get("lap_number"),
                pit_duration=p.get("pit_duration"),
                lane_duration=p.get("lane_duration"),
                stop_duration=p.get("stop_duration"),
                timestamp=p.get("date"),
            )
        print(f"  Pit: {len(pit_data)}")
        total_pit += len(pit_data)

    # ── 5. 统计输出 ─────────────────────────────────────────────────
    stats = db.get_stats()
    print(f"\n{'='*55}")
    print(f"  Database Summary (FP1 → Qualifying)")
    print(f"{'='*55}")
    print(f"  Sessions:     {stats['sessions']}")
    print(f"  Drivers:      {stats['drivers']}")
    print(f"  Stints:       {stats['stints']}")
    print(f"  Laps:         {stats['laps']}")
    print(f"  Pit Events:   {stats['pit_events']}")
    print(f"  DB Size:      {stats['db_size_mb']} MB")
    print(f"\n  Per Session:")
    for s in stats["per_session"]:
        print(f"    {s['name']:20s} ({s['type']:12s}): {s['laps']:5d} laps, {s['drivers']:2d} drivers")

    db.close()
    print(f"\n  Done: {DB_PATH}")
    print(f"  Note: Race session (11307) excluded — simulating pre-race data collection")


if __name__ == "__main__":
    fetch_all()
