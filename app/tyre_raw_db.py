"""
RawTyreDB — 轮胎原始数据库模块

SQLite 单文件数据库，以不可变方式存储每个比赛周末的所有轮胎生数据。
数据层级: Session/Event → Driver → Stint → Lap

写入原则:
- 仅 INSERT，永不 UPDATE/DELETE lap 数据
- 使用 ON CONFLICT DO NOTHING 防重复
- 保留原始时间戳和来源标记
"""

from __future__ import annotations
import os
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional, Any

logger = logging.getLogger(__name__)

_LOCAL_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tyre_raw.db")
DB_PATH = os.environ.get("TYRE_DB_PATH") or _LOCAL_DB_PATH


class RawTyreDB:
    """轮胎原始数据库。

    用法:
        db = RawTyreDB("tyre_raw.db")
        sid = db.get_or_create_session(11307, "Barcelona GP", "Race", "Race", "2026-06-14T13:00:00Z")
        db.upsert_driver(1, "NORRIS", "McLaren", "#F47600")
        stint_id = db.insert_stint(sid, 1, 1, "MEDIUM", 0, 1)
        db.insert_lap(sid, 1, stint_id, 1, 1, 0, 82.251, 23.744, 33.611, 24.896, ...)
    """

    def __init__(self, db_path: str = ""):
        self.db_path = db_path or DB_PATH
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_key INTEGER UNIQUE NOT NULL,
            meeting_name TEXT NOT NULL,
            session_type TEXT NOT NULL,
            session_name TEXT,
            start_time TEXT NOT NULL,
            end_time TEXT,
            track_name TEXT,
            total_laps INTEGER
        );

        CREATE TABLE IF NOT EXISTS drivers (
            driver_number INTEGER PRIMARY KEY,
            driver_name TEXT NOT NULL,
            team_name TEXT,
            color TEXT
        );

        CREATE TABLE IF NOT EXISTS stints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL REFERENCES sessions(id),
            driver_number INTEGER NOT NULL,
            stint_number INTEGER NOT NULL,
            compound TEXT NOT NULL,
            tyre_age_at_start INTEGER NOT NULL,
            lap_start INTEGER,
            lap_end INTEGER,
            total_laps INTEGER,
            start_time TEXT,
            end_time TEXT,
            UNIQUE(session_id, driver_number, stint_number)
        );

        CREATE TABLE IF NOT EXISTS laps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL REFERENCES sessions(id),
            driver_number INTEGER NOT NULL,
            stint_id INTEGER REFERENCES stints(id),
            lap_number INTEGER NOT NULL,
            lap_in_stint INTEGER,
            tyre_age INTEGER,
            lap_time REAL,
            sector1 REAL,
            sector2 REAL,
            sector3 REAL,
            speed_i1 REAL,
            speed_i2 REAL,
            speed_st REAL,
            segments_s1 TEXT,
            segments_s2 TEXT,
            segments_s3 TEXT,
            is_outlap INTEGER DEFAULT 0,
            is_inlap INTEGER DEFAULT 0,
            is_personal_best INTEGER DEFAULT 0,
            track_status TEXT,
            air_temp REAL,
            track_temp REAL,
            wind_speed REAL,
            humidity REAL,
            recorded_at TEXT NOT NULL,
            source TEXT DEFAULT 'live_timing',
            UNIQUE(session_id, driver_number, lap_number)
        );

        CREATE TABLE IF NOT EXISTS pit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL REFERENCES sessions(id),
            driver_number INTEGER NOT NULL,
            lap_number INTEGER,
            pit_duration REAL,
            lane_duration REAL,
            stop_duration REAL,
            compound_after TEXT,
            timestamp TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_laps_session_driver ON laps(session_id, driver_number);
        CREATE INDEX IF NOT EXISTS idx_laps_stint ON laps(stint_id);
        """)
        self.conn.commit()

    # ── Session ──────────────────────────────────────────────────────

    def get_or_create_session(self, session_key: int, meeting_name: str,
                               session_type: str, session_name: str,
                               start_time: str, track_name: str = None) -> int:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO sessions(session_key, meeting_name, session_type, session_name, start_time, track_name) "
            "VALUES (?,?,?,?,?,?)",
            (session_key, meeting_name, session_type, session_name, start_time, track_name)
        )
        self.conn.commit()
        if cur.lastrowid:
            return cur.lastrowid
        row = self.conn.execute(
            "SELECT id FROM sessions WHERE session_key = ?", (session_key,)
        ).fetchone()
        return row[0] if row else -1

    def update_session_end(self, session_id: int, end_time: str, total_laps: int = None):
        if total_laps is not None:
            self.conn.execute(
                "UPDATE sessions SET end_time=?, total_laps=? WHERE id=?",
                (end_time, total_laps, session_id)
            )
        else:
            self.conn.execute(
                "UPDATE sessions SET end_time=? WHERE id=?",
                (end_time, session_id)
            )
        self.conn.commit()

    # ── Driver ───────────────────────────────────────────────────────

    def upsert_driver(self, driver_number: int, driver_name: str,
                       team_name: str = None, color: str = None):
        self.conn.execute(
            "INSERT OR REPLACE INTO drivers(driver_number, driver_name, team_name, color) "
            "VALUES (?,?,?,?)",
            (driver_number, driver_name, team_name, color)
        )
        self.conn.commit()

    # ── Stint ────────────────────────────────────────────────────────

    def insert_stint(self, session_id: int, driver_number: int,
                      stint_number: int, compound: str,
                      tyre_age_at_start: int = 0, lap_start: int = None) -> int:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO stints(session_id, driver_number, stint_number, "
            "compound, tyre_age_at_start, lap_start, start_time) "
            "VALUES (?,?,?,?,?,?,?)",
            (session_id, driver_number, stint_number, compound, tyre_age_at_start,
             lap_start, datetime.now(timezone.utc).isoformat())
        )
        self.conn.commit()
        if cur.lastrowid:
            return cur.lastrowid
        row = self.conn.execute(
            "SELECT id FROM stints WHERE session_id=? AND driver_number=? AND stint_number=?",
            (session_id, driver_number, stint_number)
        ).fetchone()
        return row[0] if row else -1

    def close_stint(self, session_id: int, driver_number: int,
                     stint_number: int, lap_end: int):
        lap_start = self._get_stint_lap_start(session_id, driver_number, stint_number)
        total_laps = lap_end - lap_start if lap_start else None
        self.conn.execute(
            "UPDATE stints SET lap_end=?, total_laps=?, end_time=? "
            "WHERE session_id=? AND driver_number=? AND stint_number=?",
            (lap_end, total_laps, datetime.now(timezone.utc).isoformat(),
             session_id, driver_number, stint_number)
        )
        self.conn.commit()

    def _get_stint_lap_start(self, session_id: int, driver_number: int,
                              stint_number: int) -> Optional[int]:
        row = self.conn.execute(
            "SELECT lap_start FROM stints WHERE session_id=? AND driver_number=? AND stint_number=?",
            (session_id, driver_number, stint_number)
        ).fetchone()
        return row[0] if row else None

    def get_stint_id(self, session_id: int, driver_number: int,
                      stint_number: int) -> Optional[int]:
        row = self.conn.execute(
            "SELECT id FROM stints WHERE session_id=? AND driver_number=? AND stint_number=?",
            (session_id, driver_number, stint_number)
        ).fetchone()
        return row[0] if row else None

    # ── Lap ──────────────────────────────────────────────────────────

    def insert_lap(self, session_id: int, driver_number: int,
                    stint_id: int, lap_number: int,
                    lap_in_stint: int, tyre_age: int,
                    lap_time: float,
                    sector1: float = None, sector2: float = None, sector3: float = None,
                    speed_i1: float = None, speed_i2: float = None, speed_st: float = None,
                    segments_s1: str | list = None,
                    segments_s2: str | list = None,
                    segments_s3: str | list = None,
                    is_outlap: int = 0, is_inlap: int = 0,
                    is_personal_best: int = 0,
                    track_status: str = None,
                    air_temp: float = None, track_temp: float = None,
                    wind_speed: float = None, humidity: float = None):
        """插入一圈的完整原始数据。使用 UNIQUE 约束防重复。"""
        def _seg_str(s):
            if isinstance(s, str):
                return s
            if isinstance(s, (list, tuple)):
                return ",".join(str(v) if v is not None else "0" for v in s)
            return ""

        self.conn.execute("""
            INSERT INTO laps(session_id, driver_number, stint_id, lap_number,
                   lap_in_stint, tyre_age, lap_time,
                   sector1, sector2, sector3,
                   speed_i1, speed_i2, speed_st,
                   segments_s1, segments_s2, segments_s3,
                   is_outlap, is_inlap, is_personal_best,
                   track_status, air_temp, track_temp, wind_speed, humidity,
                   recorded_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(session_id, driver_number, lap_number) DO NOTHING
        """, (
            session_id, driver_number, stint_id, lap_number,
            lap_in_stint, tyre_age, lap_time,
            sector1, sector2, sector3,
            speed_i1, speed_i2, speed_st,
            _seg_str(segments_s1), _seg_str(segments_s2), _seg_str(segments_s3),
            is_outlap, is_inlap, is_personal_best,
            track_status, air_temp, track_temp, wind_speed, humidity,
            datetime.now(timezone.utc).isoformat()
        ))
        self.conn.commit()

    # ── Pit Event ────────────────────────────────────────────────────

    def insert_pit_event(self, session_id: int, driver_number: int,
                          lap_number: int = None, pit_duration: float = None,
                          lane_duration: float = None, stop_duration: float = None,
                          compound_after: str = None, timestamp: str = None):
        self.conn.execute("""
            INSERT INTO pit_events(session_id, driver_number, lap_number,
                   pit_duration, lane_duration, stop_duration, compound_after, timestamp)
            VALUES (?,?,?,?,?,?,?,?)
        """, (session_id, driver_number, lap_number,
              pit_duration, lane_duration, stop_duration,
              compound_after, timestamp or datetime.now(timezone.utc).isoformat()))
        self.conn.commit()

    # ── 查询接口 ─────────────────────────────────────────────────────

    def get_laps_by_stint(self, stint_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM laps WHERE stint_id = ? ORDER BY lap_number", (stint_id,)
        ).fetchall()

    def get_laps_by_driver(self, session_id: int, driver_number: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM laps WHERE session_id = ? AND driver_number = ? ORDER BY lap_number",
            (session_id, driver_number)
        ).fetchall()

    def get_stint_summary(self, session_id: int) -> list[sqlite3.Row]:
        return self.conn.execute("""
            SELECT st.driver_number, st.stint_number, st.compound,
                   st.lap_start, st.lap_end, st.total_laps,
                   d.driver_name, d.team_name
            FROM stints st
            JOIN drivers d ON d.driver_number = st.driver_number
            WHERE st.session_id = ?
            ORDER BY st.driver_number, st.stint_number
        """, (session_id,)).fetchall()

    def get_lap_count(self, session_id: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM laps WHERE session_id = ?", (session_id,)
        ).fetchone()
        return row[0] if row else 0

    def get_driver_count(self, session_id: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(DISTINCT driver_number) FROM laps WHERE session_id = ?",
            (session_id,)
        ).fetchone()
        return row[0] if row else 0

    # ── 统计 ─────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """返回数据库概览统计"""
        session_count = self.conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        driver_count = self.conn.execute("SELECT COUNT(*) FROM drivers").fetchone()[0]
        stint_count = self.conn.execute("SELECT COUNT(*) FROM stints").fetchone()[0]
        lap_count = self.conn.execute("SELECT COUNT(*) FROM laps").fetchone()[0]
        pit_count = self.conn.execute("SELECT COUNT(*) FROM pit_events").fetchone()[0]

        sizes = self.conn.execute("""
            SELECT s.session_name, s.session_type,
                   COUNT(DISTINCT l.driver_number) as drivers,
                   COUNT(l.id) as laps
            FROM sessions s
            LEFT JOIN laps l ON l.session_id = s.id
            GROUP BY s.id
            ORDER BY s.id
        """).fetchall()

        db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0

        return {
            "sessions": session_count,
            "drivers": driver_count,
            "stints": stint_count,
            "laps": lap_count,
            "pit_events": pit_count,
            "db_size_bytes": db_size,
            "db_size_mb": round(db_size / 1048576, 2),
            "per_session": [
                {"name": r[0], "type": r[1], "drivers": r[2], "laps": r[3]}
                for r in sizes
            ],
        }

    # ── 生命周期 ─────────────────────────────────────────────────────

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
