"""Replay recorded OpenF1 data through the real F1 live-timing pipeline.

Uses cursor-based iteration for O(n) total complexity.



Usage: $env:REPLAY_MODE=1; $env:REPLAY_SPEED=10; python run.py

"""

from __future__ import annotations

import asyncio

import json

import logging

import os

import time

from collections import defaultdict

from datetime import datetime

from typing import Any, Callable, Optional

from ..codec.decoder import decode_topic_data
from .base import BaseFeed

from ..circuit_data import load_circuits



logger = logging.getLogger(__name__)



_EMBEDDED = os.path.join(os.path.dirname(os.path.dirname(__file__)), "replay_data")
_ARCHIVE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    "archive", "design_prototypes", "data",
)
REPLAY_DATA_DIR = (
    os.environ.get("REPLAY_DATA_DIR")
    or (_ARCHIVE if os.path.isdir(_ARCHIVE) else None)
    or _EMBEDDED
)




TOPICS = [

    "Heartbeat", "CarData.z", "Position.z", "ExtrapolatedClock",

    "TopThree", "TimingStats", "TimingAppData", "WeatherData",

    "TrackStatus", "DriverList", "RaceControlMessages",

    "SessionInfo", "SessionData", "LapCount", "TimingData",

    "TeamRadio", "AudioStreams", "ContentStreams",

]





def _ts(date_str: str) -> float:

    return datetime.fromisoformat(date_str.replace("Z", "+00:00")).timestamp()





class ReplayClient(BaseFeed):

    """Replays recorded OpenF1 data through the real backend pipeline.



    Mirrors F1SignalRClient interface so main.py can swap it in

    via REPLAY_MODE env var.  Respects REPLAY_SPEED.



    Uses cursor-based per-data-type iteration so total work is O(N_records).

    """

    @property
    def is_connected(self) -> bool:
        return self._running

    @property
    def name(self) -> str:
        return "replay"

    def __init__(

        self,

        token: str = "",

        on_message: Optional[Callable] = None,

        topics: Optional[list[str]] = None,

        data_dir: str = "",

    ):

        self.token = token

        self.on_message = on_message

        self.topics = topics or TOPICS

        self.data_dir = data_dir or REPLAY_DATA_DIR

        self._running = False

        self._session_key: int = 0



        # 闁冲厜鍋撻柍鍏夊亾 sorted data arrays (one list per type, sorted by ts) 闁冲厜鍋撻柍鍏夊亾

        self._records: dict[str, list[dict]] = {}

        # 闁冲厜鍋撻柍鍏夊亾 per-type cursor (index into _records) 闁冲厜鍋撻柍鍏夊亾

        self._cursors: dict[str, int] = {}

        # 闁冲厜鍋撻柍鍏夊亾 state trackers 闁冲厜鍋撻柍鍏夊亾

        self._weather_idx = -1

        self._weather_cursor = 0

        self._last_lap_sent = 0
        self._last_stint_lap = -1
        self._stint_schedule: dict[int, list[dict]] = {}
        self._current_driver_stints: dict[int, dict] = {}

        self._pos_cursor: int = 0

        self._last_sent_pos: dict[int, int] = {}

        self._replay_wall_start: float = 0.0

        self._first_ts: float = 0.0



    # 闁冲厜鍋撻柍鍏夊亾 public interface (mirrors F1SignalRClient) 闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾



    async def start(self):

        self._running = True

        await self._replay()



    async def stop(self):

        self._running = False



    # 闁冲厜鍋撻柍鍏夊亾 replay engine 闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾



    async def _replay(self):

        replay_speed = float(os.environ.get("REPLAY_SPEED", "10"))



        self._load_and_sort()

        self._send_initial_state()





        timeline = self._build_timeline()
        # Skip pre-race data: filter timeline to only include timestamps >= first lap start
        laps = self._records.get("laps", [])
        first_lap_ts = None
        for l in laps:
            if l.get("lap_number") == 1:
                try:
                    first_lap_ts = _ts(l["date_start"])
                    break
                except Exception:
                    pass
        if first_lap_ts:
            timeline = [t for t in timeline if t >= first_lap_ts - 180]


        record_count = sum(len(v) for v in self._records.values() if isinstance(v, list))

        # RaceControlMessages - send immediately so processor has them from the start
        rc = self._records.get("race_control", [])
        if rc:
            msgs = {}
            _now = time.time()
            for i, m in enumerate(rc):
                msgs[str(i)] = {
                    "Category": m.get("category",""),
                    "Message": m.get("message","") or m.get("flag",""),
                    "Flag": m.get("flag"),
                    "Lap": m.get("lap_number"),
                }
            self.on_message("RaceControlMessages", {"Messages": msgs}, _now)
        logger.info(f"Replay: {len(timeline):,} frames, {record_count:,} records, {replay_speed}x")



        self._first_ts = timeline[0] if timeline else 0.0

        self._replay_wall_start = time.perf_counter()

        frame_count = 0



        for frame_ts in timeline:

            if not self._running:

                break



            # Advance all cursors to frame_ts

            self._advance_cursor("car_data", frame_ts)

            self._advance_cursor("location", frame_ts)

            self._advance_cursor("intervals", frame_ts)

            self._advance_cursor("weather", frame_ts)



            # Send everything at or before this ts

            self._send_position()

            self._send_car_data()

            self._send_timing(frame_ts)
            self._send_stint_updates(frame_ts)

            self._process_positions(frame_ts)

            self._send_weather_if_changed(frame_ts)

            self._send_sector_events(frame_ts)



            # Wall-clock-based pacing (avoids Windows ~15ms timer granularity)

            elapsed_race = frame_ts - self._first_ts

            target_wall = elapsed_race / replay_speed

            actual_wall = time.perf_counter() - self._replay_wall_start

            to_sleep = target_wall - actual_wall

            if to_sleep > 0.001:

                await asyncio.sleep(to_sleep)

            else:

                await asyncio.sleep(0)  # yield to event loop even when no sleep needed

            frame_count += 1



            if frame_count % 500 == 0:

                self.on_message("Heartbeat", {}, time.time())



        logger.info(f"Replay complete: {frame_count} frames, "

                    f"cursors: car_data={self._cursors.get('car_data',0)}, "

                    f"location={self._cursors.get('location',0)}, "

                    f"intervals={self._cursors.get('intervals',0)}")



    # 闁冲厜鍋撻柍鍏夊亾 load & sort 闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾



    def _load_and_sort(self):

        raw: dict[str, list] = {}

        for name in ["drivers","car_data","location","position",

                      "intervals","weather","laps","pit","stints",

                      "race_control","meeting","sessions"]:

            path = os.path.join(self.data_dir, f"{name}.json")

            if os.path.exists(path):

                with open(path, "r", encoding="utf-8") as f:

                    raw[name] = json.load(f)



        # Determine race session_key

        for s in raw.get("sessions", []):

            if s.get("session_type") == "Race":

                self._session_key = s["session_key"]

                break

        if not self._session_key and raw.get("sessions"):

            self._session_key = raw["sessions"][0]["session_key"]



        # Filter to race session and sort by timestamp

        for key in ["car_data", "intervals", "location", "weather", "position", "laps"]:

            records = raw.get(key, [])

            filtered = [

                r for r in records

                if r.get("session_key") == self._session_key and r.get("date")

            ]

            try:

                filtered.sort(key=lambda r: _ts(r["date"]))

            except Exception as e:

                logger.warning(f"Sort failed for {key}: {e}")

                filtered.sort(key=lambda r: str(r.get("date", "")))

            self._records[key] = filtered

            self._cursors[key] = 0



        # Assign sequential index to each car_data record for telemetry generation

        for idx, rec in enumerate(self._records.get("car_data", [])):

            rec["_idx"] = idx



        # Build per-driver position timeline for O(log n) lookups

        self._pos_by_driver: dict[int, list[tuple[float, int]]] = {}

        for p in self._records.get("position", []):

            dn = p["driver_number"]

            try:

                ts = _ts(p["date"])

                pos = p.get("position", 0)

                self._pos_by_driver.setdefault(dn, []).append((ts, pos))

            except Exception:

                pass



        # Store other data

        self._records["drivers"] = raw.get("drivers", [])

        self._records["meeting"] = raw.get("meeting", [])

        self._records["sessions"] = raw.get("sessions", [])

        self._records["laps"] = raw.get("laps", [])

        self._records["stints"] = raw.get("stints", [])

        self._records["race_control"] = raw.get("race_control", [])
        # Build InPit windows from pit.json for status detection
        self._inpit_windows: dict[int, list[tuple]] = {}
        for rec in raw.get("pit", []):
            try:
                dn = rec["driver_number"]
                entry_ts = _ts(rec["date"])
                exit_ts = entry_ts + rec.get("pit_duration", 0)
                if dn not in self._inpit_windows:
                    self._inpit_windows[dn] = []
                self._inpit_windows[dn].append((entry_ts, exit_ts))
            except Exception:
                pass

        # ---- Build sector completion events from laps data ----
        self._sector_events: list[tuple] = []
        for lap in self._records.get("laps", []):
            try:
                start_ts = _ts(lap["date_start"])
            except (KeyError, ValueError):
                continue
            dn = lap["driver_number"]
            # Build combined 22-element Segments array
            s1_raw = lap.get("segments_sector_1") or []
            s2_raw = lap.get("segments_sector_2") or []
            s3_raw = lap.get("segments_sector_3") or []
            s1s = [2048 if v is None else v for v in s1_raw]
            s2s = [2048 if v is None else v for v in s2_raw]
            s3s = [2048 if v is None else v for v in s3_raw]
            combined = s1s + s2s + s3s
            if not combined:
                continue
            if len(combined) < 22:
                combined = combined + [2048] * (22 - len(combined))
            combined = combined[:22]
            d1 = lap.get("duration_sector_1")
            d2 = lap.get("duration_sector_2")
            d3 = lap.get("duration_sector_3")
            # Build progressively revealed Segments arrays:
            # S1 event: first 7 segments filled (0-6)
            # S2 event: first 15 segments filled (0-14)
            # S3 event: all 22 filled
            if d1 is not None and start_ts > 0:
                s1_segs = [0] * 22
                for si in range(0, 7):
                    s1_segs[si] = combined[si]
                self._sector_events.append((start_ts + d1, dn, 0, f"{d1:.3f}", s1_segs))
            if d2 is not None and d1 is not None and start_ts > 0:
                s2_ts = start_ts + d1 + d2
                s2_segs = [0] * 22
                for si in range(0, 15):
                    s2_segs[si] = combined[si]
                self._sector_events.append((s2_ts, dn, 1, f"{d2:.3f}", s2_segs))
            if d3 is not None and d1 is not None and d2 is not None and start_ts > 0:
                s3_ts = start_ts + d1 + d2 + d3
                self._sector_events.append((s3_ts, dn, 2, f"{d3:.3f}", list(combined)))
        self._sector_events.sort(key=lambda x: x[0])
        self._sector_cursor: int = 0
        # Build stint schedule per driver for incremental updates
        self._stint_schedule = {}
        for s in self._records.get("stints", []):
            dn = s["driver_number"]
            if dn not in self._stint_schedule:
                self._stint_schedule[dn] = []
            self._stint_schedule[dn].append(s)
        for dn in self._stint_schedule:
            self._stint_schedule[dn].sort(key=lambda x: x.get("lap_start", 0))

    def _advance_cursor(self, key: str, ts: float):

        """Advance cursor for *key* to include all records with date <= ts."""

        records = self._records.get(key, [])

        cursor = self._cursors.get(key, 0)

        while cursor < len(records):

            try:

                if _ts(records[cursor]["date"]) <= ts:

                    cursor += 1

                else:

                    break

            except Exception:

                cursor += 1  # skip bad records

        self._cursors[key] = cursor



    def _cursor_window(self, key: str) -> list[dict]:

        """Return records between previous cursor and current cursor (= new ones)."""

        records = self._records.get(key, [])

        cursor = self._cursors.get(key, 0)

        # We need the previous cursor; store a "prev_cursor" for this.

        # Actually we track "sent up to" as a separate concept.

        # Simpler: get records from last_sent_idx to cursor

        return records[:cursor]  # caller will filter by last_sent_idx



    def _new_records(self, key: str, last_sent_idx_key: str) -> list[dict]:

        """Return records not yet sent: between stored sent_idx and current cursor."""

        records = self._records.get(key, [])

        cursor = self._cursors.get(key, 0)

        sent_idx = getattr(self, f"_sent_{last_sent_idx_key}", 0)

        result = records[sent_idx:cursor]

        return result



    def _mark_sent(self, key: str):

        """Set sent marker to current cursor for *key*."""

        setattr(self, f"_sent_{key}", self._cursors.get(key, 0))



    # 闁冲厜鍋撻柍鍏夊亾 initial state 闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾



    def _send_initial_state(self):

        now = time.time()

        ms = self._records.get("meeting", [])

        ss = self._records.get("sessions", [])

        race_s = next((s for s in ss if s.get("session_type") == "Race"), ss[0] if ss else {})



        # SessionInfo

        if ms:

            m = ms[0]

            self.on_message("SessionInfo", {

                "Meeting": {

                    "Key": m.get("meeting_key"),

                    "Name": m.get("meeting_name", ""),

                    "Circuit": {"ShortName": race_s.get("circuit_short_name", "")},

                    "Country": {"Name": m.get("country_name", "")},

                },

                "Key": race_s.get("session_key"),

                "Name": race_s.get("session_name", "Race"),

                "Type": race_s.get("session_type", "Race"),

                "SessionStatus": "Started",

            }, now)



        # DriverList

        drivers = self._records.get("drivers", [])

        if drivers:

            lines = {}

            for d in drivers:

                dn = str(d["driver_number"])

                tc = d.get("team_colour", "#cccccc")

                if tc and not tc.startswith("#"):

                    tc = "#" + tc

                lines[dn] = {

                    "RacingNumber": d.get("driver_number"),

                    "BroadcastName": d.get("broadcast_name", ""),

                    "FullName": d.get("full_name", ""),

                    "FirstName": d.get("first_name",""),

                    "LastName": d.get("last_name",""),

                    "TeamName": d.get("team_name",""),

                    "TeamColour": tc,

                    "Tla": d.get("name_acronym",""),

                    "HeadshotUrl": d.get("headshot_url"),

                    "CountryCode": d.get("country_code",""),

                }

            self.on_message("DriverList", {"Lines": lines}, now)



    # TimingData (first intervals per driver)
        intervals = self._records.get("intervals", [])
        # Build first interval per driver, detect leader from gap data
        first_iv = {}
        for iv in intervals:
            dn = iv["driver_number"]
            if dn not in first_iv:
                first_iv[dn] = iv

        positions = self._records.get("position", [])
        pos_map = {}
        for p in positions:
            if p["driver_number"] not in pos_map:
                pos_map[p["driver_number"]] = p.get("position", 0)

        if first_iv:
            lines = {}
            for dn, iv in first_iv.items():
                gap = iv.get("gap_to_leader", 0)

                ival = iv.get("interval")

                lines[str(dn)] = {

                    "Position": pos_map.get(dn, len(first_iv)),

                    "GapToLeader": f"+{gap:.3f}" if gap is not None else None,

                    "Interval": f"+{ival:.3f}" if ival is not None else None,

                }

            self.on_message("TimingData", {"Lines": lines}, now)



        # LapCount

        laps = self._records.get("laps", [])

        if laps:

            total = max(l.get("lap_number", 0) for l in laps)

            self.on_message("LapCount", {"TotalLaps": total, "CurrentLap": 1}, now)



        # Weather

        weather_data = self._records.get("weather", [])

        if weather_data:

            self._send_weather(weather_data[0], now)

            self._weather_idx = 0



        # TimingAppData (stints) - only send initial stint per driver
        if self._stint_schedule:
            initial_lines = {}
            for dn, driver_stints in self._stint_schedule.items():
                if not driver_stints:
                    continue
                s = driver_stints[0]
                initial_lines[str(dn)] = {
                    "Stint": s.get("stint_number", 0),
                    "Compound": s.get("compound", ""),
                    "TyreAge": s.get("tyre_age_at_start", 0),
                    "FreshTyre": s.get("tyre_age_at_start", 999) <= 1,
                }
                self._current_driver_stints[dn] = s
            if initial_lines:
                self.on_message("TimingAppData", {"Lines": initial_lines}, now)



    # 闁冲厜鍋撻柍鍏夊亾 timeline 闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋?



    def _build_timeline(self) -> list[float]:

        timestamps: set[float] = set()

        for key in ["car_data", "intervals", "location", "weather"]:

            for rec in self._records.get(key, []):

                try:

                    timestamps.add(_ts(rec["date"]))

                except Exception:

                    pass

        result = sorted(timestamps)

        return result



    # 闁冲厜鍋撻柍鍏夊亾 per-frame senders 闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾闁冲厜鍋撻柍鍏夊亾





    # ?? synthetic telemetry generator (design data has speed=0) ?????



    def _process_positions(self, frame_ts: float):
        """Update positions using cursor-based iteration (O(n) total)."""
        records = self._records.get("position", [])
        cursor = self._pos_cursor
        batch = {}
        while cursor < len(records):
            rec = records[cursor]
            try:
                rec_ts = _ts(rec["date"])
                if rec_ts <= frame_ts:
                    dn = rec["driver_number"]
                    pos = rec.get("position", 0)
                    last = self._last_sent_pos.get(dn)
                    if pos != last and isinstance(pos, int):
                        batch[str(dn)] = {"Position": pos}
                        self._last_sent_pos[dn] = pos
                    cursor += 1
                else:
                    break
            except Exception:
                cursor += 1
        self._pos_cursor = cursor
        if batch:
            self.on_message("TimingData", {"Lines": batch}, frame_ts)


    def _gen_speed_profile(self, laps_list: list, track_len: float) -> None:


        """Build a per-driver-per-lap speed lookup.



        For each (driver, lap) pair, stores a 100-point speed profile

        interpolated from the three OpenF1 speed-trap values

        (i1_speed, i2_speed, st_speed) and the sector times.



        Each car_data record is then enriched with synthetic Speed,

        RPM, Throttle, Brake, nGear, DRS and distance values.

        """

        if not laps_list:

            return



        # Barcelona corner locations as fraction of lap length (approximate)

        corner_frac = [0.05, 0.08, 0.12, 0.16, 0.20, 0.28, 0.32, 0.35,

                       0.40, 0.45, 0.55, 0.60, 0.65, 0.70]

        # Typical speed reduction factor at each corner (relative to avg)

        corner_mult = [0.30, 0.40, 0.50, 0.40, 0.45, 0.35, 0.40, 0.35,

                       0.55, 0.45, 0.35, 0.30, 0.35, 0.35]



        # Group laps by driver

        by_driver: dict = {}

        for l in laps_list:

            dn = l["driver_number"]

            if dn not in by_driver:

                by_driver[dn] = {}

            by_driver[dn][l["lap_number"]] = l



        # For each car_data record, compute lap info

        car_data = self._records.get("car_data", [])

        for rec in car_data:

            dn = rec["driver_number"]

            rec_ts = None

            try:

                from datetime import datetime as _dt

                rec_ts = _dt.fromisoformat(rec["date"].replace("Z", "+00:00"))

            except Exception:

                continue



            driver_laps = by_driver.get(dn, {})

            if not driver_laps:

                continue



            # Find which lap this record belongs to

            best_lap = None

            best_ldata = None

            best_diff = float("inf")

            for ln, ldata in driver_laps.items():

                try:

                    ls = _dt.fromisoformat(ldata["date_start"].replace("Z", "+00:00"))

                    diff = (rec_ts - ls).total_seconds()

                    lap_dur = ldata.get("lap_duration", 0)

                    if 0 <= diff < (lap_dur or 120) and diff < best_diff:

                        best_diff = diff

                        best_lap = ln

                        best_ldata = ldata

                except Exception:

                    continue



            if best_lap is None:

                # Pre-race data: use first available lap as fallback

                if not driver_laps:

                    continue

                first_lap_num = min(driver_laps.keys())

                best_lap = first_lap_num

                best_ldata = driver_laps[first_lap_num]

                best_diff = 0.0



            lap_dur = best_ldata.get("lap_duration") or 90

            progress = max(0.0, min(0.995, best_diff / lap_dur))



            # Speed trap values

            i1 = best_ldata.get("i1_speed", 250) or 250

            i2 = best_ldata.get("i2_speed", 250) or 250

            st = best_ldata.get("st_speed", 300) or 300



            # Compute synthetic speed: blend of trap values with corner slowdown

            # Identify which sector we are in (by progress)

            # S1: 0-0.30, S2: 0.30-0.65, S3: 0.65-1.0 (approximate for Barcelona)

            if progress < 0.30:

                # Sector 1: entry speed ~st, exit speed ~i1

                p_in_sector = progress / 0.30

                target_speed = i1 + (st - i1) * (1.0 - p_in_sector)

            elif progress < 0.65:

                # Sector 2: entry ~i1, exit ~i2

                p_in_sector = (progress - 0.30) / 0.35

                target_speed = i1 + (i2 - i1) * p_in_sector

            else:

                # Sector 3: entry ~i2, exit ~st

                p_in_sector = (progress - 0.65) / 0.35

                target_speed = i2 + (st - i2) * p_in_sector



            # Corner effect: reduce speed near corners

            for i, cf in enumerate(corner_frac):

                d = abs(progress - cf)

                if d < 0.04:

                    blend = 1.0 - d / 0.04

                    corner_spd = target_speed * corner_mult[i]

                    target_speed = target_speed * (1.0 - blend * 0.6) + corner_spd * blend * 0.6



            speed = max(30.0, min(st * 1.1, target_speed))

            n_gear = min(8, max(1, int(speed / 40) + 1))

            rpm = int(speed * (2000 + n_gear * 800) / 300)

            throttle = max(0, min(100, int(120 - (st - speed) * 0.3)))

            brake = max(0, min(100, int(100 - throttle)))



            # Write back to the record (only as fallback for genuinely missing data)

            if rec.get("speed") is None:

                rec["speed"] = round(speed)

            if rec.get("rpm") is None:

                rec["rpm"] = rpm

            if rec.get("throttle") is None:

                rec["throttle"] = throttle

            if rec.get("brake") is None:

                rec["brake"] = brake

            if rec.get("n_gear") is None:

                rec["n_gear"] = n_gear

            rec["_lap"] = best_lap

            rec["_distance"] = round(progress * track_len, 1)



    def _get_telemetry(self, rec: dict) -> dict:

        """Return telemetry, using REAL data from FastF1 if available, else fallback."""

        # Brake normalization: FastF1 may return 0/1 or 0-100

        return {

            "Speed": rec.get("speed", 0),

            "RPM": rec.get("rpm", 0) or 0,

            "Throttle": rec.get("throttle", 0),

            "Brake": rec.get("brake", 0) if isinstance(rec.get("brake"), (int, float)) else 0,

            "DRS": rec.get("drs"),

            "nGear": rec.get("n_gear", 0) or 0,

            "date": rec.get("date", ""),

        }

    def _send_car_data(self):

        new = self._new_records("car_data", "car_data")

        if not new:

            return

        # Group by approximate time (records within 0.3s)

        batches: list[dict] = []

        cur_batch: dict = {}

        cur_ts: float | None = None

        for rec in new:

            dn = str(rec["driver_number"])

            rec_ts = _ts(rec["date"])

            if cur_ts is not None and abs(rec_ts - cur_ts) > 0.3 and cur_batch:

                batches.append(cur_batch)

                cur_batch = {}

            cur_batch[dn] = self._get_telemetry(rec)

        if cur_batch:

            batches.append(cur_batch)

        for batch in batches:

            self.on_message("CarData.z", {"Entries": [{"Cars": batch}]}, time.time())

        self._mark_sent("car_data")



    def _is_in_pit(self, driver_number: int, ts: float) -> bool:
        windows = self._inpit_windows.get(driver_number, [])
        for entry_ts, exit_ts in windows:
            if entry_ts <= ts <= exit_ts:
                return True
        return False
 
 
    def _send_position(self):

        new = self._new_records("location", "location")

        if not new:

            return

        batches: list[dict] = []

        cur_batch: dict = {}

        cur_ts: float | None = None

        for rec in new:

            dn = str(rec["driver_number"])

            rec_ts = _ts(rec["date"])

            if cur_ts is not None and abs(rec_ts - cur_ts) > 0.3 and cur_batch:

                batches.append(cur_batch)

                cur_batch = {}

            cur_ts = rec_ts

            cur_batch[dn] = {

                "X": rec.get("x", 0),

                "Y": rec.get("y", 0),

                "Z": rec.get("z", 0),

                "Status": ("InPit" if self._is_in_pit(int(dn), rec_ts) else "OnTrack"),

            }

        if cur_batch:

            batches.append(cur_batch)

        for batch in batches:

            self.on_message("Position.z", {"Entries": [{"Cars": batch}]}, time.time())

        self._mark_sent("location")



    def _send_timing(self, frame_ts: float):

        new = self._new_records("intervals", "intervals")

        if not new:

            return

        # Group by approximate time

        batches: list[dict] = []

        cur_batch: dict = {}

        cur_ts: float | None = None

        for rec in new:

            dn = str(rec["driver_number"])

            rec_ts = _ts(rec["date"])

            if cur_ts is not None and abs(rec_ts - cur_ts) > 2.0 and cur_batch:

                batches.append(cur_batch)

                cur_batch = {}

            cur_ts = rec_ts

            gap = rec.get("gap_to_leader")

            ival = rec.get("interval")

            cur_batch[dn] = {

                "GapToLeader": f"+{gap:.3f}" if isinstance(gap, (int, float)) else (str(gap) if gap is not None else None),

                "Interval": f"+{ival:.3f}" if isinstance(ival, (int, float)) else (str(ival) if ival is not None else None),

            }

        if cur_batch:

            batches.append(cur_batch)



        # Merge with race position data (O(log n) per driver via pre-built index)

        for batch in batches:

            for dn_str in list(batch.keys()):

                dn = int(dn_str)

            self.on_message("TimingData", {"Lines": batch}, frame_ts)

        self._mark_sent("intervals")



        # LapCount

        laps = self._records.get("laps", [])

        if laps:

            lap_starts = 0

            for l in laps:

                try:

                    if _ts(l["date_start"]) <= frame_ts:

                        lap_starts = max(lap_starts, l.get("lap_number", 0))

                except Exception:

                    pass

            if lap_starts > 0 and lap_starts != self._last_lap_sent:

                self._last_lap_sent = lap_starts

                total = max(l.get("lap_number", 0) for l in laps)

                self.on_message("LapCount", {"TotalLaps": total, "CurrentLap": lap_starts}, frame_ts)



    def _send_weather(self, w: dict, ts: float):

        self.on_message("WeatherData", {

            "AirTemp": w.get("air_temperature", 0),

            "TrackTemp": w.get("track_temperature", 0),

            "Humidity": w.get("humidity", 0),

            "Pressure": w.get("pressure", 0),

            "WindSpeed": w.get("wind_speed", 0),

            "WindDirection": w.get("wind_direction", 0),

            "Rainfall": w.get("rainfall", False),

        }, ts)



    def _send_weather_if_changed(self, frame_ts: float):

        weather_data = self._records.get("weather", [])

        if not weather_data:

            return

        # Cursor-based O(1) per frame instead of O(N) full scan

        cursor = self._weather_cursor

        while cursor < len(weather_data):

            try:

                if _ts(weather_data[cursor]["date"]) <= frame_ts:

                    cursor += 1

                else:

                    break

            except Exception:

                cursor += 1

        self._weather_cursor = cursor

        if cursor > 0 and cursor - 1 != self._weather_idx:

            self._weather_idx = cursor - 1

            self._send_weather(weather_data[cursor - 1], frame_ts)



    def _send_sector_events(self, frame_ts: float):
        """Send TimingData with sector times and Segments at sector completion timestamps."""
        while self._sector_cursor < len(self._sector_events):
            ev = self._sector_events[self._sector_cursor]
            ev_ts, dn, sec_idx, sec_time_str, segs = ev
            if ev_ts > frame_ts:
                break
            # Build TimingData message
            sec_key = ["Sector1", "Sector2", "Sector3"][sec_idx]
            lines = {str(dn): {sec_key: sec_time_str, "Segments": segs}}
            self.on_message("TimingData", {"Lines": lines}, ev_ts)
            self._sector_cursor += 1

    def _send_stint_updates(self, frame_ts: float):
        """Send compound updates when a driver changes tires during replay."""
        cur_lap = self._last_lap_sent
        if cur_lap < 1 or cur_lap == self._last_stint_lap:
            return
        self._last_stint_lap = cur_lap
        updates = {}
        for dn, driver_stints in self._stint_schedule.items():
            if not driver_stints:
                continue
            active = None
            for s in driver_stints:
                ls = s.get("lap_start", 0)
                le = s.get("lap_end", 999)
                if ls <= cur_lap <= le:
                    active = s
                    break
            if active is None:
                active = driver_stints[-1]
            cur_compound = (self._current_driver_stints.get(dn) or {}).get("compound", "")
            new_compound = active.get("compound", "")
            if new_compound and new_compound != cur_compound:
                updates[str(dn)] = {
                    "Stint": active.get("stint_number", 0),
                    "Compound": new_compound,
                    "TyreAge": active.get("tyre_age_at_start", 0),
                    "FreshTyre": active.get("tyre_age_at_start", 999) <= 1,
                }
                self._current_driver_stints[dn] = active
        if updates:
            self.on_message('TimingAppData', {'Lines': updates}, frame_ts)



