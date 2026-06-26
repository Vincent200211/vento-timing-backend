from __future__ import annotations
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from .transports.live_replay_feed import LiveReplayFeed
from .data_processor import DataProcessor
from .transports.signalr_feed import F1SignalRClient, TOPICS
from .transports.replay_feed import ReplayClient
from .storage import SessionRecorder, cleanup_old_data
from .websocket_manager import manager
from .tyre_manager import process_tyre_message
from .tyre_raw_db import RawTyreDB
from .tyre_analysis import router as tyre_analysis_router
from .tyre_database import get_tyre_database
from .degradation_model import fit_best_model, model_to_dict
from .auth import ensure_valid_token

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("vento_timing")

processor = DataProcessor()
recorder = SessionRecorder()
f1_client: F1SignalRClient = None
_replay_start_signal = asyncio.Event()

f1_task = None
_replay_requested = {"active": False, "speed": 10.0}

_dirty_types: set[str] = set()
_BROADCAST_TYPES = [
    "timing", "positions", "car_data", "track_positions",
    "weather", "track_status", "lap_count", "race_control",
    "drivers", "session", "app_data", "stint_history", "circuit",
]

# 在应用启动时加载轮胎数据库
_tyre_db_loaded = False
_raw_tyre_db: RawTyreDB = None

def handle_f1_message(topic: str, data, ts: float):
    processor.process_message(topic, data, ts)
    try:
        recorder.record(topic, data, ts)
    except Exception as e:
        logger.warning(f"Record failed: {e}")
    # 同步轮胎管理器(独立模块)
    try:
        process_tyre_message(topic, data, ts)
    except Exception as e:
        logger.warning(f"TyreManager error: {e}", exc_info=True)

    clean = topic[:-2] if topic.endswith(".z") else topic
    btype_map = {
        "TimingData": "timing", "Position": "positions", "CarData": "car_data",
        "DriverList": "drivers", "SessionInfo": "session", "WeatherData": "weather",
        "TrackStatus": "track_status", "LapCount": "lap_count",
        "TimingAppData": "app_data", "RaceControlMessages": "race_control",
    }
    btype = btype_map.get(clean)
    if btype:
        _dirty_types.add(btype)
    if clean == "Position":
        _dirty_types.add("track_positions")
    if btype == "session":
        _dirty_types.add("circuit")
    if clean == "CarData":
        _dirty_types.add("timing")
    if clean == "TimingAppData":
        _dirty_types.add("stint_history")
    # Update JSON stream feed with session path
    if clean == "SessionInfo" and isinstance(data, dict):
        path = data.get("Path", "")
        status = data.get("SessionStatus", "")
        if path and status == "Started":
            logger.info(f"Session path received: {path}")


async def _broadcast_loop():
    while True:
        await asyncio.sleep(0.1)
        if not _dirty_types:
            continue
        types = list(_dirty_types)
        _dirty_types.clear()
        for btype in types:
            data = processor.get_field(btype)
            if data is not None:
                asyncio.create_task(manager.broadcast(btype, data))


async def f1_connection_loop():
    global f1_client
    # NEW: Live replay mode replays recorded SignalR JSONL data
    live_replay_dir = os.environ.get("LIVE_REPLAY_MODE", "")
    if live_replay_dir:
        try:
            if live_replay_dir.strip() == "1":
                recordings_base = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "..", "recordings")
                if not os.path.isdir(recordings_base):
                    recordings_base = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "recordings")
                recordings_base = os.path.normpath(recordings_base)
                if os.path.isdir(recordings_base):
                    dates = sorted(d for d in os.listdir(recordings_base) if os.path.isdir(os.path.join(recordings_base, d)))
                    if dates:
                        live_replay_dir = os.path.join(recordings_base, dates[-1])
                        logger.info(f"Auto-detected recording: {live_replay_dir}")
                    else:
                        logger.error("No recording directories found")
                        live_replay_dir = ""
                else:
                    logger.error(f"Recordings directory not found: {recordings_base}")
                    live_replay_dir = ""
            if live_replay_dir and os.path.isdir(live_replay_dir):
                speed = float(os.environ.get("LIVE_REPLAY_SPEED", "1"))
                start_offset = float(os.environ.get("LIVE_REPLAY_START_OFFSET", "0"))
                logger.info(f"*** LIVE REPLAY MODE *** ({live_replay_dir}, {speed}x)")
                f1_client = LiveReplayFeed(on_message=handle_f1_message, data_dir=live_replay_dir, speed=speed, start_offset=start_offset)
                await asyncio.wait_for(f1_client.start(), timeout=30)
            else:
                logger.warning(f"Live replay directory invalid: {live_replay_dir}")
        except Exception as e:
            logger.error(f"LiveReplayFeed error: {e}", exc_info=True)
        logger.info("Live replay ended - server stays running")
        _replay_start_signal.set()
        return
    replay_mode = os.environ.get("REPLAY_MODE") or _replay_requested["active"]
    if replay_mode:
        try:
            await asyncio.wait_for(_replay_start_signal.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass
        replay_speed = _replay_requested["speed"] if _replay_requested["active"] else float(os.environ.get("REPLAY_SPEED", "10"))
        logger.info(f"*** REPLAY MODE *** ({replay_speed}x speed)")
        f1_client = ReplayClient(on_message=handle_f1_message)
        await asyncio.wait_for(f1_client.start(), timeout=30)
        logger.info("Replay finished - server stays running for frontend")
    if _replay_requested["active"]:
        _replay_requested["active"] = False
        return

    token = ensure_valid_token()
    while True:
        try:
            logger.info("Starting F1 SignalR connection...")
            f1_client = F1SignalRClient(token=token, on_message=handle_f1_message, topics=TOPICS)
            await asyncio.wait_for(f1_client.start(), timeout=30)
        except Exception as e:
            logger.error(f"F1 connection error: {e}")
            if "401" in str(e) or "Unauthorized" in str(e):
                logger.info('Auth failure detected, attempting token refresh...')
                new_token = ensure_valid_token()
                if new_token != token:
                    token = new_token
                    logger.info('Token refreshed, will retry')
        await asyncio.sleep(10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cleanup_old_data()
    global f1_task
    f1_task = asyncio.create_task(f1_connection_loop())
    broadcast_task = asyncio.create_task(_broadcast_loop())
    yield
    broadcast_task.cancel()
    f1_task.cancel()
    if f1_client:
        await f1_client.stop()
    recorder.close()


app = FastAPI(title="Vento Timing - F1 Live Dashboard", version="0.1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.include_router(tyre_analysis_router)


@app.get("/api/status")
async def get_status():
    return {
        "status": "running",
        "connected": f1_client is not None and f1_client._running if f1_client else False,
        "session_name": processor.session_info.session_name,
        "drivers": len(processor.drivers),
        "clients": manager.active_connections,
    }


@app.get("/api/snapshot")
async def get_snapshot():
    return json.loads(json.dumps(processor.get_snapshot(), default=str))


@app.get("/api/timing")
async def get_timing():
    return json.loads(json.dumps({"timing": processor.get_sorted_timing(), "app_data": {n: a.__dict__ for n, a in processor.app_data.items()}}, default=str))


@app.get("/api/drivers")
async def get_drivers():
    return json.loads(json.dumps({n: d.__dict__ for n, d in processor.drivers.items()}, default=str))


@app.get("/api/circuit")
async def get_circuit():
    if processor.current_circuit is None:
        max_dist = 0
        for pts in processor.car_data.values():
            for pt in list(pts)[-100:]:
                if hasattr(pt, 'distance') and pt.distance and pt.distance > max_dist:
                    max_dist = pt.distance
        if max_dist > 0:
            best = None
            best_diff = float('inf')
            for key, data in processor.circuits.items():
                diff = abs(data['length_m'] - max_dist)
                if diff < best_diff:
                    best_diff = diff
                    best = key
            if best and best_diff < max_dist * 0.15:
                processor.current_circuit = processor.circuits[best]
    return processor.current_circuit


@app.get("/api/circuit/set")
async def set_circuit(name: str = ""):
    if name and processor.current_circuit is None:
        for key, data in processor.circuits.items():
            if name.lower() in key.lower():
                processor.current_circuit = data
                return {"status": "ok", "circuit": key}
        for key, data in processor.circuits.items():
            if name.lower() in data["grand_prix"].lower():
                processor.current_circuit = data
                return {"status": "ok", "circuit": key}
        return {"status": "not_found", "message": f"No circuit matching '{name}'"}
    return {"status": "ok", "circuit": processor.current_circuit.get("circuit") if processor.current_circuit else None}


@app.get("/api/weather")
async def get_weather():
    return json.loads(json.dumps(processor.weather.__dict__, default=str)) if processor.weather else None


@app.get("/api/track")
async def get_track():
    return json.loads(json.dumps(processor.track_status.__dict__, default=str)) if processor.track_status else None


@app.get("/api/race_control")
async def get_race_control():
    return json.loads(json.dumps([m.__dict__ for m in processor.race_control_messages], default=str))


@app.get("/api/positions/{driver_number}")
async def get_driver_positions(driver_number: int):
    positions = processor.positions.get(driver_number, [])
    return [p.__dict__ for p in positions[-200:]]


@app.get("/api/car_data/{driver_number}")
async def get_driver_car_data(driver_number: int):
    cd = processor.car_data.get(driver_number, [])
    return [c.__dict__ for c in cd[-3000:]]


# ── 轮胎策略 API ──────────────────────────────────────────────────────

@app.on_event("startup")
async def _load_tyre_db():
    """应用启动时加载轮胎数据库"""
    global _tyre_db_loaded
    if not _tyre_db_loaded:
        try:
            db = get_tyre_database()
            db.load_session()
            _tyre_db_loaded = True
            logger.info(f"Tyre database loaded ({len(db._cached_laps)} drivers)")
        except Exception as e:
            logger.warning(f"Tyre database load skipped: {e}")
        try:
            global _raw_tyre_db
            _raw_tyre_db = RawTyreDB()
            stats = _raw_tyre_db.get_stats()
            logger.info(f"RawTyreDB: {stats['laps']} laps across {stats['sessions']} sessions ({stats['db_size_mb']} MB)")
        except Exception as e:
            logger.warning(f"RawTyreDB init skipped: {e}")


async def tyre_get_drivers():
    """获取轮胎数据库中的车手列表"""
    db = get_tyre_database()
    return {str(dn): {"driver_number": dn} for dn in sorted(db._cached_laps.keys())}


async def tyre_get_stints(driver_number: int):
    """获取车手的所有 stint 概览"""
    db = get_tyre_database()
    stints = db.get_driver_stints(driver_number)
    return {"driver_number": driver_number, "stints": [s.to_dict() for s in stints]}


async def tyre_get_laps(driver_number: int, stint: int = -1):
    """获取车手的圈速数据"""
    db = get_tyre_database()
    stint_filter = stint if stint >= 0 else None
    records = db.get_stint_laps(driver_number, stint_filter)
    return {
        "driver_number": driver_number,
        "laps": [r.to_dict() for r in records],
    }


async def tyre_get_degradation(driver_number: int, stint: int = -1,
                                compound: str = "", fit: bool = True):
    """获取车手的退化数据 + 拟合模型

    返回退化散点 + 最优模型参数 + 拟合曲线
    """
    db = get_tyre_database()
    stint_filter = stint if stint >= 0 else None
    compound_filter = compound if compound else None

    data = db.get_degradation_data(driver_number, compound_filter, stint_filter)
    ages = [p["tyre_age"] for p in data["points"]]
    degs = [p["degradation"] for p in data["points"]]

    result = {
        "driver_number": driver_number,
        "stints": data["stints"],
        "points": data["points"][-200:],  # 最多 200 点
    }

    if fit and len(ages) >= 3:
        model = fit_best_model(ages, degs)
        result["model"] = model_to_dict(model)
        result["predicted"] = {
            "deg_at_15": round(model.predict(15), 4),
            "deg_at_20": round(model.predict(20), 4),
            "deg_at_30": round(model.predict(30), 4),
        }
        # 退化阈值预测
        for thresh in [0.5, 1.0, 1.5, 2.0, 3.0]:
            laps = model.laps_to_threshold(thresh)
            if laps is not None:
                result.setdefault("key_laps", []).append({
                    "threshold": thresh,
                    "laps_needed": laps,
                })

    return result


async def tyre_compare(drivers: str):
    """多车手退化曲线对比"""
    try:
        driver_list = [int(d.strip()) for d in drivers.split(",") if d.strip().isdigit()]
    except (ValueError, TypeError):
        return {"error": "Invalid driver list. Use: /api/tyre/compare?drivers=1,44,16"}

    db = get_tyre_database()
    result = {}
    for dn in driver_list:
        data = db.get_degradation_data(dn)
        ages = [p["tyre_age"] for p in data["points"]]
        degs = [p["degradation"] for p in data["points"]]
        entry = {
            "driver_number": dn,
            "points": data["points"][-200:],
            "stints": data["stints"],
        }
        if len(ages) >= 3:
            model = fit_best_model(ages, degs)
            entry["model"] = model_to_dict(model)
        result[str(dn)] = entry

    return {"drivers": result}


async def tyre_summary():
    """轮胎配方使用汇总"""
    db = get_tyre_database()
    return {"compounds": db.get_compound_summary()}


# ── WebSocket ──────────────────────────────────────────────────────────



@app.get("/api/mode")
async def api_get_mode():
    """Return current mode (live/replay)."""
    return {
        "mode": "replay" if _replay_requested["active"] else ("replay_env" if os.environ.get("REPLAY_MODE") else "live"),
        "speed": _replay_requested["speed"],
        "connected": f1_client is not None and (f1_client._running if hasattr(f1_client, "_running") else False),
        "session": getattr(f1_client, "name", ""),
    }


@app.get("/api/replay/start")
async def api_start_replay(speed: float = 20.0):
    """Switch to replay mode at the specified speed."""
    global f1_client, f1_task, _replay_requested
    if _replay_requested["active"]:
        return {"status": "already_in_replay", "speed": _replay_requested["speed"]}
    if f1_client is not None:
        try: await f1_client.stop()
        except: pass
    if f1_task is not None and not f1_task.done():
        f1_task.cancel()
        try: await f1_task
        except: pass
    _replay_requested["active"] = True
    _replay_requested["speed"] = speed
    _replay_start_signal.set()
    f1_task = asyncio.create_task(f1_connection_loop())
    logger.info("API triggered replay at %sx speed", speed)
    return {"status": "ok", "mode": "replay", "speed": speed}


@app.get("/api/replay/stop")
async def api_stop_replay():
    """Switch back to live F1 mode."""
    global f1_client, f1_task, _replay_requested
    if not _replay_requested["active"] and not os.environ.get("REPLAY_MODE"):
        return {"status": "already_in_live_mode"}
    if f1_client is not None:
        try: await f1_client.stop()
        except: pass
    if f1_task is not None and not f1_task.done():
        f1_task.cancel()
        try: await f1_task
        except: pass
    _replay_requested["active"] = False
    _replay_start_signal.clear()
    f1_task = asyncio.create_task(f1_connection_loop())
    logger.info("API triggered live F1 mode")
    return {"status": "ok", "mode": "live"}
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    logger.info(f"WS client connected ({manager.active_connections} total)")
    snapshot = processor.get_snapshot()
    _replay_start_signal.set()
    await websocket.send_text(json.dumps({"type": "snapshot", "data": snapshot}, default=str))
    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
            elif msg.get("type") == "request_driver_history":
                dn = msg.get("driver_number")
                if dn:
                    history = {
                        "positions": [p.__dict__ for p in processor.positions.get(dn, [])[-500:]],
                        "car_data": [c.__dict__ for c in processor.car_data.get(dn, [])[-500:]],
                    }
                    await websocket.send_text(json.dumps({"type": "driver_history", "driver_number": dn, "data": history}, default=str))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning(f"WS error: {e}")
    finally:
        await manager.disconnect(websocket)


async def tyre_db_stats():
    """轮胎原始数据库统计"""
    global _raw_tyre_db
    if _raw_tyre_db:
        return _raw_tyre_db.get_stats()
    return {"error": "RawTyreDB not initialized"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")
    uvicorn.run("app.main:app", host=host, port=port, reload=True)
