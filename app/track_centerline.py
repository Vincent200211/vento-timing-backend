"""Track centerline: build from GPS track_shape, real-time projection."""
from __future__ import annotations
import json
import math
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Cache directory for pre-computed centerline files
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache")

def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)

def build_from_track_shape(points: list[dict], official_length: float) -> dict:
    """Build calibrated centerline segments from track_shape points.
    
    Args:
        points: list of {x, y} dicts tracing the full lap
        official_length: official circuit length in meters
        
    Returns:
        {"segs": [...], "length": official_length}
    """
    pts = [(p["x"], p["y"]) for p in points]
    # Close the loop if not already closed
    dx = pts[-1][0] - pts[0][0]
    dy = pts[-1][1] - pts[0][1]
    loop_gap = math.hypot(dx, dy)
    if loop_gap > 1.0:
        pts.append(pts[0])
    
    # Compute raw cumulative arc length
    segments = []
    cum = 0.0
    for i in range(len(pts) - 1):
        x0, y0 = pts[i]
        x1, y1 = pts[i + 1]
        seg_len = math.hypot(x1 - x0, y1 - y0)
        segments.append({
            "x0": x0, "y0": y0,
            "x1": x1, "y1": y1,
            "len": seg_len,
            "cum_start": cum,
        })
        cum += seg_len
    
    raw_length = cum
    scale = official_length / raw_length if raw_length > 0 else 1.0
    
    # Calibrate segments to official length
    for seg in segments:
        seg["len"] *= scale
        seg["cum_start"] *= scale
    
    logger.info("Centerline built: %d segments, raw=%.1f -> calibrated=%.1f m",
                len(segments), raw_length, official_length)
    
    return {"segs": segments, "length": official_length}


def save_centerline(circuit_key: int, data: dict):
    """Save centerline to cache."""
    _ensure_cache_dir()
    path = os.path.join(CACHE_DIR, f"centerline_{circuit_key}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info("Centerline saved: %s", path)
    return path


def load_centerline(circuit_key: int) -> Optional[dict]:
    """Load cached centerline for a circuit key. Returns None if not found."""
    path = os.path.join(CACHE_DIR, f"centerline_{circuit_key}.json")
    if not os.path.exists(path):
        logger.warning("Centerline not found: %s", path)
        return None
    with open(path) as f:
        data = json.load(f)
    logger.info("Centerline loaded: %s (%d segments, %.1f m)",
                path, len(data["segs"]), data["length"])
    return data


def project_onto_centerline(
    px: float, py: float,
    segs: list[dict],
    start_idx: int = 0,
    search_range: int = 15,
) -> tuple[float, int]:
    """Project a GPS point onto the centerline.
    
    Uses local search around start_idx, with full-scan fallback.
    
    Returns:
        (cumulative_distance_along_track, best_segment_index)
    """
    n = len(segs)
    best_dist2 = float("inf")
    best_cum = 0.0
    best_idx = start_idx
    
    # Local search
    for offset in range(-5, search_range + 5):
        idx = (start_idx + offset) % n
        seg = segs[idx]
        dx = seg["x1"] - seg["x0"]
        dy = seg["y1"] - seg["y0"]
        len2 = dx * dx + dy * dy
        if len2 < 1e-9:
            continue
        t = ((px - seg["x0"]) * dx + (py - seg["y0"]) * dy) / len2
        t = max(0.0, min(1.0, t))
        proj_x = seg["x0"] + t * dx
        proj_y = seg["y0"] + t * dy
        dist2 = (px - proj_x) ** 2 + (py - proj_y) ** 2
        if dist2 < best_dist2:
            best_dist2 = dist2
            best_cum = seg["cum_start"] + t * seg["len"]
            best_idx = idx
    
    # Full-scan fallback if projection is far (e.g. pit entry/exit)
    if best_dist2 > 100.0:
        for idx, seg in enumerate(segs):
            dx = seg["x1"] - seg["x0"]
            dy = seg["y1"] - seg["y0"]
            len2 = dx * dx + dy * dy
            if len2 < 1e-9:
                continue
            t = ((px - seg["x0"]) * dx + (py - seg["y0"]) * dy) / len2
            t = max(0.0, min(1.0, t))
            proj_x = seg["x0"] + t * dx
            proj_y = seg["y0"] + t * dy
            dist2 = (px - proj_x) ** 2 + (py - proj_y) ** 2
            if dist2 < best_dist2:
                best_dist2 = dist2
                best_cum = seg["cum_start"] + t * seg["len"]
                best_idx = idx
    
    return best_cum, best_idx


def cumdist_to_angle(cum_dist: float, track_length: float) -> float:
    """Convert cumulative distance to ring angle 0-360."""
    return (cum_dist / track_length) * 360.0 if track_length > 0 else 0.0
