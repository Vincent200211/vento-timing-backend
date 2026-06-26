"""Circuit data loader from f1_2026_circuits.csv."""
from __future__ import annotations
import csv
import math
import os
from typing import Optional


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in meters between two GPS points."""
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "f1_2026_circuits.csv")


def load_circuits() -> dict:
    """Load all circuit data from CSV. Returns dict keyed by circuit name."""
    circuits: dict = {}
    if not os.path.exists(DATA_DIR):
        return circuits
    with open(DATA_DIR, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            r = (row.get("round") or "").strip()
            if not r:
                continue
            gp = row["grand_prix"].strip()
            cname = row["circuit"].strip()
            length_km = float(row["length_km"].strip())
            if cname not in circuits:
                circuits[cname] = {
                    "round": int(r),
                    "grand_prix": gp,
                    "circuit": cname,
                    "length_km": length_km,
                    "length_m": int(length_km * 1000),
                    "elements": [],
                }
            circuits[cname]["elements"].append({
                "type": row["element_type"].strip(),
                "no": int(row["element_no"].strip()) if row.get("element_no", "").strip() else 0,
                "lat": float(row["gps_lat"].strip()),
                "lon": float(row["gps_lon"].strip()),
            })
    # Compute cumulative distances + extract corners per circuit
    for cname, cdata in circuits.items():
        elems = cdata["elements"]
        cum = 0.0
        total = 0.0
        corners = []
        start_line = None
        for i, e in enumerate(elems):
            if i > 0:
                cum += haversine(elems[i - 1]["lat"], elems[i - 1]["lon"], e["lat"], e["lon"])
            e["distance"] = int(cum)
        total = cum  # Total haversine from start to last element
        # Scale corner distances to actual track length (fixes haversine underestimation)
        if total > 0 and cdata["length_m"] > 0:
            scale = cdata["length_m"] / total
        else:
            scale = 1.0
        for i, e in enumerate(elems):
            if e["type"] == "start_line":
                start_line = {"lat": e["lat"], "lon": e["lon"]}
            elif e["type"] == "corner":
                corners.append({"no": e["no"], "distance": int(e["distance"] * scale)})
            elif e["type"] == "s1_split":
                s1_dist = int(e["distance"] * scale)
            elif e["type"] == "s2_split":
                s2_dist = int(e["distance"] * scale)
        if corners:
            # Ensure last corner doesn't exceed track length
            mx = max(c["distance"] for c in corners)
            if mx > cdata["length_m"]:
                adj = cdata["length_m"] / mx
                for c in corners:
                    c["distance"] = int(c["distance"] * adj)
        cdata["corners"] = corners
        cdata["start_line"] = start_line
        cdata["sector_splits"] = {"s1": s1_dist, "s2": s2_dist, "s3": cdata["length_m"]}
        del cdata["elements"]
    return circuits


def fetch_multiviewer_circuit(circuit_key: int, year: int = 2026) -> dict:
    """Fetch circuit data with corner XY from FastF1 MultiViewer API."""
    # Try local cache first (no FastF1 dependency)
    _xy_path = os.path.join(os.path.dirname(__file__), "circuits_xy.json")
    if os.path.exists(_xy_path):
        import json as _json
        with open(_xy_path) as _f:
            _cache = _json.load(_f)
        if str(circuit_key) in _cache:
            return _cache[str(circuit_key)]
    # Fallback to FastF1 MultiViewer API
    from fastf1.mvapi.api import get_circuit
    data = get_circuit(year=year, circuit_key=circuit_key)
    if not data:
        return None
    corners = []
    for c in data.get("corners", []):
        corners.append({
            "no": c["number"], "x": c["trackPosition"]["x"],
            "y": c["trackPosition"]["y"],
        })
    return {
        "circuit": data.get("circuitName", ""),
        "circuit_key": circuit_key,
        "start_line": {"x": data.get("x"), "y": data.get("y")},
        "rotation": data.get("rotation", 0),
        "corners": corners, "length_m": 0,
    }


def build_ref_path(location_pts: list) -> list:
    """Build reference path with cumulative XY distances."""
    if not location_pts: return []
    result = []; cum = 0.0
    for i, p in enumerate(location_pts):
        if i > 0:
            dx = p["x"] - location_pts[i - 1]["x"]
            dy = p["y"] - location_pts[i - 1]["y"]
            d = (dx * dx + dy * dy) ** 0.5
            if d < 500:
                cum += d
        result.append({"x": p["x"], "y": p["y"], "distance": int(cum)})
    return result
def project_corners(circuit_data: dict, ref_path: list):
    """KDTree projection of corner XY onto reference path to assign distances."""
    from scipy.spatial import KDTree
    if not ref_path or not circuit_data.get("corners"): return
    xy = [(p["x"], p["y"]) for p in ref_path]
    dists = [p["distance"] for p in ref_path]
    tree = KDTree(xy)
    for c in circuit_data["corners"]:
        idx = tree.query([c["x"], c["y"]])[1]
        c["distance"] = int(dists[idx])
    # length_m is set by the caller from CSV data, not from projected corners
 
 
def match_circuit(meeting_name: str, circuit_short: str, circuits: dict) -> Optional[dict]:
    """Match a circuit from the loaded dict using meeting_name or circuit_short."""
    if circuit_short:
        csl = circuit_short.lower()
        for key, data in circuits.items():
            if csl in key.lower():
                return data
    if meeting_name:
        mnl = meeting_name.lower()
        for key, data in circuits.items():
            gp = data["grand_prix"].lower().replace(" gp", "").strip()
            if gp in mnl:
                return data
    return None
