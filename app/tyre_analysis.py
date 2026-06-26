import os, sqlite3, logging
from typing import Optional
from fastapi import APIRouter, Query
_DB_PATH = os.environ.get("TYRE_DB_PATH") or os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "tyre_raw.db")

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/tyre", tags=["tyre_analysis"])


class DegradationService:
    def __init__(self, db_path: str = ""):
        self.db_path = db_path or _DB_PATH

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_available_sessions(self) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT id, session_key, session_name, session_type FROM sessions ORDER BY session_key"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_available_drivers(self) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT driver_number, driver_name, team_name FROM drivers ORDER BY driver_number"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_laps_for_driver(self, driver_number: int, session_id: int = None,
                             compound: Optional[str] = None,
                             stint_number: Optional[int] = None) -> list[dict]:
        conn = self._get_conn()
        query = """
            SELECT l.*, s.compound, s.stint_number, s.tyre_age_at_start
            FROM laps l JOIN stints s ON l.stint_id = s.id
            WHERE l.driver_number = ?
              AND l.is_outlap = 0 AND l.is_inlap = 0
              AND l.track_status = 'Green' AND l.lap_time IS NOT NULL AND l.lap_time > 0
        """
        params = [driver_number]
        if session_id is not None:
            query += " AND l.session_id = ?"
            params.append(session_id)
        if compound:
            query += " AND s.compound = ?"; params.append(compound)
        if stint_number and stint_number > 0:
            query += " AND s.stint_number = ?"; params.append(stint_number)
        query += " ORDER BY l.lap_number"
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def compute_degradation(self, laps: list[dict],
                            fuel_correction: bool = False,
                            r2_thresh: float = 0.3,
                            sigma: float = 0.0,
                            max_deg: float = 5.0) -> dict:
        if not laps:
            return {"per_stint": [], "per_compound": {}}
        stint_groups = {}
        for lap in laps:
            stint_groups.setdefault(lap["stint_number"], []).append(lap)
        per_stint = []
        for stint_num, stint_laps in stint_groups.items():
            if len(stint_laps) < 2: continue
            res = self._process_laps(stint_laps, fuel_correction, False, r2_thresh=r2_thresh, sigma=sigma, max_deg=max_deg)
            if res:
                res["stint"] = stint_num
                res["compound"] = stint_laps[0]["compound"]
                per_stint.append(res)
        compound_groups = {}
        for lap in laps:
            compound_groups.setdefault(lap["compound"], []).append(lap)
        per_compound = {}
        for comp, comp_laps in compound_groups.items():
            res = self._process_laps(comp_laps, fuel_correction, True, r2_thresh=r2_thresh, sigma=sigma, max_deg=max_deg)
            if res:
                per_compound[comp] = res
        return {"per_stint": per_stint, "per_compound": per_compound}
    def _process_laps(self, laps: list[dict],
                      fuel_correction: bool = False,
                      is_aggregate: bool = False,
                      r2_thresh: float = 0.3,
                      sigma: float = 0.0,
                      max_deg: float = 5.0) -> dict:
        if not laps:
            return None
        laps_sorted = sorted(laps, key=lambda x: x["lap_number"])
        corrected_times = [lap["lap_time"] for lap in laps_sorted]
        tyre_ages = [lap.get("tyre_age", 0) for lap in laps_sorted]
        if fuel_correction:
            for i, lap in enumerate(laps_sorted):
                corrected_times[i] += (lap["lap_number"] - 1) * 1.8 * 0.03
        global_baseline = min(corrected_times)
        losses = [t - global_baseline for t in corrected_times]
        valid = [(tyre_ages[i], losses[i]) for i in range(len(losses)) if -0.5 <= losses[i] < max_deg]
        if len(valid) < 2:
            return {"points": [], "model": {"type": "none", "params": {}, "curve": []}}
        bins = {}
        for a, l in valid:
            b = int(round(a / 2)) * 2  # 2-age bins for stability
            bins.setdefault(b, []).append(l)
        keys = sorted(bins.keys())
        meds = [sorted(bins[k])[len(bins[k]) // 2] for k in keys]
        for i in range(1, len(meds)):
            if meds[i] < meds[i-1]: meds[i] = meds[i-1]
        if len(keys) < 2:
            return {"points": [], "model": {"type": "none", "params": {}, "curve": []}}
        from .degradation_model import fit_best_model, model_to_dict, apply_gaussian_smoothing
        if sigma > 0:
            meds = apply_gaussian_smoothing(meds, sigma=sigma)
        model = fit_best_model(keys, meds, clean=True, piecewise_r2_threshold=r2_thresh, max_deg=max_deg)
        model_dict = model_to_dict(model, age_min=int(min(keys)), age_max=int(max(keys)))
        points = [{"tyre_age": a, "loss": round(l, 4)} for a, l in zip(keys, meds)]
        return {"points": points, "model": model_dict}
    def compute_grid_degradation(self, compound: str, r2_thresh: float = 0.3, sigma: float = 0.0, max_deg: float = 5.0) -> dict:
        """Aggregate degradation curve across ALL drivers for a compound."""
        conn = self._get_conn()
        drivers = [r[0] for r in conn.execute(
            "SELECT DISTINCT l.driver_number FROM laps l JOIN stints s ON l.stint_id = s.id "
            "WHERE s.compound=? AND l.is_outlap=0 AND l.is_inlap=0 "
            "AND l.track_status='Green' AND l.lap_time>0",
            (compound,)).fetchall()]
        conn.close()
        
        all_pairs = []  # (tyre_age, loss, driver_number)
        for dn in drivers:
            laps = self.get_laps_for_driver(dn, compound=compound)
            if not laps:
                continue
            laps_sorted = sorted(laps, key=lambda x: x['lap_number'])
            times = [l['lap_time'] for l in laps_sorted]
            ages = [l.get('tyre_age', 0) for l in laps_sorted]
            baseline = min(times)
            for i, t in enumerate(times):
                loss = t - baseline
                if -0.5 <= loss < max_deg:
                    all_pairs.append((ages[i], loss, dn))
        
        if len(all_pairs) < 2:
            return {'scatter': [], 'points': [], 'model': {'type': 'none', 'params': {}, 'curve': []}}
        
        # Bin + median + monotonic
        bins_data = {}
        for a, l, _ in all_pairs:
            b = int(round(a / 2)) * 2  # 2-age bins for stability
            bins_data.setdefault(b, []).append(l)
        keys = sorted(bins_data.keys())
        meds = [sorted(bins_data[k])[len(bins_data[k]) // 2] for k in keys]
        if len(keys) < 2:
            return {'scatter': [], 'points': [], 'model': {'type': 'none', 'params': {}, 'curve': []}}
        
        from .degradation_model import fit_best_model, model_to_dict, apply_gaussian_smoothing
        if sigma > 0:
            meds = apply_gaussian_smoothing(meds, sigma=sigma)
        model = fit_best_model(keys, meds, clean=True, iqr_mult=4.0, min_deg=-0.3, max_deg=max_deg, piecewise_r2_threshold=r2_thresh)
        model_dict = model_to_dict(model, age_min=int(min(keys)), age_max=int(max(keys)))
        points = [{'tyre_age': a, 'loss': round(l, 4)} for a, l in zip(keys, meds)]
        scatter = [{'tyre_age': a, 'loss': round(l, 4), 'driver': str(dn)} for a, l, dn in all_pairs]
        
        return {'points': points, 'scatter': scatter, 'model': model_dict}

    def get_qualy_best_lap(self, driver_number, compound):
        conn = self._get_conn()
        where = "l.track_status = 'Green' AND l.is_outlap = 0 AND l.is_inlap = 0 AND l.lap_time > 0"
        base = "SELECT MIN(l.lap_time) FROM laps l"
        base += " JOIN stints st ON l.stint_id = st.id"
        base += " JOIN sessions s ON l.session_id = s.id"
        base += f" WHERE l.driver_number = ? AND st.compound = ? AND {where}"

        # 优先排位
        row = conn.execute(base + " AND s.session_type = 'Qualifying'", (driver_number, compound)).fetchone()
        best = row[0] if row and row[0] else None
        source = "Qualifying" if best else None

        # 无排位数据则回退到练习赛
        if best is None:
            row = conn.execute(base + " AND s.session_type = 'Practice'", (driver_number, compound)).fetchone()
            best = row[0] if row and row[0] else None
            source = "Practice" if best else None

        conn.close()
        return {"best_lap": round(best, 3) if best else None, "source": source}


@router.get("/grid-degradation")
def api_grid_degradation(
    compound: str = Query(description="Compound, e.g. MEDIUM"),
    r2: float = Query(0.3, ge=0, le=1, description="Piecewise R2 threshold"),
    sigma: float = Query(0.0, ge=0, le=3, description="Gaussian smoothing sigma"),
    maxdeg: float = Query(5.0, ge=1, le=20, description="Max loss threshold"),
):
    svc = get_service()
    return svc.compute_grid_degradation(compound, r2_thresh=r2, sigma=sigma, max_deg=maxdeg)


@router.get("/qualy-best")
def api_qualy_best(driver: int = Query(description="Driver"), compound: str = Query(description="Compound")):
    return get_service().get_qualy_best_lap(driver, compound)


_service: DegradationService = None


def get_service() -> DegradationService:
    global _service
    if _service is None:
        _service = DegradationService()
    return _service


@router.get("/drivers")
def api_get_drivers():
    svc = get_service()
    try:
        return {"drivers": svc.get_available_drivers(), "sessions": svc.get_available_sessions()}
    except Exception as e:
        return {"error": str(e)}


@router.get("/degradation")
def api_get_degradation(
    driver: int = Query(description="Driver number"),
    session: Optional[int] = Query(None, description="Session ID"),
    compound: Optional[str] = Query(None),
    stint: Optional[int] = Query(None),
    r2: float = Query(0.3, ge=0, le=1, description="Piecewise R2 threshold"),
    sigma: float = Query(0.0, ge=0, le=3, description="Gaussian smoothing sigma"),
    maxdeg: float = Query(5.0, ge=1, le=20, description="Max loss threshold"),
):
    svc = get_service()
    laps = svc.get_laps_for_driver(driver, session, compound, stint)
    result = svc.compute_degradation(laps, r2_thresh=r2, sigma=sigma, max_deg=maxdeg)
    return {"driver": driver, "session_id": session if session else "all", "data": result}


@router.get("/compare")
def api_compare_degradation(
    drivers: str = Query(description="Comma-separated driver numbers, e.g. 1,44"),
    session: Optional[int] = Query(None),
    compound: Optional[str] = Query(None),
    stint: Optional[int] = Query(None),
    r2: float = Query(0.3, ge=0, le=1, description="Piecewise R2 threshold"),
    sigma: float = Query(0.0, ge=0, le=3, description="Gaussian smoothing sigma"),
    maxdeg: float = Query(5.0, ge=1, le=20, description="Max loss threshold"),
):
    svc = get_service()
    try:
        driver_list = [int(d.strip()) for d in drivers.split(",")]
    except (ValueError, TypeError):
        return {"error": "Invalid driver list"}
    result = {}
    for dn in driver_list:
        laps = svc.get_laps_for_driver(dn, session, compound, stint)
        result[str(dn)] = svc.compute_degradation(laps, r2_thresh=r2, sigma=sigma, max_deg=maxdeg)
    return {"drivers": result}
