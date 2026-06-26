import sys
sys.path.insert(0, "D:/Vento_Timing/backend")
from app.tyre_analysis import DegradationService

s = DegradationService(db_path="D:/Vento_Timing/tyre_raw.db")

# Test: all sessions (session_id=None)
for driver in [1, 44, 16, 63]:
    laps = s.get_laps_for_driver(driver, session_id=None)
    result = s.compute_degradation(laps)
    pts = result["points"]
    mods = result["models"]
    if pts:
        comps = set(p["compound"] for p in pts)
        ages = [p["tyre_age"] for p in pts]
        losses = [p["loss"] for p in pts]
        print(f"D#{driver} ALL: {len(pts)} pts, {len(mods)} mods, {comps}")
        print(f"  age={min(ages)}-{max(ages)}, loss={min(losses):.3f}-{max(losses):.3f}s")
        if mods:
            for m in mods:
                print(f"  fit: {m['compound']} stint={m['stint']}")
    else:
        print(f"D#{driver} ALL: NO DATA")
