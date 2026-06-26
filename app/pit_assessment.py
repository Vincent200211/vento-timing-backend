"""
Pit window assessment module.

Provides real-time pit stop threat analysis and track-ring visualization.

Core functions:
  assess_instant_pit_window(ego, cars_behind, track_params=None, leader=None) -> dict
  plot_pit_ring(assessment_result, track_params, ego_label, save_path=None)
"""

from __future__ import annotations
import math
from typing import Optional

def _interp_speed(positions, speeds, pos):
    n = len(positions)
    if pos <= positions[0]:
        return speeds[0]
    if pos >= positions[-1]:
        return speeds[-1]
    for i in range(n - 1):
        if positions[i] <= pos <= positions[i + 1]:
            t = (pos - positions[i]) / (positions[i + 1] - positions[i])
            return speeds[i] + t * (speeds[i + 1] - speeds[i])
    return speeds[-1]

def simulate_travel(speed_profile, start_pos, duration, lap_length):
    positions = speed_profile.positions
    speeds = speed_profile.speeds
    total = 0.0
    remaining = duration
    cur = start_pos
    while remaining > 0.001:
        spd = _interp_speed(positions, speeds, cur % lap_length)
        if spd <= 0.0:
            break
        dt = 10.0 / spd
        if dt <= remaining:
            total += 10.0
            cur += 10.0
            remaining -= dt
        else:
            total += spd * remaining
            remaining = 0.0
    return total

def _calc_travel_time(speed_profile, start_pos, end_pos, lap_length):
    positions = speed_profile.positions
    speeds = speed_profile.speeds
    start = start_pos
    end = end_pos
    if end <= start:
        end += lap_length
    total = 0.0
    cur = start
    while cur < end:
        spd = _interp_speed(positions, speeds, cur % lap_length)
        if spd <= 0.0:
            spd = 1.0
        step = min(10.0, end - cur)
        total += step / spd
        cur += step
    return total

def assess_instant_pit_window(ego, cars_behind, track_params=None, leader=None, safety_car_active=False):
    """Evaluate pit threat using interval-accumulated time gaps (time_diff_to_ego).

    ego.pit_loss is the extra seconds spent in pit lane vs. driving through the pit sector.
    cars_behind[].time_diff_to_ego is the cumulative interval-based time gap behind ego.
    The single nearest car in time is evaluated; if it cannot close the gap within
    pit_loss seconds, the threat is zero.
    """
    if not cars_behind:
        return {"critical_distance_m": 0.0, "threat_car_id": None}

    pit_loss_val = getattr(ego, 'pit_loss', 0.0)
    if pit_loss_val <= 0:
        return {"critical_distance_m": 0.0, "threat_car_id": None}

    valid = [c for c in cars_behind if getattr(c, 'time_diff_to_ego', 0.0) > 0]
    if not valid:
        return {"critical_distance_m": 0.0, "threat_car_id": None}

    nearest = min(valid, key=lambda c: c.time_diff_to_ego)

    if nearest.time_diff_to_ego >= pit_loss_val:
        return {"critical_distance_m": 0.0, "threat_car_id": getattr(nearest, 'car_id', None)}

    speed = getattr(nearest, 'current_speed', None)
    if speed is None or speed <= 5.0:
        speed = 80.0

    return {
        "critical_distance_m": pit_loss_val * speed,
        "threat_car_id": getattr(nearest, 'car_id', None),
    }

def plot_pit_ring(assessment_result, track_params, ego_label, save_path):
    import matplotlib.pyplot as plt
    import numpy as np
    lap_length = track_params["lap_length"]
    pit_entry_pos = track_params["pit_entry_pos"]
    pit_exit_pos = track_params["pit_exit_pos"]
    R_outer = 1.0
    R_inner = 0.75
    R_mid = (R_outer + R_inner) * 0.5
    def pos_to_angle(p):
        return (p / lap_length) * 2.0 * np.pi
    ego_pos = assessment_result["ego_track_pos"]
    threat_post = assessment_result.get("threat_post_pit_pos")
    threat_id = assessment_result.get("threat_car_id")
    critical_d = assessment_result.get("critical_distance_m", 0.0)
    margin = assessment_result.get("current_margin_m", 0.0)
    T_pit = assessment_result.get("T_pit", 0.0)
    ego_angle = pos_to_angle(ego_pos)
    exit_angle = pos_to_angle(pit_exit_pos)
    entry_angle = pos_to_angle(pit_entry_pos)
    arc_len = (pit_exit_pos - ego_pos) % lap_length
    n_steps = 80
    arc_angle = arc_len / lap_length * 2.0 * np.pi
    raw_thetas = np.linspace(ego_angle, ego_angle + arc_angle, n_steps)
    arc_thetas = raw_thetas % (2.0 * np.pi)
    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw={"projection": "polar"})
    ax.set_theta_direction(-1)
    ax.set_theta_zero_location("N")
    theta_grid = np.linspace(0, 2 * np.pi, 361)
    ax.fill_between(theta_grid, R_inner, R_outer, color="#e8e8e8", alpha=0.35)
    ax.plot([0, 0], [R_inner, R_outer], color="#333", linewidth=2)
    ax.text(0, R_outer + 0.07, "S/F", ha="center", fontsize=11, fontweight="bold")
    ax.plot([entry_angle, entry_angle], [R_inner, R_outer], "--", color="green", linewidth=1, alpha=0.5)
    ax.plot([exit_angle, exit_angle], [R_inner, R_outer], "-", color="orange", linewidth=1.5, alpha=0.7)
    ax.plot(arc_thetas, [R_mid] * n_steps, "-", color="darkorange", linewidth=4, alpha=0.7, solid_capstyle="round")
    mid_theta = (ego_angle + exit_angle) * 0.5
    ax.text(mid_theta, R_mid, f"{arc_len:.0f}m", ha="center", va="center", fontsize=10, fontweight="bold", color="#c06000", bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.85))
    ax.scatter([ego_angle], [R_outer], s=140, c="dodgerblue", edgecolors="white", linewidth=1.5, zorder=6)
    ax.text(ego_angle, R_outer + 0.12, f"{ego_label} (You)", ha="center", fontsize=10, fontweight="bold", color="dodgerblue")
    ax.scatter([exit_angle], [R_inner], s=120, marker="s", c="darkorange", edgecolors="white", linewidth=1.5, zorder=6)
    ax.text(exit_angle, R_inner - 0.10, "Pit Exit", ha="center", fontsize=9, color="darkorange")
    if threat_post is not None:
        th_angle = pos_to_angle(threat_post)
        colour = "#d62728" if threat_id else "#7f7f7f"
        lbl = f"{threat_id} (after pit)" if threat_id else "Closest (after pit)"
        ax.scatter([th_angle], [R_outer], s=120, c=colour, edgecolors="white", linewidth=1.5, zorder=6)
        ax.text(th_angle, R_outer + 0.20, lbl, ha="center", fontsize=9, fontweight="bold", color=colour)
    status = "SAFE" if margin > 0 else "DANGER"
    status_colour = "#27c93f" if margin > 0 else "#e10600"
    info_lines = [
        f"Threat Car:    {threat_id if threat_id else chr(8212)}",
        f"Critical Dist: {critical_d:.0f} m",
        f"Margin:        {margin:.0f} m",
        f"Pit Time:      {T_pit:.1f} s",
        "Status:       ",
    ]
    info = chr(10).join(info_lines)
    ax.text(0.5, -0.22, info, transform=ax.transAxes, fontsize=12, ha="center", va="top", family="monospace", bbox=dict(boxstyle="round,pad=0.5", facecolor="#fef7e0", edgecolor="#ccc"))
    ax.text(0.5, -0.42, status, transform=ax.transAxes, fontsize=16, ha="center", va="top", fontweight="bold", color=status_colour)
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.set_title(f"Pit Window Assessment - {ego_label}", pad=20, fontsize=14, fontweight="bold")
    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
    else:
        plt.show()

def predict_pit_window_at_entry(ego, cars_behind, track_params, leader):
    """Predict pit window assuming ego drives to pit entry first before pitting.
    T_to_entry accounts for travel from current position to pit_entry_pos.
    Threat car positions are predicted at that future moment."""
    lap_length = track_params["lap_length"]
    pit_entry_pos = track_params["pit_entry_pos"]
    pit_exit_pos = track_params["pit_exit_pos"]
    S_sector = pit_exit_pos - pit_entry_pos
    if S_sector < 0:
        S_sector += lap_length
    T_normal = _calc_travel_time(ego.speed_profile, pit_entry_pos, pit_exit_pos, lap_length)
    T_pit = ego.pit_loss + T_normal
    T_to_entry = _calc_travel_time(ego.speed_profile, ego.track_pos, pit_entry_pos, lap_length)
    threats = []
    results = []
    for car in cars_behind:
        D_to_entry = simulate_travel(car.speed_profile, car.track_pos, T_to_entry, lap_length)
        threat_pos_at_entry = (car.track_pos + D_to_entry) % lap_length
        gap_at_entry = (pit_entry_pos - threat_pos_at_entry) % lap_length
        if gap_at_entry == 0.0:
            gap_at_entry = lap_length
        D_traveled = simulate_travel(car.speed_profile, threat_pos_at_entry, T_pit, lap_length)
        is_threat = D_traveled > gap_at_entry + S_sector
        excess = D_traveled - (gap_at_entry + S_sector) if is_threat else 0.0
        critical = D_traveled - S_sector
        margin = gap_at_entry + S_sector - D_traveled
        res = dict(car_id=car.car_id, gap_at_entry=gap_at_entry, threat_pos_at_entry=threat_pos_at_entry,
                   D_traveled=D_traveled, is_threat=is_threat, excess=excess,
                   critical_distance_m=critical, current_margin_m=margin)
        results.append(res)
        if is_threat:
            threats.append(res)
    if threats:
        worst = max(threats, key=lambda r: r["excess"])
        for car, res in zip(cars_behind, results):
            if res["car_id"] == worst["car_id"]:
                threat_post = (res["threat_pos_at_entry"] + res["D_traveled"]) % lap_length
                return dict(threat_car_id=car.car_id, critical_distance_m=res["critical_distance_m"],
                           current_margin_m=res["current_margin_m"], ego_track_pos=ego.track_pos,
                           ego_post_pit_pos=pit_exit_pos, threat_track_pos=car.track_pos,
                           threat_post_pit_pos=threat_post, T_to_entry=T_to_entry, T_pit=T_pit,
                           S_sector=S_sector, pit_entry_pos=pit_entry_pos, pit_exit_pos=pit_exit_pos)
    if results:
        closest = min(results, key=lambda r: r["gap_at_entry"])
        return dict(threat_car_id=None, critical_distance_m=closest["critical_distance_m"],
                   current_margin_m=closest["current_margin_m"], ego_track_pos=ego.track_pos,
                   ego_post_pit_pos=pit_exit_pos, threat_track_pos=None, threat_post_pit_pos=None,
                   T_to_entry=T_to_entry, T_pit=T_pit, S_sector=S_sector,
                   pit_entry_pos=pit_entry_pos, pit_exit_pos=pit_exit_pos)
    return dict(threat_car_id=None, critical_distance_m=0.0, current_margin_m=0.0,
               ego_track_pos=ego.track_pos, ego_post_pit_pos=pit_exit_pos,
               threat_track_pos=None, threat_post_pit_pos=None,
               T_to_entry=T_to_entry, T_pit=T_pit, S_sector=S_sector,
               pit_entry_pos=pit_entry_pos, pit_exit_pos=pit_exit_pos)

def _compute_single_at_entry(ego_pos, ego_profile, car_pos, car_profile, track_params, T_pit):
    """Core computation for a single car behind.
    Returns dict with gap_at_entry, threat_pos_at_entry, D_traveled, critical_distance_m, current_margin_m."""
    lap_length = track_params["lap_length"]
    pit_entry_pos = track_params["pit_entry_pos"]
    pit_exit_pos = track_params["pit_exit_pos"]
    S_sector = pit_exit_pos - pit_entry_pos
    if S_sector < 0:
        S_sector += lap_length
    T_to_entry = _calc_travel_time(ego_profile, ego_pos, pit_entry_pos, lap_length)
    D_to_entry = simulate_travel(car_profile, car_pos, T_to_entry, lap_length)
    threat_pos_at_entry = (car_pos + D_to_entry) % lap_length
    gap_at_entry = (pit_entry_pos - threat_pos_at_entry) % lap_length
    if gap_at_entry == 0.0:
        gap_at_entry = lap_length
    D_traveled = simulate_travel(car_profile, threat_pos_at_entry, T_pit, lap_length)
    critical = D_traveled - S_sector
    margin = gap_at_entry + S_sector - D_traveled
    return dict(threat_pos_at_entry=threat_pos_at_entry, gap_at_entry=gap_at_entry,
                D_traveled=D_traveled, critical_distance_m=critical, current_margin_m=margin,
                is_threat=(margin < 0))
