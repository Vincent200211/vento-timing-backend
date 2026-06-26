"""Recursive deep merge for F1 differential updates."""
from __future__ import annotations
import copy
from typing import Any


def deep_merge(base: dict, delta: dict) -> dict:
    """Recursively merge *delta* into a *base* and return a new dict.

    Pure function — does not modify either input.
    Arrays are replaced, not merged (F1 formats use full-snapshot arrays).

    Args:
        base: Existing state (will not be mutated).
        delta: F1 differential update (only changed fields).

    Returns:
        A new dict with *delta* applied on top of *base*.
    """
    if not isinstance(base, dict) or not isinstance(delta, dict):
        return copy.deepcopy(delta) if isinstance(delta, (dict, list)) else delta

    result = {}
    # Copy all existing keys from base
    for k, v in base.items():
        result[k] = copy.deepcopy(v) if hasattr(v, '__dict__') else v
    # Apply delta (recursive for nested dicts)
    for k, v in delta.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = copy.deepcopy(v) if isinstance(v, (dict, list)) else v
    return result


def merge_snapshot(base: dict, delta: dict) -> dict:
    """Convenience: deep_merge with None guard."""
    if delta is None:
        return copy.deepcopy(base) if base else {}
    return deep_merge(base or {}, delta)
