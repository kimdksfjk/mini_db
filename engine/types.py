from __future__ import annotations
from typing import Any

def normalize_type(t: str) -> str:
    return (t or "").upper()

def coerce_by_type(value: Any, col_type: str):
    t = normalize_type(col_type)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if t in ("INT", "INTEGER"):
            return int(value)
        if t in ("FLOAT", "DOUBLE", "REAL", "NUMERIC", "DECIMAL"):
            return float(value)
        return value
    s = str(value)
    if s.upper() == "NULL":
        return None
    if t in ("INT", "INTEGER"):
        try: return int(s)
        except Exception: return s
    if t in ("FLOAT", "DOUBLE", "REAL", "NUMERIC", "DECIMAL"):
        try: return float(s)
        except Exception: return s
    return s

def strip_alias(col: str) -> str:
    name = col.split(" AS ")[0]
    return name.split(".")[-1]
