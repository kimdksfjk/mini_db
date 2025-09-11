
from __future__ import annotations
from typing import Dict, Any, Iterable, List, Tuple, Optional, Callable

Row = Dict[str, Any]

def make_type_casts(schema: List[Dict[str, Any]]) -> Dict[str, Callable[[str], Any]]:
    casts = {}
    for col in schema:
        name = col["name"]
        t = (col.get("type") or "").upper()
        if t in ("INT", "INTEGER"):
            casts[name] = lambda x, _int=int: _int(x)
        elif t in ("FLOAT", "DOUBLE", "REAL"):
            casts[name] = lambda x, _float=float: _float(x)
        else:
            casts[name] = lambda x: x
    return casts

def apply_where(row: Row, where: Dict[str, Any]) -> bool:
    col = where.get("column")
    op = where.get("operator")
    val = where.get("value")
    # try numeric comparison
    left = row.get(col)
    # if either is None -> False except !=
    if left is None:
        return (op in ("!=", "<>") and val is not None)
    # attempt cast numeric if both look numeric
    def coerce(v):
        if isinstance(v, (int, float)):
            return v
        try:
            if "." in str(v):
                return float(v)
            return int(v)
        except Exception:
            return str(v)
    a = coerce(left)
    b = coerce(val)
    if op == "=":
        return a == b
    if op in ("!=", "<>"):
        return a != b
    if op == ">":
        return a > b
    if op == ">=":
        return a >= b
    if op == "<":
        return a < b
    if op == "<=":
        return a <= b
    return False

def project_row(row: Row, columns: List[str]) -> Row:
    if len(columns) == 1 and columns[0] == "*":
        return dict(row)
    out = {}
    for c in columns:
        out[c] = row.get(c)
    return out
