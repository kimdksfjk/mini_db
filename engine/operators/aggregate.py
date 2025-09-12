# engine/operators/aggregate.py
from __future__ import annotations
from typing import Dict, Any, Iterable, List
from collections import defaultdict

Row = Dict[str, Any]

def _is_num(x: Any) -> bool:
    if isinstance(x, (int, float)): return True
    try: float(x); return True
    except Exception: return False

def _to_float(x: Any) -> float:
    return float(x) if isinstance(x, (int, float)) else float(str(x))

class AggregateOperator:
    """
    内存聚合：支持 COUNT/SUM/AVG/MIN/MAX；WHERE 在它之前、ORDER BY/LIMIT 在它之后。
    输入：可迭代的行
    """
    def __init__(self, group_by: List[str], aggregates: List[Dict[str, Any]]):
        self.group_by = group_by or []
        self.aggs = []
        for a in aggregates or []:
            func = (a.get("func") or "").upper()
            col = a.get("column")
            alias = a.get("as")
            if not func: continue
            if alias is None:
                alias = func.lower() if not col or col=="*" else f"{func.lower()}_{str(col).split('.')[-1]}"
            self.aggs.append({"func":func, "column":col, "as":alias})

    def run(self, rows: Iterable[Row]) -> List[Row]:
        groups = defaultdict(list)
        if self.group_by:
            for r in rows:
                key = tuple(r.get(k) for k in self.group_by)
                groups[key].append(r)
        else:
            key = tuple()
            for r in rows:
                groups[key].append(r)

        out: List[Row] = []
        for key, items in groups.items():
            rr: Row = {}
            for n, v in zip(self.group_by, key):
                rr[n] = v
            for a in self.aggs:
                f, c, alias = a["func"], a.get("column"), a["as"]
                if f == "COUNT":
                    if c in (None, "*"):
                        rr[alias] = len(items)
                    else:
                        rr[alias] = sum(1 for it in items if it.get(c) is not None)
                    continue
                if f in ("SUM","AVG"):
                    total, n = 0.0, 0
                    for it in items:
                        v = it.get(c)
                        if _is_num(v):
                            total += _to_float(v); n += 1
                    rr[alias] = total if f=="SUM" else (total/n if n>0 else None)
                    continue
                if f in ("MIN","MAX"):
                    best = None; best_is_num = False
                    for it in items:
                        v = it.get(c)
                        if v is None: continue
                        if _is_num(v):
                            fv = _to_float(v)
                            if best is None or not best_is_num or (f=="MIN" and fv < best) or (f=="MAX" and fv > best):
                                best = fv; best_is_num = True
                        else:
                            sv = str(v)
                            if best is None:
                                best = sv; best_is_num = False
                            elif not best_is_num:
                                if (f=="MIN" and sv < best) or (f=="MAX" and sv > best):
                                    best = sv
                    rr[alias] = best
                    continue
                rr[alias] = None
            out.append(rr)
        return out
