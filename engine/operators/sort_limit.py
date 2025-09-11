# engine/operators/sort_limit.py
from __future__ import annotations
from typing import Dict, Any, Iterable, List
from .base import Operator

def _base_col(name: str) -> str:
    return name.split(" AS ")[0].split(".")[-1]

class OrderBy(Operator):
    """多键稳定排序；无论升降序，None 永远排在最后。"""
    def __init__(self, child: Operator, keys: List[Dict[str, str]] | None) -> None:
        self.child = child
        self.keys = keys or []

    @property
    def schema(self) -> List[Dict[str, Any]]:
        return self.child.schema

    def execute(self) -> Iterable[Dict[str, Any]]:
        rows = list(self.child)
        for spec in reversed(self.keys):
            col = _base_col(spec.get("column",""))
            desc = str(spec.get("direction","ASC")).upper() == "DESC"
            if not desc:
                rows.sort(key=lambda x: (x.get(col) is None, x.get(col)))
            else:
                rows.sort(key=lambda x: (x.get(col) is None, x.get(col)), reverse=True)
                rows.sort(key=lambda x: (x.get(col) is None,))
        for r in rows:
            yield r

class Limit(Operator):
    def __init__(self, child: Operator, limit: int | None, offset: int | None) -> None:
        self.child = child
        self.limit = limit
        self.offset = offset or 0

    @property
    def schema(self) -> List[Dict[str, Any]]:
        return self.child.schema

    def execute(self) -> Iterable[Dict[str, Any]]:
        n = 0
        for idx, r in enumerate(self.child):
            if idx < self.offset:
                continue
            if self.limit is not None and n >= self.limit:
                break
            yield r
            n += 1
