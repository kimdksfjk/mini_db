# engine/operators/project.py
from __future__ import annotations
from typing import Dict, Any, Iterable, List
import re
from .base import Operator

_AS_SPLIT = re.compile(r"\s+AS\s+", flags=re.IGNORECASE)

def _base_col(name: str) -> str:
    left = _AS_SPLIT.split(name, maxsplit=1)[0]
    return left.split(".")[-1]

def _alias(name: str) -> str | None:
    parts = _AS_SPLIT.split(name, maxsplit=1)
    return parts[1] if len(parts) == 2 else None

class Project(Operator):
    def __init__(self, child: Operator, columns: List[str]) -> None:
        self.child = child
        self.columns = columns

    @property
    def schema(self) -> List[Dict[str, Any]]:
        if self.columns == ["*"]:
            return self.child.schema
        return [{"name": c, "type": ""} for c in self.columns]

    def execute(self) -> Iterable[Dict[str, Any]]:
        if self.columns == ["*"]:
            for r in self.child:
                yield r
            return
        specs = []
        for s in self.columns:
            ali = _alias(s)
            base_full = _AS_SPLIT.split(s, maxsplit=1)[0]
            base_leaf = base_full.split(".")[-1]
            specs.append((s, ali, base_full, base_leaf))

        for r in self.child:
            out = {}
            for out_key, ali, base_full, base_leaf in specs:
                val = r.get(ali) if ali else None
                if val is None:
                    val = r.get(base_full)
                if val is None:
                    val = r.get(base_leaf)
                out[out_key] = val
            yield out
