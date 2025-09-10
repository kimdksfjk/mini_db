# engine/operators/project.py
from __future__ import annotations
from typing import Dict, Any, Iterable, List
from .base import Operator

def _base_col(name: str) -> str:
    return name.split(" AS ")[0].split(".")[-1]

class Project(Operator):
    def __init__(self, child: Operator, columns: List[str]) -> None:
        self.child = child
        self.columns = columns

    @property
    def schema(self) -> List[Dict[str, Any]]:
        if self.columns == ["*"]:
            return self.child.schema
        wanted = {_base_col(c) for c in self.columns}
        return [c for c in self.child.schema if c.get("name") in wanted]

    def execute(self) -> Iterable[Dict[str, Any]]:
        if self.columns == ["*"]:
            for r in self.child:
                yield r
            return
        names = [_base_col(c) for c in self.columns]
        for r in self.child:
            yield {n: r.get(n) for n in names}
