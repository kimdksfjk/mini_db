# engine/operators/filter.py
from __future__ import annotations
from typing import Dict, Any, Iterable, Iterator, List, Callable
from .base import Operator

class Filter(Operator):
    def __init__(self, child: Operator, predicate: Callable[[Dict[str, Any]], bool]) -> None:
        self.child = child
        self.predicate = predicate

    @property
    def schema(self) -> List[Dict[str, Any]]:
        return self.child.schema

    def execute(self) -> Iterable[Dict[str, Any]]:
        for row in self.child:
            if self.predicate(row):
                yield row
