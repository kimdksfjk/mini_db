
from __future__ import annotations
from typing import Dict, Any, Iterable, Iterator
from .base import apply_where

class FilterOperator:
    def __init__(self, where: Dict[str, Any] | None) -> None:
        self.where = where

    def run(self, rows: Iterable[dict]) -> Iterator[dict]:
        if not self.where:
            for r in rows:
                yield r
            return
        for r in rows:
            if apply_where(r, self.where):
                yield r
