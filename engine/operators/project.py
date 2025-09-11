
from __future__ import annotations
from typing import List, Iterable, Iterator, Dict, Any
from .base import project_row

class ProjectOperator:
    def __init__(self, columns: List[str]) -> None:
        self.columns = columns if columns else ["*"]

    def run(self, rows: Iterable[dict]) -> Iterator[dict]:
        for r in rows:
            yield project_row(r, self.columns)
