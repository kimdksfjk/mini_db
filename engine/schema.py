
"""Basic type system for INT and STRING."""
from dataclasses import dataclass
from typing import List

@dataclass
class Column:
    name: str
    type: str  # "INT" or "STRING"

@dataclass
class Schema:
    columns: List[Column]

    def index_of(self, name: str) -> int:
        for i, c in enumerate(self.columns):
            if c.name == name:
                return i
        raise KeyError(name)
