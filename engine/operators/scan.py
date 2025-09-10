# engine/operators/scan.py
from __future__ import annotations
from typing import Dict, Any, Iterable, Iterator, List
from .base import Operator
from ..storage_iface import Storage

class SeqScan(Operator):
    def __init__(self, table: str, schema: List[Dict[str, Any]], storage: Storage) -> None:
        self.table = table
        self._schema = schema
        self.storage = storage

    @property
    def schema(self) -> List[Dict[str, Any]]:
        return self._schema

    def execute(self) -> Iterable[Dict[str, Any]]:
        for row in self.storage.scan_rows(self.table):
            yield row
