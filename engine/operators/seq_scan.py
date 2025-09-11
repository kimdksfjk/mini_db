
from __future__ import annotations
from typing import Dict, Any, Iterable, List
from ..catalog import Catalog
from ..storage_adapter import StorageAdapter

class SeqScanOperator:
    def __init__(self, catalog: Catalog, storage: StorageAdapter) -> None:
        self.catalog = catalog
        self.storage = storage

    def scan(self, table: str) -> Iterable[dict]:
        meta = self.catalog.get_table(table)
        opened = self.storage.open_table(table, meta["storage"])
        yield from self.storage.scan_rows(opened)
