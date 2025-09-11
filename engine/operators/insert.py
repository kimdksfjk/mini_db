
from __future__ import annotations
from typing import Dict, Any, List
from ..catalog import Catalog
from ..storage_adapter import StorageAdapter
from .base import make_type_casts

class InsertOperator:
    def __init__(self, catalog: Catalog, storage: StorageAdapter) -> None:
        self.catalog = catalog
        self.storage = storage

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        table = plan["table_name"]
        cols: List[str] = plan.get("columns", [])
        values: List[List[str]] = plan.get("values", [])
        meta = self.catalog.get_table(table)
        casts = make_type_casts(meta["columns"])
        opened = self.storage.open_table(table, meta["storage"])
        count = 0
        for rowvals in values:
            row = {}
            for c, v in zip(cols, rowvals):
                # type cast if possible
                try:
                    row[c] = casts.get(c, lambda x: x)(v)
                except Exception:
                    row[c] = v
            self.storage.insert_row(opened, row)
            count += 1
        return {"ok": True, "message": f"{count} rows inserted."}
