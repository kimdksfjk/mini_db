
from __future__ import annotations
from typing import Dict, Any, List
from ..catalog import Catalog
from ..storage_adapter import StorageAdapter

class CreateTableOperator:
    def __init__(self, catalog: Catalog, storage: StorageAdapter, data_dir: str) -> None:
        self.catalog = catalog
        self.storage = storage
        self.data_dir = data_dir

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        name = plan["table_name"]
        columns: List[Dict[str, Any]] = plan.get("columns", [])
        if self.catalog.has_table(name):
            return {"ok": False, "error": f"table '{name}' already exists"}
        storage_desc = self.storage.create_table(name, columns)
        self.catalog.create_table(name, columns, storage_desc)
        return {"ok": True, "message": f"Table {name} created."}
