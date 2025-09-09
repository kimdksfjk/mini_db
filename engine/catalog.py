from __future__ import annotations
import json, os
from typing import Dict, List, Any, Optional

class Catalog:
    """Very small JSON-backed system catalog.
    - Stored at <data_dir>/catalog.json
    - Tables: { table_name: {"columns":[{"name":...,"type":...}, ...]} }
    This is a minimal workable catalog for the execution engine. The real storage
    layer can later replace this with a page-backed catalog; API stays the same.
    """
    def __init__(self, data_dir: str = "data") -> None:
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.path = os.path.join(self.data_dir, "catalog.json")
        self._data: Dict[str, Any] = {"tables": {}}
        if os.path.exists(self.path):
            self._data = json.load(open(self.path, "r", encoding="utf-8"))
        else:
            self._flush()

    # ---- basic ops ----
    def _flush(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)

    def has_table(self, name: str) -> bool:
        return name in self._data["tables"]

    def create_table(self, name: str, columns: List[Dict[str, Any]], if_not_exists: bool = True) -> None:
        if self.has_table(name):
            if if_not_exists:
                return
            raise ValueError(f"Table '{name}' already exists")
        self._data["tables"][name] = {"columns": columns}
        self._flush()

    def drop_table(self, name: str) -> None:
        if not self.has_table(name):
            return
        del self._data["tables"][name]
        self._flush()

    def schema(self, name: str) -> List[Dict[str, Any]]:
        t = self._data["tables"].get(name)
        if not t:
            raise ValueError(f"Unknown table '{name}'")
        return t["columns"]
