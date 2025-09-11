
from __future__ import annotations
import json, os
from typing import Dict, List, Any, Optional

class Catalog:
    """Very simple JSON-backed catalog storing table schemas and storage info."""
    def __init__(self, data_dir: str) -> None:
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.path = os.path.join(self.data_dir, "catalog.json")
        self._data = {"tables": {}}  # name -> {columns:[{name,type}], storage:{kind:..., path:...}}
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception:
                # if corrupted, keep empty new catalog
                self._data = {"tables": {}}

    def save(self) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    # ---- table APIs ----
    def has_table(self, name: str) -> bool:
        return name in self._data["tables"]

    def create_table(self, name: str, columns: List[Dict[str, Any]], storage: Dict[str, Any]) -> None:
        if self.has_table(name):
            raise ValueError(f"table '{name}' already exists")
        self._data["tables"][name] = {"columns": columns, "storage": storage}
        self.save()

    def drop_table(self, name: str) -> None:
        if not self.has_table(name):
            raise ValueError(f"table '{name}' does not exist")
        self._data["tables"].pop(name)
        self.save()

    def get_table(self, name: str) -> Dict[str, Any]:
        t = self._data["tables"].get(name)
        if not t:
            raise KeyError(f"table '{name}' not found")
        return t

    def list_tables(self) -> Dict[str, Any]:
        return self._data["tables"]
