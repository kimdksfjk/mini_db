from __future__ import annotations
import os, json
from typing import Any, Dict, List, Optional

class Catalog:
    """Very small JSON-backed catalog.
    Structure:
    {
      "tables": {
        "<name>": { "columns": [{"name":"id","type":"INT"}, ...] }
      }
    }
    """
    def __init__(self, data_dir: str = "data") -> None:
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.path = os.path.join(self.data_dir, "catalog.json")
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self._data = json.load(f)
        else:
            self._data = {"tables": {}}
            self._flush()

    def _flush(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)

    def create_table(self, name: str, columns: List[Dict[str, Any]], if_not_exists: bool = True) -> None:
        if name in self._data["tables"]:
            if if_not_exists:
                return
            raise ValueError(f"Table '{name}' already exists")
        self._data["tables"][name] = {"columns": columns}
        self._flush()

    def drop_table(self, name: str) -> None:
        if name in self._data["tables"]:
            del self._data["tables"][name]
            self._flush()

    def has_table(self, name: str) -> bool:
        return name in self._data["tables"]

    def schema(self, name: str) -> List[Dict[str, Any]]:
        t = self._data["tables"].get(name)
        if not t:
            raise ValueError(f"Unknown table '{name}'")
        return t["columns"]

    def list_tables(self) -> List[str]:
        return sorted(self._data["tables"].keys())
