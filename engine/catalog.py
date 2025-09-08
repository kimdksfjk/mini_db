
"""
Catalog persistence mirror. On startup load data/catalog.meta, on changes save it.
Structure example:
  tables: {
    "users": {"file_id": "...", "columns":[{"name":"id","type":"INT"},...], "pk": null}
  }
"""
import json, os
from typing import Dict, Any

META_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "catalog.meta")

class Catalog:
    def __init__(self):
        self.data = {"tables": {}}
        self.load()

    def load(self):
        if os.path.exists(META_PATH):
            with open(META_PATH, "r", encoding="utf-8") as f:
                self.data = json.load(f)

    def save(self):
        os.makedirs(os.path.dirname(META_PATH), exist_ok=True)
        with open(META_PATH, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def get_schema(self, table: str):
        return self.data["tables"][table]["columns"]

    def get_file_id(self, table: str):
        return self.data["tables"][table]["file_id"]

    def create_table(self, table: str, columns):
        if table in self.data["tables"]:
            raise ValueError(f"Table exists: {table}")
        # file_id assigned by storage/files.py at execution time
        self.data["tables"][table] = {"columns": columns, "file_id": None, "pk": None}
        self.save()
