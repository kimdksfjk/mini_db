# engine/catalog.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
import os
from .storage_adapter import StorageAdapter
from .sys_catalog import SysCatalog

class Catalog:
    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        self._storage = StorageAdapter(self.data_dir)
        self._sys = SysCatalog(self.data_dir, self._storage)

    def get_table(self, name: str) -> Dict[str, Any]:
        return self._sys.get_table(name)

    def create_table(self, name: str, columns: List[Dict[str, Any]],
                     storage_desc: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        兼容旧调用：允许传入 storage_desc（若已由外部创建 .mdb）。
        若未提供，则由系统表逻辑自行创建 .mdb。
        """
        return self._sys.create_table_and_register(name, columns, storage_desc)

    def list_tables(self) -> List[str]:
        return self._sys.list_tables()

    def has_table(self, name: str) -> bool:
        try:
            self._sys.get_table(name); return True
        except KeyError:
            return False
