# engine/index_registry.py
from __future__ import annotations
from typing import Dict, Any, Optional
from .bptree import BPlusTree
from .sys_catalog import SysCatalog
from .storage_adapter import StorageAdapter

class IndexRegistry:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self._storage = StorageAdapter(data_dir)
        self._sys = SysCatalog(data_dir, self._storage)
        self._trees: Dict[tuple, BPlusTree] = {}
        self._loaded: Dict[tuple, bool] = {}

    def list_indexes(self, table: Optional[str] = None) -> Dict[str, Any]:
        return self._sys.list_indexes(table)

    def add_index(self, table: str, index_name: str, column: str, storage_desc: Dict[str, Any], unique: bool=False):
        self._sys.add_index(table, index_name, column, storage_desc, itype="BTREE", unique=unique)

    def drop_index(self, table: str, index_name: str):
        # 先拿到存储描述，准备删除物理文件
        meta_all = self._sys.list_indexes(table) or {}
        meta = meta_all.get(index_name)
        if meta and "storage" in meta:
            try:
                desc = meta["storage"]
                # 名称随你建索引时的命名规则，和 ensure_loaded_from_storage 一致
                idx_table_name = f"__idx__{table}__{index_name}"
                opened = self._storage.open_table(idx_table_name, desc)
                self._storage.clear_table(opened)  # 会强制释放句柄并删除 .mdb
            except Exception:
                pass  # 删不掉也别阻断流程（例如文件已被手工删除）

        # 再删系统表元信息与内存缓存
        self._sys.drop_index(table, index_name)
        key = (table, index_name)
        self._trees.pop(key, None)
        self._loaded.pop(key, None)

    def find_index_by_column(self, table: str, column: str) -> Optional[Dict[str, Any]]:
        return self._sys.find_index_by_column(table, column)

    def get_tree(self, table: str, index_name: str) -> BPlusTree:
        key = (table, index_name)
        if key not in self._trees:
            self._trees[key] = BPlusTree(order=64)
        return self._trees[key]

    def mark_unloaded(self, table: str, index_name: str) -> None:
        self._loaded[(table, index_name)] = False

    def ensure_loaded_from_storage(self, table: str, index_name: str, storage_adapter) -> None:
        key = (table, index_name)
        if self._loaded.get(key):
            return
        meta = self.list_indexes(table).get(index_name)
        if not meta:
            return
        storage_desc = meta["storage"]
        opened = storage_adapter.open_table(f"__idx__{table}__{index_name}", storage_desc)
        self._trees[key] = BPlusTree(order=64)
        tree = self._trees[key]
        for row in storage_adapter.scan_rows(opened):  # {"k":..., "row": {...}}
            tree.insert(row.get("k"), row.get("row"))
        self._loaded[key] = True
