# engine/index_registry.py
from __future__ import annotations
from typing import Dict, Any, Optional
from .bptree import BPlusTree
from .sys_catalog import SysCatalog
from .storage_adapter import StorageAdapter

class IndexRegistry:
    """
    索引注册表：基于 __sys_indexes（.mdb）持久化。
    仍然提供运行态 B+ 树缓存，以支持 search_eq / search_range 的 O(logN)+顺序扫描。
    """
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self._storage = StorageAdapter(data_dir)
        self._sys = SysCatalog(data_dir, self._storage)  # 复用同一套系统表
        self._trees: Dict[tuple, BPlusTree] = {}
        self._loaded: Dict[tuple, bool] = {}

    # ---- 元数据层（委托给系统表） ----
    def list_indexes(self, table: Optional[str] = None) -> Dict[str, Any]:
        return self._sys.list_indexes(table)

    def add_index(self, table: str, index_name: str, column: str, path_or_storage: Dict[str, Any], unique: bool=False):
        # 这里接收 storage 描述（CreateIndexOperator 会传来）
        self._sys.add_index(table, index_name, column, path_or_storage, itype="BTREE", unique=unique)

    def drop_index(self, table: str, index_name: str):
        self._sys.drop_index(table, index_name)
        key = (table, index_name)
        self._trees.pop(key, None)
        self._loaded.pop(key, None)

    def find_index_by_column(self, table: str, column: str) -> Optional[Dict[str, Any]]:
        return self._sys.find_index_by_column(table, column)

    # ---- 运行态 B+ 树缓存 ----
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
        # 重置为一棵空树
        self._trees[key] = BPlusTree(order=64)
        tree = self._trees[key]
        for row in storage_adapter.scan_rows(opened):  # {"k":..., "row": {...}}
            tree.insert(row.get("k"), row.get("row"))
        self._loaded[key] = True
