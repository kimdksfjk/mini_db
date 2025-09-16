# engine/index_registry.py
from __future__ import annotations
from typing import Dict, Any, Optional

from .bptree import BPlusTree
from .sys_catalog import SysCatalog
from .storage_adapter import StorageAdapter

class IndexRegistry:
    """
    索引注册表：__sys_indexes（.mdb）里持久化索引元数据，
    运行态缓存内存 B+ 树以支持 O(logN) 的查找与范围扫描。
    """
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self._storage = StorageAdapter(data_dir)
        self._sys = SysCatalog(data_dir, self._storage)
        self._trees: Dict[tuple, BPlusTree] = {}
        self._loaded: Dict[tuple, bool] = {}

    # -------- 元数据委托到系统表 --------
    def list_indexes(self, table: Optional[str] = None) -> Dict[str, Any]:
        return self._sys.list_indexes(table)

    def add_index(self, table: str, index_name: str, column: str,
                  path_or_storage: Dict[str, Any], unique: bool = False):
        """
        在系统表登记索引。如果 SysCatalog.add_index 的签名不同，依次尝试三种常见形式。
        """
        # 形式1：常见的命名关键字（name / storage_desc）
        try:
            return self._sys.add_index(
                table=table,
                name=index_name,
                column=column,
                storage_desc=path_or_storage,
                itype="BTREE",
                unique=unique
            )
        except TypeError:
            pass

        # 形式2：纯位置参数（table, name, column, storage_desc, itype, unique）
        try:
            return self._sys.add_index(
                table, index_name, column, path_or_storage, "BTREE", unique
            )
        except TypeError:
            pass

        # 形式3：命名关键字但用 storage（有些实现用这个键名）
        return self._sys.add_index(
            table=table,
            name=index_name,
            column=column,
            storage=path_or_storage,
            itype="BTREE",
            unique=unique
        )

    def drop_index(self, table: str, index_name: str):
        self._sys.drop_index(table, index_name)
        key = (table, index_name)
        self._trees.pop(key, None)
        self._loaded.pop(key, None)

    def find_index_by_column(self, table: str, column: str) -> Optional[Dict[str, Any]]:
        return self._sys.find_index_by_column(table, column)

    # -------- 运行态 B+ 树缓存 --------
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
        # 重置并回放
        self._trees[key] = BPlusTree(order=64)
        tree = self._trees[key]
        for row in storage_adapter.scan_rows(opened):  # 形如 {"k":..., "row": {...}}
            tree.insert(row.get("k"), row.get("row"))
        self._loaded[key] = True
