# engine/index_registry.py
from __future__ import annotations
from typing import Dict, Any, Optional

from .bptree import BPlusTree
from .sys_catalog import SysCatalog
from .storage_adapter import StorageAdapter


class IndexRegistry:
    """
    索引注册表（进程内缓存 + 系统表持久化）

    设计要点：
    1）元数据持久化：使用系统表 __sys_indexes（页式 .mdb）记录索引的基本信息与存储描述。
    2）运行时缓存：为每个索引维护一棵进程内 B+ 树，以便提供 O(logN) 的点查与顺序范围扫描。
    3）延迟装载：首次使用某索引时，从对应索引表文件加载键值对重建 B+ 树，加载完成后标记为已加载。
    """

    def __init__(self, data_dir: str):
        """
        初始化索引注册表。

        参数：
            data_dir: 数据根目录，系统表与索引文件均在该目录下管理。
        """
        self.data_dir = data_dir
        self._storage = StorageAdapter(data_dir)
        self._sys = SysCatalog(data_dir, self._storage)
        self._trees: Dict[tuple, BPlusTree] = {}   # (table, index_name) -> BPlusTree
        self._loaded: Dict[tuple, bool] = {}       # (table, index_name) -> 是否已从磁盘加载

    # =========================
    # 元数据接口（委托系统表）
    # =========================
    def list_indexes(self, table: Optional[str] = None) -> Dict[str, Any]:
        """
        列出索引。
        参数：
            table: 可选，指定表名时仅返回该表的索引；为 None 时返回所有表的索引。
        返回：
            若指定表名：{index_name: meta, ...}
            若未指定：{table: {index_name: meta, ...}, ...}
        """
        return self._sys.list_indexes(table)

    def add_index(
        self,
        table: str,
        index_name: str,
        column: str,
        path_or_storage: Dict[str, Any],
        unique: bool = False
    ) -> None:
        """
        注册一个索引元数据记录。

        参数：
            table: 表名
            index_name: 索引名
            column: 索引列
            path_or_storage: 索引存储描述（由创建索引算子生成）
            unique: 是否唯一索引（当前实现未强制校验，供后续扩展）
        """
        self._sys.add_index(
            table=table,
            index_name=index_name,
            column=column,
            storage=path_or_storage,
            itype="BTREE",
            unique=unique
        )

    def drop_index(self, table: str, index_name: str) -> None:
        """
        删除索引：元数据 + 运行时缓存同步移除。
        注意：不负责删除底层索引文件，清理由上层算子决定。
        """
        self._sys.drop_index(table, index_name)
        key = (table, index_name)
        self._trees.pop(key, None)
        self._loaded.pop(key, None)

    def find_index_by_column(self, table: str, column: str) -> Optional[Dict[str, Any]]:
        """
        根据列名在指定表中查找索引元数据。
        返回：
            命中时返回索引元信息 dict；否则返回 None。
        """
        return self._sys.find_index_by_column(table, column)

    # =========================
    # 运行时 B+ 树缓存
    # =========================
    def get_tree(self, table: str, index_name: str) -> BPlusTree:
        """
        获取（或创建）一棵 B+ 树实例，但不保证数据已加载。
        返回的树用于内存内查询与插入更新。
        """
        key = (table, index_name)
        if key not in self._trees:
            self._trees[key] = BPlusTree(order=64)
        return self._trees[key]

    def mark_unloaded(self, table: str, index_name: str) -> None:
        """
        将某索引标记为“未加载”，下次访问时触发重新加载。
        典型场景：索引文件被重建或批量更新后，需要刷新内存视图。
        """
        self._loaded[(table, index_name)] = False

    def ensure_loaded_from_storage(self, table: str, index_name: str, storage_adapter) -> None:
        """
        若内存中尚未装载该索引，则从索引文件扫描全部键值对并重建 B+ 树。

        流程：
        1）查询系统表获取该索引的存储描述。
        2）通过 storage_adapter 打开对应索引表（通常命名为 __idx__{table}__{index_name}）。
        3）扫描索引表的所有记录，每条记录应形如 {"k": 键, "row": 原始行或定位信息}。
        4）将键值对插入到进程内 B+ 树，并标记为已加载。
        """
        key = (table, index_name)
        if self._loaded.get(key):
            return

        meta = self.list_indexes(table).get(index_name)
        if not meta:
            return

        storage_desc = meta["storage"]
        opened = storage_adapter.open_table(f"__idx__{table}__{index_name}", storage_desc)

        # 重置并重建内存树
        self._trees[key] = BPlusTree(order=64)
        tree = self._trees[key]

        for row in storage_adapter.scan_rows(opened):
            tree.insert(row.get("k"), row.get("row"))

        self._loaded[key] = True
