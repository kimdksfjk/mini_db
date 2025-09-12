# engine/operators/index_scan.py
from __future__ import annotations
from typing import Dict, Any, Iterable
from ..index_registry import IndexRegistry

class IndexScanOperator:
    """根据 where 的单列谓词，尝试用索引扫描，返回行迭代器；若不可用则返回 None。"""
    def __init__(self, catalog, storage, indexes: IndexRegistry):
        self.catalog = catalog
        self.storage = storage
        self.indexes = indexes

    def try_scan(self, table: str, where: Dict[str, Any]) -> Iterable[Dict[str, Any]] | None:
        if not where or not isinstance(where, dict):
            return None
        col = where.get("column")
        op = (where.get("operator") or "").upper()
        val = where.get("value")
        if not col or op not in ("=", ">", ">=", "<", "<="):
            return None
        meta = self.indexes.find_index_by_column(table, col)
        if not meta:
            return None
        # 确保 B+ 树已从索引堆文件加载
        self.indexes.ensure_loaded_from_storage(table, meta["name"], self.storage)
        tree = self.indexes.get_tree(table, meta["name"])

        # 将字符串尝试转成数字，便于和插入时一致
        v = val
        if isinstance(v, str):
            try:
                if v.isdigit() or (v.startswith('-') and v[1:].isdigit()):
                    v = int(v)
                else:
                    v = float(v)
            except Exception:
                pass

        if op == "=":
            return tree.search_eq(v)
        if op in (">", ">="):
            return tree.search_range(low=v, high=None, incl_low=(op==">="), incl_high=True)
        if op in ("<", "<="):
            return tree.search_range(low=None, high=v, incl_low=True, incl_high=(op=="<="))
        return None
