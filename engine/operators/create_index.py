# engine/operators/create_index.py
from __future__ import annotations
from typing import Dict, Any
from ..index_registry import IndexRegistry

class CreateIndexOperator:
    def __init__(self, catalog, storage, indexes: IndexRegistry):
        self.catalog = catalog
        self.storage = storage
        self.indexes = indexes

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        table = plan.get("table_name")
        column = plan.get("column")
        index_name = plan.get("index_name") or f"idx_{column}"

        # 打开数据表
        meta = self.catalog.get_table(table)
        opened_tbl = self.storage.open_table(table, meta["storage"])

        # 创建“索引堆文件”（页式 .mdb）
        idx_table_name = f"__idx__{table}__{index_name}"
        storage_desc = self.storage.create_table(
            idx_table_name,
            [{"name": "k", "type": "ANY"}, {"name": "row", "type": "JSON"}]
        )

        # 注册索引（标记未加载到内存树）
        self.indexes.add_index(table, index_name, column, storage_desc)
        self.indexes.mark_unloaded(table, index_name)

        # 全表扫描 -> 仅写入索引堆文件（不写内存树，首次查询再统一加载）
        opened_idx = self.storage.open_table(idx_table_name, storage_desc)
        n = 0
        for row in self.storage.scan_rows(opened_tbl):
            key = row.get(column)
            self.storage.insert_row(opened_idx, {"k": key, "row": row})
            n += 1

        return {"ok": True, "message": f"Index {index_name} ON {table}({column}) created with {n} entries."}
