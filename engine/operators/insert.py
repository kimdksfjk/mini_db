# engine/operators/insert.py
from __future__ import annotations
from typing import Dict, Any, List
from ..index_registry import IndexRegistry

class InsertOperator:
    def __init__(self, catalog, storage, indexes: IndexRegistry):
        self.catalog = catalog
        self.storage = storage
        self.indexes = indexes

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        table = plan.get("table_name")
        columns: List[str] = plan.get("columns", [])
        values: List[List[Any]] = plan.get("values", [])
        if not table or not columns or not values:
            return {"ok": False, "error": "invalid insert plan"}
        meta = self.catalog.get_table(table)
        opened = self.storage.open_table(table, meta["storage"])

        idxs = self.indexes.list_indexes(table)
        n = 0
        for row_vals in values:
            row = dict(zip(columns, row_vals))
            # 把字符串数值尽量转为数字，便于比较/聚合
            for k, v in list(row.items()):
                if isinstance(v, str):
                    try:
                        if v.isdigit() or (v.startswith('-') and v[1:].isdigit()):
                            row[k] = int(v)
                        else:
                            row[k] = float(v)
                    except Exception:
                        pass

            self.storage.insert_row(opened, row)
            n += 1

            # 维护索引：写索引堆文件 + （必要时）更新内存树
            for iname, imeta in idxs.items():
                col = imeta.get("column")
                if col in row:
                    idx_desc = imeta["storage"]
                    opened_idx = self.storage.open_table(f"__idx__{table}__{iname}", idx_desc)
                    self.storage.insert_row(opened_idx, {"k": row[col], "row": row})
                    # 确保内存树已加载旧条目，再插入新条目，避免未来首次加载时重复
                    self.indexes.ensure_loaded_from_storage(table, iname, self.storage)
                    self.indexes.get_tree(table, iname).insert(row[col], row)

        return {"ok": True, "message": f"{n} rows inserted."}
