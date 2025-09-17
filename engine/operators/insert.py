# engine/operators/insert.py
from __future__ import annotations
from typing import Dict, Any, List, Optional

def _parse_literal(v: Any) -> Any:
    """把编译器给的常量转成合适的 Python 值（编译器已去掉字符串引号）。"""
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v
        # 数值尝试
        try:
            if s.strip().lower() in ("null", "none"):
                return None
            if "." in s:
                return float(s)
            return int(s)
        except Exception:
            return s
    return v

def _cast_by_type(val: Any, typ: str) -> Any:
    t = (typ or "").upper()
    if val is None:
        return None
    if t in ("INT", "INTEGER"):
        return int(val)
    if t in ("FLOAT", "DOUBLE", "REAL"):
        return float(val)
    # CHAR/VARCHAR 默认转 str
    return str(val)

class InsertOperator:
    """
    将 INSERT 计划写入页式存储，并在存在索引时同步追加到索引底表。
    允许 self.indexes 为 None（无索引场景）。
    """
    def __init__(self, catalog, storage, indexes=None):
        self.catalog = catalog
        self.storage = storage
        self.indexes = indexes  # 可能是 None

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        table = plan["table_name"]
        cols: List[str] = plan["columns"] or []
        values: List[List[Any]] = plan["values"] or []

        meta = self.catalog.get_table(table)  # {'columns':[{'name','type'},...], 'storage':...}
        opened = self.storage.open_table(table, meta["storage"])

        # 列类型映射：name -> type
        col_types = {c["name"]: c.get("type", "") for c in (meta.get("columns") or [])}

        n = 0
        for row_vals in values:
            row: Dict[str, Any] = {}
            for c, v in zip(cols, row_vals):
                # 去掉可能的表前缀 alias.col
                cname = c.split(".", 1)[-1] if "." in c else c
                py_v = _parse_literal(v)
                py_v = _cast_by_type(py_v, col_types.get(cname, ""))
                row[cname] = py_v

            # 写入堆表
            self.storage.insert_row(opened, row)
            n += 1

            # 索引同步
            if self.indexes:
                try:
                    # 兼容两种 API：list_indexes(table) / list_for_table(table)
                    try:
                        idxs = self.indexes.list_indexes(table)  # type: ignore
                    except Exception:
                        idxs = self.indexes.list_for_table(table)  # type: ignore
                    idxs = idxs or {}
                    for iname, imeta in idxs.items():
                        idx_tbl = f"__idx__{table}__{iname}"
                        istg = imeta.get("storage") or {}
                        kcol = imeta.get("column")
                        iopen = self.storage.open_table(idx_tbl, istg)
                        self.storage.insert_row(iopen, {"k": row.get(kcol), "row": row})
                    # 索引缓存（如有）标记失效
                    try:
                        for iname in idxs.keys():
                            self.indexes.mark_unloaded(table, iname)  # type: ignore
                    except Exception:
                        pass
                except Exception:
                    # 索引失败不影响主数据插入
                    pass

        return {"ok": True, "message": f"{n} rows inserted."}
