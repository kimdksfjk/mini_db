# engine/operators/delete.py
from __future__ import annotations
from typing import Dict, Any, List, Optional

def _parse_value(v: Any):
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v
        if len(s) >= 2 and ((s[0] == s[-1] == "'") or (s[0] == s[-1] == '"')):
            return s[1:-1]
        try:
            if "." in s:
                return float(s)
            return int(s)
        except Exception:
            return s
    return v

def _cmp(a, op: str, b) -> bool:
    try:
        if op == "=":  return a == b
        if op in ("!=", "<>"): return a != b
        if op == ">":  return a > b
        if op == "<":  return a < b
        if op == ">=": return a >= b
        if op == "<=": return a <= b
    except Exception:
        return False
    return False

def _match_where(row: Dict[str, Any], where: Optional[Dict[str, Any]]) -> bool:
    if not where:
        return True
    col = where.get("column")
    op  = where.get("operator", "=")
    val = _parse_value(where.get("value"))
    if isinstance(col, str) and "." in col:
        col = col.split(".", 1)[1]
    a = row.get(col)
    return _cmp(a, op, val)

class DeleteOperator:
    """
    条件删除：
      - WHERE 为空 => 清空表
      - WHERE 非空 => 读出全部行，过滤掉命中的，再重写回去
      - 若存在索引：清空并基于剩余行重建索引底表
    """
    def __init__(self, catalog, storage, indexes=None):
        self.catalog = catalog
        self.storage = storage
        self.indexes = indexes  # 允许为 None（无索引时自动跳过）

    def _rebuild_indexes(self, table: str, rows: List[Dict[str, Any]]):
        if self.indexes is None:
            return
        try:
            all_idx = self.indexes.list_for_table(table) or {}
        except Exception:
            return
        for iname, idx_meta in all_idx.items():
            try:
                idx_tbl = f"__idx__{table}__{iname}"
                istg    = idx_meta.get("storage") or {}
                col     = idx_meta.get("column")
                # 清空并重建索引底表
                iopen = self.storage.open_table(idx_tbl, istg)
                self.storage.clear_table(iopen)
                iopen = self.storage.open_table(idx_tbl, istg)
                for r in rows:
                    self.storage.insert_row(iopen, {"k": r.get(col), "row": r})
                try:
                    self.indexes.mark_unloaded(table, iname)
                except Exception:
                    pass
            except Exception:
                # 单个索引失败不影响整体 DELETE
                continue

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        table = plan["table_name"]
        where = plan.get("where")

        meta = self.catalog.get_table(table)                  # {'columns':..., 'storage':...}
        opened = self.storage.open_table(table, meta["storage"])

        # 无 WHERE：清空整表
        if not where:
            self.storage.clear_table(opened)
            # 清空并重建索引底表（为空）
            self._rebuild_indexes(table, [])
            return {"ok": True, "message": f"Table {table} cleared."}

        # 有 WHERE：过滤写回
        kept: List[Dict[str, Any]] = []
        deleted = 0
        for row in self.storage.scan_rows(opened):
            if _match_where(row, where):
                deleted += 1
            else:
                kept.append(row)

        # 重写
        self.storage.clear_table(opened)
        reopened = self.storage.open_table(table, meta["storage"])
        for r in kept:
            self.storage.insert_row(reopened, r)

        # 索引重建
        self._rebuild_indexes(table, kept)

        return {"ok": True, "message": f"{deleted} rows deleted."}
