# engine/operators/update.py
from __future__ import annotations
from typing import Dict, Any, Iterable, List, Optional

def _parse_value(v: str):
    # 尝试把字符串常量转成 int/float，否则原样
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v
        # 允许带引号的字符串常量
        if (len(s) >= 2) and ((s[0] == s[-1] == "'") or (s[0] == s[-1] == '"')):
            return s[1:-1]
        # 数字
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
    # 支持别名/前缀：a.b -> 取 b；直接 col -> 直接取
    if isinstance(col, str) and "." in col:
        col = col.split(".", 1)[1]
    a = row.get(col)
    return _cmp(a, op, val)

class UpdateOperator:
    """
    安全可用的 UPDATE：
      - 读取所有行，命中 WHERE 的按 SET 改值
      - 清空表文件 -> 重新写回
      - 若存在索引，尽量重建底表（无索引时自动跳过）
    """
    def __init__(self, catalog, storage, indexes=None):
        self.catalog  = catalog
        self.storage  = storage
        self.indexes  = indexes  # 允许 None

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        table = plan["table_name"]
        sets  = plan.get("set_clauses", [])  # [{'column':..., 'value':...}]
        where = plan.get("where")

        meta = self.catalog.get_table(table)                  # {'columns':..., 'storage':...}
        opened = self.storage.open_table(table, meta["storage"])

        # 1) 读取所有行，做内存里的修改
        new_rows: List[Dict[str, Any]] = []
        affected = 0
        for row in self.storage.scan_rows(opened):
            if _match_where(row, where):
                for kv in sets:
                    col = kv["column"]
                    val = _parse_value(kv["value"])
                    row[col] = val
                affected += 1
            new_rows.append(row)

        # 2) 清空并重建数据文件，写回所有行
        self.storage.clear_table(opened)
        reopened = self.storage.open_table(table, meta["storage"])
        for r in new_rows:
            self.storage.insert_row(reopened, r)

        # 3) 尝试重建索引（如果工程里有 IndexRegistry）
        try:
            if self.indexes is not None:
                # 简单策略：把该表上的索引底表清空并重建
                for iname, idx_meta in (self.indexes.list_for_table(table) or {}).items():
                    idx_tbl = f"__idx__{table}__{iname}"
                    istg    = idx_meta.get("storage") or {}
                    # 清空索引底表
                    iopen = self.storage.open_table(idx_tbl, istg)
                    self.storage.clear_table(iopen)
                    iopen = self.storage.open_table(idx_tbl, istg)
                    col = idx_meta.get("column")
                    cnt = 0
                    for r in new_rows:
                        key = r.get(col)
                        self.storage.insert_row(iopen, {"k": key, "row": r})
                        cnt += 1
                    # 让缓存状态失效，下次查询会重新加载 B+ 树
                    try:
                        self.indexes.mark_unloaded(table, iname)
                    except Exception:
                        pass
        except Exception:
            # 没有索引或实现细节不同都忽略，不影响 UPDATE 主流程
            pass

        return {"ok": True, "message": f"{affected} rows affected."}
