# engine/cli/poptable_bridge.py
from __future__ import annotations
from typing import Any, Dict, List, Sequence, Iterable, Optional, Union
from . import poptable  # 只用它公开的两个函数

_Row = Union[Dict[str, Any], Sequence[Any]]

# 最近一次查询结果（标准 {columns, rows} 结构）
_LAST: Optional[Dict[str, Any]] = None

def _rows_to_table(rows: Iterable[_Row], columns: Optional[List[str]] = None) -> Dict[str, Any]:
    rs = list(rows)
    if not rs:
        return {"columns": columns or [], "rows": []}
    # 字典行：自动列并集（按字母序）
    if isinstance(rs[0], dict):
        cols = columns or sorted({k for r in rs for k in r.keys()})
        norm = [[r.get(c, None) for c in cols] for r in rs]
        return {"columns": cols, "rows": norm}
    # 序列行：需要 columns 或自动命名 col1..n
    if isinstance(rs[0], Sequence):
        cols = columns or [f"col{i+1}" for i in range(len(rs[0]))]
        return {"columns": cols, "rows": [list(r) for r in rs]}
    return {"columns": columns or [], "rows": []}

def set_last_result(result_or_rows: Union[Dict[str, Any], Iterable[_Row]],
                    columns: Optional[List[str]] = None) -> None:
    """保存最近一次查询结果；result_or_rows 可为 {columns, rows} 或 行序列。"""
    global _LAST
    if isinstance(result_or_rows, dict) and "columns" in result_or_rows and "rows" in result_or_rows:
        _LAST = {"columns": list(result_or_rows["columns"]),
                 "rows": [list(r) if isinstance(r, Sequence) else r for r in result_or_rows["rows"]]}
    else:
        _LAST = _rows_to_table(result_or_rows, columns)

def show_last_popup(title: str = "查询结果") -> None:
    """弹出最近一次结果。"""
    if not _LAST:
        print("[popup] 暂无可展示的结果（先执行一次查询）")
        return
    poptable.show_table_popup(_LAST, title=title)

def export_last_to_excel(file_path: Optional[str] = None,
                         directory: Optional[str] = None) -> Optional[str]:
    """将最近一次结果导出（xlsx/缺 openpyxl 回退 csv）。"""
    if not _LAST:
        print("[popup] 暂无可导出的结果（先执行一次查询）")
        return None
    return poptable.export_table_to_excel(_LAST, file_path=file_path, directory=directory)

def show_rows_popup(rows: Iterable[_Row], columns: Optional[List[str]] = None,
                    title: str = "查询结果") -> None:
    """直接用行序列弹窗（内部自动标准化）。"""
    table = _rows_to_table(rows, columns)
    set_last_result(table)
    poptable.show_table_popup(table, title=title)

def export_rows_to_excel(rows: Iterable[_Row], columns: Optional[List[str]] = None,
                         file_path: Optional[str] = None,
                         directory: Optional[str] = None) -> str:
    """直接把行序列导出为 Excel/CSV。"""
    table = _rows_to_table(rows, columns)
    set_last_result(table)
    return poptable.export_table_to_excel(table, file_path=file_path, directory=directory)
