# engine/cli/poptable_bridge.py
from __future__ import annotations
from typing import Any, Dict, List, Sequence, Iterable, Optional, Union
import json, sys, subprocess

_Row = Union[Dict[str, Any], Sequence[Any]]

# 最近一次查询结果（标准 {columns, rows} 结构）
_LAST: Optional[Dict[str, Any]] = None

def _rows_to_table(rows: Iterable[_Row], columns: Optional[List[str]] = None) -> Dict[str, Any]:
    rs = list(rows)
    if not rs:
        return {"columns": columns or [], "rows": []}
    if isinstance(rs[0], dict):
        cols = columns or sorted({k for r in rs for k in r.keys()})
        norm = [[r.get(c, None) for c in cols] for r in rs]
        return {"columns": cols, "rows": norm}
    if isinstance(rs[0], Sequence):
        cols = columns or [f"col{i+1}" for i in range(len(rs[0]))]
        return {"columns": cols, "rows": [list(r) for r in rs]}
    return {"columns": columns or [], "rows": []}

def set_last_result(result_or_rows: Union[Dict[str, Any], Iterable[_Row]],
                    columns: Optional[List[str]] = None) -> None:
    """保存最近一次查询结果；result_or_rows 可为 {columns, rows} 或 行序列。"""
    global _LAST
    if isinstance(result_or_rows, dict) and "columns" in result_or_rows and "rows" in result_or_rows:
        _LAST = {
            "columns": list(result_or_rows["columns"]),
            "rows": [list(r) if isinstance(r, Sequence) else r for r in result_or_rows["rows"]],
        }
    else:
        _LAST = _rows_to_table(result_or_rows, columns)

def show_last_popup(title: str = "查询结果") -> None:
    """用 子进程 弹出最近一次结果，不阻塞 CLI。"""
    if not _LAST:
        print("[popup] 暂无可展示的结果（先执行一次查询）")
        return
    payload = json.dumps(_LAST, ensure_ascii=False).encode("utf-8")
    try:
        # 以模块方式启动子进程：engine.cli.poptable_child
        cmd = [sys.executable, "-m", "engine.cli.poptable_child", title]
        # 不等待子进程结束；把数据写到它的 stdin
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        try:
            assert p.stdin is not None
            p.stdin.write(payload)
            p.stdin.close()
        except Exception:
            pass
    except Exception as e:
        # 兜底：若子进程失败，主进程内直接弹窗（会阻塞）
        print(f"[popup] 子进程启动失败，改为阻塞模式：{e}")
        from . import poptable  # type: ignore
        poptable.show_table_popup(_LAST, title=title)

def export_last_to_excel(file_path: Optional[str] = None,
                         directory: Optional[str] = None) -> Optional[str]:
    """将最近一次结果导出（xlsx/缺 openpyxl 回退 csv）。"""
    if not _LAST:
        print("[popup] 暂无可导出的结果（先执行一次查询）")
        return None
    from . import poptable  # type: ignore
    return poptable.export_table_to_excel(_LAST, file_path=file_path, directory=directory)

def show_rows_popup(rows: Iterable[_Row], columns: Optional[List[str]] = None,
                    title: str = "查询结果") -> None:
    """直接用行序列弹窗（内部自动标准化，子进程显示）。"""
    table = _rows_to_table(rows, columns)
    set_last_result(table)
    show_last_popup(title)

def export_rows_to_excel(rows: Iterable[_Row], columns: Optional[List[str]] = None,
                         file_path: Optional[str] = None,
                         directory: Optional[str] = None) -> str:
    """直接把行序列导出为 Excel/CSV。"""
    table = _rows_to_table(rows, columns)
    set_last_result(table)
    from . import poptable  # type: ignore
    return poptable.export_table_to_excel(table, file_path=file_path, directory=directory)
