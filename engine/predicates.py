# engine/predicates.py
from __future__ import annotations
from typing import Any, Dict, List, Callable
import operator as _op
from .types import coerce_by_type

_OPS = {
    "=": _op.eq, "==": _op.eq,
    "!=": _op.ne, "<>": _op.ne,
    ">": _op.gt, "<": _op.lt,
    ">=": _op.ge, "<=": _op.le,
}

def _base_col(name: str) -> str:
    # 去掉别名与表前缀： "t.col AS c" / "t.col" / "col"
    return name.split(" AS ")[0].split(".")[-1]

def build_predicate(where: Dict[str, Any] | None,
                    schema: List[Dict[str, Any]]) -> Callable[[Dict[str, Any]], bool]:
    """把编译器的 where dict -> 可执行谓词函数"""
    if not where:
        return lambda r: True
    col = str(where.get("column"))
    op = str(where.get("operator"))
    val = where.get("value")

    if op not in _OPS:
        raise ValueError(f"Unsupported operator: {op}")

    col_name = _base_col(col)
    col_type = next((c.get("type","") for c in schema if c.get("name")==col_name), "")
    rhs = coerce_by_type(val, col_type)

    def pred(row: Dict[str, Any]) -> bool:
        lv = row.get(col_name)
        if col_type:
            lv = coerce_by_type(lv, col_type)
        try:
            return bool(_OPS[op](lv, rhs))
        except Exception:
            return False

    return pred
