# engine/operators/aggregate.py
from __future__ import annotations
from typing import Dict, Any, Iterable, List, Tuple
import re
from .base import Operator

def _base_col(name: str) -> str:
    return name.split(" AS ")[0].split(".")[-1]

_AGG_PATTERN = re.compile(
    r"(?i)^(COUNT|SUM|MIN|MAX|AVG)\((\*|[A-Za-z_][A-Za-z0-9_\.]*)\)(?:\s+AS\s+([A-Za-z_][A-Za-z0-9_]*))?$"
)

def _parse_agg_columns(columns: List[str]) -> Tuple[List[Tuple[str,str,str,str]], List[str]]:
    """
    解析 SELECT 列，返回：
      - aggs: [(FUNC, target, alias, default_key)], default_key 形如 "COUNT(*)"
      - passthrough: 非聚合列（一般为分组列）的基础名
    """
    aggs, passthrough = [], []
    for raw in (columns or []):
        s = raw.strip()
        m = _AGG_PATTERN.match(s)
        if m:
            func = m.group(1).upper()
            target = m.group(2)
            default_key = f"{func}({target})".upper()
            alias = m.group(3) or default_key
            aggs.append((func, target, alias, default_key))
        else:
            passthrough.append(_base_col(s))
    return aggs, passthrough

# 简单比较运算符
_OPS = {
    "=": lambda a,b: a == b,
    "==": lambda a,b: a == b,
    "!=": lambda a,b: a != b,
    "<>": lambda a,b: a != b,
    ">": lambda a,b: a is not None and b is not None and a > b,
    "<": lambda a,b: a is not None and b is not None and a < b,
    ">=": lambda a,b: a is not None and b is not None and a >= b,
    "<=": lambda a,b: a is not None and b is not None and a <= b,
}

def _to_number(v: Any):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    s = str(v)
    try:
        if "." in s:
            return float(s)
        return int(s)
    except Exception:
        return None

class HashAggregate(Operator):
    """
    哈希聚合（可跑）：
    - 支持：COUNT(*) / COUNT(col) / SUM / MIN / MAX / AVG
    - group_cols: GROUP BY 列名（可为空 => 全表一组）
    - agg_cols: SELECT 中的所有列（用于识别聚合表达式和别名）
    - having: 形如 {'column':'COUNT(*)', 'operator':'>', 'value':'0'}
    产出：
    - 每组一行：包含 group_cols 以及每个聚合列（别名存在则用别名，同时保留默认名，便于 HAVING/ORDER BY 引用）
    """
    def __init__(self, child: Operator, group_cols: List[str], agg_cols: List[str], having: Dict[str, Any] | None = None) -> None:
        self.child = child
        self.group_cols = [_base_col(c) for c in (group_cols or [])]
        parsed_aggs, passthrough = _parse_agg_columns(agg_cols or [])
        self.agg_specs = parsed_aggs                        # [(FUNC, target, alias, default_key)]
        self.pass_cols = passthrough                        # 一般是 group cols
        self.having = having

        # 若 HAVING 使用了聚合表达式，但 SELECT 中没出现，也要计算
        if having and isinstance(having.get("column"), str):
            m = _AGG_PATTERN.match(having["column"].strip())
            if m:
                func = m.group(1).upper()
                target = m.group(2)
                default_key = f"{func}({target})".upper()
                if all(default_key != s[3] for s in self.agg_specs):
                    # alias 使用默认名
                    self.agg_specs.append((func, target, default_key, default_key))

    @property
    def schema(self) -> List[Dict[str, Any]]:
        cols = [{"name": c, "type": ""} for c in self.group_cols]
        # 用用户可见的列名（别名）
        cols += [{"name": alias, "type": ""} for (_, _, alias, _) in self.agg_specs]
        return cols

    def _group_key(self, row: Dict[str, Any]):
        return tuple(row.get(c) for c in self.group_cols)

    def execute(self) -> Iterable[Dict[str, Any]]:
        groups: Dict[tuple, Dict[str, Any]] = {}
        # 初始化
        for row in self.child:
            gk = self._group_key(row)
            g = groups.get(gk)
            if g is None:
                g = {
                    "__count__": 0,                  # COUNT(*)
                    "__vals__": dict((spec[3], {"sum":0.0, "min":None, "max":None, "cnt":0}) for spec in self.agg_specs)
                }
                groups[gk] = g

            g["__count__"] += 1
            # 更新每个聚合
            for (func, target, alias, default_key) in self.agg_specs:
                slot = g["__vals__"][default_key]
                if func == "COUNT" and target == "*":
                    slot["cnt"] += 1
                    continue
                # 取值
                col = _base_col(target)
                v = row.get(col)
                if v is None:
                    continue
                if func == "COUNT":
                    slot["cnt"] += 1
                else:
                    num = _to_number(v)
                    if num is None:
                        continue
                    slot["cnt"] += 1
                    slot["sum"] += float(num)
                    slot["min"] = num if slot["min"] is None else min(slot["min"], num)
                    slot["max"] = num if slot["max"] is None else max(slot["max"], num)

        # 产出每组
        for gk, g in groups.items():
            out: Dict[str, Any] = {}
            # group by 列
            for i, c in enumerate(self.group_cols):
                out[c] = gk[i] if self.group_cols else None
            # 计算聚合值
            for (func, target, alias, default_key) in self.agg_specs:
                slot = g["__vals__"][default_key]
                val = None
                if func == "COUNT":
                    val = slot["cnt"] if target != "*" else g["__count__"]
                elif func == "SUM":
                    val = slot["sum"] if slot["cnt"] > 0 else None
                elif func == "MIN":
                    val = slot["min"]
                elif func == "MAX":
                    val = slot["max"]
                elif func == "AVG":
                    val = (slot["sum"] / slot["cnt"]) if slot["cnt"] > 0 else None
                # 同时提供默认名与别名两个键，便于 HAVING 与 ORDER BY/SELECT 引用
                out[default_key] = val
                out[alias] = val

            # HAVING 过滤（如 COUNT(*) > 0）
            if self.having:
                col = str(self.having.get("column", "")).strip()
                op = str(self.having.get("operator", "")).upper()
                val = self.having.get("value")
                if op not in _OPS:
                    raise ValueError(f"Unsupported HAVING operator: {op}")
                lv = out.get(col)
                rv = _to_number(val) if isinstance(val, str) else val
                if rv is None and isinstance(val, str):
                    # 不能转数字则按原值比较
                    rv = val
                if not _OPS[op](lv, rv):
                    continue

            yield out
