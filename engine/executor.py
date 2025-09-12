# engine/executor.py
from __future__ import annotations
from typing import Dict, Any, List, Iterable
import re

from .catalog import Catalog
from .storage_adapter import StorageAdapter

from .operators.create_table import CreateTableOperator
from .operators.insert import InsertOperator
from .operators.seq_scan import SeqScanOperator
from .operators.filter import FilterOperator
from .operators.project import ProjectOperator
from .operators.aggregate import AggregateOperator
from .operators.create_index import CreateIndexOperator
from .operators.index_scan import IndexScanOperator
from .index_registry import IndexRegistry
from .operators.update import UpdateOperator
from .operators.delete import DeleteOperator


_AGG_RE = re.compile(
    r'^(?P<func>COUNT|SUM|AVG|MIN|MAX)\('
    r'(?P<arg>\*|[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)'
    r'\)(?:\s+AS\s+(?P<alias>[A-Za-z_]\w*))?$',
    re.IGNORECASE
)

def _parse_agg_and_columns(cols: List[str]):
    """从编译器给的 columns 列表中识别聚合项。返回 (final_columns, aggregates)"""
    final_cols: List[str] = []
    aggs: List[Dict[str, Any]] = []
    for raw in cols or []:
        s = raw.strip()
        m = _AGG_RE.match(s)
        if m:
            func = m.group('func').upper()
            arg = m.group('arg')
            alias = m.group('alias')
            if not alias:
                alias = func.lower() if arg == '*' else f"{func.lower()}_{arg.split('.')[-1]}"
            aggs.append({"func": func, "column": arg, "as": alias})
            final_cols.append(alias)
        else:
            parts = s.split(" AS ")
            if len(parts) == 2:
                final_cols.append(parts[1].strip())
            else:
                final_cols.append(s)
    return final_cols, aggs

def _rewrite_having(having, aggs):
    """把 HAVING 中的 COUNT(*) 等表达式映射成聚合别名，便于 FilterOperator 使用。"""
    if not having: return None
    col = str(having.get("column", "")).strip()
    if not col: return None
    m = _AGG_RE.match(col)
    if not m: return having
    func = m.group('func').upper()
    arg = m.group('arg')
    alias = None
    for a in aggs:
        if a["func"] == func and a.get("column") == arg:
            alias = a["as"]; break
    if not alias:
        alias = func.lower() if arg == "*" else f"{func.lower()}_{arg.split('.')[-1]}"
    new_h = dict(having)
    new_h["column"] = alias
    return new_h

class Executor:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self.catalog = Catalog(data_dir)
        self.storage = StorageAdapter(data_dir)
        self.indexes = IndexRegistry(data_dir)
        self.op_update = UpdateOperator(self.catalog, self.storage, self.indexes)
        self.op_delete = DeleteOperator(self.catalog, self.storage, self.indexes)

    def execute_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        ptype = plan.get("type")

        # ---------- DDL ----------
        if ptype == "CreateTable":
            op = CreateTableOperator(self.catalog, self.storage, self.data_dir)
            return op.execute(plan)

        if ptype == "CreateIndex":
            op = CreateIndexOperator(self.catalog, self.storage, self.indexes)
            return op.execute(plan)

        # ---------- INSERT ----------
        if ptype == "Insert":
            op = InsertOperator(self.catalog, self.storage, self.indexes)
            return op.execute(plan)

        # ---------- SELECT / ExtendedSelect ----------
        if ptype in ("Select", "ExtendedSelect"):
            table = plan.get("table_name")
            if not table:
                return {"ok": False, "error": "no table specified"}

            where = plan.get("where") or plan.get("where_condition")
            # 0) 索引尝试
            idx_rows = IndexScanOperator(self.catalog, self.storage, self.indexes).try_scan(table, where)
            if idx_rows is not None:
                rows: Iterable[dict] = idx_rows
            else:
                seq = SeqScanOperator(self.catalog, self.storage)
                rows = seq.scan(table)
                rows = FilterOperator(where).run(rows)

            # 1) 聚合：从 columns 推导 aggregates；group_by 兼容 dict 或 list
            raw_cols: List[str] = plan.get("columns") or ["*"]
            final_cols, aggregates = _parse_agg_and_columns(raw_cols)
            gb = plan.get("group_by")
            having = None
            if isinstance(gb, dict):
                group_by = gb.get("columns") or []
                having = gb.get("having")
            else:
                group_by = gb or []
            if group_by or aggregates:
                agg_op = AggregateOperator(group_by, aggregates)
                rows = agg_op.run(rows)
                hv = _rewrite_having(having, aggregates)
                if hv:
                    rows = list(FilterOperator(hv).run(rows))
                if final_cols and final_cols != ["*"]:
                    rows = list(ProjectOperator(final_cols).run(rows))
            else:
                rows = ProjectOperator(raw_cols).run(rows)

            # 2) ORDER BY
            order_by = plan.get("order_by") or []
            if order_by:
                tmp = list(rows)
                for spec in reversed(order_by):
                    col = spec.get("column")
                    desc = (spec.get("direction","ASC").upper() == "DESC")
                    tmp.sort(key=lambda r: r.get(col), reverse=desc)
                rows = tmp

            # 3) LIMIT/OFFSET
            limit = plan.get("limit")
            offset = plan.get("offset", 0)
            out: List[dict] = []
            skipped = 0
            for r in rows:
                if offset and skipped < offset:
                    skipped += 1; continue
                out.append(r)
                if isinstance(limit, int) and limit >= 0 and len(out) >= limit:
                    break
            if not out and not isinstance(rows, list):
                out = list(rows)
            return {"ok": True, "rows": out}

        # ---------- DELETE ----------
        if ptype == "Delete":
            return self.op_delete.execute(plan)

        # ---------- UPDATE ----------
        if ptype == "Update":
            # 调用已实现的 UpdateOperator
            return self.op_update.execute(plan)

        return {"ok": False, "error": f"Unsupported plan type: {ptype}"}
