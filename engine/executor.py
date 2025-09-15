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
from .operators.update import UpdateOperator

# 可选：条件删除（你已实现就会被导入，否则保持 None）
try:
    from .operators.delete import DeleteOperator  # type: ignore
except Exception:
    DeleteOperator = None  # type: ignore

# ✅ 正确导入索引注册表
try:
    from .index_registry import IndexRegistry  # type: ignore
    _HAS_INDEX = True
except Exception:
    IndexRegistry = None  # type: ignore
    _HAS_INDEX = False

from .operators.join import JoinOperator

_AGG_RE = re.compile(
    r'^(?P<func>COUNT|SUM|AVG|MIN|MAX)\('
    r'(?P<arg>\*|[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)'
    r'\)(?:\s+AS\s+(?P<alias>[A-Za-z_]\w*))?$',
    re.IGNORECASE
)

def _parse_agg_and_columns(cols: List[str]):
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
        # ✅ 这里一定要实例化 IndexRegistry；若不可用则置 None，但算子需做容错
        self.indexes = IndexRegistry(data_dir) if _HAS_INDEX else None
        self.op_update = UpdateOperator(self.catalog, self.storage, self.indexes)
        self.op_delete = DeleteOperator(self.catalog, self.storage, self.indexes) if DeleteOperator else None
        self._seq = SeqScanOperator(self.catalog, self.storage)
        self._join = JoinOperator(self.catalog, self.storage)

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
            joins = plan.get("joins") or []

            # JOIN：先联接，再过滤；无 JOIN：尝试索引扫描→顺扫
            if joins:
                rows: Iterable[dict] = self._join.execute(table, joins, self._seq)
                rows = FilterOperator(where).run(rows)
            else:
                idx_rows = None
                try:
                    idx_rows = IndexScanOperator(self.catalog, self.storage, self.indexes).try_scan(table, where)
                except Exception:
                    idx_rows = None
                if idx_rows is not None:
                    rows = idx_rows
                else:
                    rows = self._seq.scan(table)
                    rows = FilterOperator(where).run(rows)

            # 聚合 / GROUP BY / HAVING / 投影
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

            # ORDER BY
            order_by = plan.get("order_by") or []
            if order_by:
                tmp = list(rows)
                for spec in reversed(order_by):
                    col = spec.get("column")
                    desc = (spec.get("direction","ASC").upper() == "DESC")
                    tmp.sort(key=lambda r: r.get(col), reverse=desc)
                rows = tmp

            # LIMIT/OFFSET
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
            if self.op_delete is None:
                table = plan.get("table_name")
                where = plan.get("where")
                if where:
                    return {"ok": False, "error": "DELETE with WHERE is not implemented"}
                meta = self.catalog.get_table(table)
                opened = self.storage.open_table(table, meta["storage"])
                self.storage.clear_table(opened)
                return {"ok": True, "message": f"Table {table} cleared."}
            else:
                return self.op_delete.execute(plan)

        # ---------- UPDATE ----------
        if ptype == "Update":
            return self.op_update.execute(plan)

        return {"ok": False, "error": f"Unsupported plan type: {ptype}"}
