from __future__ import annotations
from typing import Any, Dict, List, Optional, Callable, Iterable
import operator as _op

from .catalog import Catalog
from .storage_iface import Storage, JsonlStorage
from .types import coerce_by_type, strip_alias, normalize_type

_OP_MAP = {
    "=": _op.eq,
    "==": _op.eq,
    "!=": _op.ne,
    "<>": _op.ne,
    ">": _op.gt,
    "<": _op.lt,
    ">=": _op.ge,
    "<=": _op.le,
}

class Executor:
    """Execute plans produced by sql.compiler.SQLCompiler (execution_plan dict)."""
    def __init__(self, storage: Storage, catalog: Catalog) -> None:
        self.storage = storage
        self.catalog = catalog

    def execute_sql_plan(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        t = (plan or {}).get("type")
        if t == "CreateTable":   return self._exec_create(plan)
        if t == "Insert":        return self._exec_insert(plan)
        if t == "Select":        return self._exec_select_like(plan)
        if t == "ExtendedSelect":return self._exec_select_like(plan)
        if t == "Delete":        return self._exec_delete(plan)
        if t == "Update":        return self._exec_update(plan)
        raise ValueError(f"Unsupported plan type: {t}")

    def _base_table(self, table_name: str) -> str:
        return (table_name or "").split(" AS ")[0]

    def _build_predicate(self, where: Optional[Dict[str, Any]], schema: List[Dict[str, Any]]) -> Callable[[Dict[str, Any]], bool]:
        if not where: return lambda r: True
        col = where.get("column"); op = where.get("operator"); val = where.get("value")
        if col is None or op is None: return lambda r: True
        pyop = _OP_MAP.get(op)
        if pyop is None: raise ValueError(f"Unsupported operator in WHERE: {op}")
        col_name = strip_alias(str(col))
        col_type = None
        for c in schema:
            if c.get("name") == col_name:
                col_type = c.get("type"); break
        rhs = coerce_by_type(val, col_type or "")
        def pred(row: Dict[str, Any]) -> bool:
            lv = row.get(col_name)
            if col_type: lv = coerce_by_type(lv, col_type)
            try: return bool(pyop(lv, rhs))
            except Exception: return False
        return pred

    def _exec_create(self, plan: Dict[str, Any]):
        table = self._base_table(plan.get("table_name"))
        columns = plan.get("columns") or []
        for c in columns:
            c["type"] = normalize_type(c.get("type", ""))
        self.catalog.create_table(table, columns, if_not_exists=True)
        self.storage.create_table(table, columns)
        return [{"ok": True}]

    def _exec_insert(self, plan: Dict[str, Any]):
        table = self._base_table(plan.get("table_name"))
        cols = plan.get("columns") or []
        values = plan.get("values") or []
        schema = self.catalog.schema(table)
        col_type = {c["name"]: c.get("type","") for c in schema}
        affected = 0
        for rowvals in values:
            row: Dict[str, Any] = {}
            for name, v in zip(cols, rowvals):
                base = strip_alias(name)
                row[base] = coerce_by_type(v, col_type.get(base, ""))
            for c in schema:
                if c["name"] not in row: row[c["name"]] = None
            self.storage.append_row(table, row)
            affected += 1
        return [{"affected": 1} for _ in range(affected)]

    def _apply_project(self, rows: Iterable[Dict[str, Any]], columns: List[str]) -> List[Dict[str, Any]]:
        if columns == ["*"]:
            return list(rows)
        cols = [strip_alias(c.split(" AS ")[0]) for c in columns]
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({c: r.get(c) for c in cols})
        return out

    def _apply_order_limit(self, rows: List[Dict[str, Any]], order_by, limit, offset):
        if order_by:
            for spec in reversed(order_by):
                col = strip_alias(spec.get("column",""))
                desc = (spec.get("direction","ASC").upper() == "DESC")
                rows.sort(key=lambda x: x.get(col), reverse=desc)
        if offset:
            rows = rows[offset:]
        if limit is not None:
            rows = rows[:limit]
        return rows

    def _exec_select_like(self, plan: Dict[str, Any]):
        table = self._base_table(plan.get("table_name"))
        schema = self.catalog.schema(table)
        where = plan.get("where")
        pred = self._build_predicate(where, schema)
        filtered = (r for r in self.storage.scan_rows(table) if pred(r))
        rows = self._apply_project(filtered, plan.get("columns") or ["*"])
        rows = list(rows)
        rows = self._apply_order_limit(rows, plan.get("order_by"), plan.get("limit"), plan.get("offset"))
        if plan.get("joins") or plan.get("group_by"):
            raise NotImplementedError("JOIN / GROUP BY / HAVING are not supported in this engine version.")
        return rows

    def _exec_delete(self, plan: Dict[str, Any]):
        table = self._base_table(plan.get("table_name"))
        schema = self.catalog.schema(table)
        where = plan.get("where")
        pred = self._build_predicate(where, schema)
        n = self.storage.delete_where(table, pred)
        return [{"affected": n}]

    def _exec_update(self, plan: Dict[str, Any]):
        table = self._base_table(plan.get("table_name"))
        schema = self.catalog.schema(table)
        where = plan.get("where")
        pred = self._build_predicate(where, schema)
        set_clauses = plan.get("set_clauses") or []
        type_map = {c["name"]: c.get("type","") for c in schema}
        def updater(row: Dict[str, Any]):
            newr = dict(row)
            for sc in set_clauses:
                col = strip_alias(sc.get("column",""))
                val = coerce_by_type(sc.get("value"), type_map.get(col,""))
                newr[col] = val
            return newr
        n = self.storage.update_where(table, pred, updater)
        return [{"affected": n}]
