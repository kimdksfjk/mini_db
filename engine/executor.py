
from __future__ import annotations
from typing import Dict, Any, List, Iterable
from .catalog import Catalog
from .storage_adapter import StorageAdapter
from .operators.create_table import CreateTableOperator
from .operators.insert import InsertOperator
from .operators.seq_scan import SeqScanOperator
from .operators.filter import FilterOperator
from .operators.project import ProjectOperator

class Executor:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self.catalog = Catalog(data_dir)
        self.storage = StorageAdapter(data_dir)

    # Executes an execution plan dict from the user's SQLCompiler.
    def execute_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        ptype = plan.get("type")
        if ptype == "CreateTable":
            op = CreateTableOperator(self.catalog, self.storage, self.data_dir)
            return op.execute(plan)
        if ptype == "Insert":
            op = InsertOperator(self.catalog, self.storage)
            return op.execute(plan)
        if ptype in ("Select", "ExtendedSelect"):
            table = plan.get("table_name")
            if not table:
                return {"ok": False, "error": "no table specified"}
            # Basic support: WHERE, ORDER BY, LIMIT/OFFSET (in-memory)
            seq = SeqScanOperator(self.catalog, self.storage)
            rows: Iterable[dict] = seq.scan(table)
            where = plan.get("where") or plan.get("where_condition")
            rows = FilterOperator(where).run(rows)
            columns: List[str] = plan.get("columns") or ["*"]
            rows = ProjectOperator(columns).run(rows)
            # ORDER BY
            order_by = plan.get("order_by") or []
            if order_by:
                tmp = list(rows)
                for spec in reversed(order_by):  # apply last key first
                    col = spec.get("column")
                    desc = (spec.get("direction", "ASC").upper() == "DESC")
                    tmp.sort(key=lambda r: r.get(col), reverse=desc)
                rows = tmp
            # LIMIT/OFFSET
            limit = plan.get("limit")
            offset = plan.get("offset", 0)
            out = []
            skipped = 0
            for r in rows:
                if offset and skipped < offset:
                    skipped += 1
                    continue
                out.append(r)
                if isinstance(limit, int) and limit >= 0 and len(out) >= limit:
                    break
            # If rows is still a generator and no limit, consume to list for consistent return
            if not out and not isinstance(rows, list):
                out = list(rows)
            return {"ok": True, "rows": out}
        if ptype == "Delete":
            # For simplicity: full table clear if no WHERE; WHERE-specific delete not implemented
            table = plan.get("table_name")
            where = plan.get("where")
            if where:
                return {"ok": False, "error": "DELETE with WHERE is not implemented in this demo"}
            meta = self.catalog.get_table(table)
            opened = self.storage.open_table(table, meta["storage"])
            self.storage.clear_table(opened)
            return {"ok": True, "message": f"Table {table} cleared."}
        if ptype == "Update":
            return {"ok": False, "error": "UPDATE is not implemented"}
        return {"ok": False, "error": f"Unsupported plan type: {ptype}"}
