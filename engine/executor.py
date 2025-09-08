
"""Bind logical plan to physical operators and execute (stub)."""
from typing import Dict, Any, List
from .operators import seq_scan, filter as filt, project, insert, delete, create_table

class ExecContext:
    def __init__(self, catalog, table_resolver):
        self.catalog = catalog
        self.table = table_resolver  # callable: name -> TableHeap

def build(plan: Dict[str, Any], ctx: ExecContext):
    op = plan["op"]
    if op == "SeqScan":
        return seq_scan.SeqScan(ctx.table(plan["table"]))
    if op == "Filter":
        return filt.Filter(build(plan["child"], ctx), plan["predicate"])
    if op == "Project":
        return project.Project(build(plan["child"], ctx), plan["columns"])
    if op == "Insert":
        return insert.Insert(ctx.table(plan["table"]), plan["values"])
    if op == "Delete":
        return delete.Delete(ctx.table(plan["table"]), plan["predicate"])
    if op == "CreateTable":
        return create_table.CreateTable(ctx.catalog, plan)
    raise ValueError(f"Unsupported op: {op}")

def run(plan: Dict[str, Any], ctx: ExecContext) -> List[dict]:
    op = build(plan, ctx)
    op.open()
    rows = []
    while True:
        r = op.next()
        if r is None: break
        rows.append(r)
    op.close()
    return rows
