from __future__ import annotations
from typing import Any, Dict, List, Optional

from .plan import Plan, CreateTable as PCreateTable, InsertValues as PInsertValues, SeqScan as PSeqScan, Filter as PFilter, Project as PProject, Delete as PDelete, plan_from_dict
from .operator import CreateTable as OCreateTable, InsertValues as OInsertValues, SeqScan as OSeqScan, Filter as OFilter, Project as OProject, Delete as ODelete
from .storage_iface import Storage
from .catalog import Catalog

class Executor:
    """Plan executor that builds operator trees and runs them."""
    def __init__(self, storage: Storage, catalog: Catalog) -> None:
        self.storage = storage
        self.catalog = catalog

    def build(self, plan: Plan):
        if isinstance(plan, PCreateTable):
            return OCreateTable(self.catalog, self.storage, plan.table, plan.columns, plan.if_not_exists)
        if isinstance(plan, PInsertValues):
            return OInsertValues(self.storage, plan.table, plan.columns, plan.values)
        if isinstance(plan, PSeqScan):
            return OSeqScan(self.storage, plan.table)
        if isinstance(plan, PFilter):
            return OFilter(self.build(plan.input), plan.predicate)
        if isinstance(plan, PProject):
            return OProject(self.build(plan.input), plan.cols)
        if isinstance(plan, PDelete):
            return ODelete(self.storage, plan.table, plan.predicate)
        raise ValueError(f"Unsupported plan node: {type(plan).__name__}")

    def execute(self, plan: Plan | Dict[str, Any]):
        if isinstance(plan, dict):
            plan = plan_from_dict(plan)
        op = self.build(plan)
        op.open()
        out: List[Any] = []
        while True:
            row = op.next()
            if row is None:
                break
            out.append(row)
        op.close()
        return out
