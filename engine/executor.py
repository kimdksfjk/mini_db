# engine/executor.py
from __future__ import annotations
from typing import Any, Dict, List
from .catalog import Catalog
from .storage_iface import Storage, JsonlStorage
from .planner.builder import PlanBuilder
from .operators.base import Operator

class Executor:
    def __init__(self, storage: Storage, catalog: Catalog) -> None:
        self.storage = storage
        self.catalog = catalog
        self.builder = PlanBuilder(storage, catalog)

    def execute_sql_plan(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        node = self.builder.build(plan)
        # 流式算子（SELECT...）→ 物化为 list[dict]
        if isinstance(node, Operator):
            return list(node)
        # DDL/DML → 调 run() 返回 [{"affected":...}] 或 [{"ok":True}]
        return node.run()
