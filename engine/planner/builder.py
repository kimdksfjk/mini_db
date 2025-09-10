# engine/planner/builder.py
from __future__ import annotations
from typing import Any, Dict, List

from ..catalog import Catalog
from ..storage_iface import Storage
from ..predicates import build_predicate

from ..operators.base import Operator
from ..operators.scan import SeqScan
from ..operators.filter import Filter
from ..operators.project import Project
from ..operators.sort_limit import OrderBy, Limit

from ..operators.dml import CreateTableOp, InsertOp, DeleteOp, UpdateOp


class PlanBuilder:
    def __init__(self, storage: Storage, catalog: Catalog) -> None:
        self.storage = storage
        self.catalog = catalog

    def _base_table(self, name: str) -> str:
        # 去掉 " AS alias"
        return (name or "").split(" AS ")[0]

    def build(self, plan: Dict[str, Any]) -> Operator | Any:
        t = (plan or {}).get("type")

        if t in ("Select", "ExtendedSelect"):
            table = self._base_table(plan.get("table_name"))
            schema = self.catalog.schema(table)

            # 1) 顺序扫描
            node: Operator = SeqScan(table, schema, self.storage)

            # 2) WHERE 过滤
            pred = build_predicate(plan.get("where"), schema)
            node = Filter(node, pred)

            # 3) 排序 / 截取（必须在投影之前，避免投影把排序键丢掉）
            if t == "ExtendedSelect":
                if plan.get("order_by"):
                    node = OrderBy(node, plan.get("order_by"))
                if plan.get("limit") is not None or plan.get("offset") is not None:
                    node = Limit(node, plan.get("limit"), plan.get("offset"))

                # 目前不实现 JOIN / GROUP BY / HAVING
                if plan.get("joins") or plan.get("group_by"):
                    raise NotImplementedError("JOIN / GROUP BY / HAVING not implemented")

            # 4) 投影（最后一步：只输出需要的列）
            cols = plan.get("columns") or ["*"]
            node = Project(node, cols)
            return node

        elif t == "CreateTable":
            return CreateTableOp(
                self.catalog,
                self.storage,
                self._base_table(plan.get("table_name")),
                plan.get("columns") or [],
            )

        elif t == "Insert":
            return InsertOp(
                self.catalog,
                self.storage,
                self._base_table(plan.get("table_name")),
                plan.get("columns") or [],
                plan.get("values") or [],
            )

        elif t == "Delete":
            return DeleteOp(
                self.catalog,
                self.storage,
                self._base_table(plan.get("table_name")),
                plan.get("where"),
            )

        elif t == "Update":
            return UpdateOp(
                self.catalog,
                self.storage,
                self._base_table(plan.get("table_name")),
                plan.get("set_clauses") or [],
                plan.get("where"),
            )

        else:
            raise ValueError(f"Unsupported plan type: {t}")
