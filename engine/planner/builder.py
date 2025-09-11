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
from ..operators.join import NestedLoopJoin
from ..operators.aggregate import HashAggregate

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
            # 1) FROM 主表
            table = self._base_table(plan.get("table_name"))
            schema = self.catalog.schema(table)
            node: Operator = SeqScan(table, schema, self.storage)

            # 2) JOIN 串起来（顺序执行）
            for j in (plan.get("joins") or []):
                rtab = self._base_table(j.get("right_table"))
                rschema = self.catalog.schema(rtab)
                rnode: Operator = SeqScan(rtab, rschema, self.storage)
                node = NestedLoopJoin(node, rnode, j.get("type", "INNER"), j.get("on_condition"))

            # 3) WHERE 过滤（在 JOIN 之后，语义正确；需要可再做谓词下推优化）
            #    schema 取当前 node.schema
            pred = build_predicate(plan.get("where"), node.schema)
            node = Filter(node, pred)

            # 4) GROUP BY / HAVING
            if plan.get("group_by"):
                gb = plan["group_by"]
                node = HashAggregate(
                    node,
                    group_cols=gb.get("columns") or [],
                    agg_cols=plan.get("columns") or [],
                    having=gb.get("having"),
                )

            # 5) ORDER BY / LIMIT（在投影之前，避免排序键被投影丢掉）
            if plan.get("order_by"):
                node = OrderBy(node, plan.get("order_by"))
            if plan.get("limit") is not None or plan.get("offset") is not None:
                node = Limit(node, plan.get("limit"), plan.get("offset"))

            # 6) 投影（最后一步）：列名使用原始列串，便于 CLI 正确显示（包含 "AS" 时仍能取值）
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
