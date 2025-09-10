# engine/operators/dml.py
from __future__ import annotations
from typing import Dict, Any, List, Callable
from ..storage_iface import Storage
from ..catalog import Catalog
from ..types import coerce_by_type
from ..predicates import build_predicate

class CreateTableOp:
    def __init__(self, catalog: Catalog, storage: Storage, table: str, columns: List[Dict[str, Any]]):
        self.catalog = catalog; self.storage = storage
        self.table = table; self.columns = columns

    def run(self) -> List[Dict[str, Any]]:
        # 规范化类型大写
        for c in self.columns:
            c["type"] = (c.get("type","") or "").upper()
        self.catalog.create_table(self.table, self.columns, if_not_exists=True)
        self.storage.create_table(self.table, self.columns)
        return [{"ok": True}]

class InsertOp:
    def __init__(self, catalog: Catalog, storage: Storage, table: str,
                 columns: List[str], values: List[List[Any]]):
        self.catalog=catalog; self.storage=storage
        self.table=table; self.columns=columns; self.values=values

    def run(self) -> List[Dict[str, Any]]:
        schema = self.catalog.schema(self.table)
        type_map = {c["name"]: c.get("type","") for c in schema}
        res = []
        for rowvals in self.values:
            row = {}
            for name, v in zip(self.columns, rowvals):
                base = name.split(" AS ")[0].split(".")[-1]
                row[base] = coerce_by_type(v, type_map.get(base, ""))
            # 填充缺失列为 None
            for c in schema:
                row.setdefault(c["name"], None)
            self.storage.append_row(self.table, row)
            res.append({"affected": 1})
        return res

class DeleteOp:
    def __init__(self, catalog: Catalog, storage: Storage, table: str, where: Dict[str, Any] | None):
        self.catalog=catalog; self.storage=storage; self.table=table; self.where=where

    def run(self) -> List[Dict[str, Any]]:
        schema = self.catalog.schema(self.table)
        pred = build_predicate(self.where, schema)
        n = self.storage.delete_where(self.table, pred)
        return [{"affected": n}]

class UpdateOp:
    def __init__(self, catalog: Catalog, storage: Storage, table: str,
                 set_clauses: List[Dict[str, Any]], where: Dict[str, Any] | None):
        self.catalog=catalog; self.storage=storage; self.table=table
        self.set_clauses=set_clauses; self.where=where

    def run(self) -> List[Dict[str, Any]]:
        schema = self.catalog.schema(self.table)
        type_map = {c["name"]: c.get("type","") for c in schema}
        pred = build_predicate(self.where, schema)
        def updater(row: Dict[str, Any]) -> Dict[str, Any]:
            out = dict(row)
            for sc in self.set_clauses:
                col = sc.get("column","").split(" AS ")[0].split(".")[-1]
                out[col] = coerce_by_type(sc.get("value"), type_map.get(col,""))
            return out
        n = self.storage.update_where(self.table, pred, updater)
        return [{"affected": n}]
