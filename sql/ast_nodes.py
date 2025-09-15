#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AST 节点定义
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass


class ASTNode:
    pass


@dataclass
class CreateTableNode(ASTNode):
    table_name: str
    columns: List[Dict[str, Any]]


@dataclass
class InsertNode(ASTNode):
    table_name: str
    columns: List[str]
    values: List[List[str]]


@dataclass
class SelectNode(ASTNode):
    columns: List[str]
    table_name: str
    where_condition: Optional[Dict[str, Any]] = None


@dataclass
class DeleteNode(ASTNode):
    table_name: str
    where_condition: Optional[Dict[str, Any]] = None


@dataclass
class UpdateNode(ASTNode):
    table_name: str
    set_clauses: List[Dict[str, str]]
    where_condition: Optional[Dict[str, Any]] = None


@dataclass
class JoinNode(ASTNode):
    left_table: str
    right_table: str
    join_type: str
    on_condition: Dict[str, Any]


@dataclass
class OrderByNode(ASTNode):
    columns: List[Dict[str, str]]


@dataclass
class GroupByNode(ASTNode):
    columns: List[str]
    having_condition: Optional[Dict[str, Any]] = None


@dataclass
class ExtendedSelectNode(ASTNode):
    columns: List[str]
    table_name: str
    joins: List[JoinNode] = None
    where_condition: Optional[Dict[str, Any]] = None
    group_by: Optional[GroupByNode] = None
    order_by: Optional[OrderByNode] = None
    limit: Optional[int] = None
    offset: Optional[int] = None


