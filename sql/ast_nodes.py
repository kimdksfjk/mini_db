#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AST 节点定义
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass

#撰写共同父类
class ASTNode:
    pass

#CT的AST
@dataclass
class CreateTableNode(ASTNode):
    table_name: str
    columns: List[Dict[str, Any]]

#插入insert的AST
@dataclass
class InsertNode(ASTNode):
    table_name: str
    columns: List[str]
    values: List[List[str]]

#基础的select的AST
@dataclass
class SelectNode(ASTNode):
    columns: List[str]
    table_name: str
    where_condition: Optional[Dict[str, Any]] = None

#delete的AST
@dataclass
class DeleteNode(ASTNode):
    table_name: str
    #可选where
    where_condition: Optional[Dict[str, Any]] = None

#update的AST
@dataclass
class UpdateNode(ASTNode):
    table_name: str
    set_clauses: List[Dict[str, str]]
    where_condition: Optional[Dict[str, Any]] = None

#子句节点之join
@dataclass
class JoinNode(ASTNode):
    left_table: str
    right_table: str
    #join类型
    join_type: str
    on_condition: Dict[str, Any]

# order节点AST
@dataclass
class OrderByNode(ASTNode):
    columns: List[Dict[str, str]]

# group节点
@dataclass
class GroupByNode(ASTNode):
    columns: List[str]
    having_condition: Optional[Dict[str, Any]] = None

# 拓展版select节点的AST
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


