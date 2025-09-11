#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL编译器实现
支持CREATE TABLE、INSERT、SELECT、DELETE语句
"""

import re
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum
import json


class TokenType(Enum):
    """Token类型枚举"""
    KEYWORD = "KEYWORD"
    IDENTIFIER = "IDENTIFIER"
    CONSTANT = "CONSTANT"
    OPERATOR = "OPERATOR"
    DELIMITER = "DELIMITER"
    EOF = "EOF"


@dataclass
class Token:
    """Token数据结构"""
    type: TokenType
    value: str
    line: int
    column: int


class LexicalAnalyzer:
    """词法分析器"""

    def __init__(self):
        # SQL关键字
        self.keywords = {
            'SELECT', 'FROM', 'WHERE', 'CREATE', 'TABLE', 'INSERT', 'INTO',
            'VALUES', 'DELETE', 'UPDATE', 'SET', 'INT', 'VARCHAR', 'CHAR',
            'FLOAT', 'DOUBLE', 'AND', 'OR', 'NOT', 'NULL', 'PRIMARY', 'KEY',
            'UNIQUE', 'ORDER', 'BY', 'GROUP', 'HAVING', 'ASC', 'DESC',
            'INNER', 'LEFT', 'RIGHT', 'OUTER', 'JOIN', 'ON', 'AS', 'COUNT',
            'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT', 'LIMIT', 'OFFSET'
        }

        # 运算符
        self.operators = {
            '=', '>', '<', '>=', '<=', '!=', '<>', '+', '-', '*', '/',
            'AND', 'OR', 'NOT'
        }

        # 分隔符
        self.delimiters = {
            '(', ')', ',', ';', "'", '"', '.'
        }

        # 正则表达式模式
        self.patterns = [
            ('STRING', r"'([^']*)'|\"([^\"]*)\""),  # 字符串常量
            ('NUMBER', r'\d+(\.\d+)?'),  # 数字常量
            ('IDENTIFIER', r'[a-zA-Z_][a-zA-Z0-9_]*'),  # 标识符
            ('OPERATOR', r'[=<>!]+|[+\-*/]'),  # 运算符
            ('DELIMITER', r'[(),;.]'),  # 分隔符
            ('WHITESPACE', r'\s+'),  # 空白字符
        ]

        # 编译正则表达式
        self.compiled_patterns = [(name, re.compile(pattern))
                                  for name, pattern in self.patterns]

    def tokenize(self, sql_text: str) -> List[Token]:
        """将SQL文本转换为Token流"""
        tokens = []
        lines = sql_text.split('\n')

        for line_num, line in enumerate(lines, 1):
            column = 1
            pos = 0

            while pos < len(line):
                matched = False

                # 尝试匹配各种模式
                for pattern_name, pattern in self.compiled_patterns:
                    match = pattern.match(line, pos)
                    if match:
                        if pattern_name == 'WHITESPACE':
                            # 跳过空白字符
                            pos = match.end()
                            column += match.end() - match.start()
                            matched = True
                            break
                        elif pattern_name == 'STRING':
                            # 字符串常量
                            value = match.group(1) or match.group(2)
                            token_type = TokenType.CONSTANT
                            tokens.append(Token(token_type, value, line_num, column))
                        elif pattern_name == 'NUMBER':
                            # 数字常量
                            value = match.group(0)
                            token_type = TokenType.CONSTANT
                            tokens.append(Token(token_type, value, line_num, column))
                        elif pattern_name == 'IDENTIFIER':
                            # 标识符或关键字
                            value = match.group(0).upper()
                            if value in self.keywords:
                                token_type = TokenType.KEYWORD
                            else:
                                token_type = TokenType.IDENTIFIER
                                value = match.group(0)  # 保持原始大小写
                            tokens.append(Token(token_type, value, line_num, column))
                        elif pattern_name == 'OPERATOR':
                            # 运算符
                            value = match.group(0)
                            token_type = TokenType.OPERATOR
                            tokens.append(Token(token_type, value, line_num, column))
                        elif pattern_name == 'DELIMITER':
                            # 分隔符
                            value = match.group(0)
                            token_type = TokenType.DELIMITER
                            tokens.append(Token(token_type, value, line_num, column))

                        pos = match.end()
                        column += match.end() - match.start()
                        matched = True
                        break

                if not matched:
                    # 未识别的字符
                    char = line[pos]
                    raise SyntaxError(f"第{line_num}行第{column}列：未识别的字符 '{char}'")

        # 添加EOF token
        tokens.append(Token(TokenType.EOF, "EOF", len(lines), 1))
        return tokens


class ASTNode:
    """抽象语法树节点基类"""
    pass


@dataclass
class CreateTableNode(ASTNode):
    """CREATE TABLE语句节点"""
    table_name: str
    columns: List[Dict[str, Any]]


@dataclass
class InsertNode(ASTNode):
    """INSERT语句节点"""
    table_name: str
    columns: List[str]
    values: List[List[str]]


@dataclass
class SelectNode(ASTNode):
    """SELECT语句节点"""
    columns: List[str]
    table_name: str
    where_condition: Optional[Dict[str, Any]] = None


@dataclass
class DeleteNode(ASTNode):
    """DELETE语句节点"""
    table_name: str
    where_condition: Optional[Dict[str, Any]] = None


@dataclass
class UpdateNode(ASTNode):
    """UPDATE语句节点"""
    table_name: str
    set_clauses: List[Dict[str, str]]  # [{'column': 'name', 'value': 'Alice'}]
    where_condition: Optional[Dict[str, Any]] = None


@dataclass
class JoinNode(ASTNode):
    """JOIN节点"""
    left_table: str
    right_table: str
    join_type: str  # 'INNER', 'LEFT', 'RIGHT', 'OUTER'
    on_condition: Dict[str, Any]


@dataclass
class OrderByNode(ASTNode):
    """ORDER BY节点"""
    columns: List[Dict[str, str]]  # [{'column': 'name', 'direction': 'ASC'}]


@dataclass
class GroupByNode(ASTNode):
    """GROUP BY节点"""
    columns: List[str]
    having_condition: Optional[Dict[str, Any]] = None


@dataclass
class ExtendedSelectNode(ASTNode):
    """扩展的SELECT语句节点"""
    columns: List[str]
    table_name: str
    joins: List[JoinNode] = None
    where_condition: Optional[Dict[str, Any]] = None
    group_by: Optional[GroupByNode] = None
    order_by: Optional[OrderByNode] = None
    limit: Optional[int] = None
    offset: Optional[int] = None


class SyntaxAnalyzer:
    """语法分析器"""

    def __init__(self):
        self.tokens = []
        self.current_token_index = 0

    def parse(self, tokens: List[Token]) -> ASTNode:
        """解析Token流，构建AST"""
        self.tokens = tokens
        self.current_token_index = 0

        if not tokens:
            raise SyntaxError("空的Token流")

        return self.parse_statement()

    def current_token(self) -> Token:
        """获取当前Token"""
        if self.current_token_index < len(self.tokens):
            return self.tokens[self.current_token_index]
        return Token(TokenType.EOF, "EOF", 0, 0)

    def next_token(self) -> Token:
        """移动到下一个Token"""
        if self.current_token_index < len(self.tokens) - 1:
            self.current_token_index += 1
        return self.current_token()

    def expect_token(self, expected_type: TokenType, expected_value: str = None) -> Token:
        """期望特定类型的Token"""
        token = self.current_token()
        if token.type != expected_type:
            raise SyntaxError(f"第{token.line}行第{token.column}列：期望{expected_type.value}，但得到{token.type.value}")

        if expected_value and token.value.upper() != expected_value.upper():
            raise SyntaxError(f"第{token.line}行第{token.column}列：期望'{expected_value}'，但得到'{token.value}'")

        self.next_token()
        return token

    def parse_statement(self) -> ASTNode:
        """解析语句"""
        token = self.current_token()

        if token.type == TokenType.KEYWORD:
            if token.value == 'CREATE':
                return self.parse_create_table()
            elif token.value == 'INSERT':
                return self.parse_insert()
            elif token.value == 'SELECT':
                return self.parse_extended_select()
            elif token.value == 'DELETE':
                return self.parse_delete()
            elif token.value == 'UPDATE':
                return self.parse_update()

        raise SyntaxError(f"第{token.line}行第{token.column}列：不支持的语句类型")

    def parse_create_table(self) -> CreateTableNode:
        """解析CREATE TABLE语句"""
        # CREATE TABLE table_name (column_definitions)
        self.expect_token(TokenType.KEYWORD, 'CREATE')
        self.expect_token(TokenType.KEYWORD, 'TABLE')

        table_name_token = self.expect_token(TokenType.IDENTIFIER)
        table_name = table_name_token.value

        self.expect_token(TokenType.DELIMITER, '(')

        columns = []
        while True:
            column_name_token = self.expect_token(TokenType.IDENTIFIER)
            column_type_token = self.expect_token(TokenType.KEYWORD)

            columns.append({
                'name': column_name_token.value,
                'type': column_type_token.value
            })

            token = self.current_token()
            if token.type == TokenType.DELIMITER and token.value == ')':
                break
            elif token.type == TokenType.DELIMITER and token.value == ',':
                self.next_token()
            else:
                raise SyntaxError(f"第{token.line}行第{token.column}列：期望')'或','")

        self.expect_token(TokenType.DELIMITER, ')')
        self.expect_token(TokenType.DELIMITER, ';')

        return CreateTableNode(table_name, columns)

    def parse_insert(self) -> InsertNode:
        """解析INSERT语句"""
        # INSERT INTO table_name (columns) VALUES (values)
        self.expect_token(TokenType.KEYWORD, 'INSERT')
        self.expect_token(TokenType.KEYWORD, 'INTO')

        table_name_token = self.expect_token(TokenType.IDENTIFIER)
        table_name = table_name_token.value

        self.expect_token(TokenType.DELIMITER, '(')

        columns = []
        while True:
            # 支持表前缀：alias.column 或 直接 column
            first = self.expect_token(TokenType.IDENTIFIER)
            col_name = first.value
            dot = self.current_token()
            if dot.type == TokenType.DELIMITER and dot.value == '.':
                self.next_token()
                second = self.expect_token(TokenType.IDENTIFIER)
                col_name = f"{col_name}.{second.value}"
            columns.append(col_name)

            token = self.current_token()
            if token.type == TokenType.DELIMITER and token.value == ')':
                break
            elif token.type == TokenType.DELIMITER and token.value == ',':
                self.next_token()
            else:
                raise SyntaxError(f"第{token.line}行第{token.column}列：期望')'或','")

        self.expect_token(TokenType.DELIMITER, ')')
        self.expect_token(TokenType.KEYWORD, 'VALUES')

        # 支持 VALUES (...),(...),...
        all_values = []
        while True:
            self.expect_token(TokenType.DELIMITER, '(')
            row_values = []
            while True:
                value_token = self.current_token()
                if value_token.type == TokenType.CONSTANT:
                    row_values.append(value_token.value)
                    self.next_token()
                else:
                    raise SyntaxError(f"第{value_token.line}行第{value_token.column}列：期望常量值")

                token = self.current_token()
                if token.type == TokenType.DELIMITER and token.value == ')':
                    break
                elif token.type == TokenType.DELIMITER and token.value == ',':
                    self.next_token()
                else:
                    raise SyntaxError(f"第{token.line}行第{token.column}列：期望')'或','")

            self.expect_token(TokenType.DELIMITER, ')')
            all_values.append(row_values)

            token = self.current_token()
            if token.type == TokenType.DELIMITER and token.value == ',':
                self.next_token()
                continue
            elif token.type == TokenType.DELIMITER and token.value == ';':
                break
            else:
                raise SyntaxError(f"第{token.line}行第{token.column}列：期望','或';'")

        self.expect_token(TokenType.DELIMITER, ';')

        return InsertNode(table_name, columns, all_values)

    def parse_select(self) -> SelectNode:
        """解析SELECT语句"""
        # SELECT columns FROM table_name [WHERE condition]
        self.expect_token(TokenType.KEYWORD, 'SELECT')

        columns = []
        token = self.current_token()

        # 检查是否是 SELECT *
        if token.type == TokenType.OPERATOR and token.value == '*':
            columns = ['*']  # 表示选择所有列
            self.next_token()
        else:
            # 解析列名列表
            while True:
                column_token = self.expect_token(TokenType.IDENTIFIER)
                columns.append(column_token.value)

                token = self.current_token()
                if token.type == TokenType.KEYWORD and token.value == 'FROM':
                    break
                elif token.type == TokenType.DELIMITER and token.value == ',':
                    self.next_token()
                else:
                    raise SyntaxError(f"第{token.line}行第{token.column}列：期望'FROM'或','")

        self.expect_token(TokenType.KEYWORD, 'FROM')
        table_name_token = self.expect_token(TokenType.IDENTIFIER)
        table_name = table_name_token.value

        # 解析WHERE条件（可选）
        where_condition = None
        token = self.current_token()
        if token.type == TokenType.KEYWORD and token.value == 'WHERE':
            where_condition = self.parse_where_condition()

        self.expect_token(TokenType.DELIMITER, ';')

        return SelectNode(columns, table_name, where_condition)

    def parse_delete(self) -> DeleteNode:
        """解析DELETE语句"""
        # DELETE FROM table_name [WHERE condition]
        self.expect_token(TokenType.KEYWORD, 'DELETE')
        self.expect_token(TokenType.KEYWORD, 'FROM')

        table_name_token = self.expect_token(TokenType.IDENTIFIER)
        table_name = table_name_token.value

        # 解析WHERE条件（可选）
        where_condition = None
        token = self.current_token()
        if token.type == TokenType.KEYWORD and token.value == 'WHERE':
            where_condition = self.parse_where_condition()

        self.expect_token(TokenType.DELIMITER, ';')

        return DeleteNode(table_name, where_condition)

    def parse_update(self) -> UpdateNode:
        """解析UPDATE语句"""
        # UPDATE table_name SET column1=value1, column2=value2 [WHERE condition]
        self.expect_token(TokenType.KEYWORD, 'UPDATE')

        table_name_token = self.expect_token(TokenType.IDENTIFIER)
        table_name = table_name_token.value

        self.expect_token(TokenType.KEYWORD, 'SET')

        # 解析SET子句
        set_clauses = []
        while True:
            column_token = self.expect_token(TokenType.IDENTIFIER)
            self.expect_token(TokenType.OPERATOR, '=')
            value_token = self.expect_token(TokenType.CONSTANT)

            set_clauses.append({
                'column': column_token.value,
                'value': value_token.value
            })

            token = self.current_token()
            if token.type == TokenType.KEYWORD and token.value == 'WHERE':
                break
            elif token.type == TokenType.DELIMITER and token.value == ',':
                self.next_token()
            elif token.type == TokenType.DELIMITER and token.value == ';':
                break
            else:
                raise SyntaxError(f"第{token.line}行第{token.column}列：期望','、'WHERE'或';'")

        # 解析WHERE条件（可选）
        where_condition = None
        token = self.current_token()
        if token.type == TokenType.KEYWORD and token.value == 'WHERE':
            where_condition = self.parse_where_condition()

        self.expect_token(TokenType.DELIMITER, ';')

        return UpdateNode(table_name, set_clauses, where_condition)

    def parse_extended_select(self) -> ExtendedSelectNode:
        """解析扩展的SELECT语句"""
        # SELECT columns FROM table_name [JOIN ...] [WHERE ...] [GROUP BY ...] [ORDER BY ...] [LIMIT ...]
        self.expect_token(TokenType.KEYWORD, 'SELECT')

        columns = []
        token = self.current_token()

        # 检查是否是 SELECT *
        if token.type == TokenType.OPERATOR and token.value == '*':
            columns = ['*']
            self.next_token()
        else:
            # 解析列名列表
            while True:
                # 检查是否是聚合函数
                token = self.current_token()
                if token.type == TokenType.KEYWORD and token.value in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']:
                    func_name = token.value
                    self.next_token()
                    self.expect_token(TokenType.DELIMITER, '(')

                    # 解析函数参数
                    param_token = self.current_token()
                    if param_token.type == TokenType.OPERATOR and param_token.value == '*':
                        func_param = '*'
                        self.next_token()
                    else:
                        param_token = self.expect_token(TokenType.IDENTIFIER)
                        func_param = param_token.value

                    self.expect_token(TokenType.DELIMITER, ')')

                    # 检查是否有别名
                    alias = None
                    token = self.current_token()
                    if token.type == TokenType.KEYWORD and token.value == 'AS':
                        self.next_token()
                        alias_token = self.expect_token(TokenType.IDENTIFIER)
                        alias = alias_token.value

                    column_name = f"{func_name}({func_param})"
                    if alias:
                        column_name += f" AS {alias}"
                    columns.append(column_name)
                else:
                    # 普通列名
                    column_token = self.expect_token(TokenType.IDENTIFIER)
                    column_name = column_token.value

                    # 检查是否有表前缀
                    token = self.current_token()
                    if token.type == TokenType.DELIMITER and token.value == '.':
                        self.next_token()
                        table_name_token = self.expect_token(TokenType.IDENTIFIER)
                        column_name = f"{column_name}.{table_name_token.value}"

                    # 检查是否有别名
                    token = self.current_token()
                    if token.type == TokenType.KEYWORD and token.value == 'AS':
                        self.next_token()
                        alias_token = self.expect_token(TokenType.IDENTIFIER)
                        column_name += f" AS {alias_token.value}"

                    columns.append(column_name)

                token = self.current_token()
                if token.type == TokenType.KEYWORD and token.value == 'FROM':
                    break
                elif token.type == TokenType.DELIMITER and token.value == ',':
                    self.next_token()
                else:
                    raise SyntaxError(f"第{token.line}行第{token.column}列：期望'FROM'或','")

        self.expect_token(TokenType.KEYWORD, 'FROM')
        table_name_token = self.expect_token(TokenType.IDENTIFIER)
        table_name = table_name_token.value

        # 检查是否有表别名
        token = self.current_token()
        if token.type == TokenType.IDENTIFIER:
            # 表别名
            alias = token.value
            self.next_token()
            table_name = f"{table_name} AS {alias}"
        elif token.type == TokenType.KEYWORD and token.value == 'AS':
            self.next_token()
            alias_token = self.expect_token(TokenType.IDENTIFIER)
            table_name = f"{table_name} AS {alias_token.value}"

        # 解析JOIN子句（可选）
        joins = []
        token = self.current_token()
        while token.type == TokenType.KEYWORD and token.value in ['INNER', 'LEFT', 'RIGHT', 'OUTER']:
            join_node = self.parse_join()
            joins.append(join_node)
            token = self.current_token()

        # 解析WHERE条件（可选）
        where_condition = None
        if token.type == TokenType.KEYWORD and token.value == 'WHERE':
            where_condition = self.parse_where_condition()
            token = self.current_token()

        # 解析GROUP BY子句（可选）
        group_by = None
        if token.type == TokenType.KEYWORD and token.value == 'GROUP':
            group_by = self.parse_group_by()
            token = self.current_token()

        # 解析ORDER BY子句（可选）
        order_by = None
        if token.type == TokenType.KEYWORD and token.value == 'ORDER':
            order_by = self.parse_order_by()
            token = self.current_token()

        # 解析LIMIT子句（可选）
        limit = None
        offset = None
        if token.type == TokenType.KEYWORD and token.value == 'LIMIT':
            self.next_token()  # 跳过LIMIT关键字
            limit_token = self.expect_token(TokenType.CONSTANT)
            limit = int(limit_token.value)

            token = self.current_token()
            if token.type == TokenType.KEYWORD and token.value == 'OFFSET':
                self.next_token()  # 跳过OFFSET关键字
                offset_token = self.expect_token(TokenType.CONSTANT)
                offset = int(offset_token.value)

        self.expect_token(TokenType.DELIMITER, ';')

        return ExtendedSelectNode(
            columns=columns,
            table_name=table_name,
            joins=joins if joins else None,
            where_condition=where_condition,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            offset=offset
        )

    def parse_join(self) -> JoinNode:
        """解析JOIN子句"""
        join_type = 'INNER'  # 默认JOIN类型
        token = self.current_token()

        if token.value == 'INNER':
            self.next_token()
        elif token.value in ['LEFT', 'RIGHT']:
            join_type = token.value
            self.next_token()
            # 检查是否有OUTER
            next_token = self.current_token()
            if next_token.type == TokenType.KEYWORD and next_token.value == 'OUTER':
                join_type = f"{join_type} OUTER"
                self.next_token()
        elif token.value == 'OUTER':
            # LEFT OUTER JOIN 或 RIGHT OUTER JOIN
            self.next_token()
            next_token = self.current_token()
            if next_token.type == TokenType.KEYWORD and next_token.value in ['LEFT', 'RIGHT']:
                join_type = f"{next_token.value} OUTER"
                self.next_token()

        self.expect_token(TokenType.KEYWORD, 'JOIN')

        right_table_token = self.expect_token(TokenType.IDENTIFIER)
        right_table = right_table_token.value

        # 检查是否有表别名
        token = self.current_token()
        if token.type == TokenType.IDENTIFIER:
            # 表别名
            alias = token.value
            self.next_token()
            right_table = f"{right_table} AS {alias}"
        elif token.type == TokenType.KEYWORD and token.value == 'AS':
            self.next_token()
            alias_token = self.expect_token(TokenType.IDENTIFIER)
            right_table = f"{right_table} AS {alias_token.value}"

        self.expect_token(TokenType.KEYWORD, 'ON')

        # 解析ON条件
        left_column_token = self.expect_token(TokenType.IDENTIFIER)
        left_column = left_column_token.value

        # 检查是否有表前缀
        token = self.current_token()
        if token.type == TokenType.DELIMITER and token.value == '.':
            self.next_token()
            table_name_token = self.expect_token(TokenType.IDENTIFIER)
            left_column = f"{left_column}.{table_name_token.value}"

        operator_token = self.expect_token(TokenType.OPERATOR)

        right_column_token = self.expect_token(TokenType.IDENTIFIER)
        right_column = right_column_token.value

        # 检查是否有表前缀
        token = self.current_token()
        if token.type == TokenType.DELIMITER and token.value == '.':
            self.next_token()
            table_name_token = self.expect_token(TokenType.IDENTIFIER)
            right_column = f"{right_column}.{table_name_token.value}"

        on_condition = {
            'left_column': left_column,
            'operator': operator_token.value,
            'right_column': right_column
        }

        return JoinNode('', right_table, join_type, on_condition)

    def parse_group_by(self) -> GroupByNode:
        """解析GROUP BY子句"""
        self.expect_token(TokenType.KEYWORD, 'GROUP')
        self.expect_token(TokenType.KEYWORD, 'BY')

        columns = []
        while True:
            # lookahead：如果遇到子句关键字或语句结束，立即返回外层处理
            token = self.current_token()
            if token.type == TokenType.KEYWORD and token.value in ['HAVING', 'ORDER', 'LIMIT']:
                break
            if token.type == TokenType.DELIMITER and token.value == ';':
                break

            # 读取一列，支持表前缀 alias.column
            first = self.expect_token(TokenType.IDENTIFIER)
            col_name = first.value
            dot = self.current_token()
            if dot.type == TokenType.DELIMITER and dot.value == '.':
                self.next_token()
                second = self.expect_token(TokenType.IDENTIFIER)
                col_name = f"{col_name}.{second.value}"
            columns.append(col_name)

            # 分隔或结束
            token = self.current_token()
            if token.type == TokenType.DELIMITER and token.value == ',':
                self.next_token()
                continue
            elif token.type == TokenType.KEYWORD and token.value in ['HAVING', 'ORDER', 'LIMIT']:
                break
            elif token.type == TokenType.DELIMITER and token.value in [')', ';']:
                break
            else:
                # 其他情况视为语法问题
                raise SyntaxError(f"第{token.line}行第{token.column}列：期望','、'HAVING'、'ORDER'、'LIMIT'或';'")

        # 解析HAVING条件（可选）
        having_condition = None
        token = self.current_token()
        if token.type == TokenType.KEYWORD and token.value == 'HAVING':
            # 专用HAVING条件解析（不以WHERE开头）
            self.next_token()  # 跳过HAVING
            having_condition = self.parse_condition_core()

        return GroupByNode(columns, having_condition)

    def parse_order_by(self) -> OrderByNode:
        """解析ORDER BY子句"""
        self.expect_token(TokenType.KEYWORD, 'ORDER')
        self.expect_token(TokenType.KEYWORD, 'BY')

        columns = []
        while True:
            # 支持别名或表前缀：alias 或 alias.column
            first = self.expect_token(TokenType.IDENTIFIER)
            order_col = first.value
            dot = self.current_token()
            if dot.type == TokenType.DELIMITER and dot.value == '.':
                self.next_token()
                second = self.expect_token(TokenType.IDENTIFIER)
                order_col = f"{order_col}.{second.value}"
            direction = 'ASC'  # 默认升序

            token = self.current_token()
            if token.type == TokenType.KEYWORD and token.value in ['ASC', 'DESC']:
                direction = token.value
                self.next_token()

            columns.append({
                'column': order_col,
                'direction': direction
            })

            token = self.current_token()
            if token.type == TokenType.KEYWORD and token.value == 'LIMIT':
                break
            elif token.type == TokenType.DELIMITER and token.value == ',':
                self.next_token()
            elif token.type == TokenType.DELIMITER and token.value == ';':
                break
            else:
                raise SyntaxError(f"第{token.line}行第{token.column}列：期望','、'LIMIT'或';'")

        return OrderByNode(columns)

    def parse_where_condition(self) -> Dict[str, Any]:
        """解析WHERE条件"""
        self.expect_token(TokenType.KEYWORD, 'WHERE')
        return self.parse_condition_core()

    def parse_condition_core(self) -> Dict[str, Any]:
        """解析条件表达式核心：支持table.column或聚合函数，如COUNT(*) > 0"""
        token = self.current_token()
        # 支持聚合函数
        if token.type == TokenType.KEYWORD and token.value in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']:
            func_name = token.value
            self.next_token()
            self.expect_token(TokenType.DELIMITER, '(')
            param_token = self.current_token()
            if param_token.type == TokenType.OPERATOR and param_token.value == '*':
                func_param = '*'
                self.next_token()
            else:
                ident = self.expect_token(TokenType.IDENTIFIER)
                func_param = ident.value
                # 可选表前缀 COUNT(table.column)
                dot = self.current_token()
                if dot.type == TokenType.DELIMITER and dot.value == '.':
                    self.next_token()
                    col = self.expect_token(TokenType.IDENTIFIER)
                    func_param = f"{func_param}.{col.value}"
            self.expect_token(TokenType.DELIMITER, ')')
            operator_token = self.expect_token(TokenType.OPERATOR)
            value_token = self.expect_token(TokenType.CONSTANT)
            return {
                'column': f"{func_name}({func_param})",
                'operator': operator_token.value,
                'value': value_token.value
            }
        # 普通标识符，支持table.column
        left = self.expect_token(TokenType.IDENTIFIER)
        column_name = left.value
        dot = self.current_token()
        if dot.type == TokenType.DELIMITER and dot.value == '.':
            self.next_token()
            right = self.expect_token(TokenType.IDENTIFIER)
            column_name = f"{column_name}.{right.value}"
        operator_token = self.expect_token(TokenType.OPERATOR)
        value_token = self.expect_token(TokenType.CONSTANT)
        return {
            'column': column_name,
            'operator': operator_token.value,
            'value': value_token.value
        }


class CatalogManager:
    """系统目录管理器"""

    def __init__(self):
        self.tables = {}  # 表名 -> 表结构

    def create_table(self, table_name: str, columns: List[Dict[str, Any]]) -> bool:
        """创建表"""
        if table_name in self.tables:
            return False  # 表已存在

        self.tables[table_name] = {
            'columns': columns,
            'data': []
        }
        return True

    def get_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表结构"""
        return self.tables.get(table_name)

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.tables

    def column_exists(self, table_name: str, column_name: str) -> bool:
        """检查列是否存在"""
        if not self.table_exists(table_name):
            return False

        table = self.tables[table_name]
        return any(col['name'] == column_name for col in table['columns'])


class SemanticAnalyzer:
    """语义分析器"""

    def __init__(self, catalog: CatalogManager):
        self.catalog = catalog

    def analyze(self, ast: ASTNode) -> Dict[str, Any]:
        """进行语义分析"""
        if isinstance(ast, CreateTableNode):
            return self.analyze_create_table(ast)
        elif isinstance(ast, InsertNode):
            return self.analyze_insert(ast)
        elif isinstance(ast, SelectNode):
            return self.analyze_select(ast)
        elif isinstance(ast, ExtendedSelectNode):
            return self.analyze_extended_select(ast)
        elif isinstance(ast, DeleteNode):
            return self.analyze_delete(ast)
        elif isinstance(ast, UpdateNode):
            return self.analyze_update(ast)
        else:
            return {'error': '不支持的语句类型'}

    def analyze_create_table(self, ast: CreateTableNode) -> Dict[str, Any]:
        """分析CREATE TABLE语句"""
        # 仅做语句自洽检查：列名重复
        column_names = [col['name'] for col in ast.columns]
        if len(column_names) != len(set(column_names)):
            return {
                'error': '列名重复',
                'type': 'DUPLICATE_COLUMN_ERROR'
            }
        # 不进行Catalog访问或更新
        return {'success': True, 'message': '语义检查通过'}

    def analyze_insert(self, ast: InsertNode) -> Dict[str, Any]:
        """分析INSERT语句"""
        # 仅做自洽检查：每一行的值数量与列数量一致
        for row in ast.values:
            if len(row) != len(ast.columns):
                return {
                    'error': f"值的数量({len(row)})与列的数量({len(ast.columns)})不匹配",
                    'type': 'VALUE_COUNT_ERROR'
                }
        return {'success': True, 'message': '语义检查通过'}

    def analyze_select(self, ast: SelectNode) -> Dict[str, Any]:
        """分析SELECT语句"""
        # 不做Catalog相关检查
        return {'success': True, 'message': '语义检查通过'}

    def analyze_delete(self, ast: DeleteNode) -> Dict[str, Any]:
        """分析DELETE语句"""
        # 不做Catalog相关检查
        return {'success': True, 'message': '语义检查通过'}

    def analyze_update(self, ast: UpdateNode) -> Dict[str, Any]:
        """分析UPDATE语句"""
        # 不做Catalog相关检查
        return {'success': True, 'message': '语义检查通过'}

    def analyze_extended_select(self, ast: ExtendedSelectNode) -> Dict[str, Any]:
        """分析扩展的SELECT语句"""
        # 扩展SELECT阶段不做Catalog相关检查
        return {'success': True, 'message': '语义检查通过'}

    def _resolve_table_alias(self, alias: str, main_table: str, joins: List[JoinNode]) -> str:
        """解析表别名为实际表名"""
        # 检查主表别名
        if ' AS ' in main_table:
            main_alias = main_table.split(' AS ')[1]
            if alias == main_alias:
                return main_table.split(' AS ')[0]

        # 检查JOIN表别名
        if joins:
            for join in joins:
                if ' AS ' in join.right_table:
                    join_alias = join.right_table.split(' AS ')[1]
                    if alias == join_alias:
                        return join.right_table.split(' AS ')[0]

        # 如果没有找到别名，返回原名称
        return alias


class ExecutionPlanGenerator:
    """执行计划生成器"""

    def generate_plan(self, ast: ASTNode) -> Dict[str, Any]:
        """生成执行计划"""
        if isinstance(ast, CreateTableNode):
            return self.generate_create_table_plan(ast)
        elif isinstance(ast, InsertNode):
            return self.generate_insert_plan(ast)
        elif isinstance(ast, SelectNode):
            return self.generate_select_plan(ast)
        elif isinstance(ast, ExtendedSelectNode):
            return self.generate_extended_select_plan(ast)
        elif isinstance(ast, DeleteNode):
            return self.generate_delete_plan(ast)
        elif isinstance(ast, UpdateNode):
            return self.generate_update_plan(ast)
        else:
            return {'error': '不支持的语句类型'}

    def generate_create_table_plan(self, ast: CreateTableNode) -> Dict[str, Any]:
        """生成CREATE TABLE执行计划"""
        return {
            'type': 'CreateTable',
            'table_name': ast.table_name,
            'columns': ast.columns
        }

    def generate_insert_plan(self, ast: InsertNode) -> Dict[str, Any]:
        """生成INSERT执行计划"""
        return {
            'type': 'Insert',
            'table_name': ast.table_name,
            'columns': ast.columns,
            'values': ast.values
        }

    def generate_select_plan(self, ast: SelectNode) -> Dict[str, Any]:
        """生成SELECT执行计划"""
        plan = {
            'type': 'Select',
            'table_name': ast.table_name,
            'columns': ast.columns
        }

        if ast.where_condition:
            plan['where'] = ast.where_condition

        return plan

    def generate_delete_plan(self, ast: DeleteNode) -> Dict[str, Any]:
        """生成DELETE执行计划"""
        plan = {
            'type': 'Delete',
            'table_name': ast.table_name
        }

        if ast.where_condition:
            plan['where'] = ast.where_condition

        return plan

    def generate_update_plan(self, ast: UpdateNode) -> Dict[str, Any]:
        """生成UPDATE执行计划"""
        plan = {
            'type': 'Update',
            'table_name': ast.table_name,
            'set_clauses': ast.set_clauses
        }

        if ast.where_condition:
            plan['where'] = ast.where_condition

        return plan

    def generate_extended_select_plan(self, ast: ExtendedSelectNode) -> Dict[str, Any]:
        """生成扩展SELECT执行计划"""
        plan = {
            'type': 'ExtendedSelect',
            'table_name': ast.table_name,
            'columns': ast.columns
        }

        if ast.joins:
            plan['joins'] = []
            for join in ast.joins:
                plan['joins'].append({
                    'type': join.join_type,
                    'right_table': join.right_table,
                    'on_condition': join.on_condition
                })

        if ast.where_condition:
            plan['where'] = ast.where_condition

        if ast.group_by:
            plan['group_by'] = {
                'columns': ast.group_by.columns,
                'having': ast.group_by.having_condition
            }

        if ast.order_by:
            plan['order_by'] = ast.order_by.columns

        if ast.limit:
            plan['limit'] = ast.limit

        if ast.offset:
            plan['offset'] = ast.offset

        return plan


class SQLCompiler:
    """SQL编译器主类"""

    def __init__(self):
        self.lexical_analyzer = LexicalAnalyzer()
        self.syntax_analyzer = SyntaxAnalyzer()
        self.catalog_manager = CatalogManager()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog_manager)
        self.plan_generator = ExecutionPlanGenerator()

    def compile(self, sql_text: str) -> Dict[str, Any]:
        """编译SQL语句"""
        try:
            # 1. 词法分析
            tokens = self.lexical_analyzer.tokenize(sql_text)

            # 2. 语法分析
            ast = self.syntax_analyzer.parse(tokens)

            # 3. 语义分析
            semantic_result = self.semantic_analyzer.analyze(ast)
            # 若语义分析失败，直接返回，不生成执行计划
            if isinstance(semantic_result, dict) and 'error' in semantic_result:
                return {
                    'tokens': [{'type': t.type.value, 'value': t.value, 'line': t.line, 'column': t.column}
                               for t in tokens if t.type != TokenType.EOF],
                    'ast': self.ast_to_dict(ast),
                    'error_type': 'SEMANTIC_ERROR',
                    'message': semantic_result.get('error', ''),
                    'semantic_result': semantic_result,
                    'sql': sql_text,
                    'success': False
                }

            # 4. 执行计划生成（仅在语义通过时）
            execution_plan = self.plan_generator.generate_plan(ast)

            return {
                'tokens': [{'type': t.type.value, 'value': t.value, 'line': t.line, 'column': t.column}
                           for t in tokens if t.type != TokenType.EOF],
                'ast': self.ast_to_dict(ast),
                'semantic_result': semantic_result,
                'execution_plan': execution_plan,
                'success': True
            }
        except SyntaxError as e:
            # 统一提取“第X行第Y列：消息”并结构化输出
            import re
            m = re.search(r"第(\d+)行第(\d+)列：(.+)", str(e))
            if m:
                line, col, msg = int(m.group(1)), int(m.group(2)), m.group(3)
                # 提取原SQL对应行与指示符
                src_lines = sql_text.split('\n') if sql_text else []
                line_text = src_lines[line - 1] if 1 <= line <= len(src_lines) else ''
                pointer = (' ' * (col - 1)) + '^'
                return {
                    'error_type': 'SYNTAX_ERROR',
                    'line': line,
                    'column': col,
                    'message': msg,
                    'line_text': line_text,
                    'pointer': pointer,
                    'sql': sql_text,
                    'success': False
                }
            return {
                'error_type': 'SYNTAX_ERROR',
                'message': str(e),
                'success': False
            }
        except Exception as e:
            return {
                'error_type': 'INTERNAL_ERROR',
                'message': str(e),
                'success': False
            }

    def ast_to_dict(self, ast: ASTNode) -> Dict[str, Any]:
        """将AST转换为字典格式"""
        if isinstance(ast, CreateTableNode):
            return {
                'type': 'CreateTable',
                'table_name': ast.table_name,
                'columns': ast.columns
            }
        elif isinstance(ast, InsertNode):
            return {
                'type': 'Insert',
                'table_name': ast.table_name,
                'columns': ast.columns,
                'values': ast.values
            }
        elif isinstance(ast, SelectNode):
            result = {
                'type': 'Select',
                'columns': ast.columns,
                'table_name': ast.table_name
            }
            if ast.where_condition:
                result['where_condition'] = ast.where_condition
            return result
        elif isinstance(ast, ExtendedSelectNode):
            result = {
                'type': 'ExtendedSelect',
                'columns': ast.columns,
                'table_name': ast.table_name
            }
            if ast.joins:
                result['joins'] = [{'type': j.join_type, 'right_table': j.right_table, 'on_condition': j.on_condition}
                                   for j in ast.joins]
            if ast.where_condition:
                result['where_condition'] = ast.where_condition
            if ast.group_by:
                result['group_by'] = {'columns': ast.group_by.columns, 'having': ast.group_by.having_condition}
            if ast.order_by:
                result['order_by'] = ast.order_by.columns
            if ast.limit:
                result['limit'] = ast.limit
            if ast.offset:
                result['offset'] = ast.offset
            return result
        elif isinstance(ast, DeleteNode):
            result = {
                'type': 'Delete',
                'table_name': ast.table_name
            }
            if ast.where_condition:
                result['where_condition'] = ast.where_condition
            return result
        elif isinstance(ast, UpdateNode):
            result = {
                'type': 'Update',
                'table_name': ast.table_name,
                'set_clauses': ast.set_clauses
            }
            if ast.where_condition:
                result['where_condition'] = ast.where_condition
            return result
        else:
            return {'type': 'Unknown'}


def main():
    """主函数，用于测试"""
    compiler = SQLCompiler()

    # 测试用例（整合基础与扩展）
    test_cases = [
        # 基础：CREATE / INSERT / SELECT / DELETE
        "CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);",
        "CREATE TABLE course(course_id INT, course_name VARCHAR, teacher VARCHAR);",
        "INSERT INTO student(id,name,age,grade) VALUES (1,'Alice',20,'A');",
        # 多行插入
        "INSERT INTO student(id,name,age,grade) VALUES (2,'Bob',20,'B'),(3,'Carol',21,'A');",
        # 值数量不匹配（错误）
        "INSERT INTO student(id,name,age,grade) VALUES (4,'Dave','A');",
        "INSERT INTO course(course_id,course_name,teacher) VALUES (101,'Database','Dr.Smith');",
        "SELECT id,name FROM student WHERE age > 18;",
        "DELETE FROM student WHERE id = 1;",

        # UPDATE
        "UPDATE student SET age=21, grade='A+' WHERE id=1;",

        # 扩展SELECT：*, ORDER BY, LIMIT/OFFSET, JOIN, 别名
        "SELECT * FROM student WHERE grade = 'A+';",
        "SELECT id, name, age FROM student ORDER BY age DESC, name ASC;",
        "SELECT * FROM student ORDER BY age DESC LIMIT 5 OFFSET 0;",
        "SELECT s.name, c.course_name FROM student s INNER JOIN course c ON s.id = c.course_id;",
        "SELECT s.name, c.course_name FROM student s LEFT JOIN course c ON s.id = c.course_id;",

        # GROUP BY + HAVING（已支持HAVING条件解析）
        "SELECT grade, COUNT(*) FROM student GROUP BY grade HAVING COUNT(*) > 0;",

        # 复杂查询（JOIN + WHERE + GROUP BY + HAVING + ORDER BY + LIMIT）
        "SELECT s.grade, COUNT(*) AS student_count FROM student s INNER JOIN course c ON s.id = c.course_id WHERE s.age > 18 GROUP BY s.grade HAVING COUNT(*) > 0 ORDER BY student_count DESC LIMIT 10;",

        # 错误用例：缺少分号、未闭合字符串、拼写错误
        "SELECT * FROM student",
        "INSERT INTO student(id,name) VALUES (1,'Alice);",
        "SELEC id FROM student;",
    ]

    print("=== SQL编译器测试（整合） ===\n")

    for i, sql in enumerate(test_cases, 1):
        print(f"测试用例 {i}: {sql}")
        print("-" * 50)
        result = compiler.compile(sql)
        if result['success']:
            print("✓ 编译成功")
            print(f"Token流: {json.dumps(result['tokens'], ensure_ascii=False)}")
            print(f"AST: {json.dumps(result['ast'], indent=2, ensure_ascii=False)}")
            print(f"语义分析: {result['semantic_result']}")
            print(f"执行计划: {json.dumps(result['execution_plan'], indent=2, ensure_ascii=False)}")
        else:
            if 'error_type' in result:
                if result['error_type'] == 'SYNTAX_ERROR':
                    print(
                        f"✗ 语法错误: 行{result.get('line', '?')} 列{result.get('column', '?')} - {result.get('message', '')}")
                    # 显示原SQL与指示符
                    if 'line_text' in result:
                        print(result['line_text'])
                        print(result.get('pointer', ''))
                else:
                    print(f"✗ {result['error_type']}: {result.get('message', '')}")
            else:
                print(f"✗ 编译失败: {result.get('error', '未知错误')}")
        print()


if __name__ == "__main__":
    main()
