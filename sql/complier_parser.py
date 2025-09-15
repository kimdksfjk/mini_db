#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
语法分析器
"""

from typing import List, Dict, Any, Optional

from .complier_lex import TokenType, Token
from .ast_nodes import (
    ASTNode,
    CreateTableNode,
    InsertNode,
    SelectNode,
    DeleteNode,
    UpdateNode,
    JoinNode,
    OrderByNode,
    GroupByNode,
    ExtendedSelectNode,
)


class SyntaxAnalyzer:
    """语法分析器"""

    def __init__(self):
        self.tokens: List[Token] = []
        self.current_token_index = 0

    def parse(self, tokens: List[Token]) -> ASTNode:
        self.tokens = tokens
        self.current_token_index = 0
        if not tokens:
            raise SyntaxError("空的Token流")
        return self.parse_statement()

    def current_token(self) -> Token:
        if self.current_token_index < len(self.tokens):
            return self.tokens[self.current_token_index]
        return Token(TokenType.EOF, "EOF", 0, 0)

    def next_token(self) -> Token:
        if self.current_token_index < len(self.tokens) - 1:
            self.current_token_index += 1
        return self.current_token()

    def expect_token(self, expected_type: TokenType, expected_value: str = None) -> Token:
        token = self.current_token()
        if token.type != expected_type:
            raise SyntaxError(f"第{token.line}行第{token.column}列：期望{expected_type.value}，但得到{token.type.value}")
        if expected_value and token.value.upper() != expected_value.upper():
            raise SyntaxError(f"第{token.line}行第{token.column}列：期望'{expected_value}'，但得到'{token.value}'")
        self.next_token()
        return token

    def parse_statement(self) -> ASTNode:
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
        self.expect_token(TokenType.KEYWORD, 'CREATE')
        self.expect_token(TokenType.KEYWORD, 'TABLE')
        table_name_token = self.expect_token(TokenType.IDENTIFIER)
        table_name = table_name_token.value
        self.expect_token(TokenType.DELIMITER, '(')
        columns: List[Dict[str, Any]] = []
        while True:
            column_name_token = self.expect_token(TokenType.IDENTIFIER)
            column_type_token = self.expect_token(TokenType.KEYWORD)
            columns.append({'name': column_name_token.value, 'type': column_type_token.value})
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
        self.expect_token(TokenType.KEYWORD, 'INSERT')
        self.expect_token(TokenType.KEYWORD, 'INTO')
        table_name_token = self.expect_token(TokenType.IDENTIFIER)
        table_name = table_name_token.value
        self.expect_token(TokenType.DELIMITER, '(')
        columns: List[str] = []
        while True:
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
        all_values: List[List[str]] = []
        while True:
            self.expect_token(TokenType.DELIMITER, '(')
            row_values: List[str] = []
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
        self.expect_token(TokenType.KEYWORD, 'SELECT')
        columns: List[str] = []
        token = self.current_token()
        if token.type == TokenType.OPERATOR and token.value == '*':
            columns = ['*']
            self.next_token()
        else:
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
        where_condition: Optional[Dict[str, Any]] = None
        token = self.current_token()
        if token.type == TokenType.KEYWORD and token.value == 'WHERE':
            where_condition = self.parse_where_condition()
        self.expect_token(TokenType.DELIMITER, ';')
        return SelectNode(columns, table_name, where_condition)

    def parse_delete(self) -> DeleteNode:
        self.expect_token(TokenType.KEYWORD, 'DELETE')
        self.expect_token(TokenType.KEYWORD, 'FROM')
        table_name_token = self.expect_token(TokenType.IDENTIFIER)
        table_name = table_name_token.value
        where_condition: Optional[Dict[str, Any]] = None
        token = self.current_token()
        if token.type == TokenType.KEYWORD and token.value == 'WHERE':
            where_condition = self.parse_where_condition()
        self.expect_token(TokenType.DELIMITER, ';')
        return DeleteNode(table_name, where_condition)

    def parse_update(self) -> UpdateNode:
        self.expect_token(TokenType.KEYWORD, 'UPDATE')
        table_name_token = self.expect_token(TokenType.IDENTIFIER)
        table_name = table_name_token.value
        self.expect_token(TokenType.KEYWORD, 'SET')
        set_clauses: List[Dict[str, str]] = []
        while True:
            column_token = self.expect_token(TokenType.IDENTIFIER)
            self.expect_token(TokenType.OPERATOR, '=')
            value_token = self.expect_token(TokenType.CONSTANT)
            set_clauses.append({'column': column_token.value, 'value': value_token.value})
            token = self.current_token()
            if token.type == TokenType.KEYWORD and token.value == 'WHERE':
                break
            elif token.type == TokenType.DELIMITER and token.value == ',':
                self.next_token()
            elif token.type == TokenType.DELIMITER and token.value == ';':
                break
            else:
                raise SyntaxError(f"第{token.line}行第{token.column}列：期望','、'WHERE'或';'")
        where_condition: Optional[Dict[str, Any]] = None
        token = self.current_token()
        if token.type == TokenType.KEYWORD and token.value == 'WHERE':
            where_condition = self.parse_where_condition()
        self.expect_token(TokenType.DELIMITER, ';')
        return UpdateNode(table_name, set_clauses, where_condition)

    def parse_extended_select(self) -> ExtendedSelectNode:
        self.expect_token(TokenType.KEYWORD, 'SELECT')
        columns: List[str] = []
        token = self.current_token()
        if token.type == TokenType.OPERATOR and token.value == '*':
            columns = ['*']
            self.next_token()
        else:
            while True:
                token = self.current_token()
                if token.type == TokenType.KEYWORD and token.value in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']:
                    func_name = token.value
                    self.next_token()
                    self.expect_token(TokenType.DELIMITER, '(')
                    param_token = self.current_token()
                    if param_token.type == TokenType.OPERATOR and param_token.value == '*':
                        func_param = '*'
                        self.next_token()
                    else:
                        param_token = self.expect_token(TokenType.IDENTIFIER)
                        func_param = param_token.value
                    self.expect_token(TokenType.DELIMITER, ')')
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
                    column_token = self.expect_token(TokenType.IDENTIFIER)
                    column_name = column_token.value
                    token = self.current_token()
                    if token.type == TokenType.DELIMITER and token.value == '.':
                        self.next_token()
                        table_name_token = self.expect_token(TokenType.IDENTIFIER)
                        column_name = f"{column_name}.{table_name_token.value}"
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
        token = self.current_token()
        if token.type == TokenType.IDENTIFIER:
            alias = token.value
            self.next_token()
            table_name = f"{table_name} AS {alias}"
        elif token.type == TokenType.KEYWORD and token.value == 'AS':
            self.next_token()
            alias_token = self.expect_token(TokenType.IDENTIFIER)
            table_name = f"{table_name} AS {alias_token.value}"
        joins = []
        token = self.current_token()
        while token.type == TokenType.KEYWORD and token.value in ['INNER', 'LEFT', 'RIGHT', 'OUTER']:
            join_node = self.parse_join()
            joins.append(join_node)
            token = self.current_token()
        where_condition = None
        if token.type == TokenType.KEYWORD and token.value == 'WHERE':
            where_condition = self.parse_where_condition()
            token = self.current_token()
        group_by = None
        if token.type == TokenType.KEYWORD and token.value == 'GROUP':
            group_by = self.parse_group_by()
            token = self.current_token()
        order_by = None
        if token.type == TokenType.KEYWORD and token.value == 'ORDER':
            order_by = self.parse_order_by()
            token = self.current_token()
        limit = None
        offset = None
        if token.type == TokenType.KEYWORD and token.value == 'LIMIT':
            self.next_token()
            limit_token = self.expect_token(TokenType.CONSTANT)
            limit = int(limit_token.value)
            token = self.current_token()
            if token.type == TokenType.KEYWORD and token.value == 'OFFSET':
                self.next_token()
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
            offset=offset,
        )

    def parse_join(self) -> JoinNode:
        join_type = 'INNER'
        token = self.current_token()
        if token.value == 'INNER':
            self.next_token()
        elif token.value in ['LEFT', 'RIGHT']:
            join_type = token.value
            self.next_token()
            next_token = self.current_token()
            if next_token.type == TokenType.KEYWORD and next_token.value == 'OUTER':
                join_type = f"{join_type} OUTER"
                self.next_token()
        elif token.value == 'OUTER':
            self.next_token()
            next_token = self.current_token()
            if next_token.type == TokenType.KEYWORD and next_token.value in ['LEFT', 'RIGHT']:
                join_type = f"{next_token.value} OUTER"
                self.next_token()
        self.expect_token(TokenType.KEYWORD, 'JOIN')
        right_table_token = self.expect_token(TokenType.IDENTIFIER)
        right_table = right_table_token.value
        token = self.current_token()
        if token.type == TokenType.IDENTIFIER:
            alias = token.value
            self.next_token()
            right_table = f"{right_table} AS {alias}"
        elif token.type == TokenType.KEYWORD and token.value == 'AS':
            self.next_token()
            alias_token = self.expect_token(TokenType.IDENTIFIER)
            right_table = f"{right_table} AS {alias_token.value}"
        self.expect_token(TokenType.KEYWORD, 'ON')
        left_column_token = self.expect_token(TokenType.IDENTIFIER)
        left_column = left_column_token.value
        token = self.current_token()
        if token.type == TokenType.DELIMITER and token.value == '.':
            self.next_token()
            table_name_token = self.expect_token(TokenType.IDENTIFIER)
            left_column = f"{left_column}.{table_name_token.value}"
        operator_token = self.expect_token(TokenType.OPERATOR)
        right_column_token = self.expect_token(TokenType.IDENTIFIER)
        right_column = right_column_token.value
        token = self.current_token()
        if token.type == TokenType.DELIMITER and token.value == '.':
            self.next_token()
            table_name_token = self.expect_token(TokenType.IDENTIFIER)
            right_column = f"{right_column}.{table_name_token.value}"
        on_condition = {'left_column': left_column, 'operator': operator_token.value, 'right_column': right_column}
        return JoinNode('', right_table, join_type, on_condition)

    def parse_group_by(self) -> GroupByNode:
        self.expect_token(TokenType.KEYWORD, 'GROUP')
        self.expect_token(TokenType.KEYWORD, 'BY')
        columns: List[str] = []
        while True:
            token = self.current_token()
            if token.type == TokenType.KEYWORD and token.value in ['HAVING', 'ORDER', 'LIMIT']:
                break
            if token.type == TokenType.DELIMITER and token.value == ';':
                break
            first = self.expect_token(TokenType.IDENTIFIER)
            col_name = first.value
            dot = self.current_token()
            if dot.type == TokenType.DELIMITER and dot.value == '.':
                self.next_token()
                second = self.expect_token(TokenType.IDENTIFIER)
                col_name = f"{col_name}.{second.value}"
            columns.append(col_name)
            token = self.current_token()
            if token.type == TokenType.DELIMITER and token.value == ',':
                self.next_token()
                continue
            elif token.type == TokenType.KEYWORD and token.value in ['HAVING', 'ORDER', 'LIMIT']:
                break
            elif token.type == TokenType.DELIMITER and token.value in [')', ';']:
                break
            else:
                raise SyntaxError(f"第{token.line}行第{token.column}列：期望','、'HAVING'、'ORDER'、'LIMIT'或';'")
        having_condition = None
        token = self.current_token()
        if token.type == TokenType.KEYWORD and token.value == 'HAVING':
            self.next_token()
            having_condition = self.parse_condition_core()
        return GroupByNode(columns, having_condition)

    def parse_order_by(self) -> OrderByNode:
        self.expect_token(TokenType.KEYWORD, 'ORDER')
        self.expect_token(TokenType.KEYWORD, 'BY')
        columns: List[Dict[str, str]] = []
        while True:
            first = self.expect_token(TokenType.IDENTIFIER)
            order_col = first.value
            dot = self.current_token()
            if dot.type == TokenType.DELIMITER and dot.value == '.':
                self.next_token()
                second = self.expect_token(TokenType.IDENTIFIER)
                order_col = f"{order_col}.{second.value}"
            direction = 'ASC'
            token = self.current_token()
            if token.type == TokenType.KEYWORD and token.value in ['ASC', 'DESC']:
                direction = token.value
                self.next_token()
            columns.append({'column': order_col, 'direction': direction})
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
        self.expect_token(TokenType.KEYWORD, 'WHERE')
        return self.parse_condition_core()

    def parse_condition_core(self) -> Dict[str, Any]:
        token = self.current_token()
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
                dot = self.current_token()
                if dot.type == TokenType.DELIMITER and dot.value == '.':
                    self.next_token()
                    col = self.expect_token(TokenType.IDENTIFIER)
                    func_param = f"{func_param}.{col.value}"
            self.expect_token(TokenType.DELIMITER, ')')
            operator_token = self.expect_token(TokenType.OPERATOR)
            value_token = self.expect_token(TokenType.CONSTANT)
            return {'column': f"{func_name}({func_param})", 'operator': operator_token.value, 'value': value_token.value}
        left = self.expect_token(TokenType.IDENTIFIER)
        column_name = left.value
        dot = self.current_token()
        if dot.type == TokenType.DELIMITER and dot.value == '.':
            self.next_token()
            right = self.expect_token(TokenType.IDENTIFIER)
            column_name = f"{column_name}.{right.value}"
        operator_token = self.expect_token(TokenType.OPERATOR)
        value_token = self.expect_token(TokenType.CONSTANT)
        return {'column': column_name, 'operator': operator_token.value, 'value': value_token.value}


