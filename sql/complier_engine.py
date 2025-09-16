#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
编译引擎与语义/计划
"""

import json
from typing import List, Dict, Any, Optional
#统一包内导入
from .complier_lex import LexicalAnalyzer, TokenType
from .ast_nodes import (
    ASTNode,
    CreateTableNode,
    InsertNode,
    SelectNode,
    DeleteNode,
    UpdateNode,
    ExtendedSelectNode,
    JoinNode,
)
from .complier_parser import SyntaxAnalyzer


class CatalogManager:
    def __init__(self):
        self.tables: Dict[str, Dict[str, Any]] = {}

    def create_table(self, table_name: str, columns: List[Dict[str, Any]]) -> bool:
        if table_name in self.tables:
            return False
        self.tables[table_name] = {'columns': columns, 'data': []}
        return True

    def get_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        return self.tables.get(table_name)

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.tables

    def column_exists(self, table_name: str, column_name: str) -> bool:
        if not self.table_exists(table_name):
            return False
        table = self.tables[table_name]
        return any(col['name'] == column_name for col in table['columns'])


class SemanticAnalyzer:
    """语句检查类"""
    def __init__(self, catalog: CatalogManager):
        self.catalog = catalog

    def analyze(self, ast: ASTNode) -> Dict[str, Any]:
        if isinstance(ast, CreateTableNode):
            return self.analyze_create_table(ast)
        elif isinstance(ast, InsertNode):
            return self.analyze_insert(ast)
        elif isinstance(ast, SelectNode):
            return {'success': True, 'message': '语义检查通过'}
        elif isinstance(ast, ExtendedSelectNode):
            return {'success': True, 'message': '语义检查通过'}
        elif isinstance(ast, DeleteNode):
            return {'success': True, 'message': '语义检查通过'}
        elif isinstance(ast, UpdateNode):
            return {'success': True, 'message': '语义检查通过'}
        else:
            return {'error': '不支持的语句类型'}

    def analyze_create_table(self, ast: CreateTableNode) -> Dict[str, Any]:
        column_names = [col['name'] for col in ast.columns]
        if len(column_names) != len(set(column_names)):
            return {'error': '列名重复', 'type': 'DUPLICATE_COLUMN_ERROR'}
        return {'success': True, 'message': '语义检查通过'}

    def analyze_insert(self, ast: InsertNode) -> Dict[str, Any]:
        for row in ast.values:
            if len(row) != len(ast.columns):
                return {'error': f"值的数量({len(row)})与列的数量({len(ast.columns)})不匹配", 'type': 'VALUE_COUNT_ERROR'}
        return {'success': True, 'message': '语义检查通过'}


class ExecutionPlanGenerator:
    """生成执行计划"""
    def generate_plan(self, ast: ASTNode) -> Dict[str, Any]:
        if isinstance(ast, CreateTableNode):
            return {'type': 'CreateTable', 'table_name': ast.table_name, 'columns': ast.columns}
        elif isinstance(ast, InsertNode):
            return {'type': 'Insert', 'table_name': ast.table_name, 'columns': ast.columns, 'values': ast.values}
        elif isinstance(ast, SelectNode):
            plan = {'type': 'Select', 'table_name': ast.table_name, 'columns': ast.columns}
            if ast.where_condition:
                plan['where'] = ast.where_condition
            return plan
        elif isinstance(ast, DeleteNode):
            plan = {'type': 'Delete', 'table_name': ast.table_name}
            if ast.where_condition:
                plan['where'] = ast.where_condition
            return plan
        elif isinstance(ast, UpdateNode):
            plan = {'type': 'Update', 'table_name': ast.table_name, 'set_clauses': ast.set_clauses}
            if ast.where_condition:
                plan['where'] = ast.where_condition
            return plan
        elif isinstance(ast, ExtendedSelectNode):
            plan: Dict[str, Any] = {'type': 'ExtendedSelect', 'table_name': ast.table_name, 'columns': ast.columns}
            if ast.joins:
                plan['joins'] = []
                for join in ast.joins:
                    plan['joins'].append({'type': join.join_type, 'right_table': join.right_table, 'on_condition': join.on_condition})
            if ast.where_condition:
                plan['where'] = ast.where_condition
            if ast.group_by:
                plan['group_by'] = {'columns': ast.group_by.columns, 'having': ast.group_by.having_condition}
            if ast.order_by:
                plan['order_by'] = ast.order_by.columns
            if ast.limit:
                plan['limit'] = ast.limit
            if ast.offset:
                plan['offset'] = ast.offset
            return plan
        else:
            return {'error': '不支持的语句类型'}


class SQLCompiler:
    def __init__(self):
        self.lexical_analyzer = LexicalAnalyzer()
        self.syntax_analyzer = SyntaxAnalyzer()
        self.catalog_manager = CatalogManager()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog_manager)
        self.plan_generator = ExecutionPlanGenerator()

    def compile(self, sql_text: str) -> Dict[str, Any]:
        #流程总控
        try:
            tokens = self.lexical_analyzer.tokenize(sql_text)
            ast = self.syntax_analyzer.parse(tokens)
            semantic_result = self.semantic_analyzer.analyze(ast)
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
            import re
            m = re.search(r"第(\d+)行第(\d+)列：(.+)", str(e))
            if m:
                line, col, msg = int(m.group(1)), int(m.group(2)), m.group(3)
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
            return {'error_type': 'SYNTAX_ERROR', 'message': str(e), 'success': False}
        except Exception as e:
            return {'error_type': 'INTERNAL_ERROR', 'message': str(e), 'success': False}

    def ast_to_dict(self, ast: ASTNode) -> Dict[str, Any]:
        if isinstance(ast, CreateTableNode):
            return {'type': 'CreateTable', 'table_name': ast.table_name, 'columns': ast.columns}
        elif isinstance(ast, InsertNode):
            return {'type': 'Insert', 'table_name': ast.table_name, 'columns': ast.columns, 'values': ast.values}
        elif isinstance(ast, SelectNode):
            result: Dict[str, Any] = {'type': 'Select', 'columns': ast.columns, 'table_name': ast.table_name}
            if ast.where_condition:
                result['where_condition'] = ast.where_condition
            return result
        elif isinstance(ast, ExtendedSelectNode):
            result: Dict[str, Any] = {'type': 'ExtendedSelect', 'columns': ast.columns, 'table_name': ast.table_name}
            if ast.joins:
                result['joins'] = [{'type': j.join_type, 'right_table': j.right_table, 'on_condition': j.on_condition} for j in ast.joins]
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
            result = {'type': 'Delete', 'table_name': ast.table_name}
            if ast.where_condition:
                result['where_condition'] = ast.where_condition
            return result
        elif isinstance(ast, UpdateNode):
            result = {'type': 'Update', 'table_name': ast.table_name, 'set_clauses': ast.set_clauses}
            if ast.where_condition:
                result['where_condition'] = ast.where_condition
            return result
        else:
            return {'type': 'Unknown'}


def main():
    compiler = SQLCompiler()
    test_cases = [
        "CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);",
        "CREATE TABLE course(course_id INT, course_name VARCHAR, teacher VARCHAR);",
        "INSERT INTO student(id,name,age,grade) VALUES (1,'Alice',20,'A');",
        "INSERT INTO student(id,name,age,grade) VALUES (2,'Bob',20,'B'),(3,'Carol',21,'A');",
        "INSERT INTO student(id,name,age,grade) VALUES (4,'Dave','A');",
        "INSERT INTO course(course_id,course_name,teacher) VALUES (101,'Database','Dr.Smith');",
        "SELECT id,name FROM student WHERE age > 18;",
        "DELETE FROM student WHERE id = 1;",
        "UPDATE student SET age=21, grade='A+' WHERE id=1;",
        "SELECT * FROM student WHERE grade = 'A+';",
        "SELECT id, name, age FROM student ORDER BY age DESC, name ASC;",
        "SELECT * FROM student ORDER BY age DESC LIMIT 5 OFFSET 0;",
        "SELECT s.name, c.course_name FROM student s INNER JOIN course c ON s.id = c.course_id;",
        "SELECT s.name, c.course_name FROM student s LEFT JOIN course c ON s.id = c.course_id;",
        "SELECT grade, COUNT(*) FROM student GROUP BY grade HAVING COUNT(*) > 0;",
        "SELECT s.grade, COUNT(*) AS student_count FROM student s INNER JOIN course c ON s.id = c.course_id WHERE s.age > 18 GROUP BY s.grade HAVING COUNT(*) > 0 ORDER BY student_count DESC LIMIT 10;",
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
                    print(f"✗ 语法错误: 行{result.get('line', '?')} 列{result.get('column', '?')} - {result.get('message', '')}")
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


