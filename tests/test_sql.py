#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
非阻塞版本2：使用非阻塞弹窗参数
"""

import json
import os
import sys
import time
from typing import Dict, Any

# 调整路径以导入本地模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sql.sql_compiler import SQLCompiler
from engine.cli.poptable import show_table_popup, export_table_to_excel


def compile_sql(sql: str) -> Dict[str, Any]:
    compiler = SQLCompiler()
    return compiler.compile(sql)


def main() -> None:
    sql = "SELECT SUM(age),COUNT(*) FROM student GROUP BY grade HAVING COUNT(*) > 1;"
    # 也可以尝试语法或语义错误用例：
    # sql = "SELECT * FROM student"
    # sql = "INSERT INTO student(id,name) VALUES (1,'Alice);"
    # sql = "SELEC id FROM student;"
    # sql = "INSERT INTO student(id,name,grade) VALUES (1,'Alice');"

    result = compile_sql(sql)
    if result.get('success'):
        print("✓ 编译成功")
        print(f"Token流: {json.dumps(result['tokens'], ensure_ascii=False)}")
        print(f"AST: {json.dumps(result['ast'], indent=2, ensure_ascii=False)}")
        print(f"语义分析: {result['semantic_result']}")
        # 将执行计划以JSON形式传给其他模块
        execution_plan_json = json.dumps(result['execution_plan'], ensure_ascii=False)
        print(f"执行计划: {json.dumps(result['execution_plan'], indent=2, ensure_ascii=False)}")
        # 示例：传递给其它模块的函数（此处仅打印）
        forward_to_other_module(execution_plan_json)

        # 表格数据演示
        data = {
            "columns": ["id", "name", "age", "grade", "course"],
            "rows": [
                [1, "Alice", 20, "A", "Database"],
                [2, "Bob", 20, "B", "OS"],
                {"id": 3, "name": "Carol", "age": 21, "grade": "A", "course": "Networks"},
                [4, "Dave", 22, "B+", "Algorithms"],
            ],
        }
        # 非阻塞显示弹窗
        print("正在非阻塞显示弹窗...")
        show_table_popup(data, title="SQL编译器演示结果", blocking=False)
        # 先导出表格
        print("正在导出表格...")
        file_path4 = export_table_to_excel(data, "C:/Users/表格.xlsx")''




    else:
        if 'error_type' in result:
            if result['error_type'] == 'SYNTAX_ERROR':
                print(
                    f"✗ 语法错误: 行{result.get('line', '?')} 列{result.get('column', '?')} - {result.get('message', '')}")
                if 'line_text' in result:
                    print(result['line_text'])
                    print(result.get('pointer', ''))
            elif result['error_type'] == 'SEMANTIC_ERROR':
                print(f"✗ 语义错误: {result.get('message', '')}")
            else:
                print(f"✗ {result['error_type']}: {result.get('message', '')}")
        else:
            print(f"✗ 编译失败: {result.get('error', '未知错误')}")


def forward_to_other_module(plan_json: str) -> None:
    # 这里模拟把执行计划JSON传递给其他模块
    print("[传递给其他模块的执行计划JSON]", plan_json)


if __name__ == "__main__":
    main()
