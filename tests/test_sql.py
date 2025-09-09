#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
示例：在其他文件中引用 SQLCompiler，编译SQL并输出结果，
并将执行计划以JSON形式传给另一个模块。
"""

import json
import sys
import os
from typing import Dict, Any

# 添加项目根目录到Python路径，以便可以导入sql包
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.insert(0, os.path.abspath(project_root))

# 使用包导入方式导入SQLCompiler
from sql import SQLCompiler


def compile_sql(sql: str) -> Dict[str, Any]:
    compiler = SQLCompiler()
    result = compiler.compile(sql)
    if not result.get('success'):
        raise RuntimeError(result.get('error', '编译失败'))
    return result


def main():
    sql = "INSERT INTO student(id,name,age,grade) VALUES (1,'Alice',20,'A');"

    try:
        result = compile_sql(sql)

        # 1) 在本模块中输出编译产物
        print("=== Demo 输出 ===")
        print("Token流:")
        print(json.dumps(result['tokens'], ensure_ascii=False))
        print("\nAST:")
        print(json.dumps(result['ast'], ensure_ascii=False, indent=2))
        print("\n语义:")
        print(json.dumps(result['semantic_result'], ensure_ascii=False, indent=2))
        print("\n执行计划:")
        print(json.dumps(result['execution_plan'], ensure_ascii=False, indent=2))

        # 2) 将执行计划以JSON形式传给另一个模块
        plan_json = json.dumps(result['execution_plan'], ensure_ascii=False)
        print(f"\n执行计划JSON: {plan_json}")

    except Exception as e:
        print(f"执行过程中出错: {e}")


if __name__ == "__main__":
    main()