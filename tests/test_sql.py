# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL编译器扩展功能测试脚本
测试所有扩展功能：UPDATE、JOIN、ORDER BY、GROUP BY等
"""

from sql.sql_compiler import SQLCompiler
import json


def test_extended_sql_compiler():
    """测试扩展SQL编译器"""
    compiler = SQLCompiler()

    print("=== SQL编译器扩展功能测试 ===\n")

    # 测试用例1：CREATE TABLE - 基础表
    print("测试1: CREATE TABLE - 学生表")
    print("-" * 50)
    sql1 = "CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);"
    print(f"SQL: {sql1}")

    result1 = compiler.compile(sql1)
    if result1['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result1['semantic_result']}")
    else:
        print(f"✗ 编译失败: {result1['error']}")
    print()

    # 测试用例3：INSERT - 插入学生数据
    print("测试3: INSERT - 插入学生数据")
    print("-" * 50)
    sql3 = "INSERT INTO student(id,name,age,grade) VALUES (1,'Alice',20,'A');"
    print(f"SQL: {sql3}")

    result3 = compiler.compile(sql3)
    if result3['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result3['semantic_result']}")
    else:
        print(f"✗ 编译失败: {result3['error']}")
    print()

    # 测试用例4：INSERT - 插入课程数据
    print("测试4: INSERT - 插入课程数据")
    print("-" * 50)
    sql4 = "INSERT INTO course(course_id,course_name,teacher) VALUES (101,'Database','Dr.Smith');"
    print(f"SQL: {sql4}")

    result4 = compiler.compile(sql4)
    if result4['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result4['semantic_result']}")
    else:
        print(f"✗ 编译失败: {result4['error']}")
    print()

    # 测试用例5：UPDATE - 更新学生信息
    print("测试5: UPDATE - 更新学生信息")
    print("-" * 50)
    sql5 = "UPDATE student SET age=21, grade='A+' WHERE id=1;"
    print(f"SQL: {sql5}")

    result5 = compiler.compile(sql5)
    if result5['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result5['semantic_result']}")
        print(f"执行计划: {json.dumps(result5['execution_plan'], indent=2, ensure_ascii=False)}")
    else:
        print(f"✗ 编译失败: {result5['error']}")
    print()

    # 测试用例6：SELECT - 基础查询
    print("测试6: SELECT - 基础查询")
    print("-" * 50)
    sql6 = "SELECT id, name FROM student WHERE age > 18;"
    print(f"SQL: {sql6}")

    result6 = compiler.compile(sql6)
    if result6['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result6['semantic_result']}")
        print(f"执行计划: {json.dumps(result6['execution_plan'], indent=2, ensure_ascii=False)}")
    else:
        print(f"✗ 编译失败: {result6['error']}")
    print()


    # 测试用例13：复杂查询 - 多表JOIN + WHERE + GROUP BY + ORDER BY + LIMIT
    print("测试13: 复杂查询 - 多表JOIN + WHERE + GROUP BY + ORDER BY + LIMIT")
    print("-" * 50)
    sql13 = "SELECT s.grade, COUNT(*) as student_count FROM student s INNER JOIN course c ON s.id = c.course_id WHERE s.age > 18 GROUP BY s.grade HAVING COUNT(*) > 0 ORDER BY student_count DESC LIMIT 10;"
    print(f"SQL: {sql13}")

    result13 = compiler.compile(sql13)
    if result13['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result13['semantic_result']}")
        print(f"执行计划: {json.dumps(result13['execution_plan'], indent=2, ensure_ascii=False)}")
    else:
        print(f"✗ 编译失败: {result13['error']}")
    print()

    # 测试用例14：DELETE - 删除记录
    print("测试14: DELETE - 删除记录")
    print("-" * 50)
    sql14 = "DELETE FROM student WHERE age < 18;"
    print(f"SQL: {sql14}")

    result14 = compiler.compile(sql14)
    if result14['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result14['semantic_result']}")
        print(f"执行计划: {json.dumps(result14['execution_plan'], indent=2, ensure_ascii=False)}")
    else:
        print(f"✗ 编译失败: {result14['error']}")
    print()

    print("=== 扩展功能测试完成 ===")


if __name__ == "__main__":
    test_extended_sql_compiler()
