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

    # 测试用例2：CREATE TABLE - 课程表
    print("测试2: CREATE TABLE - 课程表")
    print("-" * 50)
    sql2 = "CREATE TABLE course(course_id INT, course_name VARCHAR, teacher VARCHAR);"
    print(f"SQL: {sql2}")

    result2 = compiler.compile(sql2)
    if result2['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result2['semantic_result']}")
    else:
        print(f"✗ 编译失败: {result2['error']}")
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

    # 测试用例7：SELECT * - 通配符查询
    print("测试7: SELECT * - 通配符查询")
    print("-" * 50)
    sql7 = "SELECT * FROM student WHERE grade = 'A+';"
    print(f"SQL: {sql7}")

    result7 = compiler.compile(sql7)
    if result7['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result7['semantic_result']}")
        print(f"执行计划: {json.dumps(result7['execution_plan'], indent=2, ensure_ascii=False)}")
    else:
        print(f"✗ 编译失败: {result7['error']}")
    print()

    # 测试用例8：SELECT - ORDER BY排序
    print("测试8: SELECT - ORDER BY排序")
    print("-" * 50)
    sql8 = "SELECT id, name, age FROM student ORDER BY age DESC, name ASC;"
    print(f"SQL: {sql8}")

    result8 = compiler.compile(sql8)
    if result8['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result8['semantic_result']}")
        print(f"执行计划: {json.dumps(result8['execution_plan'], indent=2, ensure_ascii=False)}")
    else:
        print(f"✗ 编译失败: {result8['error']}")
    print()

    # 测试用例9：SELECT - GROUP BY分组
    print("测试9: SELECT - GROUP BY分组")
    print("-" * 50)
    sql9 = "SELECT grade, COUNT(*) FROM student GROUP BY grade HAVING COUNT(*) > 0;"
    print(f"SQL: {sql9}")

    result9 = compiler.compile(sql9)
    if result9['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result9['semantic_result']}")
        print(f"执行计划: {json.dumps(result9['execution_plan'], indent=2, ensure_ascii=False)}")
    else:
        print(f"✗ 编译失败: {result9['error']}")
    print()

    # 测试用例10：SELECT - LIMIT限制
    print("测试10: SELECT - LIMIT限制")
    print("-" * 50)
    sql10 = "SELECT * FROM student ORDER BY age DESC LIMIT 5 OFFSET 0;"
    print(f"SQL: {sql10}")

    result10 = compiler.compile(sql10)
    if result10['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result10['semantic_result']}")
        print(f"执行计划: {json.dumps(result10['execution_plan'], indent=2, ensure_ascii=False)}")
    else:
        print(f"✗ 编译失败: {result10['error']}")
    print()

    # 测试用例11：SELECT - INNER JOIN
    print("测试11: SELECT - INNER JOIN")
    print("-" * 50)
    sql11 = "SELECT s.name, c.course_name FROM student s INNER JOIN course c ON s.id = c.course_id;"
    print(f"SQL: {sql11}")

    result11 = compiler.compile(sql11)
    if result11['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result11['semantic_result']}")
        print(f"执行计划: {json.dumps(result11['execution_plan'], indent=2, ensure_ascii=False)}")
    else:
        print(f"✗ 编译失败: {result11['error']}")
    print()

    # 测试用例12：SELECT - LEFT JOIN
    print("测试12: SELECT - LEFT JOIN")
    print("-" * 50)
    sql12 = "SELECT s.name, c.course_name FROM student s LEFT JOIN course c ON s.id = c.course_id;"
    print(f"SQL: {sql12}")

    result12 = compiler.compile(sql12)
    if result12['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result12['semantic_result']}")
        print(f"执行计划: {json.dumps(result12['execution_plan'], indent=2, ensure_ascii=False)}")
    else:
        print(f"✗ 编译失败: {result12['error']}")
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

    # 测试用例15：错误测试 - 表不存在
    print("测试15: 错误测试 - 表不存在")
    print("-" * 50)
    sql15 = "SELECT * FROM non_existent_table;"
    print(f"SQL: {sql15}")

    result15 = compiler.compile(sql15)
    if result15['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result15['semantic_result']}")
    else:
        print(f"✗ 编译失败: {result15['error']}")
    print()

    # 测试用例16：错误测试 - 列不存在
    print("测试16: 错误测试 - 列不存在")
    print("-" * 50)
    sql16 = "SELECT non_existent_column FROM student;"
    print(f"SQL: {sql16}")

    result16 = compiler.compile(sql16)
    if result16['success']:
        print("✓ 编译成功")
        print(f"语义分析: {result16['semantic_result']}")
    else:
        print(f"✗ 编译失败: {result16['error']}")
    print()

    print("=== 扩展功能测试完成 ===")


if __name__ == "__main__":
    test_extended_sql_compiler()
