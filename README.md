mini-db 教学版 — 使用说明与设计文档

一个基于页式存储与B+树索引的极简关系型原型数据库。
特色：中文命令行、页面缓存（Buffer Pool）跨语句复用、系统表元数据持久化、单列 B+树索引、聚合/分组/排序/连接、缓冲池统计与替换日志。

目录

环境要求

快速开始

命令行用法

支持的 SQL 特性

索引

缓冲池与性能观测

示例：功能与性能测试

数据与目录结构

架构与代码入口

常见问题

已知限制

许可证

环境要求

Python 3.8+（推荐 3.10+）

纯标准库即可运行
（可选）导出 Excel 需安装：pip install openpyxl

终端/控制台支持中文（Windows 建议使用 PowerShell）

快速开始
# 进入项目根目录
python -m engine.cli.mysql_cli --data data_heap


--data 指定数据库根目录（系统表、业务表及索引都会保存在此处）

Windows PowerShell 也可：python -m engine.cli.mysql_cli --data .\data_heap

首次体验：

CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);

INSERT INTO student (id,name,age,grade) VALUES
  (1,'Alice',20,'A'),
  (2,'Bob',19,'B'),
  (3,'Carol',21,'A'),
  (4,'Dave',22,'B');

SELECT * FROM student;

-- 建索引与查询
\create_index student id idx_id
SELECT name, age FROM student WHERE id = 3;

-- 聚合/分组/排序
SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age
FROM student
GROUP BY grade
ORDER BY grade ASC;

命令行用法

进入 CLI 后除 SQL 外，还支持以下元命令（无需分号）：

命令	说明
\dt	列出当前所有用户表
\create_index <table> <column> [index_name]	创建 B+树索引
\list_indexes [table]	查看索引（按表或全部）
\drop_index <table> <index_name>	删除索引（同时清理运行态缓存）
\popup	弹窗显示最近一次查询结果（需 Tk）
\export <路径或文件名>	导出最近一次查询结果：优先 .xlsx，无 openpyxl 时回退 .csv
\bpstat	打印缓冲池统计（命中率、读写、淘汰数等）
`\bplog on	off`
\q	退出

Windows 路径参数请不要加方括号/JSON 数组，正确示例：
\export C:\Users\Me\Desktop\result.xlsx

支持的 SQL 特性

DDL

CREATE TABLE name(col type, ...)

DML

INSERT INTO ... VALUES (...), (...), ...

UPDATE ... SET col=val [, col=val] [WHERE ...]

DELETE FROM table;（无 WHERE 的整表清空；若存在 operators/delete.py 则支持条件删除）

DQL

SELECT ... FROM ...

WHERE：=, !=/<> , >, >=, <, <=

ORDER BY col [ASC|DESC], ...

LIMIT n [OFFSET m]

GROUP BY + 聚合：COUNT/SUM/AVG/MIN/MAX（支持 AS 别名、HAVING）

JOIN：INNER JOIN、LEFT JOIN（等值 ON 条件）

索引
类型与结构

单列 B+树索引（内存树 + 磁盘持久化表）

索引页存放在独立的 .mdb 文件，系统表 __sys_indexes 仅登记元信息

首次使用索引时，从索引 .mdb 读取 {k, row} 重建内存 B+树；之后常驻内存

当前索引条目保存的是“整行对象”（非 RID），等值/范围命中后可直接得到行，避免回表；代价是索引文件更大。

可利用的条件

等值：WHERE col = ?

范围：>=, >, <=, <，以及闭/开区间组合

可将前缀查询改写为区间：name LIKE 'A%' ≈ name >= 'A' AND name < 'B'

示例：

\create_index student id idx_id
SELECT name FROM student WHERE id = 3;

\create_index student name idx_name
SELECT id, name FROM student
WHERE name >= 'A' AND name < 'B'
ORDER BY name
LIMIT 10;

缓冲池与性能观测

跨语句复用：StorageAdapter 使用“句柄池”，让同一 .mdb 共享一个 Pager + BufferPool

默认：page_size=4096，BufferPool.capacity=256（可在 engine/storage_adapter.py 调整）

统计与日志：

\bpstat：JSON 格式输出命中数/缺页数/淘汰数/读写次数/命中率等

\bplog on/off：淘汰日志写入 buffer_pool.log（包含被替换页号、是否脏写回）

示例：功能与性能测试
1) 功能演示
CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);
CREATE TABLE course(course_id INT, course_name VARCHAR, teacher VARCHAR);

INSERT INTO student (id,name,age,grade) VALUES
  (1,'Alice',20,'A'),(2,'Bob',19,'B'),(3,'Carol',21,'A'),(4,'Dave',22,'B');

INSERT INTO course (course_id,course_name,teacher) VALUES
  (1,'DB','Prof.X'),(3,'OS','Dr.Y');

-- 连接
SELECT s.name, c.course_name
FROM student s
INNER JOIN course c ON s.id = c.course_id
ORDER BY s.name;

-- 左连接
SELECT s.name, c.course_name
FROM student s
LEFT JOIN course c ON s.id = c.course_id
ORDER BY s.name;

-- 聚合/分组/过滤/排序
SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age
FROM student
GROUP BY grade
HAVING COUNT(*) > 0
ORDER BY grade ASC;

2) 索引效果对比（等值 + 范围）
-- 假设存在大表 bench(id INT, name VARCHAR, grade VARCHAR)

-- 首查（冷缓存，无索引）
SELECT name FROM bench WHERE id = 199990;

-- 建索引
\create_index bench id idx_id

-- 再查（热缓存 + 索引命中）
SELECT name FROM bench WHERE id = 199990;

-- 范围 + 排序（索引顺序扫描）
SELECT id, name FROM bench
WHERE id BETWEEN 150000 AND 170000
ORDER BY id ASC
LIMIT 10;

-- 观察缓存
\bpstat
\bplog on
-- 再做几次范围查询以产生替换日志
\bplog off

数据与目录结构

以 --data data_heap 为例：

data_heap/
  __sys_tables/
    __sys_tables.mdb            # 系统表：登记用户表
  __sys_indexes/
    __sys_indexes.mdb           # 系统表：登记索引
  student/
    student.mdb                 # 用户表数据（页式堆）
  course/
    course.mdb
  __idx__student__idx_id/
    __idx__student__idx_id.mdb  # 索引页（保存 {k, row}）
  ...


项目不再使用 meta.json；系统表负责所有登记，打开表时由文件大小推断数据页集合。

架构与代码入口

命令行：engine/cli/mysql_cli.py
解析输入、调用 SQL 编译器与执行器，负责输出与元命令。

SQL 编译器：sql/sql_compiler.py
词法/语法/语义分析 → 生成执行计划（支持扩展 SELECT、JOIN、GROUP BY、HAVING、ORDER、LIMIT/OFFSET 等）。

执行器：engine/executor.py
将“执行计划”分派给算子：连接 → 过滤 → 聚合/分组/Having → 投影 → 排序 → 分页。

目录与系统表：engine/catalog.py、engine/sys_catalog.py
维护 __sys_tables / __sys_indexes。

页式存储：storage/pager.py、storage/buffer_pool.py、storage/data_page.py、storage/table_heap.py
页管理、缓冲池、数据页布局与堆表接口。

适配层：engine/storage_adapter.py
将上层“表”的读写转换为底层“页”的操作；引入句柄池以跨语句复用 Pager + BufferPool。

索引：engine/bptree.py（内存 B+树）、engine/index_registry.py（索引元数据与加载）、engine/operators/index_scan.py

算子（operators）：

create_table.py、insert.py、seq_scan.py、filter.py、project.py、aggregate.py、join.py、create_index.py、update.py、（可选）delete.py

常见问题

Q1：第一次查询很慢、第二次很快？
A：第一次需要从磁盘加载数据页与索引页（miss 多）；之后命中缓冲池与内存 B+树（hit 多），速度显著提升。

Q2：删除索引失败/重建很慢？
A：使用 \drop_index 表 索引名 删除会清理系统表与内存树；不要手动删索引目录。若确实手动删除，请清理系统表条目或重建 --data 目录。

Q3：Windows \export 路径无效？
A：请传入标准路径字符串，不要使用数组/多余引号。例：\export C:\Users\Me\Desktop\out.xlsx。

已知限制

单列 B+树索引；不支持复合/覆盖索引。

仅对等值/范围过滤使用索引；不可 sargable 表达式会回退顺扫。

连接算法为嵌套循环连接（Nested-Loop）；尚无基于索引/哈希的连接优化。

无事务日志与崩溃恢复（无 ACID 事务）；不支持并发写。

计划器为规则驱动（RBO），无代价估算（CBO）。

DELETE ... WHERE 仅在存在 operators/delete.py 实现时可用；默认仅支持整表清空。