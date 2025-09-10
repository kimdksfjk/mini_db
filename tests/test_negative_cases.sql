-- ============================================================
-- mini_db 负向/边界测试（验证错误与未实现路径）
-- 建议逐条在交互式 CLI 输入，便于逐个观察错误信息。
-- 运行：python -m engine.mysql_cli   （然后逐条输入回车）
-- ============================================================

-- A) 语法错误：未闭合字符串
-- INSERT INTO student (id,name,age,grade) VALUES (10,'BadString,20,'X');

-- B) 语义错误：列数与值数不一致（VALUE_COUNT_ERROR）
-- INSERT INTO student (id,name,age,grade) VALUES (11,'TooFew');

-- C) 语法错误：非法标识符（列名中间有空格）
-- CREATE TABLE teacher (t_id INT, t_ name VARCHAR);

-- D) （如果你的 parse_insert 尚未支持多组 VALUES）会报：期望 ';' 但得到 ','
-- INSERT INTO student (id,name,age,grade) VALUES (12,'Multi',18,'A'), (13,'Row',19,'B');

-- E) 未实现：JOIN（编译能识别，执行会 NotImplementedError）
-- SELECT s.name FROM student s INNER JOIN student t ON s.id = t.id;

-- F) 未实现：GROUP BY/HAVING（编译能识别，执行会 NotImplementedError）
-- SELECT grade, COUNT(*) FROM student GROUP BY grade HAVING COUNT(*) > 0;

-- G) 正则与转义边界：使用 \g 结束输入（交互式模式下）
-- 在交互式 CLI 中输入：
--   SELECT name FROM student WHERE name = 'Alice' \g
-- 这会在不输入分号的情况下执行上一条语句。
