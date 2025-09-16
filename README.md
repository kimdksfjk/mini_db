1.启动

python -m engine.cli.mysql_cli --data 文件夹名

python -m engine.cli.mysql_cli --data 文件夹名 --debug

2.支持的代码如下

基本代码

CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);

CREATE TABLE course(course_id INT, course_name VARCHAR, teacher VARCHAR);

INSERT INTO student (id,name,age,grade) VALUES

(1,'Alice',20,'A'),

(3,'Bob',19,'B'),

(3,'Carol',21,'A'),

(1,'Dave',22,'B'),

(1,'Eve',23,'C'),

(3,'Frank',20,'A'),

(1,'Grace',21,'B'),

(3,'Heidi',22,'D'),

(1,'Ivan',19,'A'),

(3,'Judy',27,'B');

INSERT INTO course (course_id,course_name,teacher) VALUES

(1,'DB','Prof.X'),(3,'OS','Dr.Y'); 

DELETE FROM student;

SELECT id,name,age,grade FROM student WHERE id=3;

DELETE FROM student WHERE grade = 'C';

关键字拓展

SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age

FROM student

GROUP BY grade

ORDER BY grade ASC;

连接+别名

SELECT s.name, c.course_name

FROM student s

INNER JOIN course c ON s.id = c.course_id

ORDER BY s.name;

复杂语句

SELECT s.grade, COUNT(*) AS student_count FROM student s INNER JOIN course c ON s.id = c.course_id WHERE s.age > 18 GROUP BY s.grade HAVING COUNT(*) > 0 ORDER BY student_count DESC LIMIT 10;

索引

\create_index student id

拓展功能

\popup:最近的语句结果可视化显示

\export C:\用户名 \Desktop\结果.xlsx 结果导出为excel


