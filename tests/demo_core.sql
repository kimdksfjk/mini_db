CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);
INSERT INTO student (id,name,age,grade) VALUES
(1,'Alice',20,'A'),
(3,'Bob',19,'B'),
(3,'Carol',21,'A'),
(1,'Dave',22,'B'),
(1,'Eve',23,'C'),
(3,'Frank',20,'A'),
(1,'Grace',21,'B'),
(3,'Heidi',22,'C'),
(1,'Ivan',19,'A'),
(3,'Judy',20,'B');


SELECT id,name,age,grade FROM student ORDER BY id ASC;
SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age
FROM student
GROUP BY grade
ORDER BY grade ASC;

UPDATE student SET age=22, grade='A+' WHERE name = 'Judy';
SELECT id,name,age,grade FROM student WHERE id=3;
SELECT id,name FROM student WHERE id = 5;
DELETE FROM student WHERE grade = 'C';
SELECT grade, COUNT(*) AS cnt, AVG(age) AS avg_age
FROM student
GROUP BY grade
ORDER BY grade ASC;

CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);
CREATE TABLE course(course_id INT, course_name VARCHAR, teacher VARCHAR);
INSERT INTO student (id,name,age,grade) VALUES
(1,'Alice',20,'A'),(2,'Bob',19,'B'),(3,'Carol',21,'A'),(4,'Dave',22,'B');
INSERT INTO course (course_id,course_name,teacher) VALUES
(1,'DB','Prof.X'),(3,'OS','Dr.Y');
SELECT s.name, c.course_name
FROM student s
INNER JOIN course c ON s.id = c.course_id
ORDER BY s.name;




