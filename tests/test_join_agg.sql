CREATE TABLE student (id INT, name VARCHAR, age INT, grade VARCHAR);
INSERT INTO student (id,name,age,grade) VALUES (1,'Alice',20,'A');
INSERT INTO student (id,name,age,grade) VALUES (2,'Bob',17,'B');
INSERT INTO student (id,name,age,grade) VALUES (3,'Carol',22,'A');
INSERT INTO student (id,name,age,grade) VALUES (4,'Dave',18,'B');
INSERT INTO student (id,name,age,grade) VALUES (5,'Eve',18,'A');

CREATE TABLE course (course_id INT, course_name VARCHAR, teacher VARCHAR);
INSERT INTO course (course_id,course_name,teacher) VALUES (1,'Database','Dr.Smith');
INSERT INTO course (course_id,course_name,teacher) VALUES (3,'AI','Dr.Lee');
INSERT INTO course (course_id,course_name,teacher) VALUES (5,'OS','Dr.Wang');

SELECT s.name, c.course_name FROM student s INNER JOIN course c ON s.id = c.course_id ORDER BY c.course_name ASC;
SELECT s.name, c.course_name FROM student s LEFT JOIN course c ON s.id = c.course_id ORDER BY s.id ASC;
SELECT c.course_name, s.name FROM student s RIGHT JOIN course c ON s.id = c.course_id ORDER BY c.course_id ASC;

SELECT grade, COUNT(*) AS cnt FROM student GROUP BY grade HAVING COUNT(*) > 0 ORDER BY cnt DESC LIMIT 10;

SELECT s.grade, COUNT(*) AS student_count
FROM student s INNER JOIN course c ON s.id = c.course_id
WHERE s.age >= 18
GROUP BY s.grade
HAVING COUNT(*) > 0
ORDER BY student_count DESC
LIMIT 5;
