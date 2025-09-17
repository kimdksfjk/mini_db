SELEC id,name,age,grade FROM student ORDER BY id ASC;
SELECT id,name,age FROM student ORDER BY id ASC;
SELECT id,name,age,grade FOM student ORDER BY id ASC;
SELECT s.name, COUNT(*) AS cnt
FROM student s
INNER JOIN course c ON s.id = c.student_id
WHERE s.> 18
GROUP BY s.grade
HAVING COUNT(*) > 0
ORDER BY cnt DESC
LIMIT 10;
INSERT INTO student (id,name,age,grade) VALUES
('jack','Alice',20,'A');
INSERT INTO students (id,name,age,grade) VALUES
(255,'Alice',20,'A');
INSERT INTO student (id,name,age,grade) VALUES
(255,'Alice',20);
CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);
